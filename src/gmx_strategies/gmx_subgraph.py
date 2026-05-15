"""GMX V2 subgraph adapter — position discovery for liquidation triggering.

Queries a GMX V2 synthetics subgraph (Goldsky-hosted, configurable URL)
for open positions. Maps the raw GraphQL rows into `GMXPosition` objects
that `liquidation_trigger.detect_trigger` can score.

Two layers:
  - PURE parsing (`parse_subgraph_position`, `MARKET_ADDRESS_TO_ALIAS`):
    GraphQL JSON shape → GMXPosition. Tested in isolation.
  - ASYNC fetcher (`fetch_open_positions`): thin httpx wrapper that POSTs
    the GraphQL query. Tested with a stub client.

Pagination: GMX V2 markets have thousands of positions; we paginate via
`first` + `skip` until we hit `max_pages` or an empty page.

The subgraph URL is set via settings.gmx_subgraph_url. If empty, the
caller is expected to no-op (we don't fail loudly — this lets the
container start without the URL and log a clean "not configured"
state until Ben pastes it in).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from gmx_strategies.liquidation_trigger import GMXPosition

log = logging.getLogger(__name__)


# GMX V2 synthetics-Arbitrum market addresses → asset alias.
# Maintained manually — the subgraph returns the market ADDRESS, we map
# to the alias used elsewhere in the stack (chainlink:<alias>:latest).
# Add new markets here as they're listed.
MARKET_ADDRESS_TO_ALIAS: dict[str, str] = {
    # BTC/USD [WBTC.b-USDC]
    "0x47c031236e19d024b42f8ae6780e44a573170703": "btc",
    # ETH/USD [WETH-USDC]
    "0x70d95587d40a2caf56bd97485ab3eec10bee6336": "eth",
    # SOL/USD [SOL-USDC]
    "0x09400d9db990d5ed3f35d7be61dfaeb900af03c9": "sol",
    # LINK/USD [LINK-USDC]
    "0x7f1fa204bb700853d36994da19f830b6ad18455c": "link",
    # AVAX/USD [AVAX-USDC]
    "0x7bbbf946883a5701350007320f525c5379b8178a": "avax",
    # BNB/USD [BNB-USDC]
    "0x2d340912aa47e33c90efb078e69e70efe2b34b9b": "bnb",
    # XRP/USD [ETH-USDC]
    "0x0caf6c66c1e0ed8ad24a96a8c0fa1f1fbe0e9be0": "xrp",
    # DOGE/USD [ETH-USDC]
    "0x6853ea96ff216fab11d2d930ce3c508556a4bdc4": "doge",
    # HYPE/USD [WBTC.b-USDC]  (added 2026-Q1)
    "0xc6c9ea3a7b34770b3e6a26da6deddd0a1ef60ffb": "hype",
    # AAVE/USD [AAVE-USDC]
    "0x1cbba6346f110c8a5ea739ef2d1eb182990e4eb2": "aave",
}


# ─── Pure parsing ──────────────────────────────────────────────────


@dataclass(frozen=True)
class RawSubgraphPosition:
    """GraphQL row shape as we parse it — pre-conversion to GMXPosition."""
    id: str
    account: str
    market_address: str
    is_long: bool
    size_usd: float
    collateral_usd: float
    entry_price: float | None    # None if subgraph doesn't expose entry price


def _to_float(raw: Any, *, scale: float = 1.0) -> float | None:
    """Pure: GraphQL string-encoded uint → float, with optional scale."""
    if raw is None:
        return None
    try:
        return float(raw) * scale
    except (TypeError, ValueError):
        return None


def parse_raw_subgraph_position(row: dict[str, Any]) -> RawSubgraphPosition | None:
    """Pure: GraphQL position row → RawSubgraphPosition, or None.

    GMX V2 amounts are returned as decimal strings of uint256 values:
      - sizeInUsd / collateralAmount: 1e30 USD precision
      - sizeInTokens: token-native precision (varies per token)

    We convert sizeInUsd from 1e30 → USD and skip collateralAmount
    (token-precision; we use the USD field instead via `collateralUsd`).
    GMX exposes a derived `collateralUsd` in some subgraph variants;
    if missing, fall back to sizeInUsd / leverage (less precise but
    acceptable for the watch-list pre-filter; the on-chain
    re-check is authoritative).
    """
    if not isinstance(row, dict):
        return None

    pid = row.get("id")
    account = row.get("account")
    market_address = row.get("market")
    if not pid or not account or not market_address:
        return None

    # GMX V2 USD precision is 1e30; convert to USD.
    size_usd = _to_float(row.get("sizeInUsd"), scale=1e-30)
    if size_usd is None or size_usd <= 0:
        return None

    # Prefer derived collateralUsd if present, else fall back.
    col_usd = _to_float(row.get("collateralUsd"), scale=1e-30)
    if col_usd is None:
        # Some subgraphs expose collateralAmountUsd; some only have the
        # token-amount + a token-price field. Caller can enrich later.
        col_usd = _to_float(row.get("collateralAmountUsd"), scale=1e-30)
    if col_usd is None:
        # Subsquid endpoint exposes collateralAmount in TOKEN-native precision
        # (e.g., USDC at 6 decimals). For a quick fallback approximation:
        # assume collateral is in USDC (USDC = $1) and divide by 1e6.
        # This is a rough proxy; on-chain re-check is authoritative.
        col_amt = _to_float(row.get("collateralAmount"), scale=1e-6)
        if col_amt is not None and col_amt > 0:
            col_usd = col_amt
    if col_usd is None or col_usd <= 0:
        return None

    is_long_raw = row.get("isLong")
    if isinstance(is_long_raw, bool):
        is_long = is_long_raw
    elif isinstance(is_long_raw, str):
        is_long = is_long_raw.strip().lower() in ("true", "1", "yes")
    else:
        return None

    # entryPrice is optional — newer GMX V2 subgraph versions expose it,
    # older ones don't. When absent, caller falls back to the current
    # oracle price (which gives a degenerate health calc but is safe).
    entry_price = _to_float(row.get("entryPrice"), scale=1e-30)
    if entry_price is not None and entry_price <= 0:
        entry_price = None

    return RawSubgraphPosition(
        id=str(pid),
        account=str(account).lower(),
        market_address=str(market_address).lower(),
        is_long=is_long,
        size_usd=size_usd,
        collateral_usd=col_usd,
        entry_price=entry_price,
    )


def raw_to_gmx_position(
    raw: RawSubgraphPosition,
    *,
    entry_price: float | None = None,
    liquidation_threshold_pct: float = 0.005,
    alias_map: dict[str, str] | None = None,
) -> GMXPosition | None:
    """Pure: enrich a RawSubgraphPosition → full GMXPosition.

    Entry price precedence:
      1. Explicit `entry_price` kwarg (override — used when caller wants to
         simulate or backfill).
      2. `raw.entry_price` (from subgraph `entryPrice` field).

    Returns None when:
      - alias map has no entry for this market address
      - no entry price is available from either source
      - entry price is non-positive

    `entry_price` kwarg lets the caller override for backtesting / replay
    scenarios; in the production watcher this kwarg is left as the default
    so the subgraph's value is used.
    """
    amap = alias_map or MARKET_ADDRESS_TO_ALIAS
    alias = amap.get(raw.market_address)
    if alias is None:
        return None

    effective_entry = entry_price if entry_price is not None else raw.entry_price
    if effective_entry is None or effective_entry <= 0:
        return None

    leverage = raw.size_usd / raw.collateral_usd if raw.collateral_usd > 0 else 0.0
    return GMXPosition(
        user=raw.account,
        market=alias,
        is_long=raw.is_long,
        size_usd=raw.size_usd,
        collateral_usd=raw.collateral_usd,
        entry_price=effective_entry,
        leverage=leverage,
        liquidation_threshold_pct=liquidation_threshold_pct,
    )


# ─── GraphQL query ─────────────────────────────────────────────────


# GraphQL query in Subsquid dialect (used by the public
# https://gmx.squids.live/gmx-synthetics-arbitrum:prod/api/graphql endpoint):
#   - `limit` not `first`
#   - enum suffix `_DESC` not `orderDirection: desc`
#   - `where: { sizeInUsd_gt: "0" }` works same as Graph (numeric string)
#
# If you point at a Graph hosted-service / Goldsky endpoint instead, the
# alternative query (POSITIONS_QUERY_GRAPH) is provided. fetch_open_positions
# auto-detects by URL host and picks the right one.
POSITIONS_QUERY_SUBSQUID = """
query OpenPositions($limit: Int!, $skip: Int!) {
  positions(
    limit: $limit,
    offset: $skip,
    where: { sizeInUsd_gt: "0" },
    orderBy: sizeInUsd_DESC
  ) {
    id
    account
    market
    isLong
    sizeInUsd
    collateralAmount
  }
}
""".strip()

POSITIONS_QUERY_GRAPH = """
query OpenPositions($first: Int!, $skip: Int!) {
  positions(
    first: $first,
    skip: $skip,
    where: { sizeInUsd_gt: "0" },
    orderBy: sizeInUsd,
    orderDirection: desc
  ) {
    id
    account
    market
    isLong
    sizeInUsd
    collateralUsd
    collateralAmountUsd
    entryPrice
  }
}
""".strip()

# Back-compat alias for existing tests that reference POSITIONS_QUERY
POSITIONS_QUERY = POSITIONS_QUERY_GRAPH


def _query_for_url(url: str) -> tuple[str, dict[str, int]]:
    """Pure: return (query, variables_template_keys) for a given endpoint host.

    Subsquid endpoints use `limit/offset` + `_DESC` enums; Graph/Goldsky use
    `first/skip` + `orderDirection`.
    """
    host = (url or "").lower()
    if "squids.live" in host or "subsquid" in host or "satsuma" in host:
        return (POSITIONS_QUERY_SUBSQUID, {"limit_key": "limit", "skip_key": "skip"})
    return (POSITIONS_QUERY_GRAPH, {"limit_key": "first", "skip_key": "skip"})


# ─── Async fetcher ─────────────────────────────────────────────────


def _is_safe_subgraph_url(url: str) -> bool:
    """Pure: enforce HTTPS-only to block obvious SSRF / dev mistakes.

    Accepts:  https://...
    Rejects:  http://..., file://, gopher://, internal IPs via http
    """
    return isinstance(url, str) and url.startswith("https://") and len(url) <= 1024


async def fetch_open_positions(
    httpx_client: Any,
    subgraph_url: str,
    *,
    page_size: int = 200,
    max_pages: int = 10,
    timeout_sec: float = 8.0,
) -> list[RawSubgraphPosition]:
    """POST the OpenPositions query and return parsed rows.

    Defensive on every layer — HTTP error / non-200 / missing data / malformed
    row all degrade to "skip" rather than raising. The caller's loop never
    crashes on a subgraph hiccup.

    `subgraph_url` is the full HTTPS endpoint (e.g.
    https://api.goldsky.com/.../gmx-synthetics-arbitrum/<v>/gn).
    Returns [] when `subgraph_url` is empty or doesn't pass the safety check.
    """
    if not subgraph_url:
        return []
    if not _is_safe_subgraph_url(subgraph_url):
        log.warning(
            "gmx_subgraph.unsafe_url_rejected url=%s — require https://",
            subgraph_url[:64],
        )
        return []
    import asyncio

    query, var_keys = _query_for_url(subgraph_url)
    out: list[RawSubgraphPosition] = []
    for page in range(max_pages):
        skip = page * page_size
        try:
            resp = await asyncio.wait_for(
                httpx_client.post(
                    subgraph_url,
                    json={
                        "query": query,
                        "variables": {
                            var_keys["limit_key"]: page_size,
                            var_keys["skip_key"]: skip,
                        },
                    },
                ),
                timeout=timeout_sec,
            )
        except TimeoutError:
            log.warning("gmx_subgraph.timeout page=%d", page)
            break
        except Exception as exc:
            log.warning("gmx_subgraph.http_error page=%d err=%s", page, exc)
            break
        if getattr(resp, "status_code", 500) >= 400:
            log.warning("gmx_subgraph.non_200 page=%d", page)
            break
        try:
            body = resp.json()
        except (ValueError, Exception):
            log.warning("gmx_subgraph.non_json page=%d", page)
            break

        # GraphQL errors come in `errors` even with HTTP 200. Skip and stop.
        errors = body.get("errors") if isinstance(body, dict) else None
        if errors:
            log.warning("gmx_subgraph.graphql_errors page=%d errors=%s", page, errors)
            break

        data = body.get("data") if isinstance(body, dict) else None
        rows = (data or {}).get("positions") or []
        if not rows:
            break

        for row in rows:
            parsed = parse_raw_subgraph_position(row)
            if parsed is not None:
                out.append(parsed)

        if len(rows) < page_size:
            break    # last page
    return out


__all__ = [
    "MARKET_ADDRESS_TO_ALIAS",
    "POSITIONS_QUERY",
    "RawSubgraphPosition",
    "_is_safe_subgraph_url",
    "fetch_open_positions",
    "parse_raw_subgraph_position",
    "raw_to_gmx_position",
]
