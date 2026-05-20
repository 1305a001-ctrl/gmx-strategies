"""Live Binance USDT-M perp funding-rate reader (G3) — CEX hedge leg.

Replaces the mocked `fetch_cex_funding` placeholder in
`funding_arb_runtime.py` with a real call to Binance's public
`/fapi/v1/premiumIndex` endpoint. Public — no auth required.

Sign convention (verified against Binance docs + funding-arb.py):
    lastFundingRate > 0  ⇒  longs pay shorts (matches GMX convention).
This means `net_rate = gmx_rate - cex_rate` (no flip needed) is the
delta-neutral funding-arb edge.

Failure modes (ALL return None — caller must keep the loop alive):
  - Unmapped market alias (not one of our 5: btc/eth/sol/doge/xrp)
  - HTTP error / timeout / non-200 status
  - Malformed JSON body
  - Missing `lastFundingRate` field
  - Non-numeric `lastFundingRate` value
  - For the batched path: unknown symbols in response are silently ignored

Two entry points:
  - `fetch_cex_funding_live(market)` — single-market call (one HTTP per
    market). Used when `settings.binance_funding_batch=False`.
  - `fetch_all_cex_fundings()` — batched call (one HTTP for all 5
    markets, ~50KB response). Cheaper at 60s cadence. Used when
    `settings.binance_funding_batch=True` (the default). Callers cache
    the result inside one sweep and dispatch by alias.

Implementation note: this module uses httpx (already pinned at 0.28.1)
matching the rest of the package's async/httpx style. No new deps.

Trap-surface monitoring (added in feat/trap-monitors):
  - After parsing the rate, we also inspect `nextFundingTime` (ms since
    epoch). When `nextFundingTime - now()` is positive and less than
    `settings.binance_settlement_guard_s * 1000` (default 300_000 = 5min)
    we emit `binance_funding.near_settlement` at WARN. The signal still
    emits — the warn just flags that the rate is about to flip.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from gmx_strategies.settings import settings

log = logging.getLogger(__name__)


# Hardcoded mapping of operator's GMX market aliases -> Binance USDT-M
# perp symbols. The 5 here match `_INTENDED_MARKETS` in funding_arb_runtime.py
# and chainlink-streams' live feeds. Aliases outside this set return None
# without an HTTP round-trip (cheap reject).
BINANCE_SYMBOL_BY_ALIAS: dict[str, str] = {
    "btc": "BTCUSDT",
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
    "doge": "DOGEUSDT",
    "xrp": "XRPUSDT",
}

# Reverse map for the batched path — `BTCUSDT -> btc` etc. We rebuild this
# at module-load so any edit to BINANCE_SYMBOL_BY_ALIAS automatically updates.
_SYMBOL_TO_ALIAS: dict[str, str] = {sym: a for a, sym in BINANCE_SYMBOL_BY_ALIAS.items()}


def _check_near_settlement(alias: str, next_funding_time_ms: Any) -> None:
    """Log a WARN when `nextFundingTime` is within the settlement guard window.

    The signal is NOT suppressed — funding rates are about to flip at
    `nextFundingTime`, which is a real edge artifact and the operator should
    see it in logs. `settings.binance_settlement_guard_s` controls the window
    (default 300s = 5 min). Best-effort: malformed/missing input is silently
    ignored (we don't WARN twice on bad rate + bad time).
    """
    if not isinstance(next_funding_time_ms, (int, float)):
        return
    try:
        nft_ms = int(next_funding_time_ms)
    except (ValueError, TypeError):
        return
    now_ms = int(time.time() * 1000)
    seconds_to_settle = (nft_ms - now_ms) / 1000.0
    guard_s = settings.binance_settlement_guard_s
    # Only WARN when we're approaching settlement (positive but small). After
    # settlement nextFundingTime jumps ahead 8h, so a large positive value is
    # fine. A negative value means Binance hasn't updated the field yet —
    # treat as "post-settle" and skip the warn.
    if 0 < seconds_to_settle < guard_s:
        log.warning(
            "binance_funding.near_settlement market=%s seconds_to_settle=%d",
            alias, int(seconds_to_settle),
        )


def _parse_funding_rate(raw: Any) -> float | None:
    """Coerce Binance's stringified `lastFundingRate` to a float.

    Returns None on missing, non-numeric, or NaN values. Binance returns
    rates as strings (e.g. "0.00010000" or "-0.00007181"); we float() them.
    """
    if raw is None:
        return None
    if not isinstance(raw, (str, int, float)):
        return None
    try:
        value = float(raw)
    except (ValueError, TypeError):
        return None
    # Guard against NaN / inf — Binance shouldn't return these, but be safe.
    if value != value or value in (float("inf"), float("-inf")):
        return None
    return value


async def fetch_cex_funding_live(market: str) -> float | None:
    """Live Binance funding-rate read for one market.

    Returns the current `lastFundingRate` as a float (signed, per-8h,
    GMX convention: positive = longs pay shorts). Returns None on ANY
    failure path — never raises. Caller (funding_arb_runtime) treats
    None as a CEX-leg outage and emits with `cex_rate=0` fallback.

    Symbol mapping is hardcoded to our 5 supported aliases. Unmapped
    aliases short-circuit without an HTTP call.
    """
    symbol = BINANCE_SYMBOL_BY_ALIAS.get(market)
    if symbol is None:
        log.warning("binance_funding.unknown_alias alias=%s", market)
        return None

    url = f"{settings.binance_fapi_base_url}/fapi/v1/premiumIndex"
    timeout = httpx.Timeout(settings.binance_funding_timeout_s)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url, params={"symbol": symbol})
    except (httpx.HTTPError, httpx.TimeoutException) as exc:
        log.warning(
            "binance_funding.http_error alias=%s symbol=%s err=%s",
            market, symbol, exc,
        )
        return None
    if resp.status_code != 200:
        log.warning(
            "binance_funding.bad_status alias=%s symbol=%s status=%d",
            market, symbol, resp.status_code,
        )
        return None
    try:
        body = resp.json()
    except (ValueError, TypeError):
        log.warning("binance_funding.bad_json alias=%s symbol=%s", market, symbol)
        return None
    if not isinstance(body, dict):
        log.warning(
            "binance_funding.bad_body_shape alias=%s symbol=%s type=%s",
            market, symbol, type(body).__name__,
        )
        return None
    rate = _parse_funding_rate(body.get("lastFundingRate"))
    if rate is None:
        log.warning(
            "binance_funding.bad_rate alias=%s symbol=%s raw=%r",
            market, symbol, body.get("lastFundingRate"),
        )
        return None
    # Trap-surface WARN: when `nextFundingTime` is within the guard window
    # the rate is about to flip — the operator needs to see this in logs.
    _check_near_settlement(market, body.get("nextFundingTime"))
    log.info(
        "binance_funding.read_ok alias=%s symbol=%s rate_per_8h=%.8f",
        market, symbol, rate,
    )
    return rate


async def fetch_all_cex_fundings() -> dict[str, float]:
    """Batched Binance funding-rate read for all 5 supported markets.

    Hits `/fapi/v1/premiumIndex` without a `symbol` param — Binance returns
    every active perp market in one response (~50KB, ~745 entries today).
    We filter to our 5 aliases and ignore the rest. Cheaper than 5 separate
    HTTP calls at 60s cadence.

    Returns a dict keyed by our internal alias (e.g. `{"btc": 0.00006219,
    "eth": 0.00005819, ...}`). On HTTP failure / malformed body, returns
    an EMPTY dict — never raises. Per-symbol parse failures are dropped
    silently (logged at warning level) so the rest of the markets ship.
    """
    url = f"{settings.binance_fapi_base_url}/fapi/v1/premiumIndex"
    timeout = httpx.Timeout(settings.binance_funding_timeout_s)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url)
    except (httpx.HTTPError, httpx.TimeoutException) as exc:
        log.warning("binance_funding.batch_http_error err=%s", exc)
        return {}
    if resp.status_code != 200:
        log.warning("binance_funding.batch_bad_status status=%d", resp.status_code)
        return {}
    try:
        body = resp.json()
    except (ValueError, TypeError):
        log.warning("binance_funding.batch_bad_json")
        return {}
    if not isinstance(body, list):
        log.warning(
            "binance_funding.batch_bad_body_shape type=%s", type(body).__name__,
        )
        return {}

    out: dict[str, float] = {}
    for entry in body:
        if not isinstance(entry, dict):
            continue
        symbol = entry.get("symbol")
        if not isinstance(symbol, str):
            continue
        alias = _SYMBOL_TO_ALIAS.get(symbol)
        if alias is None:
            # Silently skip — Binance has ~745 perps; we only care about 5.
            continue
        rate = _parse_funding_rate(entry.get("lastFundingRate"))
        if rate is None:
            log.warning(
                "binance_funding.batch_bad_rate alias=%s symbol=%s raw=%r",
                alias, symbol, entry.get("lastFundingRate"),
            )
            continue
        # Trap-surface WARN: per-symbol settlement-window check in the batch
        # path. Same semantics as the single-market path.
        _check_near_settlement(alias, entry.get("nextFundingTime"))
        out[alias] = rate

    log.info(
        "binance_funding.batch_read_ok n_found=%d aliases=%s",
        len(out), ",".join(sorted(out.keys())),
    )
    return out


__all__ = [
    "BINANCE_SYMBOL_BY_ALIAS",
    "fetch_all_cex_fundings",
    "fetch_cex_funding_live",
]
