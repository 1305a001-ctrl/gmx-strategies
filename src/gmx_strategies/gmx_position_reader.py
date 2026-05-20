"""GMX V2 Reader — on-chain position state for the operator's account (G5.3).

This is the third piece of the executor stack:
  - G5.1 (`gmx_order_encoder`) — encode CreateOrderParams + simulate via eth_call
  - G5.2 (`gmx_signer`) — sign EIP-1559 tx + gated broadcast
  - G5.3 (THIS module) — read the operator's current on-chain positions

Read-only on-chain queries against `Reader.getAccountPositions` and
`Reader.getPosition` on Arbitrum. NO writes. Never raises — returns empty
lists / None on every failure path.

Why we need this:
  - GMX V2 auto-merges same-direction positions in the same market into one
    Position.Props (different from CEX where every order is a new fill).
    A consumer that submits a "MarketIncrease" on top of an existing position
    will SILENTLY add to it. The consumer needs to know.
  - Partial liquidations are detectable by `decreased_at_time > increased_at_time`
    AND `size_in_usd > 0` — but only if you read the live state, not the
    in-process intent log.
  - Risk wrappers (G5.4) need current equity = collateral + pnl, which
    requires reading the live position numbers.
  - Reconciliation: before submitting any new MarketIncrease / MarketDecrease,
    a consumer should check what's already on-chain.

ABI shape — `Position.Props` (v2.2, verified against gmx-io/gmx-synthetics
main branch contracts/position/Position.sol 2026-05-20):

    struct Props {
        Addresses addresses;
        Numbers numbers;
        Flags flags;
    }
    struct Addresses {
        address account;
        address market;
        address collateralToken;
    }
    struct Numbers {
        uint256 sizeInUsd;
        uint256 sizeInTokens;
        uint256 collateralAmount;
        int256  pendingImpactAmount;            // NEW in v2.2 — between
                                                  // collateralAmount and
                                                  // borrowingFactor
        uint256 borrowingFactor;
        uint256 fundingFeeAmountPerSize;
        uint256 longTokenClaimableFundingAmountPerSize;
        uint256 shortTokenClaimableFundingAmountPerSize;
        uint256 increasedAtTime;                 // NOTE: v2.2 dropped the
                                                  // *AtBlock fields (older
                                                  // memory referred to them);
                                                  // only *AtTime remains.
        uint256 decreasedAtTime;
    }
    struct Flags { bool isLong; }

Decode quirk to flag: any older note that mentions
`increasedAtBlock`/`decreasedAtBlock` is from a pre-v2.2 shape. The current
on-chain struct has only `increasedAtTime`/`decreasedAtTime` (seconds since
unix epoch) — block-number fields were removed.

Reader function signatures (verified against
deployments/arbitrum/Reader.json 2026-05-20):

    function getAccountPositions(
        DataStore dataStore, address account, uint256 start, uint256 end
    ) external view returns (Position.Props[] memory);

    function getPosition(
        DataStore dataStore, bytes32 key
    ) external view returns (Position.Props memory);

Position key derivation (from `contracts/position/Position.sol`):

    positionKey = keccak256(abi.encode(account, market, collateralToken, isLong));

Implementation mirrors `gmx_reader.py` — raw httpx JSON-RPC + eth_abi
encode/decode, no web3.py contract objects. Same hand-rolled pattern.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx
from eth_abi import decode, encode  # type: ignore[attr-defined]
from web3 import Web3

from gmx_strategies.markets import ARBITRUM_MARKETS
from gmx_strategies.settings import settings

if TYPE_CHECKING:
    from gmx_strategies.gmx_order_encoder import OrderIntent

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────
# Function selectors (precomputed from keccak("name(types)")[:4]).
# ──────────────────────────────────────────────────────────────────────────
SELECTOR_GET_ACCOUNT_POSITIONS = "0x" + Web3.keccak(
    text="getAccountPositions(address,address,uint256,uint256)",
)[:4].hex()

SELECTOR_GET_POSITION = "0x" + Web3.keccak(
    text="getPosition(address,bytes32)",
)[:4].hex()


# ABI type spec for Position.Props (v2.2). This is the canonical shape and
# MUST match contracts/position/Position.sol main branch. The `int256` for
# pendingImpactAmount must be in position 4 of Numbers, between
# collateralAmount and borrowingFactor — older shapes that placed it
# elsewhere will silently mis-decode subsequent fields.
_POSITION_PROPS_TYPE = (
    "("
    "(address,address,address),"
    "(uint256,uint256,uint256,int256,uint256,uint256,uint256,uint256,uint256,uint256),"
    "(bool)"
    ")"
)


# ──────────────────────────────────────────────────────────────────────────
# Public dataclasses — frozen, never mutated by callers
# ──────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Position:
    """One on-chain GMX V2 position, decoded + human-readable.

    All numeric fields are raw 1e30-scaled / native-token-decimal integers
    EXCEPT `size_in_usd_float`, which is the human-readable USD value.

    `market_alias` is None when the on-chain market address does not appear
    in `ARBITRUM_MARKETS` (e.g. a market we don't trade, or a delisted one
    still attached to the account). The caller can still inspect the raw
    `market_address` to decide what to do.

    Times are seconds since unix epoch (GMX V2 stores blocktime, not
    block-number, since v2.2 — see module docstring decode quirk note).
    """

    account: str
    market_alias: str | None
    market_address: str
    collateral_token: str
    is_long: bool
    size_in_usd: int
    size_in_usd_float: float
    size_in_tokens: int
    collateral_amount: int
    borrowing_factor: int
    funding_fee_amount_per_size: int
    increased_at_time: int
    decreased_at_time: int


@dataclass(frozen=True)
class ReconciliationResult:
    """Result of pre-flight reconcile of an OrderIntent vs current state.

    `action` is one of:
      - "PROCEED" — no conflict; the caller may submit as-is.
      - "MERGE"   — there is already a same-direction position in this market;
                    GMX V2 will silently merge. The caller should know.
      - "ABORT"   — the intent conflicts with existing state. Reasons:
                    * Decrease with no existing position (nothing to close)
                    * Increase with an opposite-direction position open
                      (GMX V2 will accept the order but execution semantics
                      get complex; safer to close first)
    """

    action: str  # PROCEED | MERGE | ABORT
    reason: str
    existing_position: Position | None


# ──────────────────────────────────────────────────────────────────────────
# Helpers — internal
# ──────────────────────────────────────────────────────────────────────────


def _build_market_address_to_alias() -> dict[str, str]:
    """Pure: reverse-lookup from market_address (lowercase) to alias.

    Used to populate `Position.market_alias` from the on-chain market
    address. Built once at module load (cheap — 8 entries).
    """
    return {
        m.market_address.lower(): alias
        for alias, m in ARBITRUM_MARKETS.items()
    }


_MARKET_ADDR_TO_ALIAS: dict[str, str] = _build_market_address_to_alias()


def _position_key(
    account: str, market: str, collateral_token: str, is_long: bool,
) -> bytes:
    """Pure: compute the bytes32 positionKey per Position.sol.

    `positionKey = keccak256(abi.encode(account, market, collateralToken, isLong))`

    Used as the second arg to `Reader.getPosition`. Same derivation lives
    in `contracts/position/Position.sol::getPositionKey`.
    """
    return Web3.keccak(
        encode(
            ["address", "address", "address", "bool"],
            [account, market, collateral_token, is_long],
        )
    )


def _decode_position_props(raw_tuple: tuple) -> Position | None:
    """Pure: convert the decoded Position.Props tuple → Position dataclass.

    Returns None if the props are a zero-struct (account == 0x0) — GMX
    returns a zero-filled struct when getPosition is called with a key
    that has no matching position, rather than reverting.

    Filters by size_in_usd == 0 happen at the caller level — this fn
    JUST decodes; the caller decides what to do with empty positions.
    """
    try:
        (addresses, numbers, flags) = raw_tuple
        (account, market, collateral_token) = addresses
        (
            size_in_usd,
            size_in_tokens,
            collateral_amount,
            _pending_impact_amount,           # int256 — not exposed today
            borrowing_factor,
            funding_fee_amount_per_size,
            _long_claim,                       # not exposed today
            _short_claim,                      # not exposed today
            increased_at_time,
            decreased_at_time,
        ) = numbers
        (is_long,) = flags
    except (ValueError, TypeError) as exc:
        log.warning("gmx_position_reader.decode_shape_mismatch err=%s", exc)
        return None

    # Zero-struct detection — GMX returns this when a positionKey has no
    # matching state, instead of reverting.
    if account == "0x0000000000000000000000000000000000000000":
        return None

    market_addr_lower = (market or "").lower()
    alias = _MARKET_ADDR_TO_ALIAS.get(market_addr_lower)

    return Position(
        account=account,
        market_alias=alias,
        market_address=market,
        collateral_token=collateral_token,
        is_long=bool(is_long),
        size_in_usd=int(size_in_usd),
        size_in_usd_float=int(size_in_usd) / 1e30,
        size_in_tokens=int(size_in_tokens),
        collateral_amount=int(collateral_amount),
        borrowing_factor=int(borrowing_factor),
        funding_fee_amount_per_size=int(funding_fee_amount_per_size),
        increased_at_time=int(increased_at_time),
        decreased_at_time=int(decreased_at_time),
    )


async def _eth_call(
    client: httpx.AsyncClient,
    rpc_url: str,
    to_address: str,
    data: str,
) -> str | None:
    """Best-effort eth_call. Returns the hex result or None on any failure.

    Mirrors the helper in `gmx_reader._eth_call` — kept module-local rather
    than imported to avoid a cross-module dependency on what's intentionally
    a private helper.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [{"to": to_address, "data": data}, "latest"],
    }
    try:
        resp = await client.post(rpc_url, json=payload)
    except (httpx.HTTPError, httpx.TimeoutException) as exc:
        log.warning("gmx_position_reader.rpc_http_error err=%s", exc)
        return None
    if resp.status_code != 200:
        log.warning(
            "gmx_position_reader.rpc_bad_status status=%d", resp.status_code,
        )
        return None
    try:
        body = resp.json()
    except (ValueError, TypeError):
        log.warning("gmx_position_reader.rpc_bad_json")
        return None
    if not isinstance(body, dict):
        log.warning("gmx_position_reader.rpc_bad_body_shape")
        return None
    if "error" in body:
        log.warning("gmx_position_reader.rpc_error err=%s", body["error"])
        return None
    result = body.get("result")
    if not isinstance(result, str):
        log.warning("gmx_position_reader.rpc_missing_result")
        return None
    return result


# ──────────────────────────────────────────────────────────────────────────
# Public — bulk read
# ──────────────────────────────────────────────────────────────────────────


async def fetch_account_positions(
    account: str,
    *,
    rpc_url: str | None = None,
    client: httpx.AsyncClient | None = None,
    start: int = 0,
    end: int = 100,
) -> list[Position]:
    """Read all current GMX V2 positions for `account` on Arbitrum.

    Calls `Reader.getAccountPositions(dataStore, account, start, end)`,
    decodes the returned `Position.Props[]`, filters out empty positions
    (size_in_usd == 0), and maps each market_address to its alias when
    known.

    Returns an empty list on:
      - RPC transport error
      - HTTP non-200
      - JSON-RPC error response (revert, etc.)
      - Decode failure (malformed bytes)
      - Empty result hex

    Never raises. Per-caller convention is to keep the loop alive on empty.
    """
    eff_rpc = rpc_url if rpc_url is not None else settings.arbitrum_rpc_url

    # Encode the call: getAccountPositions(dataStore, account, start, end)
    try:
        args = encode(
            ["address", "address", "uint256", "uint256"],
            [
                settings.gmx_datastore_address_arbitrum,
                account,
                int(start),
                int(end),
            ],
        )
    except Exception as exc:  # noqa: BLE001 — eth_abi raises a hierarchy
        log.warning(
            "gmx_position_reader.encode_failed account=%s err=%s", account, exc,
        )
        return []
    data = SELECTOR_GET_ACCOUNT_POSITIONS + args.hex()

    own_client = False
    if client is None:
        client = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.gmx_reader_timeout_s),
        )
        own_client = True
    try:
        result_hex = await _eth_call(
            client, eff_rpc, settings.gmx_reader_address_arbitrum, data,
        )
    finally:
        if own_client:
            await client.aclose()

    if result_hex is None:
        return []
    if not isinstance(result_hex, str) or not result_hex.startswith("0x"):
        return []
    try:
        body_bytes = bytes.fromhex(result_hex[2:])
    except ValueError:
        log.warning("gmx_position_reader.bad_hex result=%s", result_hex[:80])
        return []
    if len(body_bytes) == 0:
        return []

    try:
        (positions_raw,) = decode([f"{_POSITION_PROPS_TYPE}[]"], body_bytes)
    except Exception as exc:  # noqa: BLE001 — eth_abi error hierarchy
        log.warning("gmx_position_reader.decode_failed err=%s", exc)
        return []

    positions: list[Position] = []
    for raw in positions_raw:
        decoded = _decode_position_props(raw)
        if decoded is None:
            continue
        # Filter zero-size positions — these are stale slots GMX may return
        # for closed positions that haven't been GC'd from the account's
        # position list.
        if decoded.size_in_usd == 0:
            continue
        positions.append(decoded)
    return positions


# ──────────────────────────────────────────────────────────────────────────
# Public — single position by deterministic key
# ──────────────────────────────────────────────────────────────────────────


async def fetch_position(
    account: str,
    market_alias: str,
    collateral_token: str,
    is_long: bool,
    *,
    rpc_url: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> Position | None:
    """Read one specific position via `Reader.getPosition(dataStore, key)`.

    Computes the deterministic positionKey client-side, then makes a single
    `eth_call`. Faster than `fetch_account_positions` when the caller knows
    the exact (market, collateral, side) it wants to check.

    Returns None on:
      - Unknown market alias (not in ARBITRUM_MARKETS)
      - RPC / transport failure
      - Decode failure
      - Zero-struct (no matching position)
      - Size-zero result (closed but not yet GC'd)

    Never raises.
    """
    market = ARBITRUM_MARKETS.get(market_alias)
    if market is None:
        log.warning(
            "gmx_position_reader.unknown_market alias=%s", market_alias,
        )
        return None

    eff_rpc = rpc_url if rpc_url is not None else settings.arbitrum_rpc_url
    market_address = market.market_address

    key = _position_key(account, market_address, collateral_token, is_long)
    try:
        args = encode(
            ["address", "bytes32"],
            [settings.gmx_datastore_address_arbitrum, key],
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("gmx_position_reader.key_encode_failed err=%s", exc)
        return None
    data = SELECTOR_GET_POSITION + args.hex()

    own_client = False
    if client is None:
        client = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.gmx_reader_timeout_s),
        )
        own_client = True
    try:
        result_hex = await _eth_call(
            client, eff_rpc, settings.gmx_reader_address_arbitrum, data,
        )
    finally:
        if own_client:
            await client.aclose()

    if result_hex is None:
        return None
    if not isinstance(result_hex, str) or not result_hex.startswith("0x"):
        return None
    try:
        body_bytes = bytes.fromhex(result_hex[2:])
    except ValueError:
        return None
    if len(body_bytes) == 0:
        return None
    try:
        (raw,) = decode([_POSITION_PROPS_TYPE], body_bytes)
    except Exception as exc:  # noqa: BLE001
        log.warning("gmx_position_reader.single_decode_failed err=%s", exc)
        return None

    decoded = _decode_position_props(raw)
    if decoded is None:
        return None
    if decoded.size_in_usd == 0:
        return None
    return decoded


# ──────────────────────────────────────────────────────────────────────────
# Public — pure reconciliation (no I/O)
# ──────────────────────────────────────────────────────────────────────────


def reconcile_intent(
    intent: OrderIntent,
    current_positions: list[Position],
) -> ReconciliationResult:
    """Pure: decide PROCEED / MERGE / ABORT for an intent given live state.

    Decision matrix (per task spec):

      | intent.is_increase | existing position match | side match | result    |
      |--------------------|-------------------------|------------|-----------|
      | True               | False (no match)        | n/a        | PROCEED   |
      | True               | True                    | same       | MERGE     |
      | True               | True                    | opposite   | ABORT     |
      | False (decrease)   | False (no match)        | n/a        | ABORT     |
      | False (decrease)   | True                    | same       | PROCEED   |
      | False (decrease)   | True                    | opposite   | ABORT     |

    "Match" means same (market_alias, collateral_token) — GMX V2 keys
    positions by (account, market, collateralToken, isLong), so two
    positions in the same market with different collateral tokens are
    DIFFERENT positions. A side-match check is then layered on top to
    distinguish merge-vs-conflict.

    Does NOT modify `intent` or any Position. Pure analysis.

    Implementation note: we collect matches by (market_alias OR
    market_address) AND collateral_token. If `intent.market` is e.g. "sol"
    and the on-chain position's `market_alias` is also "sol", they match;
    if the on-chain `market_alias` is None (unknown market) we fall back
    to matching against the resolved address from ARBITRUM_MARKETS — but
    the intent itself is keyed by alias, so an unknown-market position
    won't shadow a known intent.
    """
    # Resolve the intent's market_address via ARBITRUM_MARKETS so we can
    # match by either alias or address. If alias unknown, fall through to
    # "no match" — the encoder would refuse to encode such an intent anyway.
    intent_market = ARBITRUM_MARKETS.get(intent.market)
    intent_market_addr = (
        intent_market.market_address.lower() if intent_market else None
    )
    intent_collat_lower = intent.collateral_token.lower()

    same_side_match: Position | None = None
    opposite_side_match: Position | None = None
    for p in current_positions:
        same_market = (
            (p.market_alias == intent.market)
            or (p.market_address.lower() == intent_market_addr)
        )
        same_collat = p.collateral_token.lower() == intent_collat_lower
        if not (same_market and same_collat):
            continue
        if p.is_long == intent.is_long:
            same_side_match = p
        else:
            opposite_side_match = p

    if intent.is_increase:
        if same_side_match is not None:
            return ReconciliationResult(
                action="MERGE",
                reason=(
                    f"existing same-direction position in market={intent.market} "
                    f"collateral={intent.collateral_token} is_long={intent.is_long} "
                    f"size_in_usd_float={same_side_match.size_in_usd_float:.2f}; "
                    f"GMX V2 will auto-merge"
                ),
                existing_position=same_side_match,
            )
        if opposite_side_match is not None:
            return ReconciliationResult(
                action="ABORT",
                reason=(
                    f"opposite-direction position open in market={intent.market} "
                    f"collateral={intent.collateral_token} "
                    f"existing.is_long={opposite_side_match.is_long}; "
                    f"close before opening other side"
                ),
                existing_position=opposite_side_match,
            )
        return ReconciliationResult(
            action="PROCEED",
            reason="no existing position; safe to open",
            existing_position=None,
        )

    # is_decrease branch
    if same_side_match is None:
        # Possibly the operator is trying to close the wrong side. Check
        # opposite side to give a clearer abort message.
        if opposite_side_match is not None:
            return ReconciliationResult(
                action="ABORT",
                reason=(
                    f"no position to decrease on intent.is_long={intent.is_long} "
                    f"side; existing position is opposite side "
                    f"(existing.is_long={opposite_side_match.is_long})"
                ),
                existing_position=opposite_side_match,
            )
        return ReconciliationResult(
            action="ABORT",
            reason=(
                f"no position to decrease in market={intent.market} "
                f"collateral={intent.collateral_token} is_long={intent.is_long}"
            ),
            existing_position=None,
        )
    return ReconciliationResult(
        action="PROCEED",
        reason=(
            f"existing position size_in_usd_float={same_side_match.size_in_usd_float:.2f}; "
            f"decrease will close some/all"
        ),
        existing_position=same_side_match,
    )


__all__ = [
    "SELECTOR_GET_ACCOUNT_POSITIONS",
    "SELECTOR_GET_POSITION",
    "Position",
    "ReconciliationResult",
    "fetch_account_positions",
    "fetch_position",
    "reconcile_intent",
]
