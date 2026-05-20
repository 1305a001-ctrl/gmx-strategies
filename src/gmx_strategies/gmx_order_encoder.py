"""GMX V2 ExchangeRouter order encoder + mainnet eth_call simulation harness.

This is G5.1 — the FIRST coding task of the executor sprint. Pure encoding
+ view-only simulation. NO signing, NO submission, NO `eth_sendTransaction`.

Hard scope:
  - Build `CreateOrderParams` struct bytes via `eth_abi.encode`
  - Wrap createOrder in `PayableMulticall(sendWnt + sendTokens + createOrder)`
    — audit C2 says a bare createOrder reverts with
    `InsufficientWntAmountForExecutionFee` because OrderVault was never funded.
  - Simulate via `eth_call` against Arbitrum mainnet head
  - Classify reverts via `gmx_errors.KNOWN_ERROR_SELECTORS`

What this module deliberately does NOT do (deferred to G5.2):
  - Sign or submit any transaction
  - Approve any ERC20 (the approval target is `Router`, NOT
    `ExchangeRouter` — audit C1; future signing module owns this)
  - Manage position state, retries, or fill reconciliation
  - Read live execution-fee floors from DataStore (caller supplies
    `execution_fee_wei` directly; the encoder validates it's > 0)

ABI shape — `IBaseOrderUtils.CreateOrderParams` (v2.2, verified 2026-05-20):

    struct CreateOrderParamsAddresses {
        address receiver;
        address cancellationReceiver;
        address callbackContract;
        address uiFeeReceiver;
        address market;
        address initialCollateralToken;
        address[] swapPath;
    }
    struct CreateOrderParamsNumbers {
        uint256 sizeDeltaUsd;
        uint256 initialCollateralDeltaAmount;
        uint256 triggerPrice;
        uint256 acceptablePrice;
        uint256 executionFee;
        uint256 callbackGasLimit;
        uint256 minOutputAmount;
        uint256 validFromTime;
    }
    struct CreateOrderParams {
        CreateOrderParamsAddresses addresses;
        CreateOrderParamsNumbers numbers;
        Order.OrderType orderType;                  // uint8
        Order.DecreasePositionSwapType decreasePositionSwapType;  // uint8
        bool isLong;
        bool shouldUnwrapNativeToken;
        bool autoCancel;
        bytes32 referralCode;
        bytes32[] dataList;                          // NEW in v2.2
    }

Function selector for `createOrder((CreateOrderParams))` = `0xf59c48eb`
(verified by probing the ExchangeRouter bytecode @ Arbitrum mainnet
2026-05-20: `curl … eth_getCode 0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41`,
grep selector bytes — confirmed present in deployed code).

`acceptablePrice` direction matrix (per audit Q5 — `BaseOrderUtils.sol`):

    | Order Type    | Side  | acceptablePrice semantics       |
    |---------------|-------|----------------------------------|
    | MarketIncrease| long  | CEILING  (executionPrice <= ap)  |
    | MarketIncrease| short | FLOOR    (executionPrice >= ap)  |
    | MarketDecrease| long  | FLOOR    (closing a long = sell) |
    | MarketDecrease| short | CEILING  (closing a short = buy) |

The encoder applies the band based on order type + side. Callers supply
`current_price_1e30` (the live oracle's current price scaled to GMX's
30-decimal-USD fixed point) and `band_bps`; the encoder produces
`acceptable_price` correctly biased per the matrix.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx
from eth_abi import encode  # type: ignore[attr-defined]

from gmx_strategies import gmx_errors
from gmx_strategies.settings import settings

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────
# Function selectors (verified against ExchangeRouter bytecode @ Arbitrum
# 2026-05-20 — `eth_getCode 0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41`
# and grep for each in the returned bytecode).
# ──────────────────────────────────────────────────────────────────────────
SELECTOR_MULTICALL = "0xac9650d8"        # multicall(bytes[])
SELECTOR_SEND_WNT = "0x7d39aaf1"          # sendWnt(address,uint256)
SELECTOR_SEND_TOKENS = "0xe6d66ac8"       # sendTokens(address,address,uint256)
SELECTOR_CREATE_ORDER = "0xf59c48eb"      # createOrder((CreateOrderParams))

# ──────────────────────────────────────────────────────────────────────────
# Order type enum values (from `contracts/order/Order.sol::OrderType`).
# Only MarketIncrease + MarketDecrease are relevant for funding-arb (G5).
# ──────────────────────────────────────────────────────────────────────────
ORDER_TYPE_MARKET_INCREASE = 2
ORDER_TYPE_MARKET_DECREASE = 4

# DecreasePositionSwapType enum: 0 = NoSwap (default), 1 = PnlToCollateral,
# 2 = CollateralToPnl. Used only on Decrease; ignored on Increase.
DECREASE_POSITION_SWAP_TYPE_NO_SWAP = 0


# ──────────────────────────────────────────────────────────────────────────
# Frozen dataclasses
# ──────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class OrderIntent:
    """One funding-arb order's intent — encoded into CreateOrderParams.

    All amounts in their native units:
      - `initial_collateral_delta_amount`: raw token units. For USDC (6 dec)
        $10 = 10_000_000. For WETH (18 dec) 0.001 ETH = 10**15.
      - `size_delta_usd`: 30-decimal-USD (GMX convention). $10 = 10**31.
      - `current_price_1e30`: GMX-scaled current oracle price (10**(30-decimals)).
        Used to derive `acceptable_price` with the band. The actual scaling
        per token convention lives in `gmx_reader._scale_price_to_gmx`.
      - `execution_fee_wei`: ETH wei budget for the keeper. Will be wrapped
        via `sendWnt` (deposits ETH→WETH, sends to OrderVault).

    `is_increase` flips between MarketIncrease (open) and MarketDecrease
    (close). `is_long` is the position side (NOT directly tied to which
    side a Decrease takes — closing a long is a Decrease with `isLong=true`).

    `account` is the EOA that would submit + own the order. For the
    simulation harness this is a dummy address (e.g.
    0x0000000000000000000000000000000000000001) since we never sign or send.
    """

    market: str  # alias, e.g. "sol"; resolved via markets.ARBITRUM_MARKETS
    is_long: bool
    is_increase: bool
    collateral_token: str  # address (USDC for shorts; index token for longs)
    initial_collateral_delta_amount: int  # raw token units
    size_delta_usd: int  # 1e30-scaled USD
    current_price_1e30: int  # GMX-scaled current price (used to derive acceptable_price)
    acceptable_price_band_bps: int  # 150 for majors, 350 for alts
    execution_fee_wei: int  # ETH wei budget for keeper
    account: str  # EOA address; becomes `receiver` + `cancellationReceiver`


@dataclass(frozen=True)
class SimulationResult:
    """Result of an `eth_call` simulation of the createOrder multicall."""

    ok: bool
    revert_selector: str | None
    revert_known_acceptable: bool
    revert_reason_name: str | None
    raw_response: str | None


# ──────────────────────────────────────────────────────────────────────────
# Helpers — internal
# ──────────────────────────────────────────────────────────────────────────


def _compute_acceptable_price(
    *,
    current_price_1e30: int,
    band_bps: int,
    is_long: bool,
    is_increase: bool,
) -> int:
    """Pure: bias the current price by `band_bps` per the audit Q5 matrix.

    | Order Type     | Side  | acceptablePrice direction              |
    |----------------|-------|-----------------------------------------|
    | MarketIncrease | long  | + band  (CEILING; willing to pay UP)    |
    | MarketIncrease | short | - band  (FLOOR; willing to sell DOWN)   |
    | MarketDecrease | long  | - band  (FLOOR; selling, want >= ap)    |
    | MarketDecrease | short | + band  (CEILING; buying, want <= ap)   |

    Collapses to: add iff `is_long == is_increase`.

    `band_bps` is in basis points (10000 = 100%).
    """
    if band_bps < 0:
        raise ValueError(f"acceptable_price_band_bps must be >= 0, got {band_bps}")
    add_band = (is_long == is_increase)
    delta = current_price_1e30 * band_bps // 10_000
    if add_band:
        return current_price_1e30 + delta
    return current_price_1e30 - delta


def _validate_intent(intent: OrderIntent) -> None:
    """Defensive runtime checks before encoding. Raises ValueError on bad input.

    These trap the worst silent-failure modes (bad zero values, malformed
    addresses) BEFORE we burn time on an eth_call. They mirror the audit's
    C1-C4 + M2 findings.
    """
    if not intent.market:
        raise ValueError("OrderIntent.market is empty")
    if not isinstance(intent.account, str) or not intent.account.startswith("0x"):
        raise ValueError(f"OrderIntent.account must be 0x-prefixed: {intent.account!r}")
    if len(intent.account) != 42:
        raise ValueError(f"OrderIntent.account must be 20-byte hex: {intent.account!r}")
    if (
        not isinstance(intent.collateral_token, str)
        or not intent.collateral_token.startswith("0x")
    ):
        raise ValueError(
            f"OrderIntent.collateral_token must be 0x-prefixed: {intent.collateral_token!r}"
        )
    if intent.initial_collateral_delta_amount < 0:
        raise ValueError("initial_collateral_delta_amount must be >= 0")
    if intent.size_delta_usd <= 0:
        raise ValueError("size_delta_usd must be > 0 (no zero-size orders)")
    if intent.current_price_1e30 <= 0:
        raise ValueError("current_price_1e30 must be > 0")
    if intent.execution_fee_wei <= 0:
        # Audit C2: a zero executionFee → InsufficientWntAmountForExecutionFee.
        # Trap here so the caller sees a clear local error.
        raise ValueError("execution_fee_wei must be > 0")


def _resolve_market(intent: OrderIntent) -> str:
    """Pure: look up the market address for an OrderIntent.

    Returns the Arbitrum market token address. Raises ValueError when the
    market alias is not in `ARBITRUM_MARKETS` — the encoder refuses to
    silently send a tx to address(0) or a stale market.
    """
    # Imported here to avoid circular imports at module load.
    from gmx_strategies.markets import ARBITRUM_MARKETS

    gmx_market = ARBITRUM_MARKETS.get(intent.market)
    if gmx_market is None:
        raise ValueError(f"unknown market alias: {intent.market!r}")
    return gmx_market.market_address


# ──────────────────────────────────────────────────────────────────────────
# Encoding — pure helpers
# ──────────────────────────────────────────────────────────────────────────


def _encode_create_order_params(intent: OrderIntent) -> bytes:
    """Pure: encode the full createOrder call (selector + struct args).

    The struct is wrapped in one outer tuple per Solidity's struct-as-calldata
    convention.

    Returns: 4-byte selector || abi.encode(CreateOrderParams).
    """
    _validate_intent(intent)
    market_address = _resolve_market(intent)

    # CreateOrderParamsAddresses — eth_abi accepts a heterogeneous tuple of
    # the field values; we annotate as `tuple[Any, ...]` because the struct
    # mixes addresses (str), an `address[]` (list[str]), and a uint256 list.
    addresses_tuple: tuple[Any, ...] = (
        intent.account,                   # receiver — refund destination
        intent.account,                   # cancellationReceiver
        "0x0000000000000000000000000000000000000000",  # callbackContract (none)
        "0x0000000000000000000000000000000000000000",  # uiFeeReceiver (none)
        market_address,
        intent.collateral_token,
        [],                                # swapPath — no in-tx swap
    )

    # CreateOrderParamsNumbers
    acceptable_price = _compute_acceptable_price(
        current_price_1e30=intent.current_price_1e30,
        band_bps=intent.acceptable_price_band_bps,
        is_long=intent.is_long,
        is_increase=intent.is_increase,
    )
    # initialCollateralDeltaAmount: per the audit Q4 nuance table, this is
    # IGNORED for MarketIncrease (taken from OrderVault.recordTransferIn)
    # but USED for MarketDecrease (the withdraw amount). We pass the value
    # the caller supplied in both cases — the contract ignores it on
    # Increase, and the caller is responsible for getting it right on
    # Decrease.
    numbers_tuple = (
        intent.size_delta_usd,
        intent.initial_collateral_delta_amount,
        0,                                  # triggerPrice — 0 for market orders
        acceptable_price,
        intent.execution_fee_wei,           # executionFee (in wei, NOT GMX-scaled)
        0,                                  # callbackGasLimit (no callback)
        0,                                  # minOutputAmount (irrelevant for non-swap)
        0,                                  # validFromTime — MUST be 0 (audit C3)
    )

    order_type = (
        ORDER_TYPE_MARKET_INCREASE if intent.is_increase else ORDER_TYPE_MARKET_DECREASE
    )

    # Full struct
    create_order_params: tuple[Any, ...] = (
        addresses_tuple,
        numbers_tuple,
        order_type,
        DECREASE_POSITION_SWAP_TYPE_NO_SWAP,
        intent.is_long,
        False,                              # shouldUnwrapNativeToken
        False,                              # autoCancel
        b"\x00" * 32,                       # referralCode (none)
        [],                                  # dataList — empty bytes32[] (v2.2)
    )

    # Type spec — MUST match v2.2 CreateOrderParams ABI shape exactly.
    # Field order: addresses, numbers, orderType, decreasePositionSwapType,
    # isLong, shouldUnwrapNativeToken, autoCancel, referralCode, dataList.
    type_spec = (
        "("
        "(address,address,address,address,address,address,address[]),"
        "(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256),"
        "uint8,"
        "uint8,"
        "bool,bool,bool,"
        "bytes32,"
        "bytes32[]"
        ")"
    )
    encoded = encode([type_spec], [create_order_params])

    # Prepend the 4-byte selector to form the full calldata.
    selector_bytes = bytes.fromhex(SELECTOR_CREATE_ORDER[2:])
    return selector_bytes + encoded


def _encode_send_wnt(receiver: str, amount: int) -> bytes:
    """Pure: encode sendWnt(receiver, amount). Receiver is always OrderVault."""
    selector_bytes = bytes.fromhex(SELECTOR_SEND_WNT[2:])
    args = encode(["address", "uint256"], [receiver, amount])
    return selector_bytes + args


def _encode_send_tokens(token: str, receiver: str, amount: int) -> bytes:
    """Pure: encode sendTokens(token, receiver, amount).

    `receiver` is OrderVault for our use case. `token` is the collateral
    address. The token must be approved to `Router` BEFORE this multicall
    is submitted on-chain (audit C1) — but for `eth_call` simulation no
    approval matters because the call is view-only.
    """
    selector_bytes = bytes.fromhex(SELECTOR_SEND_TOKENS[2:])
    args = encode(["address", "address", "uint256"], [token, receiver, amount])
    return selector_bytes + args


def _encode_multicall(intent: OrderIntent) -> bytes:
    """Pure: produce the full multicall payload for the createOrder tx.

    Returns: 4-byte selector || abi.encode([sub_call_1, sub_call_2, sub_call_3]).

    Per audit C2: createOrder MUST be wrapped in PayableMulticall with
    prior sendWnt + sendTokens. The order in the array is:
      1. sendWnt(OrderVault, execution_fee_wei) — wraps msg.value to WETH
      2. sendTokens(collateral, OrderVault, collateral_amount) — pulls via Router
      3. createOrder(params) — drains the OrderVault, builds + emits OrderCreated

    Each call uses `delegatecall` inside PayableMulticall, so msg.sender
    and msg.value are preserved. For the simulation, msg.value MUST equal
    `execution_fee_wei` exactly — otherwise sendWnt reverts.
    """
    _validate_intent(intent)
    order_vault = settings.gmx_order_vault_address_arbitrum

    # Build the 3 sub-call payloads (selector + abi-encoded args).
    call_send_wnt = _encode_send_wnt(order_vault, intent.execution_fee_wei)
    call_send_tokens = _encode_send_tokens(
        intent.collateral_token, order_vault, intent.initial_collateral_delta_amount,
    )
    call_create_order = _encode_create_order_params(intent)

    # multicall(bytes[] data) — encode the array of bytes blobs.
    selector_bytes = bytes.fromhex(SELECTOR_MULTICALL[2:])
    args = encode(["bytes[]"], [[call_send_wnt, call_send_tokens, call_create_order]])
    return selector_bytes + args


# ──────────────────────────────────────────────────────────────────────────
# Simulation payload — what we ship to eth_call
# ──────────────────────────────────────────────────────────────────────────


def build_simulation_payload(intent: OrderIntent) -> dict[str, str]:
    """Pure: produce the `params[0]` dict for an eth_call JSON-RPC request.

    Returns a dict matching the standard eth_call object shape:
      { "from": account, "to": ExchangeRouter, "value": hex_wei, "data": multicall_hex }

    The `value` is the execution fee in wei (hex-encoded) — eth_call respects
    `value` for sender-balance simulation but does not actually transfer it.
    Use directly with `httpx.post(rpc, json=...).result`.
    """
    _validate_intent(intent)
    multicall_data = _encode_multicall(intent)
    return {
        "from": intent.account,
        "to": settings.gmx_exchange_router_address_arbitrum,
        "value": hex(intent.execution_fee_wei),
        "data": "0x" + multicall_data.hex(),
    }


# ──────────────────────────────────────────────────────────────────────────
# Revert classification
# ──────────────────────────────────────────────────────────────────────────


def _extract_revert_selector(error_data: str | None) -> str | None:
    """Pure: pull the 4-byte selector from JSON-RPC revert data.

    Arbitrum nodes return reverts in the JSON-RPC error object's `data`
    field as a hex string. The first 4 bytes (after `0x`) are the selector;
    subsequent bytes are abi-encoded error args.

    Returns the selector as a lowercase 8-char hex string (no 0x prefix)
    or None if the data is missing / malformed.
    """
    if not isinstance(error_data, str):
        return None
    body = error_data.lower().removeprefix("0x")
    if len(body) < 8:
        return None
    return body[:8]


def _classify_response_body(body: dict[str, Any]) -> SimulationResult:
    """Pure: turn a JSON-RPC response body into a SimulationResult.

    Three cases:
      - Has `result` key (success / no-revert): ok=True
      - Has `error` key with `data`: a revert; extract selector + classify
      - Anything else: ok=False, all None — unknown failure
    """
    # Success case: `result` is the return data (often "0x" for void/no-return).
    if "result" in body and "error" not in body:
        return SimulationResult(
            ok=True,
            revert_selector=None,
            revert_known_acceptable=False,
            revert_reason_name=None,
            raw_response=body.get("result"),
        )

    error = body.get("error")
    if not isinstance(error, dict):
        return SimulationResult(
            ok=False,
            revert_selector=None,
            revert_known_acceptable=False,
            revert_reason_name=None,
            raw_response=None,
        )

    # Arbitrum errors carry the revert data in `error.data` (hex string).
    # Some clients put it in `error.data.data` or similar — handle both.
    raw_data = error.get("data")
    if isinstance(raw_data, dict):
        # Nested {"data": "0x..."} form
        raw_data = raw_data.get("data") or raw_data.get("originalError", {}).get("data")
    if not isinstance(raw_data, str):
        # Try to get any string from the error
        msg = error.get("message", "")
        if isinstance(msg, str) and "0x" in msg:
            # Some clients put it in the message — best-effort extract
            idx = msg.find("0x")
            tail = msg[idx:].split()[0].strip(",;)")
            if len(tail) >= 10:
                raw_data = tail
    selector = _extract_revert_selector(raw_data if isinstance(raw_data, str) else None)
    # Use the FULL classifier — handles both custom GMX errors and the
    # standard `Error(string)` selector (used by ERC20 OZ revert strings,
    # which surface as "transfer amount exceeds allowance" / etc when the
    # dummy account in the smoke test hasn't approved Router).
    raw_str = raw_data if isinstance(raw_data, str) else None
    reason_name, _bucket, known_acceptable = gmx_errors.classify_revert_payload(raw_str)
    return SimulationResult(
        ok=False,
        revert_selector=selector,
        revert_known_acceptable=known_acceptable,
        revert_reason_name=reason_name,
        raw_response=raw_str,
    )


# ──────────────────────────────────────────────────────────────────────────
# Public async entry point
# ──────────────────────────────────────────────────────────────────────────


async def simulate_order(
    intent: OrderIntent,
    *,
    rpc_url: str,
    client: httpx.AsyncClient | None = None,
) -> SimulationResult:
    """Send the createOrder multicall as `eth_call` against Arbitrum mainnet.

    This is the load-bearing live-truth check for G5.1: if the call succeeds
    OR reverts in a known-acceptable bucket (insufficient WNT / collateral),
    the encoding + multicall shape + market existence are all sound. Any
    OTHER revert (or unknown selector) means the encoding is structurally
    wrong — flag loudly.

    Never raises. Returns a `SimulationResult` even on transport failure.
    """
    params = build_simulation_payload(intent)
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [params, "latest"],
    }

    own_client = False
    if client is None:
        timeout = httpx.Timeout(settings.gmx_reader_timeout_s)
        client = httpx.AsyncClient(timeout=timeout)
        own_client = True
    try:
        try:
            resp = await client.post(rpc_url, json=payload)
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            log.warning("gmx_order_encoder.rpc_http_error err=%s", exc)
            return SimulationResult(
                ok=False, revert_selector=None, revert_known_acceptable=False,
                revert_reason_name=None, raw_response=None,
            )
        if resp.status_code != 200:
            log.warning("gmx_order_encoder.rpc_bad_status status=%d", resp.status_code)
            return SimulationResult(
                ok=False, revert_selector=None, revert_known_acceptable=False,
                revert_reason_name=None, raw_response=None,
            )
        try:
            body = resp.json()
        except (ValueError, TypeError):
            log.warning("gmx_order_encoder.rpc_bad_json")
            return SimulationResult(
                ok=False, revert_selector=None, revert_known_acceptable=False,
                revert_reason_name=None, raw_response=None,
            )
        if not isinstance(body, dict):
            return SimulationResult(
                ok=False, revert_selector=None, revert_known_acceptable=False,
                revert_reason_name=None, raw_response=None,
            )
        return _classify_response_body(body)
    finally:
        if own_client:
            await client.aclose()


__all__ = [
    "DECREASE_POSITION_SWAP_TYPE_NO_SWAP",
    "ORDER_TYPE_MARKET_DECREASE",
    "ORDER_TYPE_MARKET_INCREASE",
    "OrderIntent",
    "SELECTOR_CREATE_ORDER",
    "SELECTOR_MULTICALL",
    "SELECTOR_SEND_TOKENS",
    "SELECTOR_SEND_WNT",
    "SimulationResult",
    "build_simulation_payload",
    "simulate_order",
]
