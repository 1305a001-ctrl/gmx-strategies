"""GMX V2 cancellation-reason classifier — 4-byte selector → error name map.

This module is the canonical home for resolving an arbitrary `OrderCancelled`
or `OrderFrozen` `reasonBytes` blob to a human-readable error label. It's
populated from the audit's curated list drawn from
`gmx-io/gmx-synthetics/contracts/error/Errors.sol` (see
`/Users/benedict/.claude/projects/-Users-benedict/memory/arch_gmx_executor_audit.md`,
"Curated Errors.sol selectors for the G5 cancellation-reason classifier",
sourced from main @ 2026-05-20, 351 total errors).

Why selector → name (not name → selector): GMX has renamed errors across
v2 releases (the audit caught `MinPositionSizeUsd` → `MinPositionSize` and
two `Pool` → `OpenInterest` renames). Selectors are STABLE for a given
function signature; names drift. Always classify by 4-byte selector.

Buckets (used by `simulate_order` to decide whether a revert is "structural
encoding bug" or "expected for an unfunded account"):

  - SLIPPAGE_PRICING:    oracle / acceptablePrice failures
  - MARKET_VALIDATION:   market disabled / wrong collateral / unsupported type
  - POSITION_STATE:      empty position / size below floor / liquidatable
  - EXECUTION_FEE_GAS:   under-funded executionFee, wnt mismatch, gas
  - POOL_RESERVES:       insufficient pool / OI cap / impact too large
  - MISC_CANCELLATION:   empty receiver / unexpected validFromTime / etc
  - ERC20_ALLOWANCE:     standard Error(string) `0x08c379a0` reverts from
                         token transfers when the user hasn't approved
                         Router — exactly what the smoke-test dummy account
                         hits at the sendTokens step. Inspected by string
                         content; see `is_known_acceptable_full_data`.

The buckets are exposed via `KNOWN_ACCEPTABLE_BUCKETS` so the encoder can
declare specific buckets as "expected without funding" — the
EXECUTION_FEE_GAS + POSITION_STATE + ERC20_ALLOWANCE buckets all qualify.
"""

from __future__ import annotations

# Bucket identifiers — used to decide acceptability in the encoder.
BUCKET_SLIPPAGE_PRICING = "slippage_pricing"
BUCKET_MARKET_VALIDATION = "market_validation"
BUCKET_POSITION_STATE = "position_state"
BUCKET_EXECUTION_FEE_GAS = "execution_fee_gas"
BUCKET_POOL_RESERVES = "pool_reserves"
BUCKET_MISC_CANCELLATION = "misc_cancellation"
BUCKET_ERC20_ALLOWANCE = "erc20_allowance"

# Standard Solidity `Error(string)` selector. Triggered by `require(_, "msg")`
# reverts; common from ERC20 transfers (OZ revert strings). NOT in
# Errors.sol — Solidity ships it. Decoding requires reading the string arg.
ERROR_STRING_SELECTOR = "08c379a0"

# Acceptable Error(string) substrings (case-insensitive). When the standard
# Error(string) revert payload's decoded message contains any of these, we
# treat the revert as known-acceptable. These all signal "the contract
# accepted our call, decoded our args, and then complained about the
# unfunded dummy state" — exactly the smoke-test signal we want.
#
# The OpenZeppelin v4 ERC20 strings are the canonical patterns; native
# Arbitrum-bridged USDC and WETH9 use the same family.
ERC20_ACCEPTABLE_SUBSTRINGS: frozenset[str] = frozenset({
    "transfer amount exceeds allowance",
    "transfer amount exceeds balance",
    "insufficient allowance",
    "insufficient balance",
})


# The curated selectors, organized into the 6 buckets. Each entry is
# {selector_lc_no_prefix: (canonical_name, bucket)}. Selector =
# `keccak256("ErrorName(types)")[:4]`, lowercase, no 0x prefix.
#
# Verbatim from arch_gmx_executor_audit.md "Curated Errors.sol selectors"
# (2026-05-20). The 6 sub-tables in the audit sum to 50 entries (the audit
# body's "46 curated" remark was off-by-4 — see test in
# tests/test_gmx_order_encoder.py for the per-bucket count assertion).
KNOWN_ERROR_SELECTORS: dict[str, tuple[str, str]] = {
    # ── Slippage / oracle pricing ────────────────────────────────────────
    "e09ad0e9": ("OrderNotFulfillableAtAcceptablePrice", BUCKET_SLIPPAGE_PRICING),
    "0481a15a": ("InvalidOrderPrices", BUCKET_SLIPPAGE_PRICING),
    "cd64a025": ("EmptyPrimaryPrice", BUCKET_SLIPPAGE_PRICING),
    "be6514b6": ("InvalidFeedPrice", BUCKET_SLIPPAGE_PRICING),
    "d6b52b60": ("ChainlinkPriceFeedNotUpdated", BUCKET_SLIPPAGE_PRICING),
    "8db88ccf": ("EmptyChainlinkPriceFeed", BUCKET_SLIPPAGE_PRICING),
    "62e402cc": ("EmptyDataStreamFeedId", BUCKET_SLIPPAGE_PRICING),
    "2a74194d": ("InvalidDataStreamPrices", BUCKET_SLIPPAGE_PRICING),
    "8d56bea1": ("InvalidDataStreamBidAsk", BUCKET_SLIPPAGE_PRICING),
    "9231be69": ("EmptyValidatedPrices", BUCKET_SLIPPAGE_PRICING),
    "eb1947dd": ("EmptyMarketPrice", BUCKET_SLIPPAGE_PRICING),

    # ── Market / collateral validation ───────────────────────────────────
    "05fbc1ae": ("EmptyMarket", BUCKET_MARKET_VALIDATION),
    "09f8c937": ("DisabledMarket", BUCKET_MARKET_VALIDATION),
    "dd70e0c9": ("DisabledFeature", BUCKET_MARKET_VALIDATION),
    "182e30e3": ("InvalidPositionMarket", BUCKET_MARKET_VALIDATION),
    "839c693e": ("InvalidCollateralTokenForMarket", BUCKET_MARKET_VALIDATION),
    "bff65b3f": ("InvalidPositionSizeValues", BUCKET_MARKET_VALIDATION),
    "3784f834": ("UnsupportedOrderType", BUCKET_MARKET_VALIDATION),
    "cb9bd134": ("InvalidSwapMarket", BUCKET_MARKET_VALIDATION),
    "9fbe2cbc": ("InvalidDecreaseOrderSize", BUCKET_MARKET_VALIDATION),
    "751951f9": ("InvalidDecreasePositionSwapType", BUCKET_MARKET_VALIDATION),

    # ── Position state ───────────────────────────────────────────────────
    "4dfbbff3": ("EmptyPosition", BUCKET_POSITION_STATE),
    "2159b161": ("InsufficientCollateralUsd", BUCKET_POSITION_STATE),
    "74cc815b": ("InsufficientCollateralAmount", BUCKET_POSITION_STATE),
    # Note name `MinPositionSize` not `MinPositionSizeUsd` — audit caught
    # the v2.x rename. Selector is stable for the (uint256,uint256) sig.
    "85efb31a": ("MinPositionSize", BUCKET_POSITION_STATE),
    "ee919dd9": ("PositionShouldNotBeLiquidated", BUCKET_POSITION_STATE),
    "bc121108": ("LiquidatablePosition", BUCKET_POSITION_STATE),
    "eadaf93a": ("UsdDeltaExceedsLongOpenInterest", BUCKET_POSITION_STATE),

    # ── Execution fee / gas ──────────────────────────────────────────────
    "3a78cd7e": ("InsufficientWntAmountForExecutionFee", BUCKET_EXECUTION_FEE_GAS),
    "5dac504d": ("InsufficientExecutionFee", BUCKET_EXECUTION_FEE_GAS),
    "bb416f93": ("InsufficientExecutionGas", BUCKET_EXECUTION_FEE_GAS),
    "d3dacaac": ("InsufficientGasForCancellation", BUCKET_EXECUTION_FEE_GAS),
    "f50ce733": ("InsufficientGasLeft", BUCKET_EXECUTION_FEE_GAS),
    "3083b9e5": ("InsufficientHandleExecutionErrorGas", BUCKET_EXECUTION_FEE_GAS),
    "9b867f31": ("InvalidExecutionFee", BUCKET_EXECUTION_FEE_GAS),

    # ── Pool / reserve guardrails ────────────────────────────────────────
    "315276c9": ("InsufficientReserve", BUCKET_POOL_RESERVES),
    "b98c6179": ("InsufficientReserveForOpenInterest", BUCKET_POOL_RESERVES),
    "23090a31": ("InsufficientPoolAmount", BUCKET_POOL_RESERVES),
    "a7aebadc": ("InsufficientSwapOutputAmount", BUCKET_POOL_RESERVES),
    "d28d3eb5": ("InsufficientOutputAmount", BUCKET_POOL_RESERVES),
    "2bf127cf": ("MaxOpenInterestExceeded", BUCKET_POOL_RESERVES),
    "f0641c92": ("PriceImpactLargerThanOrderSize", BUCKET_POOL_RESERVES),

    # ── Miscellaneous cancellation paths ─────────────────────────────────
    "16307797": ("EmptyOrder", BUCKET_MISC_CANCELLATION),
    "e9b78bd4": ("EmptyHoldingAddress", BUCKET_MISC_CANCELLATION),
    "dd7016a2": ("EmptyAccount", BUCKET_MISC_CANCELLATION),
    "0d143458": ("EmptyAmount", BUCKET_MISC_CANCELLATION),
    "d551823d": ("EmptyReceiver", BUCKET_MISC_CANCELLATION),
    "730d44b1": ("OrderAlreadyFrozen", BUCKET_MISC_CANCELLATION),
    "3af14617": ("UnexpectedValidFromTime", BUCKET_MISC_CANCELLATION),
    "f0794a60": ("MaxAutoCancelOrdersExceeded", BUCKET_MISC_CANCELLATION),
}


# Acceptability classifier — used by `gmx_order_encoder.simulate_order`.
#
# When simulating an unfunded dummy account against ExchangeRouter, the
# encoding is STRUCTURALLY correct if the revert lands in one of the
# "known acceptable" buckets — meaning the contract decoded our params,
# found the market, and only then complained about missing WNT/collateral.
#
# Anything in OTHER buckets (or an unresolved selector) → flag as a
# possible encoding bug.
KNOWN_ACCEPTABLE_BUCKETS: frozenset[str] = frozenset({
    BUCKET_EXECUTION_FEE_GAS,
    BUCKET_POSITION_STATE,
})


def resolve_revert(selector: str) -> str | None:
    """Pure: resolve a 4-byte revert selector to its canonical error name.

    Accepts either with or without `0x` prefix, case-insensitive. Returns
    None for unknown selectors (the caller treats unknown as critical-fail).
    """
    if not isinstance(selector, str):
        return None
    sel = selector.lower().removeprefix("0x")
    if len(sel) != 8:
        return None
    entry = KNOWN_ERROR_SELECTORS.get(sel)
    if entry is None:
        return None
    return entry[0]


def revert_bucket(selector: str) -> str | None:
    """Pure: resolve a 4-byte revert selector to its bucket label.

    Returns None for unknown selectors.
    """
    if not isinstance(selector, str):
        return None
    sel = selector.lower().removeprefix("0x")
    if len(sel) != 8:
        return None
    entry = KNOWN_ERROR_SELECTORS.get(sel)
    if entry is None:
        return None
    return entry[1]


def is_known_acceptable(selector: str) -> bool:
    """Pure: True iff this selector resolves to an acceptability-bucket error.

    "Acceptable" = the revert means the contract validated our encoding
    (ABI parsed, market valid, function selector matched) and then complained
    about a balance/funding issue we EXPECT from a dummy account. Returns
    False for unknown selectors and for non-acceptable buckets.

    NOTE: This selector-only check does NOT cover the standard `Error(string)`
    selector `0x08c379a0` (used by ERC20 OZ revert strings). For full
    classification of an arbitrary revert payload — including string-content
    inspection — use `classify_revert_payload(raw_hex)`.
    """
    bucket = revert_bucket(selector)
    if bucket is None:
        return False
    return bucket in KNOWN_ACCEPTABLE_BUCKETS


def _decode_error_string_message(raw_hex: str) -> str | None:
    """Pure: decode the `Error(string)` payload's message arg.

    Standard Solidity `Error(string)` layout:
        4 bytes  selector (0x08c379a0)
        32 bytes offset to string (always 0x20)
        32 bytes string length
        N bytes  string (zero-padded to a 32-byte multiple)

    Returns the decoded message, or None if the payload doesn't fit the
    pattern. Uses eth_abi for the ABI-decode step.
    """
    if not isinstance(raw_hex, str):
        return None
    body = raw_hex.lower().removeprefix("0x")
    if not body.startswith(ERROR_STRING_SELECTOR):
        return None
    try:
        # Strip selector, then abi-decode (string,)
        from eth_abi import decode  # type: ignore[attr-defined]
        (msg,) = decode(["string"], bytes.fromhex(body[8:]))
        if isinstance(msg, str):
            return msg
        return None
    except Exception:  # noqa: BLE001
        return None


def classify_revert_payload(raw_hex: str | None) -> tuple[str | None, str | None, bool]:
    """Pure: classify a raw revert hex payload into (reason_name, bucket, acceptable).

    Handles three cases:
      1. Standard `Error(string)` selector (0x08c379a0): decode the string,
         check against `ERC20_ACCEPTABLE_SUBSTRINGS`. Returns
         (`"Error(string): <msg>"`, BUCKET_ERC20_ALLOWANCE, acceptable_bool).
      2. Custom GMX error (4-byte selector in `KNOWN_ERROR_SELECTORS`):
         returns (name, bucket, acceptable).
      3. Unknown selector: returns (None, None, False).

    Used by `gmx_order_encoder._classify_response_body` to do the FULL
    classification with string-content awareness.
    """
    if not isinstance(raw_hex, str):
        return (None, None, False)
    body = raw_hex.lower().removeprefix("0x")
    if len(body) < 8:
        return (None, None, False)
    selector = body[:8]
    # Path 1: Error(string)
    if selector == ERROR_STRING_SELECTOR:
        msg = _decode_error_string_message(raw_hex)
        if msg is None:
            return ("Error(string): <undecoded>", BUCKET_ERC20_ALLOWANCE, False)
        msg_lower = msg.lower()
        acceptable = any(s in msg_lower for s in ERC20_ACCEPTABLE_SUBSTRINGS)
        return (f"Error(string): {msg}", BUCKET_ERC20_ALLOWANCE, acceptable)
    # Path 2: known GMX custom error
    entry = KNOWN_ERROR_SELECTORS.get(selector)
    if entry is not None:
        name, bucket = entry
        acceptable = bucket in KNOWN_ACCEPTABLE_BUCKETS
        return (name, bucket, acceptable)
    # Path 3: unknown
    return (None, None, False)


__all__ = [
    "BUCKET_ERC20_ALLOWANCE",
    "BUCKET_EXECUTION_FEE_GAS",
    "BUCKET_MARKET_VALIDATION",
    "BUCKET_MISC_CANCELLATION",
    "BUCKET_POOL_RESERVES",
    "BUCKET_POSITION_STATE",
    "BUCKET_SLIPPAGE_PRICING",
    "ERC20_ACCEPTABLE_SUBSTRINGS",
    "ERROR_STRING_SELECTOR",
    "KNOWN_ACCEPTABLE_BUCKETS",
    "KNOWN_ERROR_SELECTORS",
    "classify_revert_payload",
    "is_known_acceptable",
    "resolve_revert",
    "revert_bucket",
]
