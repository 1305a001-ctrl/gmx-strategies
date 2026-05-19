"""Tests for the GMX V2 order encoder + eth_call simulation (G5.1).

Mocks `httpx.AsyncClient.post` for the RPC; asserts:
  - `OrderIntent` → `_encode_create_order_params` produces well-formed bytes
    starting with the verified `0xf59c48eb` selector
  - `_encode_multicall` wraps in 3 sub-calls (sendWnt, sendTokens, createOrder)
    starting with `0xac9650d8`
  - `build_simulation_payload` populates from/to/value/data correctly
  - `simulate_order` correctly classifies known-acceptable reverts
  - `simulate_order` correctly flags critical-fail reverts as not-acceptable
  - `simulate_order` returns ok=True on 0x success response
  - acceptablePrice band math is direction-correct per audit Q5 matrix
  - revert resolution map covers the 46+ curated selectors
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from eth_abi import decode  # type: ignore[attr-defined]

from gmx_strategies import gmx_errors, gmx_order_encoder
from gmx_strategies.gmx_order_encoder import (
    DECREASE_POSITION_SWAP_TYPE_NO_SWAP,
    ORDER_TYPE_MARKET_DECREASE,
    ORDER_TYPE_MARKET_INCREASE,
    SELECTOR_CREATE_ORDER,
    SELECTOR_MULTICALL,
    OrderIntent,
    SimulationResult,
    _classify_response_body,
    _compute_acceptable_price,
    _encode_create_order_params,
    _encode_multicall,
    build_simulation_payload,
    simulate_order,
)
from gmx_strategies.settings import settings

# Canonical addresses from settings + markets (verified 2026-05-20).
_DUMMY_ACCOUNT = "0x0000000000000000000000000000000000000001"
_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


def _make_btc_long_intent(**overrides: Any) -> OrderIntent:
    """Build a baseline OrderIntent for BTC market, long, $10 USDC collateral."""
    base = {
        "market": "btc",
        "is_long": True,
        "is_increase": True,
        "collateral_token": _USDC,
        "initial_collateral_delta_amount": 10_000_000,  # $10 USDC (6 dec)
        "size_delta_usd": 10 * 10**30,                   # $10 (1e30-scaled)
        "current_price_1e30": 65_000 * 10**22,           # $65k BTC, GMX-scaled for 8-dec
        "acceptable_price_band_bps": 150,
        "execution_fee_wei": 5 * 10**14,                 # 0.0005 ETH
        "account": _DUMMY_ACCOUNT,
    }
    base.update(overrides)
    return OrderIntent(**base)  # type: ignore[arg-type]


def _make_fake_response(*, status_code: int = 200, body: dict[str, Any]) -> Any:
    """Build a stand-in httpx.Response for the mocked POST."""
    class _Resp:
        def __init__(self, sc: int, body: dict[str, Any]) -> None:
            self.status_code = sc
            self._body = body

        def json(self) -> dict[str, Any]:
            return self._body

    return _Resp(status_code, body)


# ──────────────────────────────────────────────────────────────────────────
# Encoding tests
# ──────────────────────────────────────────────────────────────────────────


def test_create_order_calldata_starts_with_verified_selector() -> None:
    """First 4 bytes of createOrder calldata = 0xf59c48eb (verified vs bytecode)."""
    intent = _make_btc_long_intent()
    data = _encode_create_order_params(intent)
    assert data[:4].hex() == SELECTOR_CREATE_ORDER[2:]
    assert data[:4].hex() == "f59c48eb"


def test_create_order_calldata_roundtrip_decodes() -> None:
    """The encoded struct decodes back to the right field values."""
    intent = _make_btc_long_intent(
        size_delta_usd=42 * 10**30,
        is_long=True,
        is_increase=True,
    )
    data = _encode_create_order_params(intent)
    body = data[4:]  # strip selector
    type_spec = (
        "("
        "(address,address,address,address,address,address,address[]),"
        "(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256),"
        "uint8,uint8,bool,bool,bool,bytes32,bytes32[]"
        ")"
    )
    (decoded,) = decode([type_spec], body)
    addresses, numbers, order_type, _dec_swap, is_long, _unwrap, _autocancel, _ref, _data = decoded
    receiver = addresses[0]
    market = addresses[4]
    coll_token = addresses[5]
    swap_path = addresses[6]
    size_delta_usd = numbers[0]
    valid_from = numbers[7]
    assert receiver.lower() == _DUMMY_ACCOUNT.lower()
    assert market.lower() == "0x47c031236e19d024b42f8ae6780e44a573170703"
    assert coll_token.lower() == _USDC.lower()
    # eth_abi.decode produces tuples for dynamic-length arrays; assert empty.
    assert list(swap_path) == []
    assert size_delta_usd == 42 * 10**30
    assert valid_from == 0  # audit C3
    assert order_type == ORDER_TYPE_MARKET_INCREASE
    assert is_long is True


def test_create_order_marketdecrease_short_encodes_correctly() -> None:
    """Decrease + short side: orderType=4, isLong=false."""
    intent = _make_btc_long_intent(
        is_long=False,
        is_increase=False,
        initial_collateral_delta_amount=10_000_000,
    )
    data = _encode_create_order_params(intent)
    body = data[4:]
    type_spec = (
        "("
        "(address,address,address,address,address,address,address[]),"
        "(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256),"
        "uint8,uint8,bool,bool,bool,bytes32,bytes32[]"
        ")"
    )
    (decoded,) = decode([type_spec], body)
    _addresses, _numbers, order_type, _dec_swap, is_long, *_ = decoded
    assert order_type == ORDER_TYPE_MARKET_DECREASE
    assert is_long is False


def test_multicall_starts_with_multicall_selector() -> None:
    """First 4 bytes of full multicall payload = 0xac9650d8."""
    intent = _make_btc_long_intent()
    data = _encode_multicall(intent)
    assert data[:4].hex() == "ac9650d8"


def test_multicall_contains_three_sub_calls() -> None:
    """Multicall encodes bytes[] of exactly 3 items: sendWnt, sendTokens, createOrder."""
    intent = _make_btc_long_intent()
    data = _encode_multicall(intent)
    # Strip multicall selector, decode bytes[]
    (sub_calls,) = decode(["bytes[]"], data[4:])
    assert len(sub_calls) == 3
    # Each sub-call has its own 4-byte selector at the start.
    assert sub_calls[0][:4].hex() == "7d39aaf1"  # sendWnt
    assert sub_calls[1][:4].hex() == "e6d66ac8"  # sendTokens
    assert sub_calls[2][:4].hex() == "f59c48eb"  # createOrder


def test_multicall_send_wnt_targets_order_vault() -> None:
    """sendWnt receiver = OrderVault, amount = execution_fee_wei."""
    intent = _make_btc_long_intent(execution_fee_wei=12345)
    data = _encode_multicall(intent)
    (sub_calls,) = decode(["bytes[]"], data[4:])
    send_wnt_body = sub_calls[0][4:]
    receiver, amount = decode(["address", "uint256"], send_wnt_body)
    assert receiver.lower() == settings.gmx_order_vault_address_arbitrum.lower()
    assert amount == 12345


def test_multicall_send_tokens_targets_order_vault_with_collateral() -> None:
    """sendTokens token = collateral, receiver = OrderVault, amount = collateral_delta."""
    intent = _make_btc_long_intent(
        collateral_token=_USDC,
        initial_collateral_delta_amount=99_000_000,
    )
    data = _encode_multicall(intent)
    (sub_calls,) = decode(["bytes[]"], data[4:])
    send_tokens_body = sub_calls[1][4:]
    token, receiver, amount = decode(
        ["address", "address", "uint256"], send_tokens_body,
    )
    assert token.lower() == _USDC.lower()
    assert receiver.lower() == settings.gmx_order_vault_address_arbitrum.lower()
    assert amount == 99_000_000


# ──────────────────────────────────────────────────────────────────────────
# build_simulation_payload tests
# ──────────────────────────────────────────────────────────────────────────


def test_build_simulation_payload_has_all_fields() -> None:
    """eth_call params dict has from/to/value/data with correct values."""
    intent = _make_btc_long_intent(execution_fee_wei=10**15)
    params = build_simulation_payload(intent)
    assert set(params.keys()) == {"from", "to", "value", "data"}
    assert params["from"].lower() == _DUMMY_ACCOUNT.lower()
    assert params["to"].lower() == settings.gmx_exchange_router_address_arbitrum.lower()
    assert params["value"] == hex(10**15)
    assert params["data"].startswith("0x")
    # Data is the multicall calldata
    assert params["data"].startswith("0xac9650d8")


# ──────────────────────────────────────────────────────────────────────────
# acceptablePrice band math — per audit Q5 matrix
# ──────────────────────────────────────────────────────────────────────────


def test_acceptable_price_long_increase_adds_band() -> None:
    """Long-open: acceptable = current * (1 + band/10000) — ceiling."""
    current = 1000 * 10**24
    band = 150  # 1.5%
    result = _compute_acceptable_price(
        current_price_1e30=current, band_bps=band, is_long=True, is_increase=True,
    )
    expected = current + current * 150 // 10_000
    assert result == expected
    assert result > current


def test_acceptable_price_short_increase_subtracts_band() -> None:
    """Short-open: acceptable = current * (1 - band/10000) — floor."""
    current = 1000 * 10**24
    band = 150
    result = _compute_acceptable_price(
        current_price_1e30=current, band_bps=band, is_long=False, is_increase=True,
    )
    expected = current - current * 150 // 10_000
    assert result == expected
    assert result < current


def test_acceptable_price_long_decrease_subtracts_band() -> None:
    """Closing a long = selling = floor: acceptable < current."""
    current = 1000 * 10**24
    band = 350  # 3.5% alt band
    result = _compute_acceptable_price(
        current_price_1e30=current, band_bps=band, is_long=True, is_increase=False,
    )
    expected = current - current * 350 // 10_000
    assert result == expected
    assert result < current


def test_acceptable_price_short_decrease_adds_band() -> None:
    """Closing a short = buying = ceiling: acceptable > current."""
    current = 1000 * 10**24
    band = 350
    result = _compute_acceptable_price(
        current_price_1e30=current, band_bps=band, is_long=False, is_increase=False,
    )
    expected = current + current * 350 // 10_000
    assert result == expected
    assert result > current


def test_acceptable_price_zero_band_is_identity() -> None:
    """band_bps=0 → acceptable == current (no slippage tolerance)."""
    current = 1000 * 10**24
    for is_long in (True, False):
        for is_increase in (True, False):
            result = _compute_acceptable_price(
                current_price_1e30=current, band_bps=0,
                is_long=is_long, is_increase=is_increase,
            )
            assert result == current


def test_acceptable_price_negative_band_raises() -> None:
    """Negative band is a programming error — raise."""
    with pytest.raises(ValueError, match="band_bps"):
        _compute_acceptable_price(
            current_price_1e30=1, band_bps=-1, is_long=True, is_increase=True,
        )


# ──────────────────────────────────────────────────────────────────────────
# OrderIntent validation
# ──────────────────────────────────────────────────────────────────────────


def test_unknown_market_raises_on_encode() -> None:
    """Unknown market alias → ValueError at encode time."""
    intent = _make_btc_long_intent(market="bogus_alias")
    with pytest.raises(ValueError, match="unknown market"):
        _encode_create_order_params(intent)


def test_zero_execution_fee_raises() -> None:
    """audit C2 — zero exec fee would revert InsufficientWntAmountForExecutionFee."""
    intent = _make_btc_long_intent(execution_fee_wei=0)
    with pytest.raises(ValueError, match="execution_fee_wei"):
        _encode_create_order_params(intent)


def test_bad_account_address_raises() -> None:
    """Malformed account address → ValueError before any encoding."""
    intent = _make_btc_long_intent(account="not_an_address")
    with pytest.raises(ValueError, match="account"):
        _encode_create_order_params(intent)


# ──────────────────────────────────────────────────────────────────────────
# Simulation classification — async tests
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_simulate_order_known_acceptable_revert() -> None:
    """InsufficientWntAmountForExecutionFee selector → ok=False + acceptable=True."""
    intent = _make_btc_long_intent()
    selector = "3a78cd7e"  # InsufficientWntAmountForExecutionFee
    # Pad the error data: selector + 64 bytes of zeros (2 uint256 args)
    error_data = "0x" + selector + "0" * 128
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "error": {"code": 3, "message": "execution reverted", "data": error_data},
    }
    resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=resp)):
        result = await simulate_order(intent, rpc_url="https://arb1.arbitrum.io/rpc")
    assert result.ok is False
    assert result.revert_selector == "3a78cd7e"
    assert result.revert_known_acceptable is True
    assert result.revert_reason_name == "InsufficientWntAmountForExecutionFee"


@pytest.mark.asyncio
async def test_simulate_order_critical_fail_revert() -> None:
    """UnsupportedOrderType → ok=False + acceptable=False (encoding bug indicator)."""
    intent = _make_btc_long_intent()
    selector = "3784f834"  # UnsupportedOrderType — would mean orderType is wrong
    error_data = "0x" + selector + "0" * 64
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "error": {"code": 3, "message": "reverted", "data": error_data},
    }
    resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=resp)):
        result = await simulate_order(intent, rpc_url="https://arb1.arbitrum.io/rpc")
    assert result.ok is False
    assert result.revert_selector == "3784f834"
    assert result.revert_known_acceptable is False
    assert result.revert_reason_name == "UnsupportedOrderType"


@pytest.mark.asyncio
async def test_simulate_order_unknown_selector_not_acceptable() -> None:
    """An unknown revert selector → ok=False + acceptable=False + name=None."""
    intent = _make_btc_long_intent()
    error_data = "0xdeadbeef" + "0" * 64
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "error": {"code": 3, "message": "reverted", "data": error_data},
    }
    resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=resp)):
        result = await simulate_order(intent, rpc_url="https://arb1.arbitrum.io/rpc")
    assert result.ok is False
    assert result.revert_selector == "deadbeef"
    assert result.revert_known_acceptable is False
    assert result.revert_reason_name is None


@pytest.mark.asyncio
async def test_simulate_order_success_returns_ok() -> None:
    """JSON-RPC success (result key) → ok=True."""
    intent = _make_btc_long_intent()
    body = {"jsonrpc": "2.0", "id": 1, "result": "0x"}
    resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=resp)):
        result = await simulate_order(intent, rpc_url="https://arb1.arbitrum.io/rpc")
    assert result.ok is True
    assert result.revert_selector is None
    assert result.revert_reason_name is None


@pytest.mark.asyncio
async def test_simulate_order_transport_failure_returns_failure() -> None:
    """httpx error → returns SimulationResult(ok=False, all None) without raising."""
    import httpx
    intent = _make_btc_long_intent()
    with patch(
        "httpx.AsyncClient.post",
        new=AsyncMock(side_effect=httpx.ConnectError("nope")),
    ):
        result = await simulate_order(intent, rpc_url="https://arb1.arbitrum.io/rpc")
    assert result.ok is False
    assert result.revert_selector is None


@pytest.mark.asyncio
async def test_simulate_order_nested_error_data() -> None:
    """Some RPC clients nest the data in error.data.data — handle gracefully."""
    intent = _make_btc_long_intent()
    selector = "74cc815b"  # InsufficientCollateralAmount (acceptable bucket)
    error_data = "0x" + selector + "0" * 96
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "error": {
            "code": 3,
            "message": "execution reverted",
            "data": {"data": error_data},
        },
    }
    resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=resp)):
        result = await simulate_order(intent, rpc_url="https://arb1.arbitrum.io/rpc")
    assert result.revert_selector == "74cc815b"
    assert result.revert_known_acceptable is True


# ──────────────────────────────────────────────────────────────────────────
# Classify-response unit tests (no async / no mock needed)
# ──────────────────────────────────────────────────────────────────────────


def test_classify_response_handles_revert_in_message() -> None:
    """Some RPCs put the revert hex in `error.message` rather than `error.data`."""
    selector = "e09ad0e9"  # OrderNotFulfillableAtAcceptablePrice
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "error": {
            "code": 3,
            "message": f"execution reverted: 0x{selector}0000000000000000",
        },
    }
    result = _classify_response_body(body)
    assert result.revert_selector == "e09ad0e9"
    assert result.revert_reason_name == "OrderNotFulfillableAtAcceptablePrice"
    # OrderNotFulfillableAtAcceptablePrice is in SLIPPAGE_PRICING bucket
    # which is NOT in KNOWN_ACCEPTABLE_BUCKETS (it would mean current_price
    # is wrong, which we DO control)
    assert result.revert_known_acceptable is False


def test_classify_response_malformed_error() -> None:
    """error key is not a dict → graceful all-None result."""
    body: dict[str, Any] = {"jsonrpc": "2.0", "id": 1, "error": "string error"}
    result = _classify_response_body(body)
    assert result.ok is False
    assert result.revert_selector is None


# ──────────────────────────────────────────────────────────────────────────
# gmx_errors module tests
# ──────────────────────────────────────────────────────────────────────────


def test_known_error_selectors_classifier_size() -> None:
    """Classifier covers the audit's curated entries across 6 buckets.

    Per audit memo "Curated Errors.sol selectors for the G5 cancellation
    classifier" (drawn from main @ 2026-05-20): 11 slippage_pricing +
    10 market_validation + 7 position_state + 7 execution_fee_gas +
    7 pool_reserves + 8 misc_cancellation = 50. The audit body's prose
    mentions "46 curated" but the sub-tables sum to 50; the tables are
    authoritative.
    """
    assert len(gmx_errors.KNOWN_ERROR_SELECTORS) == 50
    # Per-bucket counts as a stability backstop — if anyone adds new
    # selectors, this test forces them to update the counts deliberately.
    counts: dict[str, int] = {}
    for _sel, (_name, bucket) in gmx_errors.KNOWN_ERROR_SELECTORS.items():
        counts[bucket] = counts.get(bucket, 0) + 1
    assert counts[gmx_errors.BUCKET_SLIPPAGE_PRICING] == 11
    assert counts[gmx_errors.BUCKET_MARKET_VALIDATION] == 10
    assert counts[gmx_errors.BUCKET_POSITION_STATE] == 7
    assert counts[gmx_errors.BUCKET_EXECUTION_FEE_GAS] == 7
    assert counts[gmx_errors.BUCKET_POOL_RESERVES] == 7
    assert counts[gmx_errors.BUCKET_MISC_CANCELLATION] == 8


def test_known_error_selectors_all_unique() -> None:
    """No duplicate selectors in the classifier."""
    keys = list(gmx_errors.KNOWN_ERROR_SELECTORS.keys())
    assert len(keys) == len(set(keys))


def test_known_error_selectors_all_8_char_lowercase() -> None:
    """Every selector is 8-char hex, lowercase, no 0x prefix."""
    for sel in gmx_errors.KNOWN_ERROR_SELECTORS:
        assert len(sel) == 8, sel
        assert sel == sel.lower()
        assert not sel.startswith("0x")
        int(sel, 16)  # raises if non-hex


def test_resolve_revert_known_selector() -> None:
    """Resolution works with + without 0x prefix, case-insensitive."""
    assert gmx_errors.resolve_revert("e09ad0e9") == "OrderNotFulfillableAtAcceptablePrice"
    assert gmx_errors.resolve_revert("0xE09AD0E9") == "OrderNotFulfillableAtAcceptablePrice"
    assert gmx_errors.resolve_revert("0xe09ad0e9") == "OrderNotFulfillableAtAcceptablePrice"


def test_resolve_revert_unknown_returns_none() -> None:
    """Unknown selectors return None — caller must treat as critical-fail."""
    assert gmx_errors.resolve_revert("deadbeef") is None
    assert gmx_errors.resolve_revert("") is None
    assert gmx_errors.resolve_revert("0x") is None
    assert gmx_errors.resolve_revert("not_a_selector") is None


def test_is_known_acceptable_buckets() -> None:
    """Only execution_fee_gas + position_state buckets are 'acceptable'."""
    # InsufficientWntAmountForExecutionFee — EXECUTION_FEE_GAS (acceptable)
    assert gmx_errors.is_known_acceptable("3a78cd7e") is True
    # InsufficientCollateralAmount — POSITION_STATE (acceptable)
    assert gmx_errors.is_known_acceptable("74cc815b") is True
    # OrderNotFulfillableAtAcceptablePrice — SLIPPAGE_PRICING (NOT acceptable
    # — it means our oracle price input is wrong, which is on us to fix)
    assert gmx_errors.is_known_acceptable("e09ad0e9") is False
    # UnsupportedOrderType — MARKET_VALIDATION (NOT acceptable — would mean
    # we sent the wrong orderType enum value)
    assert gmx_errors.is_known_acceptable("3784f834") is False
    # Unknown selector — not acceptable.
    assert gmx_errors.is_known_acceptable("deadbeef") is False


def test_critical_buckets_for_market_disabled_path() -> None:
    """DisabledMarket / DisabledFeature land in market_validation (not acceptable)."""
    # DisabledMarket
    assert gmx_errors.revert_bucket("09f8c937") == gmx_errors.BUCKET_MARKET_VALIDATION
    # DisabledFeature
    assert gmx_errors.revert_bucket("dd70e0c9") == gmx_errors.BUCKET_MARKET_VALIDATION
    # Neither is in the acceptable set
    assert gmx_errors.is_known_acceptable("09f8c937") is False
    assert gmx_errors.is_known_acceptable("dd70e0c9") is False


# ──────────────────────────────────────────────────────────────────────────
# Standard Error(string) classifier — exercises the smoke-test path
# (dummy account hitting ERC20 transferFrom with no approval).
# ──────────────────────────────────────────────────────────────────────────


def _build_error_string_payload(msg: str) -> str:
    """Encode a standard `Error(string)` revert payload for testing."""
    from eth_abi import encode  # type: ignore[attr-defined]
    body = encode(["string"], [msg])
    return "0x" + gmx_errors.ERROR_STRING_SELECTOR + body.hex()


def test_classify_revert_payload_erc20_allowance() -> None:
    """OZ-standard ERC20 'transfer amount exceeds allowance' → acceptable."""
    payload = _build_error_string_payload("ERC20: transfer amount exceeds allowance")
    name, bucket, acceptable = gmx_errors.classify_revert_payload(payload)
    assert name is not None
    assert "transfer amount exceeds allowance" in name
    assert bucket == gmx_errors.BUCKET_ERC20_ALLOWANCE
    assert acceptable is True


def test_classify_revert_payload_erc20_balance() -> None:
    """OZ-standard ERC20 'transfer amount exceeds balance' → acceptable."""
    payload = _build_error_string_payload("ERC20: transfer amount exceeds balance")
    name, bucket, acceptable = gmx_errors.classify_revert_payload(payload)
    assert acceptable is True
    assert bucket == gmx_errors.BUCKET_ERC20_ALLOWANCE


def test_classify_revert_payload_unknown_error_string() -> None:
    """A non-matching Error(string) (e.g. SafeMath underflow) → not acceptable."""
    payload = _build_error_string_payload("Pausable: paused")
    name, bucket, acceptable = gmx_errors.classify_revert_payload(payload)
    # Still classified as ERC20_ALLOWANCE bucket (it's an Error(string)), but
    # the substring check fails so acceptable=False.
    assert name is not None
    assert "Pausable: paused" in name
    assert bucket == gmx_errors.BUCKET_ERC20_ALLOWANCE
    assert acceptable is False


def test_classify_revert_payload_custom_gmx_error() -> None:
    """Custom GMX error selector → routed through KNOWN_ERROR_SELECTORS."""
    # InsufficientCollateralAmount (74cc815b, POSITION_STATE, acceptable)
    payload = "0x74cc815b" + "0" * 128
    name, bucket, acceptable = gmx_errors.classify_revert_payload(payload)
    assert name == "InsufficientCollateralAmount"
    assert bucket == gmx_errors.BUCKET_POSITION_STATE
    assert acceptable is True


def test_classify_revert_payload_unknown_selector() -> None:
    """Unknown selector → (None, None, False)."""
    name, bucket, acceptable = gmx_errors.classify_revert_payload("0xdeadbeef")
    assert name is None
    assert bucket is None
    assert acceptable is False


def test_classify_revert_payload_none_input() -> None:
    """None / empty / short input → (None, None, False)."""
    assert gmx_errors.classify_revert_payload(None) == (None, None, False)
    assert gmx_errors.classify_revert_payload("") == (None, None, False)
    assert gmx_errors.classify_revert_payload("0x12") == (None, None, False)


@pytest.mark.asyncio
async def test_simulate_order_erc20_allowance_revert_acceptable() -> None:
    """ERC20-allowance revert from sendTokens → ok=False + acceptable=True.

    This is the exact path the mainnet smoke-test dummy account hits.
    """
    intent = _make_btc_long_intent()
    # Build a real Error(string) payload for the OZ allowance message.
    from eth_abi import encode  # type: ignore[attr-defined]
    msg_body = encode(["string"], ["ERC20: transfer amount exceeds allowance"]).hex()
    error_data = "0x08c379a0" + msg_body
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "error": {
            "code": 3,
            "message": "execution reverted: ERC20: transfer amount exceeds allowance",
            "data": error_data,
        },
    }
    resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=resp)):
        result = await simulate_order(intent, rpc_url="https://arb1.arbitrum.io/rpc")
    assert result.ok is False
    assert result.revert_selector == "08c379a0"
    assert result.revert_known_acceptable is True
    assert result.revert_reason_name is not None
    assert "transfer amount exceeds allowance" in result.revert_reason_name


# ──────────────────────────────────────────────────────────────────────────
# Settings sanity — addresses match audit
# ──────────────────────────────────────────────────────────────────────────


def test_settings_has_verified_addresses() -> None:
    """The 3 new addresses match the audit's verified values."""
    assert (
        settings.gmx_exchange_router_address_arbitrum
        == "0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41"
    )
    assert (
        settings.gmx_router_proxy_address_arbitrum
        == "0x7452c558d45f8afC8c83dAe62C3f8A5BE19c71f6"
    )
    assert (
        settings.gmx_order_vault_address_arbitrum
        == "0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5"
    )
    # Approval target MUST differ from execution target (audit C1).
    assert (
        settings.gmx_router_proxy_address_arbitrum.lower()
        != settings.gmx_exchange_router_address_arbitrum.lower()
    )
    assert settings.gmx_increase_order_gas_limit == 3_000_000
    assert settings.gmx_decrease_order_gas_limit == 3_000_000


def test_settings_has_per_market_bands() -> None:
    """Majors band tighter than alts band per audit Q5+H2."""
    assert settings.gmx_default_acceptable_price_band_majors_bps == 150
    assert settings.gmx_default_acceptable_price_band_alts_bps == 350
    assert (
        settings.gmx_default_acceptable_price_band_majors_bps
        < settings.gmx_default_acceptable_price_band_alts_bps
    )


# ──────────────────────────────────────────────────────────────────────────
# Sanity: SimulationResult is a frozen dataclass with the contract surface
# ──────────────────────────────────────────────────────────────────────────


def test_simulation_result_is_frozen() -> None:
    """SimulationResult must be immutable (no accidental mutation by callers)."""
    sr = SimulationResult(
        ok=True, revert_selector=None, revert_known_acceptable=False,
        revert_reason_name=None, raw_response="0x",
    )
    with pytest.raises((AttributeError, Exception)):
        sr.ok = False  # type: ignore[misc]


def test_order_intent_is_frozen() -> None:
    """OrderIntent must be immutable."""
    intent = _make_btc_long_intent()
    with pytest.raises((AttributeError, Exception)):
        intent.market = "eth"  # type: ignore[misc]


# Avoid lint warnings for unused imports we still want to assert at import time
_ = DECREASE_POSITION_SWAP_TYPE_NO_SWAP, SELECTOR_MULTICALL, gmx_order_encoder
