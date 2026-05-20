"""Tests for the GMX V2 on-chain position reader (G5.3).

Mocks `httpx.AsyncClient.post` for the eth_call JSON-RPC. Asserts:
  - bulk read decodes Position.Props[] correctly (sizes, addresses, isLong)
  - zero-size positions are filtered out
  - market alias reverse-lookup works
  - returns empty list on RPC error / malformed response / revert
  - `fetch_position` zero-struct → None
  - `reconcile_intent` covers all 5 cases from the decision matrix
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from eth_abi import encode  # type: ignore[attr-defined]

from gmx_strategies import gmx_position_reader
from gmx_strategies.gmx_order_encoder import OrderIntent
from gmx_strategies.gmx_position_reader import (
    SELECTOR_GET_ACCOUNT_POSITIONS,
    SELECTOR_GET_POSITION,
    Position,
    ReconciliationResult,
    _decode_position_props,
    _position_key,
    fetch_account_positions,
    fetch_position,
    reconcile_intent,
)
from gmx_strategies.markets import ARBITRUM_MARKETS

_DUMMY_ACCOUNT = "0x0000000000000000000000000000000000000001"
_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
_WSOL = "0x2bcc6d6cdbbdc0a4071e48bb3b969b06b3330c07"
_ETH_ZERO = "0x0000000000000000000000000000000000000000"

_SOL_MARKET = ARBITRUM_MARKETS["sol"].market_address
_BTC_MARKET = ARBITRUM_MARKETS["btc"].market_address


def _make_position_tuple(
    *,
    account: str = "0xdEadBeefdEadBeefdEadBeefdEadBeefdEadBeef",
    market: str = _SOL_MARKET,
    collateral_token: str = _USDC,
    size_in_usd: int = 10 * 10**30,
    size_in_tokens: int = 0,
    collateral_amount: int = 10_000_000,
    pending_impact_amount: int = 0,
    borrowing_factor: int = 0,
    funding_fee_amount_per_size: int = 0,
    long_claim: int = 0,
    short_claim: int = 0,
    increased_at_time: int = 1_700_000_000,
    decreased_at_time: int = 0,
    is_long: bool = True,
) -> tuple:
    """Build a Position.Props tuple matching the v2.2 ABI shape."""
    addresses = (account, market, collateral_token)
    numbers = (
        size_in_usd,
        size_in_tokens,
        collateral_amount,
        pending_impact_amount,
        borrowing_factor,
        funding_fee_amount_per_size,
        long_claim,
        short_claim,
        increased_at_time,
        decreased_at_time,
    )
    flags = (is_long,)
    return (addresses, numbers, flags)


def _encode_account_positions_response(positions: list[tuple]) -> str:
    """Pack a list of Position.Props tuples into the hex response shape."""
    body = encode(
        [f"{gmx_position_reader._POSITION_PROPS_TYPE}[]"],
        [positions],
    )
    return "0x" + body.hex()


def _encode_single_position_response(position: tuple) -> str:
    """Pack one Position.Props tuple into the hex response shape."""
    body = encode(
        [gmx_position_reader._POSITION_PROPS_TYPE],
        [position],
    )
    return "0x" + body.hex()


def _make_fake_response(*, status_code: int = 200, body: dict[str, Any]) -> Any:
    """Stand-in for httpx.Response."""
    class _Resp:
        def __init__(self, sc: int, body: dict[str, Any]) -> None:
            self.status_code = sc
            self._body = body

        def json(self) -> dict[str, Any]:
            return self._body

    return _Resp(status_code, body)


def _make_intent(**overrides: Any) -> OrderIntent:
    """Build an OrderIntent for SOL long, $10 USDC collateral."""
    base = {
        "market": "sol",
        "is_long": True,
        "is_increase": True,
        "collateral_token": _USDC,
        "initial_collateral_delta_amount": 10_000_000,
        "size_delta_usd": 10 * 10**30,
        "current_price_1e30": 150 * 10**22,
        "acceptable_price_band_bps": 350,
        "execution_fee_wei": 5 * 10**14,
        "account": _DUMMY_ACCOUNT,
    }
    base.update(overrides)
    return OrderIntent(**base)  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────────
# Selector + key derivation
# ──────────────────────────────────────────────────────────────────────────


def test_get_account_positions_selector_matches() -> None:
    """getAccountPositions selector is the first 4 bytes of its keccak."""
    # Derived from keccak256("getAccountPositions(address,address,uint256,uint256)")
    assert SELECTOR_GET_ACCOUNT_POSITIONS == "0x77cfb162"


def test_get_position_selector_matches() -> None:
    """getPosition selector is the first 4 bytes of its keccak."""
    assert SELECTOR_GET_POSITION == "0x0fa8f516"


def test_position_key_derivation_is_deterministic() -> None:
    """positionKey(a, m, c, isLong) is deterministic + 32 bytes."""
    key1 = _position_key(_DUMMY_ACCOUNT, _SOL_MARKET, _USDC, True)
    key2 = _position_key(_DUMMY_ACCOUNT, _SOL_MARKET, _USDC, True)
    assert key1 == key2
    assert len(key1) == 32
    # Flipping isLong changes the key
    key_short = _position_key(_DUMMY_ACCOUNT, _SOL_MARKET, _USDC, False)
    assert key_short != key1


# ──────────────────────────────────────────────────────────────────────────
# Decode shape
# ──────────────────────────────────────────────────────────────────────────


def test_decode_position_props_happy_path() -> None:
    """A valid Position.Props tuple decodes into a Position dataclass."""
    raw = _make_position_tuple(
        size_in_usd=42 * 10**30,
        is_long=False,
        increased_at_time=1_700_000_000,
        decreased_at_time=1_700_001_000,
    )
    pos = _decode_position_props(raw)
    assert pos is not None
    assert pos.size_in_usd == 42 * 10**30
    assert pos.size_in_usd_float == pytest.approx(42.0)
    assert pos.is_long is False
    assert pos.market_alias == "sol"
    assert pos.market_address == _SOL_MARKET
    assert pos.collateral_token.lower() == _USDC.lower()
    assert pos.increased_at_time == 1_700_000_000
    assert pos.decreased_at_time == 1_700_001_000


def test_decode_position_props_zero_account_returns_none() -> None:
    """GMX returns a zero-filled struct for non-existent positionKeys."""
    raw = _make_position_tuple(
        account=_ETH_ZERO, market=_ETH_ZERO, collateral_token=_ETH_ZERO,
        size_in_usd=0,
    )
    assert _decode_position_props(raw) is None


def test_decode_position_props_unknown_market_alias_is_none() -> None:
    """A market address not in ARBITRUM_MARKETS → market_alias=None, not crash."""
    raw = _make_position_tuple(
        market="0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        size_in_usd=5 * 10**30,
    )
    pos = _decode_position_props(raw)
    assert pos is not None
    assert pos.market_alias is None
    assert pos.market_address == "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"


# ──────────────────────────────────────────────────────────────────────────
# fetch_account_positions — async
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fetch_account_positions_decodes_two_positions() -> None:
    """A multi-position response decodes correctly."""
    raw1 = _make_position_tuple(
        market=_SOL_MARKET, is_long=True, size_in_usd=10 * 10**30,
    )
    raw2 = _make_position_tuple(
        market=_BTC_MARKET, is_long=False, size_in_usd=20 * 10**30,
    )
    result_hex = _encode_account_positions_response([raw1, raw2])
    body = {"jsonrpc": "2.0", "id": 1, "result": result_hex}
    resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=resp)):
        positions = await fetch_account_positions(
            _DUMMY_ACCOUNT, rpc_url="https://arb1.arbitrum.io/rpc",
        )
    assert len(positions) == 2
    sol_pos = next(p for p in positions if p.market_alias == "sol")
    btc_pos = next(p for p in positions if p.market_alias == "btc")
    assert sol_pos.is_long is True
    assert sol_pos.size_in_usd_float == pytest.approx(10.0)
    assert btc_pos.is_long is False
    assert btc_pos.size_in_usd_float == pytest.approx(20.0)


@pytest.mark.asyncio
async def test_fetch_account_positions_filters_zero_size() -> None:
    """Positions with size_in_usd == 0 are filtered out."""
    raw_live = _make_position_tuple(size_in_usd=10 * 10**30)
    raw_closed = _make_position_tuple(
        market=_BTC_MARKET, size_in_usd=0,
    )
    result_hex = _encode_account_positions_response([raw_live, raw_closed])
    body = {"jsonrpc": "2.0", "id": 1, "result": result_hex}
    resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=resp)):
        positions = await fetch_account_positions(
            _DUMMY_ACCOUNT, rpc_url="https://arb1.arbitrum.io/rpc",
        )
    assert len(positions) == 1
    assert positions[0].market_alias == "sol"
    assert positions[0].size_in_usd_float == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_fetch_account_positions_empty_array() -> None:
    """An empty Position.Props[] response → empty list (NOT an error)."""
    result_hex = _encode_account_positions_response([])
    body = {"jsonrpc": "2.0", "id": 1, "result": result_hex}
    resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=resp)):
        positions = await fetch_account_positions(
            _DUMMY_ACCOUNT, rpc_url="https://arb1.arbitrum.io/rpc",
        )
    assert positions == []


@pytest.mark.asyncio
async def test_fetch_account_positions_rpc_error_returns_empty() -> None:
    """JSON-RPC error response → empty list, no raise."""
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "error": {"code": -32000, "message": "execution reverted", "data": "0x"},
    }
    resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=resp)):
        positions = await fetch_account_positions(
            _DUMMY_ACCOUNT, rpc_url="https://arb1.arbitrum.io/rpc",
        )
    assert positions == []


@pytest.mark.asyncio
async def test_fetch_account_positions_transport_error_returns_empty() -> None:
    """httpx transport error → empty list, no raise."""
    import httpx
    with patch(
        "httpx.AsyncClient.post",
        new=AsyncMock(side_effect=httpx.ConnectError("nope")),
    ):
        positions = await fetch_account_positions(
            _DUMMY_ACCOUNT, rpc_url="https://arb1.arbitrum.io/rpc",
        )
    assert positions == []


@pytest.mark.asyncio
async def test_fetch_account_positions_malformed_hex_returns_empty() -> None:
    """Non-hex string in result → empty list, no raise."""
    body = {"jsonrpc": "2.0", "id": 1, "result": "0xnotahex"}
    resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=resp)):
        positions = await fetch_account_positions(
            _DUMMY_ACCOUNT, rpc_url="https://arb1.arbitrum.io/rpc",
        )
    assert positions == []


@pytest.mark.asyncio
async def test_fetch_account_positions_bad_status_returns_empty() -> None:
    """HTTP 500 → empty list."""
    body = {"jsonrpc": "2.0", "id": 1, "result": "0x"}
    resp = _make_fake_response(status_code=500, body=body)
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=resp)):
        positions = await fetch_account_positions(
            _DUMMY_ACCOUNT, rpc_url="https://arb1.arbitrum.io/rpc",
        )
    assert positions == []


# ──────────────────────────────────────────────────────────────────────────
# fetch_position — async (single key)
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fetch_position_decodes_real_position() -> None:
    """A non-zero Position.Props → Position dataclass."""
    raw = _make_position_tuple(
        market=_SOL_MARKET,
        collateral_token=_USDC,
        is_long=True,
        size_in_usd=15 * 10**30,
        collateral_amount=10_000_000,
    )
    result_hex = _encode_single_position_response(raw)
    body = {"jsonrpc": "2.0", "id": 1, "result": result_hex}
    resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=resp)):
        pos = await fetch_position(
            _DUMMY_ACCOUNT, "sol", _USDC, True,
            rpc_url="https://arb1.arbitrum.io/rpc",
        )
    assert pos is not None
    assert pos.market_alias == "sol"
    assert pos.size_in_usd_float == pytest.approx(15.0)


@pytest.mark.asyncio
async def test_fetch_position_zero_struct_returns_none() -> None:
    """A zero-struct (no position) → None."""
    raw = _make_position_tuple(
        account=_ETH_ZERO, market=_ETH_ZERO, collateral_token=_ETH_ZERO,
        size_in_usd=0,
    )
    result_hex = _encode_single_position_response(raw)
    body = {"jsonrpc": "2.0", "id": 1, "result": result_hex}
    resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=resp)):
        pos = await fetch_position(
            _DUMMY_ACCOUNT, "sol", _USDC, True,
            rpc_url="https://arb1.arbitrum.io/rpc",
        )
    assert pos is None


@pytest.mark.asyncio
async def test_fetch_position_unknown_market_returns_none() -> None:
    """Unknown market alias → None without any RPC call."""
    pos = await fetch_position(
        _DUMMY_ACCOUNT, "bogus_alias", _USDC, True,
        rpc_url="https://arb1.arbitrum.io/rpc",
    )
    assert pos is None


@pytest.mark.asyncio
async def test_fetch_position_rpc_error_returns_none() -> None:
    """RPC error response → None, no raise."""
    body = {
        "jsonrpc": "2.0", "id": 1,
        "error": {"code": -32000, "message": "reverted", "data": "0x"},
    }
    resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=resp)):
        pos = await fetch_position(
            _DUMMY_ACCOUNT, "sol", _USDC, True,
            rpc_url="https://arb1.arbitrum.io/rpc",
        )
    assert pos is None


# ──────────────────────────────────────────────────────────────────────────
# reconcile_intent — pure (all 5 decision-matrix cases)
# ──────────────────────────────────────────────────────────────────────────


def test_reconcile_increase_no_existing_proceeds() -> None:
    """Open with no existing position → PROCEED."""
    intent = _make_intent(is_increase=True, is_long=True)
    result = reconcile_intent(intent, [])
    assert result.action == "PROCEED"
    assert result.existing_position is None


def test_reconcile_increase_same_direction_merges() -> None:
    """Open with existing same-direction position → MERGE."""
    existing = _decode_position_props(_make_position_tuple(
        market=_SOL_MARKET,
        collateral_token=_USDC,
        is_long=True,
        size_in_usd=10 * 10**30,
    ))
    assert existing is not None
    intent = _make_intent(is_increase=True, is_long=True)
    result = reconcile_intent(intent, [existing])
    assert result.action == "MERGE"
    assert result.existing_position is not None
    assert result.existing_position.size_in_usd_float == pytest.approx(10.0)


def test_reconcile_increase_opposite_direction_aborts() -> None:
    """Open long when short is open in same market → ABORT."""
    existing = _decode_position_props(_make_position_tuple(
        market=_SOL_MARKET,
        collateral_token=_USDC,
        is_long=False,  # short open
        size_in_usd=10 * 10**30,
    ))
    assert existing is not None
    intent = _make_intent(is_increase=True, is_long=True)
    result = reconcile_intent(intent, [existing])
    assert result.action == "ABORT"
    assert "opposite-direction" in result.reason
    assert result.existing_position is not None


def test_reconcile_decrease_no_existing_aborts() -> None:
    """Close with no open position → ABORT (nothing to close)."""
    intent = _make_intent(is_increase=False, is_long=True)
    result = reconcile_intent(intent, [])
    assert result.action == "ABORT"
    assert "no position to decrease" in result.reason


def test_reconcile_decrease_with_existing_proceeds() -> None:
    """Close existing same-side position → PROCEED."""
    existing = _decode_position_props(_make_position_tuple(
        market=_SOL_MARKET,
        collateral_token=_USDC,
        is_long=True,
        size_in_usd=10 * 10**30,
    ))
    assert existing is not None
    intent = _make_intent(is_increase=False, is_long=True)
    result = reconcile_intent(intent, [existing])
    assert result.action == "PROCEED"
    assert result.existing_position is existing


def test_reconcile_decrease_opposite_side_aborts() -> None:
    """Close long when only short open → ABORT (wrong side)."""
    existing = _decode_position_props(_make_position_tuple(
        market=_SOL_MARKET,
        collateral_token=_USDC,
        is_long=False,
        size_in_usd=10 * 10**30,
    ))
    assert existing is not None
    intent = _make_intent(is_increase=False, is_long=True)  # close long
    result = reconcile_intent(intent, [existing])
    assert result.action == "ABORT"
    assert result.existing_position is not None
    assert result.existing_position.is_long is False


def test_reconcile_different_market_no_interference() -> None:
    """Position in BTC does not affect a SOL intent."""
    existing_btc = _decode_position_props(_make_position_tuple(
        market=_BTC_MARKET,
        collateral_token=_USDC,
        is_long=True,
        size_in_usd=10 * 10**30,
    ))
    assert existing_btc is not None
    intent = _make_intent(market="sol", is_increase=True, is_long=True)
    result = reconcile_intent(intent, [existing_btc])
    assert result.action == "PROCEED"
    assert result.existing_position is None


def test_reconcile_different_collateral_no_interference() -> None:
    """Same market + side but different collateral → different position."""
    # In SOL market, normally USDC for short side; WSOL for long side. A
    # long with WSOL collateral is a different position from a long with
    # USDC collateral per GMX's positionKey derivation.
    existing_wsol_long = _decode_position_props(_make_position_tuple(
        market=_SOL_MARKET,
        collateral_token=_WSOL,
        is_long=True,
        size_in_usd=10 * 10**30,
    ))
    assert existing_wsol_long is not None
    intent = _make_intent(
        market="sol", collateral_token=_USDC, is_increase=True, is_long=True,
    )
    result = reconcile_intent(intent, [existing_wsol_long])
    assert result.action == "PROCEED"


# ──────────────────────────────────────────────────────────────────────────
# Sanity: Position is frozen + has the expected surface
# ──────────────────────────────────────────────────────────────────────────


def test_position_dataclass_is_frozen() -> None:
    """Position must be immutable — no accidental field mutation."""
    pos = Position(
        account=_DUMMY_ACCOUNT,
        market_alias="sol",
        market_address=_SOL_MARKET,
        collateral_token=_USDC,
        is_long=True,
        size_in_usd=10 * 10**30,
        size_in_usd_float=10.0,
        size_in_tokens=0,
        collateral_amount=10_000_000,
        borrowing_factor=0,
        funding_fee_amount_per_size=0,
        increased_at_time=1_700_000_000,
        decreased_at_time=0,
    )
    with pytest.raises((AttributeError, Exception)):
        pos.size_in_usd = 999  # type: ignore[misc]


def test_reconciliation_result_dataclass_is_frozen() -> None:
    """ReconciliationResult must be immutable."""
    rr = ReconciliationResult(action="PROCEED", reason="x", existing_position=None)
    with pytest.raises((AttributeError, Exception)):
        rr.action = "ABORT"  # type: ignore[misc]
