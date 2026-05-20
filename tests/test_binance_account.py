"""Tests for the Binance Futures read-only account state module (G6.2).

Mocks `binance_auth.signed_get` and asserts the per-helper parsing /
filtering behaviour. The auth layer (HMAC + headers + URL) is tested
separately in `test_binance_auth.py`.

Coverage:
  fetch_position_mode:
    - dualSidePosition=True → returns True (hedge)
    - dualSidePosition=False → returns False (one-way)
    - signed_get returns None → returns None
    - missing field / wrong type → returns None
    - body not a dict → returns None

  fetch_account_balance:
    - happy list → returns list as-is
    - signed_get returns None → returns None
    - non-list body → returns None

  fetch_usdt_free_margin:
    - happy path → returns float (USDT availableBalance)
    - no USDT entry → returns None
    - missing field → returns None
    - non-numeric value → returns None
    - signed_get returns None → returns None

  fetch_position_information:
    - no-symbol → returns full list
    - with symbol → passes symbol=X in params dict
    - signed_get returns None → returns None
    - non-list body → returns None
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from gmx_strategies import binance_account

# ──────────────────────────────────────────────────────────────────────────
# fetch_position_mode
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_position_mode_hedge_returns_true() -> None:
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value={"dualSidePosition": True}),
    ):
        result = await binance_account.fetch_position_mode()
    assert result is True


@pytest.mark.asyncio
async def test_position_mode_one_way_returns_false() -> None:
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value={"dualSidePosition": False}),
    ):
        result = await binance_account.fetch_position_mode()
    assert result is False


@pytest.mark.asyncio
async def test_position_mode_signed_get_none_returns_none() -> None:
    """Auth failure / HTTP error propagates as None."""
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value=None),
    ):
        result = await binance_account.fetch_position_mode()
    assert result is None


@pytest.mark.asyncio
async def test_position_mode_missing_field_returns_none() -> None:
    """Body without `dualSidePosition` field → None."""
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value={"unrelated": "data"}),
    ):
        result = await binance_account.fetch_position_mode()
    assert result is None


@pytest.mark.asyncio
async def test_position_mode_wrong_field_type_returns_none() -> None:
    """`dualSidePosition` present but not a bool → None."""
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value={"dualSidePosition": "true"}),  # str, not bool
    ):
        result = await binance_account.fetch_position_mode()
    assert result is None


@pytest.mark.asyncio
async def test_position_mode_non_dict_body_returns_none() -> None:
    """A list / string body → None."""
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value=["something"]),
    ):
        result = await binance_account.fetch_position_mode()
    assert result is None


# ──────────────────────────────────────────────────────────────────────────
# fetch_account_balance
# ──────────────────────────────────────────────────────────────────────────


_BALANCE_FIXTURE: list[dict[str, Any]] = [
    {
        "accountAlias": "futuresAccount",
        "asset": "USDT",
        "balance": "120.50000000",
        "crossWalletBalance": "120.50000000",
        "crossUnPnl": "0.00000000",
        "availableBalance": "100.50000000",
        "maxWithdrawAmount": "100.50000000",
        "marginAvailable": True,
        "updateTime": 1716170000000,
    },
    {
        "asset": "BUSD",
        "balance": "0.00000000",
        "availableBalance": "0.00000000",
    },
]


@pytest.mark.asyncio
async def test_account_balance_happy_returns_list() -> None:
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value=_BALANCE_FIXTURE),
    ):
        result = await binance_account.fetch_account_balance()
    assert result == _BALANCE_FIXTURE


@pytest.mark.asyncio
async def test_account_balance_signed_get_none_returns_none() -> None:
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value=None),
    ):
        result = await binance_account.fetch_account_balance()
    assert result is None


@pytest.mark.asyncio
async def test_account_balance_non_list_body_returns_none() -> None:
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value={"unexpected": "shape"}),
    ):
        result = await binance_account.fetch_account_balance()
    assert result is None


# ──────────────────────────────────────────────────────────────────────────
# fetch_usdt_free_margin
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_usdt_free_margin_happy_returns_float() -> None:
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value=_BALANCE_FIXTURE),
    ):
        result = await binance_account.fetch_usdt_free_margin()
    assert result == pytest.approx(100.5)
    assert isinstance(result, float)


@pytest.mark.asyncio
async def test_usdt_free_margin_no_usdt_returns_none() -> None:
    only_busd = [{"asset": "BUSD", "availableBalance": "50.0"}]
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value=only_busd),
    ):
        result = await binance_account.fetch_usdt_free_margin()
    assert result is None


@pytest.mark.asyncio
async def test_usdt_free_margin_missing_field_returns_none() -> None:
    no_avail = [{"asset": "USDT", "balance": "100.5"}]  # no availableBalance
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value=no_avail),
    ):
        result = await binance_account.fetch_usdt_free_margin()
    assert result is None


@pytest.mark.asyncio
async def test_usdt_free_margin_non_numeric_returns_none() -> None:
    bad = [{"asset": "USDT", "availableBalance": "not-a-number"}]
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value=bad),
    ):
        result = await binance_account.fetch_usdt_free_margin()
    assert result is None


@pytest.mark.asyncio
async def test_usdt_free_margin_signed_get_none_propagates() -> None:
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value=None),
    ):
        result = await binance_account.fetch_usdt_free_margin()
    assert result is None


# ──────────────────────────────────────────────────────────────────────────
# fetch_position_information
# ──────────────────────────────────────────────────────────────────────────


_POSITION_FIXTURE: list[dict[str, Any]] = [
    {
        "symbol": "BTCUSDT",
        "positionAmt": "0.012",
        "entryPrice": "67450.30",
        "markPrice": "67510.10",
        "unRealizedProfit": "0.71760",
        "liquidationPrice": "63204.10",
        "leverage": "3",
        "marginType": "isolated",
        "positionSide": "BOTH",
        "notional": "810.121",
        "updateTime": 1716170400000,
    },
]


@pytest.mark.asyncio
async def test_position_info_no_symbol_returns_full_list() -> None:
    mock_signed_get = AsyncMock(return_value=_POSITION_FIXTURE)
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=mock_signed_get,
    ):
        result = await binance_account.fetch_position_information()
    assert result == _POSITION_FIXTURE
    # No symbol filter → empty params.
    args, _kwargs = mock_signed_get.call_args
    assert args[0] == "/fapi/v2/positionRisk"
    assert args[1] == {}


@pytest.mark.asyncio
async def test_position_info_with_symbol_passes_filter_param() -> None:
    mock_signed_get = AsyncMock(return_value=_POSITION_FIXTURE)
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=mock_signed_get,
    ):
        await binance_account.fetch_position_information("BTCUSDT")
    args, _kwargs = mock_signed_get.call_args
    assert args[0] == "/fapi/v2/positionRisk"
    assert args[1] == {"symbol": "BTCUSDT"}


@pytest.mark.asyncio
async def test_position_info_signed_get_none_returns_none() -> None:
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value=None),
    ):
        result = await binance_account.fetch_position_information()
    assert result is None


@pytest.mark.asyncio
async def test_position_info_non_list_body_returns_none() -> None:
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value={"oops": "dict"}),
    ):
        result = await binance_account.fetch_position_information()
    assert result is None


@pytest.mark.asyncio
async def test_position_info_empty_list_is_success_not_failure() -> None:
    """No open positions is a SUCCESSFUL read — return [], not None."""
    with patch(
        "gmx_strategies.binance_account.signed_get",
        new=AsyncMock(return_value=[]),
    ):
        result = await binance_account.fetch_position_information()
    assert result == []
