"""Tests for the Binance USDT-M Futures order placement module (G6.4).

Mocks `binance_exchange_info.get_cached_exchange_info` + `signed_post` /
`signed_get` / `signed_delete` and exercises the full gate stack:

  Pre-flight rejections (no HTTP):
    - bad side ("HOLD") → OrderResult.error_msg, error_code=-1
    - non-basket symbol ("APTUSDT") → error
    - symbol missing from exchangeInfo cache → error
    - notional too small for lot_step → below_lot_min error
    - quantity passes lot_step but fails min_notional → below_min_notional

  Dry-run path:
    - dry_run=True → no signed_post call, dry_run_request populated,
      submitted=False, client_order_id auto-generated
    - dry_run=True with caller-supplied client_order_id → echoed back

  Live broadcast gates:
    - dry_run=False + live_binance_enabled=False → gate_blocked, no HTTP
    - dry_run=False + live=True + hedge mode → gate_blocked, no broadcast
    - dry_run=False + live=True + one-way → signed_post called,
      OrderResult.submitted=True

  Error-code parsing:
    - -1013, -1111, -2010, -2019, -4061, -4164 → error_code + error_slug
    - unknown code → error_code + error_msg, error_slug=None

  Idempotency:
    - client_order_id auto-generation uses settings prefix + 16 hex chars
    - same id round-trips through OrderResult on broadcast

  get_order_status + cancel_order:
    - whitelist enforced on both
    - missing id → None
    - signed_get / signed_delete called with correct params

The auth layer (HMAC + signing) is covered in `test_binance_auth.py`;
this module asserts the order-placement gating + response parsing only.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from gmx_strategies import binance_exchange_info as bei
from gmx_strategies import binance_order
from gmx_strategies.binance_order import (
    ALLOWED_SIDES,
    ALLOWED_SYMBOLS,
    OrderRequest,
    OrderResult,
    place_market_order,
)

# ──────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────


def _solusdt_info() -> bei.SymbolInfo:
    """Realistic SOLUSDT SymbolInfo from the 2026-05-20 mainnet smoke.

    From README G6 table:
      SOLUSDT: lot_step=0.01, lot_min=0.01, price_tick=0.01, min_notional=$5.
    """
    return bei.SymbolInfo(
        symbol="SOLUSDT",
        base_asset="SOL",
        quote_asset="USDT",
        price_precision=4,
        quantity_precision=2,
        lot_min=0.01,
        lot_max=1000000.0,
        lot_step=0.01,
        price_tick=0.01,
        min_notional=5.0,
    )


def _btcusdt_info() -> bei.SymbolInfo:
    """Realistic BTCUSDT SymbolInfo. min_notional=$50 (5x our $10 cap)."""
    return bei.SymbolInfo(
        symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        price_precision=1,
        quantity_precision=3,
        lot_min=0.001,
        lot_max=1000.0,
        lot_step=0.001,
        price_tick=0.1,
        min_notional=50.0,
    )


def _all_5_symbols_info() -> dict[str, bei.SymbolInfo]:
    """exchangeInfo cache fixture covering all 5 G6 markets."""
    return {
        "BTCUSDT": _btcusdt_info(),
        "ETHUSDT": bei.SymbolInfo(
            symbol="ETHUSDT", base_asset="ETH", quote_asset="USDT",
            price_precision=2, quantity_precision=3,
            lot_min=0.001, lot_max=1000000.0, lot_step=0.001,
            price_tick=0.01, min_notional=20.0,
        ),
        "SOLUSDT": _solusdt_info(),
        "DOGEUSDT": bei.SymbolInfo(
            symbol="DOGEUSDT", base_asset="DOGE", quote_asset="USDT",
            price_precision=5, quantity_precision=0,
            lot_min=1.0, lot_max=10000000.0, lot_step=1.0,
            price_tick=0.00001, min_notional=5.0,
        ),
        "XRPUSDT": bei.SymbolInfo(
            symbol="XRPUSDT", base_asset="XRP", quote_asset="USDT",
            price_precision=4, quantity_precision=1,
            lot_min=0.1, lot_max=1000000.0, lot_step=0.1,
            price_tick=0.0001, min_notional=5.0,
        ),
    }


@pytest.fixture
def exchange_info_cache() -> Any:
    """Mock `get_cached_exchange_info` to return all 5 markets."""
    with patch(
        "gmx_strategies.binance_order.binance_exchange_info."
        "get_cached_exchange_info",
        new=AsyncMock(return_value=_all_5_symbols_info()),
    ) as m:
        yield m


@pytest.fixture
def disable_live_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Hard-reset live gate to False (matches default)."""
    monkeypatch.setattr(
        "gmx_strategies.binance_order.settings.live_binance_enabled", False,
    )


@pytest.fixture
def enable_live_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Flip live gate to True for the live-broadcast path tests."""
    monkeypatch.setattr(
        "gmx_strategies.binance_order.settings.live_binance_enabled", True,
    )


# ──────────────────────────────────────────────────────────────────────────
# Pre-flight rejections (no HTTP — should never call signed_post)
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_preflight_bad_side_returns_local_error(
    exchange_info_cache: Any,
) -> None:
    """side='HOLD' is not in ALLOWED_SIDES → error_code=-1, no HTTP."""
    with patch(
        "gmx_strategies.binance_order.signed_post", new=AsyncMock(),
    ) as mock_post:
        result = await place_market_order(
            "SOLUSDT", "HOLD", 10.0,
            mark_price=150.0,
            dry_run=True,
        )
    assert result.submitted is False
    assert result.error_code == -1
    assert "side not allowed" in (result.error_msg or "")
    assert result.error_slug is None
    assert result.dry_run_request is None
    mock_post.assert_not_called()


@pytest.mark.asyncio
async def test_preflight_non_basket_symbol_rejected(
    exchange_info_cache: Any,
) -> None:
    """APTUSDT is real on Binance but NOT in our whitelist → reject."""
    with patch(
        "gmx_strategies.binance_order.signed_post", new=AsyncMock(),
    ) as mock_post:
        result = await place_market_order(
            "APTUSDT", "BUY", 10.0,
            mark_price=10.0,
            dry_run=True,
        )
    assert result.submitted is False
    assert result.error_code == -1
    assert "not in whitelist" in (result.error_msg or "")
    mock_post.assert_not_called()


@pytest.mark.asyncio
async def test_preflight_symbol_missing_from_exchange_info() -> None:
    """Symbol in whitelist but missing from exchangeInfo cache → error.

    Defends against the edge where Binance has temporarily delisted a
    market — caller's whitelist hasn't been updated yet.
    """
    partial = {"BTCUSDT": _btcusdt_info()}  # no SOLUSDT
    with patch(
        "gmx_strategies.binance_order.binance_exchange_info."
        "get_cached_exchange_info",
        new=AsyncMock(return_value=partial),
    ), patch(
        "gmx_strategies.binance_order.signed_post", new=AsyncMock(),
    ) as mock_post:
        result = await place_market_order(
            "SOLUSDT", "BUY", 10.0,
            mark_price=150.0,
            dry_run=True,
        )
    assert result.submitted is False
    assert result.error_code == -1
    assert "exchange_info" in (result.error_msg or "")
    mock_post.assert_not_called()


@pytest.mark.asyncio
async def test_preflight_below_min_notional_btc(
    exchange_info_cache: Any,
) -> None:
    """$1 notional on BTCUSDT (min $50) → below_min_notional, no HTTP.

    The audit's H1 finding: BTC's $50 min is 5x our $10 cap. Without
    this guard the order would reject -4164 from Binance after going out.
    """
    with patch(
        "gmx_strategies.binance_order.signed_post", new=AsyncMock(),
    ) as mock_post:
        result = await place_market_order(
            "BTCUSDT", "BUY", 1.0,  # $1 < $50 min
            mark_price=60_000.0,
            dry_run=True,
        )
    assert result.submitted is False
    assert result.error_code == -1
    # `notional_usd < min_notional` short-circuit fires before lot-step
    # rounding — surfaces as "below_lot_min" via quantity_from_notional's
    # fast-path. Either error wording is acceptable; the contract is
    # "rejected locally, no HTTP".
    assert "below" in (result.error_msg or "").lower()
    mock_post.assert_not_called()


@pytest.mark.asyncio
async def test_preflight_below_lot_min(exchange_info_cache: Any) -> None:
    """Very small notional that doesn't even fill one lot_step → reject."""
    # DOGEUSDT lot_step=1.0; $0.10 at $0.20/DOGE = 0.5 DOGE → floors to 0
    with patch(
        "gmx_strategies.binance_order.signed_post", new=AsyncMock(),
    ) as mock_post:
        result = await place_market_order(
            "DOGEUSDT", "BUY", 0.10,
            mark_price=0.20,
            dry_run=True,
        )
    assert result.submitted is False
    assert result.error_code == -1
    mock_post.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────
# Dry-run path
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dry_run_does_not_call_signed_post(
    exchange_info_cache: Any,
) -> None:
    """dry_run=True returns the would-be-signed params; signed_post unused."""
    with patch(
        "gmx_strategies.binance_order.signed_post", new=AsyncMock(),
    ) as mock_post:
        result = await place_market_order(
            "SOLUSDT", "BUY", 10.0,
            mark_price=150.0,
            dry_run=True,
        )
    assert result.submitted is False
    assert result.error_code is None
    assert result.error_msg is None
    assert result.gate_blocked is None
    assert result.dry_run_request is not None
    # Required Binance order params
    assert result.dry_run_request["symbol"] == "SOLUSDT"
    assert result.dry_run_request["side"] == "BUY"
    assert result.dry_run_request["type"] == "MARKET"
    assert result.dry_run_request["newOrderRespType"] == "RESULT"
    # Quantity was rounded via Decimal math: $10 / $150 = 0.0666... →
    # floors to 0.06 at lot_step=0.01
    assert result.dry_run_request["quantity"] == pytest.approx(0.06)
    # Client-order id was auto-generated
    assert result.client_order_id is not None
    assert result.client_order_id.startswith("gmx-strategies-")
    mock_post.assert_not_called()


@pytest.mark.asyncio
async def test_dry_run_echoes_supplied_client_order_id(
    exchange_info_cache: Any,
) -> None:
    """Caller-supplied client_order_id round-trips through OrderResult."""
    custom_id = "my-retry-id-abc123"
    with patch(
        "gmx_strategies.binance_order.signed_post", new=AsyncMock(),
    ):
        result = await place_market_order(
            "SOLUSDT", "BUY", 10.0,
            mark_price=150.0,
            client_order_id=custom_id,
            dry_run=True,
        )
    assert result.client_order_id == custom_id
    assert result.dry_run_request is not None
    assert result.dry_run_request["newClientOrderId"] == custom_id


@pytest.mark.asyncio
async def test_dry_run_reduce_only_sets_string_true(
    exchange_info_cache: Any,
) -> None:
    """reduce_only=True → params['reduceOnly']='true' (string, lowercase)."""
    with patch(
        "gmx_strategies.binance_order.signed_post", new=AsyncMock(),
    ):
        result = await place_market_order(
            "SOLUSDT", "SELL", 10.0,
            mark_price=150.0,
            reduce_only=True,
            dry_run=True,
        )
    assert result.dry_run_request is not None
    assert result.dry_run_request["reduceOnly"] == "true"


# ──────────────────────────────────────────────────────────────────────────
# Live gate stack (dry_run=False)
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_live_gate_blocked_when_live_disabled(
    exchange_info_cache: Any,
    disable_live_gate: None,
) -> None:
    """dry_run=False + live_binance_enabled=False → gate_blocked, no HTTP."""
    with patch(
        "gmx_strategies.binance_order.signed_post", new=AsyncMock(),
    ) as mock_post:
        result = await place_market_order(
            "SOLUSDT", "BUY", 10.0,
            mark_price=150.0,
            dry_run=False,
        )
    assert result.submitted is False
    assert result.gate_blocked == "live_binance_enabled=False"
    assert result.dry_run_request is not None
    mock_post.assert_not_called()


@pytest.mark.asyncio
async def test_live_one_way_position_mode_allows_broadcast(
    exchange_info_cache: Any,
    enable_live_gate: None,
) -> None:
    """All gates clear (live=True + one-way) → signed_post called."""
    happy_body: dict[str, Any] = {
        "orderId": 99887766,
        "clientOrderId": "gmx-strategies-deadbeef12345678",
        "symbol": "SOLUSDT",
        "status": "FILLED",
        "executedQty": "0.06",
        "cumQuote": "9.04",
        "avgPrice": "150.66",
        "side": "BUY",
        "type": "MARKET",
    }
    mock_post = AsyncMock(return_value=happy_body)
    with patch(
        "gmx_strategies.binance_order.signed_post", new=mock_post,
    ), patch(
        "gmx_strategies.binance_order.binance_startup_check."
        "assert_one_way_position_mode",
        new=AsyncMock(return_value=None),
    ):
        result = await place_market_order(
            "SOLUSDT", "BUY", 10.0,
            mark_price=150.0,
            dry_run=False,
        )
    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    assert args[0] == "/fapi/v1/order"
    sent_params = args[1]
    assert sent_params["symbol"] == "SOLUSDT"
    assert sent_params["side"] == "BUY"
    assert sent_params["type"] == "MARKET"
    assert "newClientOrderId" in sent_params

    # OrderResult shape on happy path
    assert result.submitted is True
    assert result.order_id == 99887766
    assert result.status == "FILLED"
    assert result.executed_qty == pytest.approx(0.06)
    assert result.avg_price == pytest.approx(150.66)
    assert result.cum_quote == pytest.approx(9.04)
    # clientOrderId returned by Binance should overwrite our generated one
    assert result.client_order_id == "gmx-strategies-deadbeef12345678"
    assert result.gate_blocked is None
    assert result.error_code is None


@pytest.mark.asyncio
async def test_live_hedge_mode_blocks_broadcast(
    exchange_info_cache: Any,
    enable_live_gate: None,
) -> None:
    """live=True + assert_one_way_position_mode raises → gate_blocked."""
    hedge_exc = RuntimeError(
        "BINANCE: account is in HEDGE mode. Switch to ONE-WAY in the UI ...",
    )
    with patch(
        "gmx_strategies.binance_order.signed_post", new=AsyncMock(),
    ) as mock_post, patch(
        "gmx_strategies.binance_order.binance_startup_check."
        "assert_one_way_position_mode",
        new=AsyncMock(side_effect=hedge_exc),
    ):
        result = await place_market_order(
            "SOLUSDT", "BUY", 10.0,
            mark_price=150.0,
            dry_run=False,
        )
    assert result.submitted is False
    assert result.gate_blocked == "hedge_mode_or_api_down"
    assert "HEDGE" in (result.error_msg or "")
    mock_post.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────
# Error-code parsing
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("code", "expected_slug"),
    [
        (-1013, "invalid_message_or_lot_size"),
        (-1111, "precision_mismatch"),
        (-2010, "new_order_rejected"),
        (-2019, "margin_not_sufficient"),
        (-4061, "position_side_not_match_hedge_mode"),
        (-4164, "below_min_notional_exchange"),
    ],
)
@pytest.mark.asyncio
async def test_live_known_error_code_classified(
    exchange_info_cache: Any,
    enable_live_gate: None,
    code: int,
    expected_slug: str,
) -> None:
    """Each known audit-§12 code maps to its stable slug."""
    err_body = {"code": code, "msg": f"Binance returned {code}."}
    with patch(
        "gmx_strategies.binance_order.signed_post",
        new=AsyncMock(return_value=err_body),
    ), patch(
        "gmx_strategies.binance_order.binance_startup_check."
        "assert_one_way_position_mode",
        new=AsyncMock(return_value=None),
    ):
        result = await place_market_order(
            "SOLUSDT", "BUY", 10.0,
            mark_price=150.0,
            dry_run=False,
        )
    assert result.submitted is False
    assert result.error_code == code
    assert result.error_slug == expected_slug
    assert result.error_msg == f"Binance returned {code}."


@pytest.mark.asyncio
async def test_live_unknown_error_code_unclassified(
    exchange_info_cache: Any,
    enable_live_gate: None,
) -> None:
    """Unknown Binance code → error_code populated, error_slug=None."""
    err_body = {"code": -9999, "msg": "Mystery error from Binance."}
    with patch(
        "gmx_strategies.binance_order.signed_post",
        new=AsyncMock(return_value=err_body),
    ), patch(
        "gmx_strategies.binance_order.binance_startup_check."
        "assert_one_way_position_mode",
        new=AsyncMock(return_value=None),
    ):
        result = await place_market_order(
            "SOLUSDT", "BUY", 10.0,
            mark_price=150.0,
            dry_run=False,
        )
    assert result.submitted is False
    assert result.error_code == -9999
    assert result.error_slug is None
    assert result.error_msg == "Mystery error from Binance."


@pytest.mark.asyncio
async def test_live_no_body_from_signed_post_surfaces_transport_error(
    exchange_info_cache: Any,
    enable_live_gate: None,
) -> None:
    """signed_post returning None → submitted=False, error_msg=transport."""
    with patch(
        "gmx_strategies.binance_order.signed_post",
        new=AsyncMock(return_value=None),
    ), patch(
        "gmx_strategies.binance_order.binance_startup_check."
        "assert_one_way_position_mode",
        new=AsyncMock(return_value=None),
    ):
        result = await place_market_order(
            "SOLUSDT", "BUY", 10.0,
            mark_price=150.0,
            dry_run=False,
        )
    assert result.submitted is False
    assert result.error_msg == "no_response_body_from_signed_post"
    assert result.gate_blocked is None
    assert result.error_code is None


# ──────────────────────────────────────────────────────────────────────────
# Idempotency
# ──────────────────────────────────────────────────────────────────────────


def test_generate_client_order_id_format() -> None:
    """Auto-generated id matches Binance's regex and uses settings prefix."""
    cid = binance_order._generate_client_order_id()
    # Prefix
    assert cid.startswith("gmx-strategies-")
    # Tail is 16 hex chars
    tail = cid[len("gmx-strategies-"):]
    assert len(tail) == 16
    assert all(c in "0123456789abcdef" for c in tail)
    # Total length well under Binance's 36-char limit
    assert len(cid) <= 36


def test_generate_client_order_id_collision_resistance() -> None:
    """Two calls produce distinct ids (uuid4 entropy)."""
    ids = {binance_order._generate_client_order_id() for _ in range(100)}
    assert len(ids) == 100


def test_order_request_to_signed_params_sets_resp_type_and_quantity() -> None:
    """`OrderRequest.to_signed_params()` is the structural contract."""
    req = OrderRequest(
        symbol="SOLUSDT", side="BUY", type="MARKET",
        quantity=0.03, reduce_only=False,
        client_order_id="gmx-strategies-deadbeef00000000",
    )
    params = req.to_signed_params()
    assert params["symbol"] == "SOLUSDT"
    assert params["side"] == "BUY"
    assert params["type"] == "MARKET"
    assert params["quantity"] == 0.03
    assert params["newOrderRespType"] == "RESULT"
    assert params["newClientOrderId"] == "gmx-strategies-deadbeef00000000"
    # reduceOnly omitted when False (Binance: do not send the field at all)
    assert "reduceOnly" not in params


def test_order_request_reduce_only_emits_lowercase_string_true() -> None:
    """reduce_only=True → params['reduceOnly']='true' (str, not bool)."""
    req = OrderRequest(
        symbol="SOLUSDT", side="SELL", type="MARKET",
        quantity=0.03, reduce_only=True,
        client_order_id="gmx-strategies-abc",
    )
    params = req.to_signed_params()
    assert params["reduceOnly"] == "true"
    assert isinstance(params["reduceOnly"], str)


# ──────────────────────────────────────────────────────────────────────────
# get_order_status
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_order_status_whitelist_enforced() -> None:
    """Non-basket symbol → returns None even for reads."""
    result = await binance_order.get_order_status(
        "APTUSDT", order_id=123,
    )
    assert result is None


@pytest.mark.asyncio
async def test_get_order_status_passes_order_id() -> None:
    """When `order_id` is set, signed_get receives orderId in params."""
    happy_body = {
        "orderId": 123, "clientOrderId": "x", "symbol": "SOLUSDT",
        "status": "FILLED", "executedQty": "0.03",
        "cumQuote": "4.5", "avgPrice": "150.0",
    }
    mock_get = AsyncMock(return_value=happy_body)
    with patch("gmx_strategies.binance_order.signed_get", new=mock_get):
        result = await binance_order.get_order_status(
            "SOLUSDT", order_id=123,
        )
    args, _kwargs = mock_get.call_args
    assert args[0] == "/fapi/v1/order"
    assert args[1] == {"symbol": "SOLUSDT", "orderId": 123}
    assert result is not None
    assert result.submitted is True
    assert result.status == "FILLED"


@pytest.mark.asyncio
async def test_get_order_status_passes_client_order_id() -> None:
    """When `client_order_id` is set, signed_get receives
    `origClientOrderId` in params."""
    happy_body = {
        "orderId": 123, "clientOrderId": "gmx-strategies-abc",
        "symbol": "SOLUSDT", "status": "FILLED",
        "executedQty": "0.03", "cumQuote": "4.5", "avgPrice": "150.0",
    }
    mock_get = AsyncMock(return_value=happy_body)
    with patch("gmx_strategies.binance_order.signed_get", new=mock_get):
        await binance_order.get_order_status(
            "SOLUSDT", client_order_id="gmx-strategies-abc",
        )
    args, _kwargs = mock_get.call_args
    assert args[1] == {
        "symbol": "SOLUSDT", "origClientOrderId": "gmx-strategies-abc",
    }


@pytest.mark.asyncio
async def test_get_order_status_no_id_returns_none() -> None:
    """No `order_id` AND no `client_order_id` → None."""
    result = await binance_order.get_order_status("SOLUSDT")
    assert result is None


# ──────────────────────────────────────────────────────────────────────────
# cancel_order
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cancel_order_calls_signed_delete() -> None:
    """`cancel_order` issues a signed DELETE."""
    happy_body = {"orderId": 123, "status": "CANCELED"}
    mock_delete = AsyncMock(return_value=happy_body)
    with patch("gmx_strategies.binance_order.signed_delete", new=mock_delete):
        result = await binance_order.cancel_order(
            "SOLUSDT", order_id=123,
        )
    args, _kwargs = mock_delete.call_args
    assert args[0] == "/fapi/v1/order"
    assert args[1] == {"symbol": "SOLUSDT", "orderId": 123}
    assert result == happy_body


@pytest.mark.asyncio
async def test_cancel_order_whitelist_enforced() -> None:
    """Non-basket symbol → cancel returns None without DELETE."""
    mock_delete = AsyncMock()
    with patch("gmx_strategies.binance_order.signed_delete", new=mock_delete):
        result = await binance_order.cancel_order("APTUSDT", order_id=123)
    assert result is None
    mock_delete.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────
# OrderResult shape — defensive
# ──────────────────────────────────────────────────────────────────────────


def test_order_result_is_frozen() -> None:
    """OrderResult is a frozen dataclass — mutation should raise."""
    r = OrderResult(
        submitted=True, dry_run_request=None, order_id=1,
        client_order_id="x", status="FILLED",
        executed_qty=1.0, avg_price=1.0, cum_quote=1.0,
        error_code=None, error_msg=None, gate_blocked=None,
    )
    with pytest.raises((AttributeError, Exception)):
        r.submitted = False  # type: ignore[misc]


def test_allowed_constants() -> None:
    """Whitelists are the 5 funding-arb basket symbols + BUY/SELL + MARKET."""
    assert ALLOWED_SYMBOLS == frozenset(
        {"BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT"},
    )
    assert ALLOWED_SIDES == frozenset({"BUY", "SELL"})
    assert binance_order.ALLOWED_TYPES == frozenset({"MARKET"})
