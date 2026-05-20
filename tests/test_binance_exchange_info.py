"""Tests for the Binance USDT-M Futures exchange-info reader (G6.1).

Mocks `httpx.AsyncClient.get` with realistic /fapi/v1/exchangeInfo bodies
covering the 5 markets G6 will hedge against (BTCUSDT, ETHUSDT, SOLUSDT,
DOGEUSDT, XRPUSDT). The filter values match what mainnet returned at the
2026-05-20 smoke run (see PR description for verbatim values).

Coverage:
  - Parse: BTCUSDT lot_step / min_notional / price_tick correctly extracted
  - Parse: MARKET_LOT_SIZE preferred over LOT_SIZE (G6 only places MARKET orders)
  - Parse: missing filters → symbol dropped silently, full map still ships
  - round_qty_down: standard sub-1 step (BTCUSDT 0.001) and whole-unit step (DOGE 1.0)
  - round_qty_down: floor never lifts below `lot_min` → returns 0.0
  - passes_min_notional: above + below threshold
  - quantity_from_notional: $100 notional at $50 with step 0.001 = 2.000 (mid scope)
  - quantity_from_notional: notional below symbol min returns 0.0 (no point computing)
  - Cache TTL respected: second call within TTL is a cache hit
  - Cache TTL: force_refresh bypasses
  - Cache TTL: refresh failure preserves stale cache (don't wipe on transient HTTP error)
  - Graceful failure: HTTP error / bad JSON / wrong shape / missing symbols field → empty dict
  - Graceful failure: never raises
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from gmx_strategies import binance_exchange_info as bei

# ──────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────


def _make_fake_response(*, status_code: int = 200, body: Any) -> Any:
    """Minimal stand-in for httpx.Response — only attrs the module reads."""

    class _Resp:
        def __init__(self, sc: int, body: Any) -> None:
            self.status_code = sc
            self._body = body

        def json(self) -> Any:
            return self._body

    return _Resp(status_code, body)


def _symbol_entry(
    *,
    symbol: str,
    base: str,
    quote: str = "USDT",
    price_precision: int,
    qty_precision: int,
    lot_min: str,
    lot_max: str,
    lot_step: str,
    market_lot_min: str | None = None,
    market_lot_max: str | None = None,
    market_lot_step: str | None = None,
    price_tick: str,
    min_notional: str,
) -> dict[str, Any]:
    """Build a realistic-shape symbol entry. Includes the MARKET_LOT_SIZE
    filter only when caller passes the market_lot_* params (lets us assert
    fallback behavior).
    """
    filters: list[dict[str, Any]] = [
        {
            "filterType": "PRICE_FILTER",
            "minPrice": "0.01",
            "maxPrice": "1000000",
            "tickSize": price_tick,
        },
        {
            "filterType": "LOT_SIZE",
            "minQty": lot_min,
            "maxQty": lot_max,
            "stepSize": lot_step,
        },
        {
            "filterType": "MIN_NOTIONAL",
            "notional": min_notional,
        },
    ]
    if market_lot_step is not None:
        filters.append({
            "filterType": "MARKET_LOT_SIZE",
            "minQty": market_lot_min or lot_min,
            "maxQty": market_lot_max or lot_max,
            "stepSize": market_lot_step,
        })
    return {
        "symbol": symbol,
        "baseAsset": base,
        "quoteAsset": quote,
        "pricePrecision": price_precision,
        "quantityPrecision": qty_precision,
        "filters": filters,
    }


def _five_markets_body() -> dict[str, Any]:
    """A realistic /fapi/v1/exchangeInfo body covering our 5 markets.

    Values reflect what mainnet historically returns; verified in the
    G6.1 smoke run. min_notional values: BTC 100, ETH 20, SOL 5, DOGE 5,
    XRP 5. lot_step / tick values match real precision.
    """
    return {
        "timezone": "UTC",
        "serverTime": 1716210000000,
        "symbols": [
            _symbol_entry(
                symbol="BTCUSDT", base="BTC",
                price_precision=2, qty_precision=3,
                lot_min="0.001", lot_max="1000", lot_step="0.001",
                price_tick="0.10", min_notional="100",
            ),
            _symbol_entry(
                symbol="ETHUSDT", base="ETH",
                price_precision=2, qty_precision=3,
                lot_min="0.001", lot_max="10000", lot_step="0.001",
                price_tick="0.01", min_notional="20",
            ),
            _symbol_entry(
                symbol="SOLUSDT", base="SOL",
                price_precision=4, qty_precision=0,
                lot_min="1", lot_max="1000000", lot_step="1",
                price_tick="0.0010", min_notional="5",
            ),
            _symbol_entry(
                symbol="DOGEUSDT", base="DOGE",
                price_precision=5, qty_precision=0,
                lot_min="1", lot_max="30000000", lot_step="1",
                price_tick="0.00001", min_notional="5",
            ),
            _symbol_entry(
                symbol="XRPUSDT", base="XRP",
                price_precision=4, qty_precision=1,
                lot_min="0.1", lot_max="2000000", lot_step="0.1",
                price_tick="0.0001", min_notional="5",
            ),
        ],
    }


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    """Ensure each test starts with a cold cache. The module-level cache
    would otherwise leak state between tests in non-deterministic order.
    """
    bei._reset_cache_for_testing()


# ──────────────────────────────────────────────────────────────────────────
# Parsing
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_parses_btcusdt_filters_correctly() -> None:
    """The headline of G6 audit H1: BTCUSDT min_notional ~= $100.

    Asserts lot_step, price_tick, and min_notional are extracted faithfully
    from the response. If this regresses, every BTC hedge will reject -4164.
    """
    fake_resp = _make_fake_response(body=_five_markets_body())
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        out = await bei.fetch_exchange_info()
    assert "BTCUSDT" in out
    btc = out["BTCUSDT"]
    assert btc.symbol == "BTCUSDT"
    assert btc.base_asset == "BTC"
    assert btc.quote_asset == "USDT"
    assert btc.lot_step == pytest.approx(0.001, rel=1e-12)
    assert btc.lot_min == pytest.approx(0.001, rel=1e-12)
    assert btc.price_tick == pytest.approx(0.10, rel=1e-12)
    assert btc.min_notional == pytest.approx(100.0, rel=1e-12)
    assert btc.price_precision == 2
    assert btc.quantity_precision == 3


@pytest.mark.asyncio
async def test_market_lot_size_preferred_over_lot_size() -> None:
    """When MARKET_LOT_SIZE exists, its stepSize wins. G6 places MARKET orders
    only and MARKET_LOT_SIZE can have a wider step than LOT_SIZE.
    """
    entry = _symbol_entry(
        symbol="BTCUSDT", base="BTC",
        price_precision=2, qty_precision=3,
        lot_min="0.001", lot_max="1000", lot_step="0.001",
        market_lot_min="0.001", market_lot_max="1000", market_lot_step="0.010",
        price_tick="0.10", min_notional="100",
    )
    body = {"symbols": [entry]}
    fake_resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        out = await bei.fetch_exchange_info()
    assert out["BTCUSDT"].lot_step == pytest.approx(0.010, rel=1e-12)


@pytest.mark.asyncio
async def test_missing_filter_drops_symbol_silently() -> None:
    """Malformed/missing required filter on ONE symbol must NOT poison the
    full map. The good symbols still ship; the bad one is silently dropped.
    """
    good = _symbol_entry(
        symbol="BTCUSDT", base="BTC",
        price_precision=2, qty_precision=3,
        lot_min="0.001", lot_max="1000", lot_step="0.001",
        price_tick="0.10", min_notional="100",
    )
    # Bad entry: no MIN_NOTIONAL filter at all.
    bad = {
        "symbol": "BADUSDT",
        "baseAsset": "BAD",
        "quoteAsset": "USDT",
        "pricePrecision": 2,
        "quantityPrecision": 3,
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
            {"filterType": "LOT_SIZE", "minQty": "0.001",
             "maxQty": "1000", "stepSize": "0.001"},
            # MIN_NOTIONAL absent.
        ],
    }
    body = {"symbols": [good, bad]}
    fake_resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        out = await bei.fetch_exchange_info()
    assert "BTCUSDT" in out
    assert "BADUSDT" not in out


# ──────────────────────────────────────────────────────────────────────────
# Pure math helpers
# ──────────────────────────────────────────────────────────────────────────


def _btc_info() -> bei.SymbolInfo:
    return bei.SymbolInfo(
        symbol="BTCUSDT", base_asset="BTC", quote_asset="USDT",
        price_precision=2, quantity_precision=3,
        lot_min=0.001, lot_max=1000.0, lot_step=0.001,
        price_tick=0.10, min_notional=100.0,
    )


def _doge_info() -> bei.SymbolInfo:
    return bei.SymbolInfo(
        symbol="DOGEUSDT", base_asset="DOGE", quote_asset="USDT",
        price_precision=5, quantity_precision=0,
        lot_min=1.0, lot_max=30_000_000.0, lot_step=1.0,
        price_tick=0.00001, min_notional=5.0,
    )


def test_round_qty_down_substep_btc() -> None:
    """BTCUSDT stepSize=0.001, qty=0.0234 → 0.023."""
    btc = _btc_info()
    assert bei.round_qty_down(btc, 0.0234) == pytest.approx(0.023, rel=1e-12)


def test_round_qty_down_whole_step_doge() -> None:
    """DOGEUSDT stepSize=1.0, qty=12345.7 → 12345.0."""
    doge = _doge_info()
    assert bei.round_qty_down(doge, 12345.7) == pytest.approx(12345.0, rel=1e-12)


def test_round_qty_down_below_lot_min_returns_zero() -> None:
    """If rounding pushes qty below lot_min, return 0.0 (the order would
    reject anyway — signal that up).
    """
    btc = _btc_info()
    # lot_min=0.001, lot_step=0.001 → floor(0.0005) = 0 → below min → 0.0
    assert bei.round_qty_down(btc, 0.0005) == 0.0


def test_round_qty_down_uses_decimal_no_float_drift() -> None:
    """The canonical float-drift gotcha: 0.7 / 0.1 floats to 6.999999...
    With float division + math.floor this returns 6 (WRONG — should be 7).
    Our Decimal-backed implementation must return 7 * 0.1 = 0.7.
    """
    info = bei.SymbolInfo(
        symbol="X", base_asset="X", quote_asset="USDT",
        price_precision=2, quantity_precision=1,
        lot_min=0.1, lot_max=1000.0, lot_step=0.1,
        price_tick=0.01, min_notional=1.0,
    )
    result = bei.round_qty_down(info, 0.7)
    assert result == pytest.approx(0.7, rel=1e-12)


def test_passes_min_notional_above_threshold() -> None:
    btc = _btc_info()
    # 0.002 BTC at $60k = $120 notional > $100 min → True
    assert bei.passes_min_notional(btc, 0.002, 60000.0) is True


def test_passes_min_notional_below_threshold() -> None:
    btc = _btc_info()
    # 0.001 BTC at $60k = $60 notional < $100 min → False
    assert bei.passes_min_notional(btc, 0.001, 60000.0) is False


def test_passes_min_notional_zero_inputs() -> None:
    btc = _btc_info()
    assert bei.passes_min_notional(btc, 0.0, 60000.0) is False
    assert bei.passes_min_notional(btc, 0.001, 0.0) is False


def test_quantity_from_notional_mid_scope() -> None:
    """Spec example: $100 notional at $50/coin with stepSize=0.001 → 2.000."""
    info = bei.SymbolInfo(
        symbol="X", base_asset="X", quote_asset="USDT",
        price_precision=2, quantity_precision=3,
        lot_min=0.001, lot_max=1000.0, lot_step=0.001,
        price_tick=0.01, min_notional=5.0,
    )
    qty = bei.quantity_from_notional(info, 100.0, 50.0)
    assert qty == pytest.approx(2.000, rel=1e-12)


def test_quantity_from_notional_below_min_notional_returns_zero() -> None:
    """If the requested notional is below the symbol's min, no point computing
    a qty — the order would reject. Return 0.0.
    """
    btc = _btc_info()  # min_notional=$100
    # $10 < $100 min → 0.0 even if qty math would otherwise work
    assert bei.quantity_from_notional(btc, 10.0, 60000.0) == 0.0


def test_quantity_from_notional_bad_inputs() -> None:
    btc = _btc_info()
    assert bei.quantity_from_notional(btc, 100.0, 0.0) == 0.0
    assert bei.quantity_from_notional(btc, 0.0, 60000.0) == 0.0
    assert bei.quantity_from_notional(btc, -50.0, 60000.0) == 0.0


def test_round_price_to_tick() -> None:
    btc = _btc_info()  # price_tick = 0.10
    # 60000.13 → 60000.10
    assert bei.round_price(btc, 60000.13) == pytest.approx(60000.10, rel=1e-12)


# ──────────────────────────────────────────────────────────────────────────
# Cache TTL behavior
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cache_hit_within_ttl_avoids_second_http_call() -> None:
    """Second call within TTL must not trigger a new HTTP request."""
    fake_resp = _make_fake_response(body=_five_markets_body())
    mock_get = AsyncMock(return_value=fake_resp)
    with patch("httpx.AsyncClient.get", new=mock_get):
        out1 = await bei.get_cached_exchange_info()
        out2 = await bei.get_cached_exchange_info()
    assert mock_get.call_count == 1  # second call hit cache
    assert out1 == out2
    assert "BTCUSDT" in out2


@pytest.mark.asyncio
async def test_cache_force_refresh_bypasses_ttl() -> None:
    """force_refresh=True must always hit HTTP, even within TTL."""
    fake_resp = _make_fake_response(body=_five_markets_body())
    mock_get = AsyncMock(return_value=fake_resp)
    with patch("httpx.AsyncClient.get", new=mock_get):
        await bei.get_cached_exchange_info()
        await bei.get_cached_exchange_info(force_refresh=True)
    assert mock_get.call_count == 2


@pytest.mark.asyncio
async def test_cache_expires_after_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    """When the cached timestamp is older than TTL, next call refreshes."""
    fake_resp = _make_fake_response(body=_five_markets_body())
    mock_get = AsyncMock(return_value=fake_resp)
    with patch("httpx.AsyncClient.get", new=mock_get):
        await bei.get_cached_exchange_info()
        # Synthesize cache age beyond TTL by rewinding _cache_ts.
        monkeypatch.setattr(
            bei, "_cache_ts",
            bei._cache_ts - bei.settings.binance_exchange_info_ttl_s - 1,
        )
        await bei.get_cached_exchange_info()
    assert mock_get.call_count == 2


@pytest.mark.asyncio
async def test_cache_refresh_failure_preserves_stale_cache() -> None:
    """A failed refresh must NOT wipe a previously-good cache.

    Mid-strategy, we'd rather serve a 1h-stale filter set than nothing —
    next sweep's refresh will retry. Wiping the cache on transient HTTP
    failure would cascade into all symbols becoming un-tradeable.
    """
    good_resp = _make_fake_response(body=_five_markets_body())
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=good_resp)):
        first = await bei.get_cached_exchange_info()
    assert "BTCUSDT" in first

    # Refresh attempt: HTTP fails. Cache stays populated.
    with patch(
        "httpx.AsyncClient.get",
        new=AsyncMock(side_effect=httpx.HTTPError("network down")),
    ):
        second = await bei.get_cached_exchange_info(force_refresh=True)
    assert "BTCUSDT" in second
    assert second == first


# ──────────────────────────────────────────────────────────────────────────
# Graceful failure (best-effort error handling)
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_http_error_returns_empty_dict_no_raise() -> None:
    """Network exception → {} (never raises). Callers must keep their loop alive."""
    with patch(
        "httpx.AsyncClient.get",
        new=AsyncMock(side_effect=httpx.HTTPError("connect failed")),
    ):
        out = await bei.fetch_exchange_info()
    assert out == {}


@pytest.mark.asyncio
async def test_bad_status_returns_empty_dict() -> None:
    fake_resp = _make_fake_response(status_code=503, body={})
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        out = await bei.fetch_exchange_info()
    assert out == {}


@pytest.mark.asyncio
async def test_bad_json_returns_empty_dict() -> None:
    class _BadJsonResp:
        status_code = 200

        def json(self) -> Any:
            raise ValueError("malformed")

    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=_BadJsonResp())):
        out = await bei.fetch_exchange_info()
    assert out == {}


@pytest.mark.asyncio
async def test_missing_symbols_field_returns_empty_dict() -> None:
    """Body present but `symbols` key absent → {}."""
    fake_resp = _make_fake_response(body={"timezone": "UTC", "serverTime": 0})
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        out = await bei.fetch_exchange_info()
    assert out == {}


@pytest.mark.asyncio
async def test_body_not_a_dict_returns_empty_dict() -> None:
    fake_resp = _make_fake_response(body=["not", "a", "dict"])
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        out = await bei.fetch_exchange_info()
    assert out == {}


# ──────────────────────────────────────────────────────────────────────────
# Cold-cache-on-error edge case
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cold_cache_with_http_error_returns_empty_dict() -> None:
    """When the cache has NEVER been populated and the refresh fails, we
    must return {} (not crash, not raise). The caller will keep retrying.
    """
    with patch(
        "httpx.AsyncClient.get",
        new=AsyncMock(side_effect=httpx.HTTPError("first call fails")),
    ):
        out = await bei.get_cached_exchange_info()
    assert out == {}


# ──────────────────────────────────────────────────────────────────────────
# Sanity: full 5-market round-trip with helper math
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_five_market_round_trip_quantity_calc() -> None:
    """End-to-end: parse 5-market response, compute quantity_from_notional
    for each at a plausible price. Validates the integration of parse +
    rounding helpers.
    """
    fake_resp = _make_fake_response(body=_five_markets_body())
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        info_map = await bei.fetch_exchange_info()
    assert set(info_map.keys()) == {
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT",
    }
    # BTC $100 notional at $60k → 0.001 BTC at floor; passes_min_notional($60 < $100) is FALSE
    btc_qty = bei.quantity_from_notional(info_map["BTCUSDT"], 100.0, 60000.0)
    # quantity_from_notional short-circuits because requested notional == min_notional;
    # actually 100.0 NOT < 100.0 so does NOT short-circuit. raw = 100/60000 = 0.00166...
    # floor to 0.001 = 0.001. lot_min = 0.001 → return 0.001.
    assert btc_qty == pytest.approx(0.001, rel=1e-12)
    # ETH at $3000 with $100 notional → 100/3000 = 0.0333 → floor step 0.001 = 0.033
    eth_qty = bei.quantity_from_notional(info_map["ETHUSDT"], 100.0, 3000.0)
    assert eth_qty == pytest.approx(0.033, rel=1e-12)
    # DOGE at $0.10 with $100 → 100/0.10 = 1000 → step 1.0 → 1000
    doge_qty = bei.quantity_from_notional(info_map["DOGEUSDT"], 100.0, 0.10)
    assert doge_qty == pytest.approx(1000.0, rel=1e-12)
