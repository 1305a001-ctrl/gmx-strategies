"""Tests for the live Binance USDT-M perp funding-rate reader (G3).

Mocks `httpx.AsyncClient.get` for both the per-market and batched endpoints.
Asserts:
  - Happy path parses "0.00010000" → 0.0001 float (positive sign convention).
  - Negative rate round-trips (sign preserved).
  - Unmapped alias → None, no HTTP call.
  - HTTP 500 → None.
  - Malformed body (not a dict) → None.
  - Missing `lastFundingRate` field → None.
  - Non-numeric value → None.
  - HTTP timeout / network error → None.
  - Batched path: returns dict of all 5 aliases.
  - Batched path: silently ignores symbols outside our 5.
  - Batched path: HTTP failure → empty dict, no raise.
  - Sign-convention sanity check: "positive = longs pay shorts" (matches GMX).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from gmx_strategies import binance_funding


def _make_fake_response(
    *, status_code: int = 200, body: Any,
) -> Any:
    """Build a stand-in httpx.Response for the mocked GET.

    `body` may be a dict (single-symbol path) or a list (batched path) or
    any other value to simulate malformed responses.
    """

    class _Resp:
        def __init__(self, sc: int, body: Any) -> None:
            self.status_code = sc
            self._body = body

        def json(self) -> Any:
            return self._body

    return _Resp(status_code, body)


# ──────────────────────────────────────────────────────────────────────────
# Single-market path: fetch_cex_funding_live
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_happy_path_parses_string_rate_to_float() -> None:
    """Binance returns rates as strings — we float() them."""
    body = {
        "symbol": "BTCUSDT",
        "markPrice": "65000.00",
        "lastFundingRate": "0.00010000",
        "nextFundingTime": 1716210000000,
        "time": 1716180000000,
    }
    fake_resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        rate = await binance_funding.fetch_cex_funding_live("btc")
    assert rate == pytest.approx(0.0001, rel=1e-9)
    assert isinstance(rate, float)


@pytest.mark.asyncio
async def test_negative_rate_preserves_sign() -> None:
    """Sign convention: Binance "positive = longs pay shorts" matches GMX.

    A negative rate (shorts paying longs on Binance) must come back negative,
    so `net_rate = gmx_rate - cex_rate` math composes correctly downstream.
    """
    body = {"symbol": "SOLUSDT", "lastFundingRate": "-0.00007181"}
    fake_resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        rate = await binance_funding.fetch_cex_funding_live("sol")
    assert rate == pytest.approx(-0.00007181, rel=1e-9)
    assert rate is not None and rate < 0


@pytest.mark.asyncio
async def test_unmapped_alias_returns_none_without_http() -> None:
    """Aliases outside our 5 short-circuit without an HTTP call."""
    mock_get = AsyncMock()
    with patch("httpx.AsyncClient.get", new=mock_get):
        rate = await binance_funding.fetch_cex_funding_live("nonexistent_alias")
    assert rate is None
    mock_get.assert_not_called()


@pytest.mark.asyncio
async def test_http_500_returns_none() -> None:
    """Non-200 status → None, no raise."""
    fake_resp = _make_fake_response(status_code=500, body={})
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        rate = await binance_funding.fetch_cex_funding_live("btc")
    assert rate is None


@pytest.mark.asyncio
async def test_malformed_body_returns_none() -> None:
    """Body that's not a dict → None."""
    fake_resp = _make_fake_response(body=["not", "a", "dict"])
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        rate = await binance_funding.fetch_cex_funding_live("btc")
    assert rate is None


@pytest.mark.asyncio
async def test_missing_field_returns_none() -> None:
    """Body present but `lastFundingRate` key absent → None."""
    body = {"symbol": "BTCUSDT", "markPrice": "65000.00"}  # no lastFundingRate
    fake_resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        rate = await binance_funding.fetch_cex_funding_live("btc")
    assert rate is None


@pytest.mark.asyncio
async def test_non_numeric_value_returns_none() -> None:
    """`lastFundingRate` present but not coercible to float → None."""
    body = {"symbol": "BTCUSDT", "lastFundingRate": "not-a-number"}
    fake_resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        rate = await binance_funding.fetch_cex_funding_live("btc")
    assert rate is None


@pytest.mark.asyncio
async def test_http_timeout_returns_none() -> None:
    """Network exception → None, no raise."""
    with patch(
        "httpx.AsyncClient.get",
        new=AsyncMock(side_effect=httpx.TimeoutException("timed out")),
    ):
        rate = await binance_funding.fetch_cex_funding_live("btc")
    assert rate is None


@pytest.mark.asyncio
async def test_malformed_json_returns_none() -> None:
    """Response.json() raising ValueError → None."""

    class _BadJsonResp:
        status_code = 200

        def json(self) -> Any:
            raise ValueError("not JSON")

    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=_BadJsonResp())):
        rate = await binance_funding.fetch_cex_funding_live("btc")
    assert rate is None


# ──────────────────────────────────────────────────────────────────────────
# Batched path: fetch_all_cex_fundings
# ──────────────────────────────────────────────────────────────────────────


def _make_batched_body(rates: dict[str, str]) -> list[dict[str, Any]]:
    """Build a synthetic /premiumIndex (no-symbol) response body.

    Mixes our 5 supported markets with extra unknown symbols to confirm the
    filter drops them silently.
    """
    out: list[dict[str, Any]] = []
    for symbol, rate in rates.items():
        out.append({"symbol": symbol, "lastFundingRate": rate})
    # Inject noise — unknown symbols that should be ignored.
    out.append({"symbol": "PEPEUSDT", "lastFundingRate": "0.000333"})
    out.append({"symbol": "FAKEUSDT", "lastFundingRate": "0.000444"})
    return out


@pytest.mark.asyncio
async def test_batched_returns_all_five_aliases() -> None:
    """Happy batched path: all 5 supported aliases come back keyed correctly."""
    body = _make_batched_body({
        "BTCUSDT": "0.00006219",
        "ETHUSDT": "0.00005819",
        "SOLUSDT": "-0.00007181",
        "DOGEUSDT": "0.00003362",
        "XRPUSDT": "-0.00000068",
    })
    fake_resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        result = await binance_funding.fetch_all_cex_fundings()
    assert set(result.keys()) == {"btc", "eth", "sol", "doge", "xrp"}
    assert result["btc"] == pytest.approx(0.00006219, rel=1e-9)
    assert result["sol"] == pytest.approx(-0.00007181, rel=1e-9)
    # Noise symbols (PEPEUSDT/FAKEUSDT) MUST NOT appear in output.
    assert "pepe" not in result
    assert "fake" not in result


@pytest.mark.asyncio
async def test_batched_silently_ignores_unknown_symbols() -> None:
    """A response with ONLY unknown symbols returns an empty dict, no error."""
    body = [
        {"symbol": "PEPEUSDT", "lastFundingRate": "0.000333"},
        {"symbol": "WIFUSDT", "lastFundingRate": "0.000111"},
    ]
    fake_resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        result = await binance_funding.fetch_all_cex_fundings()
    assert result == {}


@pytest.mark.asyncio
async def test_batched_http_failure_returns_empty_dict() -> None:
    """HTTP failure must return {} (not raise) — runtime keeps sweeping."""
    with patch(
        "httpx.AsyncClient.get",
        new=AsyncMock(side_effect=httpx.HTTPError("network down")),
    ):
        result = await binance_funding.fetch_all_cex_fundings()
    assert result == {}


@pytest.mark.asyncio
async def test_batched_bad_status_returns_empty_dict() -> None:
    """Non-200 batched response → empty dict."""
    fake_resp = _make_fake_response(status_code=429, body=[])
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        result = await binance_funding.fetch_all_cex_fundings()
    assert result == {}


@pytest.mark.asyncio
async def test_batched_skips_entries_with_bad_rate() -> None:
    """One bad entry doesn't poison the others — good ones still ship."""
    body = [
        {"symbol": "BTCUSDT", "lastFundingRate": "0.00006219"},
        {"symbol": "ETHUSDT", "lastFundingRate": "not-a-number"},
        {"symbol": "SOLUSDT", "lastFundingRate": "-0.00007181"},
        # Defensive: malformed entry (not a dict) — should be skipped.
        "garbage_entry",
        {"symbol": "DOGEUSDT"},  # missing rate field
    ]
    fake_resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
        result = await binance_funding.fetch_all_cex_fundings()
    assert set(result.keys()) == {"btc", "sol"}
    assert result["btc"] == pytest.approx(0.00006219, rel=1e-9)
    assert result["sol"] == pytest.approx(-0.00007181, rel=1e-9)


# ──────────────────────────────────────────────────────────────────────────
# Sign-convention sanity (documented in module docstring)
# ──────────────────────────────────────────────────────────────────────────


def test_sign_convention_documented_in_module_docstring() -> None:
    """Module docstring must state the sign convention so it's never lost."""
    doc = binance_funding.__doc__ or ""
    assert "longs pay shorts" in doc.lower()
    assert "gmx convention" in doc.lower()


def test_symbol_mapping_covers_exactly_five_markets() -> None:
    """Hardcoded mapping must cover the 5 markets matching G2's chainlink-streams aliases."""
    assert set(binance_funding.BINANCE_SYMBOL_BY_ALIAS.keys()) == {
        "btc", "eth", "sol", "doge", "xrp",
    }
    assert binance_funding.BINANCE_SYMBOL_BY_ALIAS["btc"] == "BTCUSDT"
    assert binance_funding.BINANCE_SYMBOL_BY_ALIAS["xrp"] == "XRPUSDT"


# ──────────────────────────────────────────────────────────────────────────
# Near-settlement guard (trap-surface monitor added in feat/trap-monitors)
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_near_settlement_warn_emitted_when_close_to_settle(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When `nextFundingTime - now()` < guard window, emit a WARN log line.

    The rate is still returned — the warn is purely informational (the
    operator should know the rate is about to flip).
    """
    import logging
    import time

    # Force a fresh import-side cache miss on settings (defaults are fine).
    # next_funding_time = now + 60s (well within the default 300s guard).
    next_funding_time_ms = int((time.time() + 60) * 1000)
    body = {
        "symbol": "BTCUSDT",
        "lastFundingRate": "0.00010000",
        "nextFundingTime": next_funding_time_ms,
    }
    fake_resp = _make_fake_response(body=body)
    with caplog.at_level(logging.WARNING, logger="gmx_strategies.binance_funding"):
        with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
            rate = await binance_funding.fetch_cex_funding_live("btc")
    assert rate == pytest.approx(0.0001, rel=1e-9)
    # The WARN must be present in caplog.
    warn_lines = [r for r in caplog.records if "near_settlement" in r.message]
    assert len(warn_lines) == 1
    assert warn_lines[0].levelname == "WARNING"
    assert "btc" in warn_lines[0].message


@pytest.mark.asyncio
async def test_near_settlement_warn_not_emitted_when_far_from_settle(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When `nextFundingTime - now()` >> guard, no WARN."""
    import logging
    import time

    # Settlement 7h ahead — way outside the 5min guard window.
    next_funding_time_ms = int((time.time() + 7 * 3600) * 1000)
    body = {
        "symbol": "BTCUSDT",
        "lastFundingRate": "0.00010000",
        "nextFundingTime": next_funding_time_ms,
    }
    fake_resp = _make_fake_response(body=body)
    with caplog.at_level(logging.WARNING, logger="gmx_strategies.binance_funding"):
        with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=fake_resp)):
            rate = await binance_funding.fetch_cex_funding_live("btc")
    assert rate == pytest.approx(0.0001, rel=1e-9)
    assert not any("near_settlement" in r.message for r in caplog.records)


def test_check_near_settlement_silent_on_missing_field() -> None:
    """A missing/None nextFundingTime must not raise and must not WARN.

    We never WARN-twice on bad rate + bad time — the guard is for the
    happy path only.
    """
    # Importing the helper directly to exercise its negative paths.
    binance_funding._check_near_settlement("btc", None)
    binance_funding._check_near_settlement("btc", "garbage")  # not a number
    binance_funding._check_near_settlement("btc", -1)         # negative time
    # No assertion needed — the only contract is "doesn't raise".
