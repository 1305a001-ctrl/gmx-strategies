"""Tests for the funding-arb runtime loop (paper mode).

The pure helpers are covered in test_funding_arb.py. These tests exercise
the wiring: per-market fetch -> detect_signal -> emit + graceful degradation
on fetcher errors.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from gmx_strategies import funding_arb_runtime as runtime_mod
from gmx_strategies.funding_arb import FundingState


class _FakeRedis:
    """Minimal async stub for redis.asyncio.Redis used by _emit_signal."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []
        self.xadded: list[tuple[str, dict[str, str]]] = []

    async def publish(self, channel: str, body: str) -> int:
        self.published.append((channel, body))
        return 1

    async def xadd(
        self,
        stream: str,
        fields: dict[str, str],
        *,
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        # match signature loosely; we only need to record the call
        _ = (maxlen, approximate)
        self.xadded.append((stream, fields))
        return "0-0"


@pytest.fixture
def fake_redis(monkeypatch: pytest.MonkeyPatch) -> _FakeRedis:
    """Patch the runtime's redis client factory `r()` with a fake."""
    fake = _FakeRedis()
    monkeypatch.setattr(runtime_mod, "r", lambda: fake)
    return fake


async def _gmx_fetcher_fires(market: str) -> FundingState:
    """Mock GMX fetcher: returns funding well above threshold so signal fires."""
    return FundingState(
        market=market,
        longs_oi_usd=90_000_000.0,
        shorts_oi_usd=10_000_000.0,
        funding_rate_per_8h=0.001,  # well above default 0.0005
    )


async def _gmx_fetcher_quiet(market: str) -> FundingState:
    """Mock GMX fetcher: returns funding below threshold — no signal."""
    return FundingState(
        market=market,
        longs_oi_usd=5_000_000.0,
        shorts_oi_usd=5_000_000.0,
        funding_rate_per_8h=0.0001,
    )


async def _cex_fetcher_zero(symbol: str) -> float:
    _ = symbol
    return 0.0


async def _gmx_fetcher_raises(market: str) -> FundingState:
    raise RuntimeError(f"simulated rpc failure for {market}")


@pytest.mark.asyncio
async def test_runtime_calls_detect_signal_per_market(
    fake_redis: _FakeRedis,
) -> None:
    """detect_signal must run once per resolved market per sweep."""
    calls: list[str] = []

    async def gmx(market: str) -> FundingState:
        calls.append(market)
        return await _gmx_fetcher_quiet(market)

    await runtime_mod.run_funding_arb_runtime(
        gmx_fetcher=gmx,
        cex_fetcher=_cex_fetcher_zero,
        iterations=1,
    )

    # Should have polled at least btc + eth + sol (the verified Arbitrum
    # markets currently in markets.py); doge/xrp will be skipped silently
    # until their addresses are added in a follow-up PR.
    resolved = runtime_mod._resolve_markets()
    assert calls == resolved
    assert "btc" in calls
    assert "eth" in calls
    assert "sol" in calls
    # Below-threshold => no emit
    assert fake_redis.published == []
    assert fake_redis.xadded == []


@pytest.mark.asyncio
async def test_runtime_emits_when_funding_exceeds_threshold(
    fake_redis: _FakeRedis,
) -> None:
    """Funding above min_rate must emit to pub/sub AND eval-log stream."""
    await runtime_mod.run_funding_arb_runtime(
        gmx_fetcher=_gmx_fetcher_fires,
        cex_fetcher=_cex_fetcher_zero,
        iterations=1,
    )

    resolved = runtime_mod._resolve_markets()
    assert len(fake_redis.published) == len(resolved)
    assert len(fake_redis.xadded) == len(resolved)

    # Inspect one payload — direction must be short_gmx_long_cex (longs pay)
    channel, body = fake_redis.published[0]
    assert channel == "funding_arb:signals"
    import json

    payload: dict[str, Any] = json.loads(body)
    assert payload["direction"] == "short_gmx_long_cex"
    assert payload["mode"] == "paper"
    assert payload["market"] in resolved
    assert payload["funding_rate_per_8h"] == pytest.approx(0.001)

    # And the xadded record carries the same market
    stream, fields = fake_redis.xadded[0]
    assert stream == "funding_arb:eval_log"
    assert fields["mode"] == "paper"
    assert fields["market"] in resolved


@pytest.mark.asyncio
async def test_runtime_does_not_emit_when_below_threshold(
    fake_redis: _FakeRedis,
) -> None:
    await runtime_mod.run_funding_arb_runtime(
        gmx_fetcher=_gmx_fetcher_quiet,
        cex_fetcher=_cex_fetcher_zero,
        iterations=1,
    )
    assert fake_redis.published == []
    assert fake_redis.xadded == []


@pytest.mark.asyncio
async def test_runtime_survives_fetcher_exception(
    fake_redis: _FakeRedis,
) -> None:
    """A single market's fetcher raising must not kill the loop."""
    call_log: list[str] = []

    async def selectively_failing_gmx(market: str) -> FundingState:
        call_log.append(market)
        if market == "btc":
            raise RuntimeError("rpc 500")
        return await _gmx_fetcher_fires(market)

    # If the loop crashed, this would raise — completion of the await is
    # itself part of the assertion.
    await runtime_mod.run_funding_arb_runtime(
        gmx_fetcher=selectively_failing_gmx,
        cex_fetcher=_cex_fetcher_zero,
        iterations=1,
    )

    # btc fetcher was attempted and failed; eth + sol still proceeded.
    assert "btc" in call_log
    assert "eth" in call_log
    assert "sol" in call_log

    # No emit for btc; emits for the other markets.
    emitted_markets = {
        next(
            (
                f["market"]
                for f_stream, f in fake_redis.xadded
                if f_stream == "funding_arb:eval_log"
            ),
            None,
        )
    }
    # Easier: collect set of markets across all xadded rows
    emitted_markets = {f["market"] for _stream, f in fake_redis.xadded}
    assert "btc" not in emitted_markets
    assert "eth" in emitted_markets


@pytest.mark.asyncio
async def test_runtime_uses_default_fetchers_when_none_passed(
    fake_redis: _FakeRedis,
) -> None:
    """When fetchers aren't injected, module-level paper stubs are used."""
    with (
        patch.object(
            runtime_mod, "fetch_gmx_funding", AsyncMock(side_effect=_gmx_fetcher_quiet)
        ) as gmx_spy,
        patch.object(
            runtime_mod, "fetch_cex_funding", AsyncMock(side_effect=_cex_fetcher_zero)
        ) as cex_spy,
    ):
        await runtime_mod.run_funding_arb_runtime(iterations=1)

    resolved = runtime_mod._resolve_markets()
    assert gmx_spy.await_count == len(resolved)
    assert cex_spy.await_count == len(resolved)


def test_resolve_markets_filters_to_arbitrum_markets() -> None:
    """Only aliases present in ARBITRUM_MARKETS survive the resolve filter."""
    resolved = runtime_mod._resolve_markets()
    # btc/eth/sol are committed; doge/xrp may or may not be present depending
    # on whether their addresses have been verified yet.
    assert "btc" in resolved
    assert "eth" in resolved
    assert "sol" in resolved
    for alias in resolved:
        # Every resolved alias must exist in ARBITRUM_MARKETS by contract.
        from gmx_strategies.markets import ARBITRUM_MARKETS

        assert alias in ARBITRUM_MARKETS
