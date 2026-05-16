"""Tests for the liquidation watcher cycle — pure helpers + cycle orchestrator."""
from __future__ import annotations

import json

import pytest

from gmx_strategies.liquidation_trigger import GMXPosition, LiquidationTrigger
from gmx_strategies.liquidation_watcher import (
    _parse_chainlink_payload,
    build_eval_log_entry,
    run_watch_cycle,
)

# ─── _parse_chainlink_payload (pure) ───────────────────────────────


def test_parse_chainlink_payload_price_field() -> None:
    payload = json.dumps({"price": "80123.45", "ts_unix": "12345"})
    assert _parse_chainlink_payload(payload) == 80123.45


def test_parse_chainlink_payload_mid_fallback() -> None:
    payload = json.dumps({"mid": "1.0"})
    assert _parse_chainlink_payload(payload) == 1.0


def test_parse_chainlink_payload_benchmark_price_fallback() -> None:
    payload = json.dumps({"benchmark_price": "0.42"})
    assert _parse_chainlink_payload(payload) == 0.42


def test_parse_chainlink_payload_empty_or_none() -> None:
    assert _parse_chainlink_payload(None) is None
    assert _parse_chainlink_payload("") is None


def test_parse_chainlink_payload_malformed_json() -> None:
    assert _parse_chainlink_payload("not-json") is None


def test_parse_chainlink_payload_non_object() -> None:
    assert _parse_chainlink_payload(json.dumps([1, 2, 3])) is None
    assert _parse_chainlink_payload(json.dumps("a string")) is None


def test_parse_chainlink_payload_missing_price() -> None:
    assert _parse_chainlink_payload(json.dumps({"other": "field"})) is None


def test_parse_chainlink_payload_zero_or_negative() -> None:
    """A non-positive price is treated as invalid — protect downstream math."""
    assert _parse_chainlink_payload(json.dumps({"price": "0"})) is None
    assert _parse_chainlink_payload(json.dumps({"price": "-100"})) is None


def test_parse_chainlink_payload_non_numeric_price() -> None:
    assert _parse_chainlink_payload(json.dumps({"price": "not-a-num"})) is None


# ─── build_eval_log_entry (pure) ───────────────────────────────────


def test_build_eval_log_entry_shape() -> None:
    pos = GMXPosition(
        user="0xuser", market="btc", is_long=True,
        size_usd=5000.0, collateral_usd=500.0, entry_price=80_000.0,
        leverage=10.0, liquidation_threshold_pct=0.005,
    )
    trig = LiquidationTrigger(
        user="0xuser", market="btc",
        distance_to_liq_pct=-0.5, estimated_fee_usd=100.0,
        confidence=0.9, reason="trigger",
    )
    entry = build_eval_log_entry(
        trigger=trig, pos=pos, current_price=79_500.0, cycle_unix=1_700_000_000,
    )
    # All values must be str (Redis stream requires)
    for k, v in entry.items():
        assert isinstance(v, str), f"{k} is not str: {type(v)}"
    assert entry["user"] == "0xuser"
    assert entry["market"] == "btc"
    assert entry["is_long"] == "1"
    assert entry["size_usd"] == "5000.0000"
    assert entry["entry_price"] == "80000.000000"
    assert entry["current_price"] == "79500.000000"
    assert entry["confidence"] == "0.9000"
    assert entry["reason"] == "trigger"


def test_build_eval_log_entry_short_position() -> None:
    pos = GMXPosition(
        user="0xuser", market="eth", is_long=False,
        size_usd=1000.0, collateral_usd=100.0, entry_price=3000.0,
        leverage=10.0, liquidation_threshold_pct=0.005,
    )
    trig = LiquidationTrigger(
        user="0xuser", market="eth",
        distance_to_liq_pct=0.0, estimated_fee_usd=50.0,
        confidence=0.5, reason="trigger",
    )
    entry = build_eval_log_entry(
        trigger=trig, pos=pos, current_price=3300.0, cycle_unix=0,
    )
    assert entry["is_long"] == "0"


# ─── run_watch_cycle (orchestrator) ────────────────────────────────


_BTC_MARKET = "0x47c031236e19d024b42f8ae6780e44a573170703"


def _gql_row(*, pid: str, market: str = _BTC_MARKET, size: float = 5000.0,
             col: float = 500.0, is_long: bool = True,
             entry_price: float = 80_000.0) -> dict:
    return {
        "id": pid,
        "account": f"0xuser_{pid}",
        "market": market,
        "isLong": is_long,
        # 1e30 USD precision
        "sizeInUsd": str(int(size * 1e30)),
        "collateralUsd": str(int(col * 1e30)),
        "entryPrice": str(int(entry_price * 1e30)),
    }


class _FakeHttpx:
    def __init__(self, pages: list[list[dict]]) -> None:
        self.pages = pages

    async def post(self, url: str, *, json: dict):
        skip = int(json.get("variables", {}).get("skip", 0))
        first = int(json.get("variables", {}).get("first", 200))
        page_idx = skip // first
        if page_idx >= len(self.pages):
            return _FakeResp(200, {"data": {"positions": []}})
        return _FakeResp(200, {"data": {"positions": self.pages[page_idx]}})


class _FakeResp:
    def __init__(self, status: int, body: dict):
        self.status_code = status
        self._body = body

    def json(self):
        return self._body


class _FakeRedis:
    def __init__(self, prices: dict[str, dict] | None = None) -> None:
        self.prices = prices or {}
        self.xadds: list[tuple[str, dict]] = []

    async def get(self, key: str):
        for alias, payload in self.prices.items():
            if f":{alias}:" in key:
                return json.dumps(payload)
        return None

    async def xadd(self, stream: str, fields: dict, *, maxlen: int,
                   approximate: bool = True):
        self.xadds.append((stream, fields))
        return f"{stream}-id-{len(self.xadds)}"


@pytest.mark.asyncio
async def test_run_watch_cycle_triggers_under_water_position() -> None:
    """Under-water position emits a trigger and XADD."""
    # Long at $80k, 10x leverage, $500 collateral on $5k size.
    # Liq price ≈ 80k * (1 - (0.1 - 0.005)) = $72.4k
    # Current $70k → under water → trigger
    pages = [[_gql_row(pid="p1")]]
    http = _FakeHttpx(pages)
    redis = _FakeRedis(prices={"btc": {"price": "70000.0"}})

    stats = await run_watch_cycle(
        httpx_client=http, redis_client=redis,
        subgraph_url="https://example.com/subgraph",
        eval_log_stream="gmx:eval_log", eval_log_maxlen=1000,
        watch_margin=1.05, estimated_fee_usd=100.0,
    )
    assert stats.fetched == 1
    assert stats.triggers == 1
    assert len(redis.xadds) == 1
    stream, fields = redis.xadds[0]
    assert stream == "gmx:eval_log"
    assert fields["market"] == "btc"


@pytest.mark.asyncio
async def test_run_watch_cycle_safe_position_no_trigger() -> None:
    """Position with healthy margin — no trigger."""
    pages = [[_gql_row(pid="p1")]]
    http = _FakeHttpx(pages)
    redis = _FakeRedis(prices={"btc": {"price": "85000.0"}})   # well above liq

    stats = await run_watch_cycle(
        httpx_client=http, redis_client=redis,
        subgraph_url="https://example.com",
        eval_log_stream="gmx:eval_log", eval_log_maxlen=1000,
        watch_margin=1.05, estimated_fee_usd=100.0,
    )
    assert stats.fetched == 1
    assert stats.triggers == 0
    assert redis.xadds == []


@pytest.mark.asyncio
async def test_run_watch_cycle_skips_unknown_market() -> None:
    pages = [[_gql_row(pid="p1", market="0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")]]
    http = _FakeHttpx(pages)
    redis = _FakeRedis(prices={"btc": {"price": "70000.0"}})

    stats = await run_watch_cycle(
        httpx_client=http, redis_client=redis,
        subgraph_url="https://example.com",
        eval_log_stream="gmx:eval_log", eval_log_maxlen=1000,
        watch_margin=1.05, estimated_fee_usd=100.0,
    )
    assert stats.fetched == 1
    assert stats.no_alias == 1
    assert stats.triggers == 0


@pytest.mark.asyncio
async def test_run_watch_cycle_skips_missing_price() -> None:
    pages = [[_gql_row(pid="p1")]]
    http = _FakeHttpx(pages)
    redis = _FakeRedis(prices={})    # no chainlink:btc:latest

    stats = await run_watch_cycle(
        httpx_client=http, redis_client=redis,
        subgraph_url="https://example.com",
        eval_log_stream="gmx:eval_log", eval_log_maxlen=1000,
        watch_margin=1.05, estimated_fee_usd=100.0,
    )
    assert stats.fetched == 1
    assert stats.no_price == 1
    assert stats.triggers == 0


@pytest.mark.asyncio
async def test_run_watch_cycle_empty_subgraph() -> None:
    """Subgraph returns no positions → stats are all zero."""
    pages: list[list[dict]] = [[]]
    http = _FakeHttpx(pages)
    redis = _FakeRedis()

    stats = await run_watch_cycle(
        httpx_client=http, redis_client=redis,
        subgraph_url="https://example.com",
        eval_log_stream="gmx:eval_log", eval_log_maxlen=1000,
        watch_margin=1.05, estimated_fee_usd=100.0,
    )
    assert stats.fetched == 0
    assert stats.triggers == 0


@pytest.mark.asyncio
async def test_run_watch_cycle_paper_execution_disabled_by_default() -> None:
    """Without execution_paper_enabled, only eval-log XADDs happen — no
    `gmx:execution:paper_log` writes. Preserves the original v0.2 contract."""
    pages = [[_gql_row(pid="p1")]]
    http = _FakeHttpx(pages)
    redis = _FakeRedis(prices={"btc": {"price": "70000.0"}})

    stats = await run_watch_cycle(
        httpx_client=http, redis_client=redis,
        subgraph_url="https://example.com",
        eval_log_stream="gmx:eval_log", eval_log_maxlen=1000,
        watch_margin=1.05, estimated_fee_usd=100.0,
    )
    assert stats.triggers == 1
    assert stats.executions_paper == 0
    assert stats.executions_rejected == 0
    # Only the eval-log XADD — no execution stream write.
    streams = [s for s, _ in redis.xadds]
    assert streams == ["gmx:eval_log"]


@pytest.mark.asyncio
async def test_run_watch_cycle_paper_execution_fires_when_enabled() -> None:
    """With execution_paper_enabled, a passing trigger writes BOTH the
    eval-log AND `gmx:execution:paper_log`. Size $5k → fee $25 net $23.5
    fails default $50 gate, so we pass a generous size to clear it."""
    # $20k size → fee $100 → net $98.5 (above $50 gate)
    pages = [[_gql_row(pid="p1", size=20_000.0, col=2000.0)]]
    http = _FakeHttpx(pages)
    redis = _FakeRedis(prices={"btc": {"price": "70000.0"}})

    stats = await run_watch_cycle(
        httpx_client=http, redis_client=redis,
        subgraph_url="https://example.com",
        eval_log_stream="gmx:eval_log", eval_log_maxlen=1000,
        watch_margin=1.05, estimated_fee_usd=100.0,
        execution_paper_enabled=True,
        execution_paper_log_stream="gmx:execution:paper_log",
        execution_min_net_profit_usd=50.0,
        execution_min_confidence=0.5,
    )
    assert stats.triggers == 1
    assert stats.executions_paper == 1
    assert stats.executions_rejected == 0
    streams = [s for s, _ in redis.xadds]
    assert "gmx:eval_log" in streams
    assert "gmx:execution:paper_log" in streams
    # Execution record has the expected shape
    exec_fields = next(f for s, f in redis.xadds if s == "gmx:execution:paper_log")
    assert exec_fields["mode"] == "paper"
    assert exec_fields["market"] == "btc"
    assert float(exec_fields["expected_net_pnl_usd"]) > 50.0


@pytest.mark.asyncio
async def test_run_watch_cycle_paper_execution_rejected_by_gate() -> None:
    """Trigger fires (eval-logged) but is below the net-profit gate →
    execution_paper stream is NOT written; rejected counter increments."""
    # Tiny size → fee well below the $50 gate
    pages = [[_gql_row(pid="p1", size=1_000.0, col=100.0)]]
    http = _FakeHttpx(pages)
    redis = _FakeRedis(prices={"btc": {"price": "70000.0"}})

    stats = await run_watch_cycle(
        httpx_client=http, redis_client=redis,
        subgraph_url="https://example.com",
        eval_log_stream="gmx:eval_log", eval_log_maxlen=1000,
        watch_margin=1.05, estimated_fee_usd=100.0,
        execution_paper_enabled=True,
        execution_min_net_profit_usd=50.0,
        execution_min_confidence=0.5,
    )
    assert stats.triggers == 1
    assert stats.executions_paper == 0
    assert stats.executions_rejected == 1
    streams = [s for s, _ in redis.xadds]
    assert streams == ["gmx:eval_log"]


@pytest.mark.asyncio
async def test_run_watch_cycle_caches_prices_per_alias() -> None:
    """Multiple positions on the same market should hit Redis once per alias."""
    pages = [[
        _gql_row(pid="p1"),
        _gql_row(pid="p2"),
        _gql_row(pid="p3"),
    ]]
    http = _FakeHttpx(pages)

    redis_get_calls: list[str] = []

    class _CountingRedis(_FakeRedis):
        async def get(self, key: str):
            redis_get_calls.append(key)
            return await super().get(key)

    redis = _CountingRedis(prices={"btc": {"price": "85000.0"}})
    await run_watch_cycle(
        httpx_client=http, redis_client=redis,
        subgraph_url="https://example.com",
        eval_log_stream="gmx:eval_log", eval_log_maxlen=1000,
        watch_margin=1.05, estimated_fee_usd=100.0,
    )
    # 3 positions, same market (btc) — only ONE Redis GET expected.
    assert redis_get_calls.count("chainlink:btc:latest") == 1
