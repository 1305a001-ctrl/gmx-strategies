"""Tests for the G7.1 funding-arb consumer (`funding_arb_executor.py`).

Asserts the per-signal pipeline contract:
  - Happy path: guard allows + reconcile PROCEED → both legs dry-run →
    ExecutionRecord with success_both_legs=True.
  - Guard denial: any gate failing → guard_block populated, no leg
    submits, success_both_legs=False, ExecutionRecord still XADD'd.
  - Reconcile ABORT: opposite-side position → reconcile_block populated,
    no submit calls.
  - Partial failure: GMX dry-run OK, Binance error → success=False,
    error composed from both legs.
  - Position-reader failure: empty positions returned → reconcile
    PROCEED (no conflict) → consumer attempts both legs.
  - Direction mapping: short_gmx_long_cex → is_long=False + BUY;
    long_gmx_short_cex → is_long=True + SELL.
  - Loop respects stop_event.
  - Loss recording: GMX result with status=0 → pilot_guard.record_loss called.
  - Loss recording: Binance result with error_code=-2019 → record_loss called.
  - Main.py: consumer-disabled path → executor.run NOT in tasks.
  - Main.py: consumer-enabled path → executor.run IS in tasks.
  - bad direction → error record, no submits.
  - bad message JSON → no crash, log only.
  - exception inside handle_signal → captured, record still XADD'd.

The Binance/GMX modules' own tests cover their per-leg gates; this
file asserts the consumer composes them correctly.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from gmx_strategies.binance_order import OrderResult
from gmx_strategies.funding_arb_executor import FundingArbExecutor
from gmx_strategies.gmx_order_encoder import OrderIntent, SimulationResult
from gmx_strategies.gmx_position_reader import Position
from gmx_strategies.gmx_signer import SendResult
from gmx_strategies.pilot_guard import GuardResult
from gmx_strategies.settings import settings

# ──────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────

_EXECUTOR_ADDRESS = "0x19E7E376E7C213B7E7e7e46cc70A5dD086DAff2A"
_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"


# ──────────────────────────────────────────────────────────────────────────
# Fakes — minimal stubs matching the real modules' surface
# ──────────────────────────────────────────────────────────────────────────


class _FakeRedis:
    """Minimal async stub: records xadd calls."""

    def __init__(self) -> None:
        self.xadded: list[tuple[str, dict[Any, Any]]] = []

    async def xadd(
        self,
        stream: str,
        fields: dict[Any, Any],
        *,
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        _ = (maxlen, approximate)
        self.xadded.append((stream, dict(fields)))
        return "0-0"


class _FakeGuard:
    """PilotGuard double — programmable per-call result."""

    def __init__(self, result: GuardResult | None = None) -> None:
        self.calls: list[tuple[str, float]] = []
        self._result = result or GuardResult(
            allowed=True, reason=None, gate=None,
        )

    async def check(
        self,
        market: str,
        notional_usd: float,
        *,
        side: str | None = None,
    ) -> GuardResult:
        _ = side
        self.calls.append((market, notional_usd))
        return self._result


# ──────────────────────────────────────────────────────────────────────────
# Common fixtures + helpers
# ──────────────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _executor_address(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch gmx_signer.get_executor_address → known address."""
    monkeypatch.setattr(
        "gmx_strategies.funding_arb_executor.gmx_signer.get_executor_address",
        lambda: _EXECUTOR_ADDRESS,
    )


@pytest.fixture
def fake_redis() -> _FakeRedis:
    return _FakeRedis()


@pytest.fixture
def allow_guard() -> _FakeGuard:
    return _FakeGuard(GuardResult(allowed=True, reason=None, gate=None))


@pytest.fixture
def deny_guard() -> _FakeGuard:
    return _FakeGuard(
        GuardResult(allowed=False, reason="default-deny", gate="not_armed"),
    )


def _good_sim() -> SimulationResult:
    return SimulationResult(
        ok=True, revert_selector=None, revert_known_acceptable=False,
        revert_reason_name=None, raw_response="0x",
    )


def _dry_run_send_result() -> SendResult:
    """Successful dry-run SendResult — sim ok, no broadcast."""
    return SendResult(
        submitted=False, dry_run_simulation=_good_sim(),
        tx_hash=None, block_number=None, gas_used=None, status=None,
        error=None,
    )


def _dry_run_order_result() -> OrderResult:
    """Successful dry-run OrderResult — dry_run_request populated."""
    return OrderResult(
        submitted=False,
        dry_run_request={"symbol": "SOLUSDT", "side": "BUY", "quantity": 0.07},
        order_id=None,
        client_order_id="gmx-strategies-test123",
        status=None,
        executed_qty=None,
        avg_price=None,
        cum_quote=None,
        error_code=None,
        error_msg=None,
        gate_blocked=None,
    )


def _binance_error_result(code: int, msg: str) -> OrderResult:
    return OrderResult(
        submitted=False, dry_run_request=None, order_id=None,
        client_order_id="gmx-strategies-test456",
        status=None, executed_qty=None, avg_price=None, cum_quote=None,
        error_code=code, error_msg=msg, gate_blocked=None,
    )


def _build_executor(
    *,
    guard: _FakeGuard,
    fake_redis: _FakeRedis,
    positions: list[Position] | None = None,
    position_raise: BaseException | None = None,
    gmx_send_result: SendResult | None = None,
    binance_order_result: OrderResult | None = None,
    gmx_sign_raise: BaseException | None = None,
    mark_price: float | None = 150.0,
) -> FundingArbExecutor:
    """Build a FundingArbExecutor with the supplied test doubles."""
    positions = positions or []

    async def _pos_reader(account: str) -> list[Position]:
        _ = account
        if position_raise is not None:
            raise position_raise
        return positions

    async def _sign_fn(intent: OrderIntent) -> dict[str, Any]:
        _ = intent
        if gmx_sign_raise is not None:
            raise gmx_sign_raise
        return {
            "raw": "0xabc",
            "hash": "0xhash",
            "nonce": 1,
            "from": _EXECUTOR_ADDRESS,
            "tx_dict": {"to": "0x0", "value": 0, "data": "0x"},
        }

    async def _submit_fn(
        signed_tx: dict[str, Any], intent: OrderIntent, *, dry_run: bool,
    ) -> SendResult:
        _ = (signed_tx, intent, dry_run)
        return gmx_send_result if gmx_send_result is not None else _dry_run_send_result()

    async def _binance_fn(
        symbol: str, side: str, notional_usd: float,
        *, mark_price: float, dry_run: bool,
        client_order_id: str | None = None,
        reduce_only: bool = False,
    ) -> OrderResult:
        _ = (symbol, side, notional_usd, mark_price, dry_run, client_order_id, reduce_only)
        return (
            binance_order_result if binance_order_result is not None
            else _dry_run_order_result()
        )

    async def _mark_price_fn(symbol: str) -> float | None:
        _ = symbol
        return mark_price

    return FundingArbExecutor(
        guard=guard,
        gmx_position_reader_fn=_pos_reader,
        gmx_sign_fn=_sign_fn,
        gmx_submit_fn=_submit_fn,
        binance_order_fn=_binance_fn,
        redis_client=fake_redis,
        mark_price_fn=_mark_price_fn,
    )


def _signal(
    *,
    market: str = "sol",
    direction: str = "short_gmx_long_cex",
    target_position_usd: float = 10.0,
) -> dict[str, Any]:
    return {
        "ts": 0,
        "market": market,
        "direction": direction,
        "funding_rate_per_8h": 0.001,
        "annualized_yield_pct": 109.5,
        "target_position_usd": target_position_usd,
        "cex_rate_per_8h": 0.0,
        "net_rate_per_8h": 0.001,
        "cex_source": "mock",
        "mode": "paper",
    }


# ──────────────────────────────────────────────────────────────────────────
# 1) Happy path — guard allows + reconcile PROCEED → both legs dry-run
# ──────────────────────────────────────────────────────────────────────────


async def test_happy_path_both_legs_dry_run_success(
    monkeypatch: pytest.MonkeyPatch,
    allow_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """Signal arrives → guard allows → reconcile PROCEED → both legs simulate."""
    monkeypatch.setattr(settings, "funding_arb_executor_dry_run", True)
    exe = _build_executor(guard=allow_guard, fake_redis=fake_redis)
    record = await exe.handle_signal(_signal())

    assert record.success_both_legs is True, f"unexpected: {record}"
    assert record.guard_block is None
    assert record.reconcile_block is None
    assert record.error is None
    assert record.gmx_result is not None, "GMX result missing"
    assert record.binance_result is not None, "Binance result missing"
    # Exactly one XADD to executions stream
    assert len(fake_redis.xadded) == 1
    stream, fields = fake_redis.xadded[0]
    assert stream == settings.funding_arb_executions_stream_key
    assert fields["success_both_legs"] == "1"
    assert fields["market"] == "sol"
    assert fields["direction"] == "short_gmx_long_cex"


# ──────────────────────────────────────────────────────────────────────────
# 2) Guard denial — gate fails, no submits called
# ──────────────────────────────────────────────────────────────────────────


async def test_guard_denial_no_submits(
    deny_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """Guard denies → no leg submit calls → record has guard_block."""
    sign_called = MagicMock()
    binance_called = MagicMock()

    async def _sign(*args: Any, **kwargs: Any) -> dict[str, Any]:
        sign_called(*args, **kwargs)
        return {}

    async def _binance(*args: Any, **kwargs: Any) -> OrderResult:
        binance_called(*args, **kwargs)
        return _dry_run_order_result()

    async def _pos(account: str) -> list[Position]:
        return []

    exe = FundingArbExecutor(
        guard=deny_guard,
        gmx_position_reader_fn=_pos,
        gmx_sign_fn=_sign,
        gmx_submit_fn=AsyncMock(return_value=_dry_run_send_result()),
        binance_order_fn=_binance,
        redis_client=fake_redis,
    )
    record = await exe.handle_signal(_signal())
    assert record.success_both_legs is False
    assert record.guard_block is not None
    assert record.guard_block["allowed"] is False
    assert record.guard_block["gate"] == "not_armed"
    assert record.gmx_result is None
    assert record.binance_result is None
    sign_called.assert_not_called()
    binance_called.assert_not_called()
    # Audit-trail XADD still happened
    assert len(fake_redis.xadded) == 1


# ──────────────────────────────────────────────────────────────────────────
# 3) Reconcile ABORT — opposite-side position → no submits
# ──────────────────────────────────────────────────────────────────────────


async def test_reconcile_abort_no_submits(
    monkeypatch: pytest.MonkeyPatch,
    allow_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """Opposite-side position open → reconcile=ABORT → no leg submits."""
    monkeypatch.setattr(settings, "funding_arb_executor_dry_run", True)
    # Intent will be short (direction=short_gmx_long_cex → is_long=False);
    # planted position is LONG → ABORT.
    existing = Position(
        account=_EXECUTOR_ADDRESS,
        market_alias="sol",
        market_address="0x09400D9DB990D5ed3f35D7be61DfAEB900Af03C9",
        collateral_token=_USDC,
        is_long=True,  # opposite of the signal's is_long=False
        size_in_usd=10 * 10**30,
        size_in_usd_float=10.0,
        size_in_tokens=0,
        collateral_amount=10_000_000,
        borrowing_factor=0,
        funding_fee_amount_per_size=0,
        increased_at_time=0,
        decreased_at_time=0,
    )
    sign_called = MagicMock()
    binance_called = MagicMock()

    async def _sign(*a: Any, **k: Any) -> dict[str, Any]:
        sign_called(*a, **k)
        return {}

    async def _binance(*a: Any, **k: Any) -> OrderResult:
        binance_called(*a, **k)
        return _dry_run_order_result()

    async def _pos(account: str) -> list[Position]:
        return [existing]

    exe = FundingArbExecutor(
        guard=allow_guard,
        gmx_position_reader_fn=_pos,
        gmx_sign_fn=_sign,
        gmx_submit_fn=AsyncMock(return_value=_dry_run_send_result()),
        binance_order_fn=_binance,
        redis_client=fake_redis,
    )
    record = await exe.handle_signal(_signal())
    assert record.success_both_legs is False
    assert record.reconcile_block is not None
    assert record.reconcile_block["action"] == "ABORT"
    sign_called.assert_not_called()
    binance_called.assert_not_called()
    # Audit-trail XADD still happened
    assert len(fake_redis.xadded) == 1


# ──────────────────────────────────────────────────────────────────────────
# 4) Partial failure — GMX OK, Binance error
# ──────────────────────────────────────────────────────────────────────────


async def test_partial_failure_binance_error(
    monkeypatch: pytest.MonkeyPatch,
    allow_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """GMX dry-run OK, Binance returns -2019 → success=False, error populated."""
    monkeypatch.setattr(settings, "funding_arb_executor_dry_run", True)
    binance_err = _binance_error_result(-2019, "margin not sufficient")
    exe = _build_executor(
        guard=allow_guard, fake_redis=fake_redis,
        binance_order_result=binance_err,
    )
    record = await exe.handle_signal(_signal())
    assert record.success_both_legs is False
    assert record.error is not None
    assert "binance_code=-2019" in record.error
    # Both legs produced a result (even though Binance failed)
    assert record.gmx_result is not None
    assert record.binance_result is not None


# ──────────────────────────────────────────────────────────────────────────
# 5) Position-reader failure → empty positions → reconcile PROCEED
# ──────────────────────────────────────────────────────────────────────────


async def test_position_reader_failure_falls_through(
    monkeypatch: pytest.MonkeyPatch,
    allow_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """RPC failure on position fetch → empty list → reconcile PROCEED."""
    monkeypatch.setattr(settings, "funding_arb_executor_dry_run", True)
    exe = _build_executor(
        guard=allow_guard, fake_redis=fake_redis,
        position_raise=RuntimeError("simulated rpc fail"),
    )
    record = await exe.handle_signal(_signal())
    # Both legs still attempted (their respective dry-runs)
    assert record.gmx_result is not None
    assert record.binance_result is not None
    # success_both_legs depends on the leg results; since both succeed here
    assert record.success_both_legs is True


# ──────────────────────────────────────────────────────────────────────────
# 6) Direction mapping — short_gmx_long_cex
# ──────────────────────────────────────────────────────────────────────────


async def test_direction_mapping_short_gmx_long_cex(
    monkeypatch: pytest.MonkeyPatch,
    allow_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """short_gmx_long_cex → GMX is_long=False + Binance side=BUY."""
    monkeypatch.setattr(settings, "funding_arb_executor_dry_run", True)
    sign_calls: list[OrderIntent] = []
    binance_calls: list[dict[str, Any]] = []

    async def _sign(intent: OrderIntent) -> dict[str, Any]:
        sign_calls.append(intent)
        return {
            "raw": "0x", "hash": "0x", "nonce": 1,
            "from": _EXECUTOR_ADDRESS,
            "tx_dict": {"to": "0x0", "value": 0, "data": "0x"},
        }

    async def _binance(
        symbol: str, side: str, notional_usd: float,
        *, mark_price: float, dry_run: bool, **_: Any,
    ) -> OrderResult:
        binance_calls.append(
            {"symbol": symbol, "side": side, "notional_usd": notional_usd},
        )
        return _dry_run_order_result()

    async def _pos(account: str) -> list[Position]:
        return []

    exe = FundingArbExecutor(
        guard=allow_guard,
        gmx_position_reader_fn=_pos,
        gmx_sign_fn=_sign,
        gmx_submit_fn=AsyncMock(return_value=_dry_run_send_result()),
        binance_order_fn=_binance,
        redis_client=fake_redis,
    )
    await exe.handle_signal(_signal(direction="short_gmx_long_cex"))
    assert len(sign_calls) == 1
    assert sign_calls[0].is_long is False
    assert len(binance_calls) == 1
    assert binance_calls[0]["side"] == "BUY"
    assert binance_calls[0]["symbol"] == "SOLUSDT"


# ──────────────────────────────────────────────────────────────────────────
# 7) Direction mapping — long_gmx_short_cex (inverse)
# ──────────────────────────────────────────────────────────────────────────


async def test_direction_mapping_long_gmx_short_cex(
    monkeypatch: pytest.MonkeyPatch,
    allow_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """long_gmx_short_cex → GMX is_long=True + Binance side=SELL."""
    monkeypatch.setattr(settings, "funding_arb_executor_dry_run", True)
    sign_calls: list[OrderIntent] = []
    binance_calls: list[dict[str, Any]] = []

    async def _sign(intent: OrderIntent) -> dict[str, Any]:
        sign_calls.append(intent)
        return {
            "raw": "0x", "hash": "0x", "nonce": 1,
            "from": _EXECUTOR_ADDRESS,
            "tx_dict": {"to": "0x0", "value": 0, "data": "0x"},
        }

    async def _binance(
        symbol: str, side: str, notional_usd: float,
        *, mark_price: float, dry_run: bool, **_: Any,
    ) -> OrderResult:
        binance_calls.append({"side": side, "symbol": symbol})
        return _dry_run_order_result()

    async def _pos(account: str) -> list[Position]:
        return []

    exe = FundingArbExecutor(
        guard=allow_guard,
        gmx_position_reader_fn=_pos,
        gmx_sign_fn=_sign,
        gmx_submit_fn=AsyncMock(return_value=_dry_run_send_result()),
        binance_order_fn=_binance,
        redis_client=fake_redis,
    )
    await exe.handle_signal(_signal(direction="long_gmx_short_cex"))
    assert len(sign_calls) == 1
    assert sign_calls[0].is_long is True
    assert binance_calls[0]["side"] == "SELL"


# ──────────────────────────────────────────────────────────────────────────
# 8) Bad direction → error record, no submits
# ──────────────────────────────────────────────────────────────────────────


async def test_bad_direction_records_error_no_submits(
    allow_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """Invalid direction string → error record, no legs called."""
    sign_called = MagicMock()
    binance_called = MagicMock()

    async def _sign(*a: Any, **k: Any) -> dict[str, Any]:
        sign_called(*a, **k)
        return {}

    async def _binance(*a: Any, **k: Any) -> OrderResult:
        binance_called(*a, **k)
        return _dry_run_order_result()

    async def _pos(account: str) -> list[Position]:
        return []

    exe = FundingArbExecutor(
        guard=allow_guard,
        gmx_position_reader_fn=_pos,
        gmx_sign_fn=_sign,
        gmx_submit_fn=AsyncMock(return_value=_dry_run_send_result()),
        binance_order_fn=_binance,
        redis_client=fake_redis,
    )
    record = await exe.handle_signal(_signal(direction="bogus_direction"))
    assert record.success_both_legs is False
    assert record.error is not None
    assert "bad_direction" in record.error
    sign_called.assert_not_called()
    binance_called.assert_not_called()
    assert len(fake_redis.xadded) == 1


# ──────────────────────────────────────────────────────────────────────────
# 9) Run loop respects stop_event
# ──────────────────────────────────────────────────────────────────────────


async def test_run_loop_respects_stop_event(
    monkeypatch: pytest.MonkeyPatch,
    allow_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """run() returns when stop_event is set; no infinite loop."""
    monkeypatch.setattr(settings, "funding_arb_executor_dry_run", True)

    class _FakePubSub:
        async def subscribe(self, channel: str) -> None:
            _ = channel

        async def unsubscribe(self, channel: str) -> None:
            _ = channel

        async def get_message(
            self, *, ignore_subscribe_messages: bool = True,
            timeout: float = 1.0,
        ) -> dict[str, Any] | None:
            # Honor the timeout so the loop yields control to other tasks
            # (including the stop-event setter). Without this the asyncio
            # scheduler never gets a chance to run the setter.
            _ = ignore_subscribe_messages
            await asyncio.sleep(min(timeout, 0.02))
            return None  # never yields a message; loop exits via stop_event

        async def close(self) -> None:
            pass

    class _FakeRedisWithPubSub(_FakeRedis):
        def pubsub(self) -> _FakePubSub:
            return _FakePubSub()

    fake = _FakeRedisWithPubSub()
    exe = FundingArbExecutor(
        guard=allow_guard,
        gmx_position_reader_fn=AsyncMock(return_value=[]),
        gmx_sign_fn=AsyncMock(return_value={}),
        gmx_submit_fn=AsyncMock(return_value=_dry_run_send_result()),
        binance_order_fn=AsyncMock(return_value=_dry_run_order_result()),
        redis_client=fake,
    )
    stop = asyncio.Event()

    # Schedule the stop event 0.05s in the future to test event response
    async def _set_stop_soon() -> None:
        await asyncio.sleep(0.05)
        stop.set()

    setter = asyncio.create_task(_set_stop_soon())
    await asyncio.wait_for(exe.run(stop), timeout=2.0)
    await setter


# ──────────────────────────────────────────────────────────────────────────
# 10) Loss recording — GMX result with status=0
# ──────────────────────────────────────────────────────────────────────────


async def test_loss_recorded_when_gmx_status_zero(
    monkeypatch: pytest.MonkeyPatch,
    allow_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """GMX submitted with on-chain revert (status=0) → record_loss called."""
    monkeypatch.setattr(settings, "funding_arb_executor_dry_run", False)
    record_loss_calls: list[int] = []

    async def _record_loss(ts_ms: int) -> None:
        record_loss_calls.append(ts_ms)

    monkeypatch.setattr(
        "gmx_strategies.funding_arb_executor.pilot_guard.record_loss",
        _record_loss,
    )

    gmx_loss_result = SendResult(
        submitted=True, dry_run_simulation=None,
        tx_hash="0xfailed", block_number=1, gas_used=300_000,
        status=0,  # on-chain revert
        error=None,
    )
    exe = _build_executor(
        guard=allow_guard, fake_redis=fake_redis,
        gmx_send_result=gmx_loss_result,
    )
    record = await exe.handle_signal(_signal())
    assert record.success_both_legs is False
    assert len(record_loss_calls) == 1, f"record_loss not called: {record_loss_calls}"


# ──────────────────────────────────────────────────────────────────────────
# 11) Loss recording — Binance result with error_code != -1
# ──────────────────────────────────────────────────────────────────────────


async def test_loss_recorded_when_binance_real_error(
    monkeypatch: pytest.MonkeyPatch,
    allow_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """Binance reject (-2019) → record_loss called even when GMX OK."""
    monkeypatch.setattr(settings, "funding_arb_executor_dry_run", True)
    record_loss_calls: list[int] = []

    async def _record_loss(ts_ms: int) -> None:
        record_loss_calls.append(ts_ms)

    monkeypatch.setattr(
        "gmx_strategies.funding_arb_executor.pilot_guard.record_loss",
        _record_loss,
    )
    exe = _build_executor(
        guard=allow_guard, fake_redis=fake_redis,
        binance_order_result=_binance_error_result(-2019, "margin not sufficient"),
    )
    await exe.handle_signal(_signal())
    assert len(record_loss_calls) == 1


# ──────────────────────────────────────────────────────────────────────────
# 12) Main.py: consumer-disabled → executor.run NOT in tasks
# ──────────────────────────────────────────────────────────────────────────


async def test_main_consumer_disabled_omits_executor_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When funding_arb_consumer_enabled=False, _build_tasks omits the consumer."""
    from gmx_strategies import main as main_mod

    monkeypatch.setattr(settings, "funding_arb_consumer_enabled", False)
    stop = asyncio.Event()
    tasks = main_mod._build_tasks(stop)
    try:
        names = {t.get_name() for t in tasks}
        assert "funding_arb_runtime" in names
        assert "funding_arb_executor" not in names, (
            "executor task must not start when consumer_enabled=False"
        )
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_main_consumer_enabled_includes_executor_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When funding_arb_consumer_enabled=True, _build_tasks adds the consumer."""
    from gmx_strategies import main as main_mod

    monkeypatch.setattr(settings, "funding_arb_consumer_enabled", True)
    stop = asyncio.Event()
    tasks = main_mod._build_tasks(stop)
    try:
        names = {t.get_name() for t in tasks}
        assert "funding_arb_runtime" in names
        assert "funding_arb_executor" in names
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


# ──────────────────────────────────────────────────────────────────────────
# 13) Bad message JSON → no crash; no XADD
# ──────────────────────────────────────────────────────────────────────────


async def test_handle_message_bad_json_does_not_crash(
    allow_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """A malformed JSON pub/sub body is logged + skipped, never crashes."""
    exe = FundingArbExecutor(
        guard=allow_guard,
        gmx_position_reader_fn=AsyncMock(return_value=[]),
        gmx_sign_fn=AsyncMock(return_value={}),
        gmx_submit_fn=AsyncMock(return_value=_dry_run_send_result()),
        binance_order_fn=AsyncMock(return_value=_dry_run_order_result()),
        redis_client=fake_redis,
    )
    # Bad message — non-JSON body
    await exe._handle_message({"type": "message", "data": "not valid json {"})
    # No XADD attempted
    assert fake_redis.xadded == []


async def test_handle_message_valid_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    allow_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """Valid JSON message → dispatched to handle_signal."""
    monkeypatch.setattr(settings, "funding_arb_executor_dry_run", True)
    exe = _build_executor(guard=allow_guard, fake_redis=fake_redis)
    body = json.dumps(_signal())
    await exe._handle_message({"type": "message", "data": body})
    assert len(fake_redis.xadded) == 1
    _, fields = fake_redis.xadded[0]
    assert fields["market"] == "sol"


# ──────────────────────────────────────────────────────────────────────────
# 14) Exception inside handle_signal still XADDs + returns
# ──────────────────────────────────────────────────────────────────────────


async def test_exception_in_pipeline_caught_and_logged(
    allow_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """An unexpected raise inside the pipeline → caught, record written."""

    async def _sign_raises(intent: OrderIntent) -> dict[str, Any]:
        _ = intent
        raise RuntimeError("simulated mid-pipeline failure")

    exe = FundingArbExecutor(
        guard=allow_guard,
        gmx_position_reader_fn=AsyncMock(return_value=[]),
        gmx_sign_fn=_sign_raises,
        gmx_submit_fn=AsyncMock(return_value=_dry_run_send_result()),
        binance_order_fn=AsyncMock(return_value=_dry_run_order_result()),
        redis_client=fake_redis,
    )
    record = await exe.handle_signal(_signal())
    # gmx leg raises → gmx_result is None; the binance leg still ran
    # successfully → success_both_legs depends on the GMX side, which is
    # None. _compose_error captures both legs' status.
    assert record.success_both_legs is False
    assert record.error is not None
    # Audit-trail XADD still happened
    assert len(fake_redis.xadded) == 1


# ──────────────────────────────────────────────────────────────────────────
# 15) ExecutionRecord realized_pnl_usd is initialized to 0.0
# ──────────────────────────────────────────────────────────────────────────


async def test_execution_record_realized_pnl_initialized_to_zero(
    monkeypatch: pytest.MonkeyPatch,
    allow_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """Every fresh ExecutionRecord starts at realized_pnl_usd=0.0 (G7.2 stub)."""
    monkeypatch.setattr(settings, "funding_arb_executor_dry_run", True)
    exe = _build_executor(guard=allow_guard, fake_redis=fake_redis)
    record = await exe.handle_signal(_signal())
    assert record.realized_pnl_usd == 0.0
    _, fields = fake_redis.xadded[0]
    assert "realized_pnl_usd" in fields
    # String-coerced to "0.000000"
    assert float(fields["realized_pnl_usd"]) == 0.0


# ──────────────────────────────────────────────────────────────────────────
# 16) No executor key → intent construction fails, no submits
# ──────────────────────────────────────────────────────────────────────────


async def test_no_executor_key_blocks_intent_construction(
    monkeypatch: pytest.MonkeyPatch,
    allow_guard: _FakeGuard,
    fake_redis: _FakeRedis,
) -> None:
    """get_executor_address()=None → intent=None → error record, no submits."""
    monkeypatch.setattr(
        "gmx_strategies.funding_arb_executor.gmx_signer.get_executor_address",
        lambda: None,
    )
    sign_called = MagicMock()
    binance_called = MagicMock()

    async def _sign(*a: Any, **k: Any) -> dict[str, Any]:
        sign_called(*a, **k)
        return {}

    async def _binance(*a: Any, **k: Any) -> OrderResult:
        binance_called(*a, **k)
        return _dry_run_order_result()

    async def _pos(account: str) -> list[Position]:
        return []

    exe = FundingArbExecutor(
        guard=allow_guard,
        gmx_position_reader_fn=_pos,
        gmx_sign_fn=_sign,
        gmx_submit_fn=AsyncMock(return_value=_dry_run_send_result()),
        binance_order_fn=_binance,
        redis_client=fake_redis,
    )
    record = await exe.handle_signal(_signal())
    assert record.success_both_legs is False
    assert record.error is not None
    assert "intent_construction_failed" in record.error
    sign_called.assert_not_called()
    binance_called.assert_not_called()
    assert len(fake_redis.xadded) == 1
