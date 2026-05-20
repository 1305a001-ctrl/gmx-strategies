"""Tests for the G7.3 pilot guard (`pilot_guard.py`).

Coverage:
  - Each of the 6 gates fires in isolation when its condition is met
  - Denial-priority: killswitch wins over every other denial
  - G2 default-empty armed_markets denies ALL markets (default-deny)
  - G3 size_cap denies $20 when cap is $10
  - G4 daily_pnl: $30 of losses → allowed; $60 → denied
  - G5 concurrent: max_concurrent=1, 1 open GMX position → denied
  - G6 cooldown: last-loss-ts within window → denied
  - All-pass path: returns allowed=True with reason=None
  - Denial XADD: verify guard_blocks stream gets an entry per denial
  - GuardState snapshot reflects the live inputs
  - Operator helpers (`trip_killswitch`, `reset_killswitch`, `record_loss`)
  - Defensive paths: Redis read failure → killswitch trips safe
"""

from __future__ import annotations

from typing import Any

import pytest

from gmx_strategies import pilot_guard as guard_mod
from gmx_strategies.pilot_guard import (
    GATE_CONCURRENT,
    GATE_COOLDOWN,
    GATE_DAILY_PNL,
    GATE_KILLSWITCH,
    GATE_NOT_ARMED,
    GATE_SIZE_CAP,
    GuardResult,
    PilotGuard,
)
from gmx_strategies.settings import settings

# ──────────────────────────────────────────────────────────────────────────
# Fake Redis — minimal async stub matching the subset we use
# ──────────────────────────────────────────────────────────────────────────


class _FakeRedis:
    """Async stub recording set/get/xadd/xrange calls."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.streams: dict[str, list[tuple[str, dict[Any, Any]]]] = {}
        self.xadded: list[tuple[str, dict[Any, Any]]] = []
        self._next_id = 1
        # When set, `get` on any matching key raises (defensive-path tests).
        self.fail_get_keys: set[str] = set()
        self.fail_xrange_streams: set[str] = set()

    async def get(self, key: str) -> str | None:
        if key in self.fail_get_keys:
            raise RuntimeError(f"simulated redis get failure for {key}")
        return self.store.get(key)

    async def set(self, key: str, value: str) -> bool:
        self.store[key] = str(value)
        return True

    async def delete(self, key: str) -> int:
        return 1 if self.store.pop(key, None) is not None else 0

    async def xadd(
        self,
        stream: str,
        fields: dict[Any, Any],
        *,
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        _ = (maxlen, approximate)
        eid = f"{self._next_id}-0"
        self._next_id += 1
        self.streams.setdefault(stream, []).append((eid, dict(fields)))
        self.xadded.append((stream, dict(fields)))
        return eid

    async def xrange(
        self,
        stream: str,
        min: int | str = "-",
        max: int | str = "+",
    ) -> list[tuple[str, dict[Any, Any]]]:
        if stream in self.fail_xrange_streams:
            raise RuntimeError(f"simulated redis xrange failure for {stream}")
        entries = self.streams.get(stream, [])
        # Best-effort filter: ids are "<ms>-<seq>". For our tests we never
        # need true range filtering — the synthesized entries are all
        # well within the day boundary; callers can pre-load. But honor
        # numeric `min` filter where the test does want to exclude older
        # entries.
        out = []
        min_ms = int(min) if isinstance(min, int) else None
        for eid, fields in entries:
            if min_ms is not None:
                try:
                    eid_ms = int(eid.split("-")[0])
                except (TypeError, ValueError):
                    eid_ms = 0
                if eid_ms < min_ms:
                    continue
            out.append((eid, fields))
        return out


@pytest.fixture
def fake_redis(monkeypatch: pytest.MonkeyPatch) -> _FakeRedis:
    """Provide a fresh _FakeRedis bound to the module-level r() helper."""
    fake = _FakeRedis()
    # Patch the module-level redis_client() helper so operator helpers
    # (trip_killswitch/reset_killswitch/record_loss) hit the fake.
    monkeypatch.setattr(guard_mod, "redis_client", lambda: fake)
    return fake


def _force_armed(monkeypatch: pytest.MonkeyPatch, csv: str) -> None:
    """Helper: set funding_arb_armed_markets_csv for the duration of one test."""
    monkeypatch.setattr(settings, "funding_arb_armed_markets_csv", csv)


async def _zero_gmx() -> int:
    return 0


async def _zero_binance() -> int:
    return 0


async def _one_gmx() -> int:
    return 1


# ──────────────────────────────────────────────────────────────────────────
# G1 — killswitch
# ──────────────────────────────────────────────────────────────────────────


async def test_g1_killswitch_value_1_denies(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Setting the killswitch key to '1' denies any check."""
    fake_redis.store[settings.pilot_killswitch_key] = "1"
    _force_armed(monkeypatch, "sol")  # would otherwise pass G2
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    result = await g.check("sol", 5.0)
    assert result.allowed is False
    assert result.gate == GATE_KILLSWITCH
    assert result.reason is not None


async def test_g1_killswitch_value_true_case_insensitive(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """'TRUE' / 'true' / 'YES' all trip the killswitch."""
    _force_armed(monkeypatch, "sol")
    for raw in ("true", "TRUE", "Yes", "on"):
        fake_redis.store[settings.pilot_killswitch_key] = raw
        g = PilotGuard(
            redis=fake_redis,
            gmx_position_reader_fn=_zero_gmx,
            binance_position_reader_fn=_zero_binance,
        )
        result = await g.check("sol", 5.0)
        assert result.allowed is False, f"raw={raw!r} should trip"
        assert result.gate == GATE_KILLSWITCH


async def test_g1_killswitch_value_0_does_not_trip(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """'0' / 'false' / 'no' do NOT trip — only truthy values do."""
    _force_armed(monkeypatch, "sol")
    fake_redis.store[settings.pilot_killswitch_key] = "0"
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    result = await g.check("sol", 5.0)
    # Should NOT be denied at killswitch (other gates pass too: armed +
    # size <= 10 + zero pnl/positions/cooldown)
    assert result.gate != GATE_KILLSWITCH


async def test_g1_killswitch_read_failure_trips_safe(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If Redis is unreachable for the killswitch read, we deny (safe default)."""
    fake_redis.fail_get_keys.add(settings.pilot_killswitch_key)
    _force_armed(monkeypatch, "sol")
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    result = await g.check("sol", 5.0)
    assert result.allowed is False
    assert result.gate == GATE_KILLSWITCH


# ──────────────────────────────────────────────────────────────────────────
# G2 — armed_markets (default-deny)
# ──────────────────────────────────────────────────────────────────────────


async def test_g2_default_empty_denies_all_markets(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default funding_arb_armed_markets_csv='' means EVERY market is denied."""
    _force_armed(monkeypatch, "")  # explicit empty
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    for market in ("btc", "eth", "sol", "doge", "xrp"):
        result = await g.check(market, 5.0)
        assert result.allowed is False, f"{market} should be denied (default-deny)"
        assert result.gate == GATE_NOT_ARMED


async def test_g2_explicit_arm_allows_that_market(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Arming 'sol' allows sol; other markets still denied."""
    _force_armed(monkeypatch, "sol")
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    sol_result = await g.check("sol", 5.0)
    btc_result = await g.check("btc", 5.0)
    assert sol_result.allowed is True
    assert sol_result.reason is None
    assert sol_result.gate is None
    assert btc_result.allowed is False
    assert btc_result.gate == GATE_NOT_ARMED


async def test_g2_case_insensitive_arming(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SOL / sol / SoL all match — CSV parsing is lowercased."""
    _force_armed(monkeypatch, "SOL, BTC")
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    for market in ("sol", "SOL", "Sol", "btc", "BTC"):
        result = await g.check(market, 5.0)
        assert result.allowed is True, f"{market} should be allowed"


# ──────────────────────────────────────────────────────────────────────────
# G3 — size_cap
# ──────────────────────────────────────────────────────────────────────────


async def test_g3_size_cap_denies_above_cap(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """notional $20 > $10 cap → deny with gate=size_cap."""
    _force_armed(monkeypatch, "sol")
    monkeypatch.setattr(settings, "funding_arb_pilot_position_cap_usd", 10.0)
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    result = await g.check("sol", 20.0)
    assert result.allowed is False
    assert result.gate == GATE_SIZE_CAP


async def test_g3_size_cap_allows_at_cap(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """notional == cap → allowed (boundary)."""
    _force_armed(monkeypatch, "sol")
    monkeypatch.setattr(settings, "funding_arb_pilot_position_cap_usd", 10.0)
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    result = await g.check("sol", 10.0)
    assert result.allowed is True


# ──────────────────────────────────────────────────────────────────────────
# G4 — daily_pnl
# ──────────────────────────────────────────────────────────────────────────


def _seed_executions(
    fake_redis: _FakeRedis,
    losses_usd: list[float],
    *,
    today_ms: int | None = None,
) -> None:
    """Seed the executions stream with synthetic realized_pnl_usd entries.

    `losses_usd` is treated as a list of realized_pnl_usd values (positive
    or negative). Each is XADD'd with an id slightly past today's start.
    """
    base_id = (today_ms or guard_mod._today_start_utc_ms()) + 1
    for i, pnl in enumerate(losses_usd):
        fake_redis.streams.setdefault(
            settings.funding_arb_executions_stream_key, [],
        ).append((f"{base_id + i}-0", {"realized_pnl_usd": str(pnl)}))


async def test_g4_pnl_ahead_of_floor_allowed(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """$30 of losses today, floor=-$50 → still allowed (G4 passes)."""
    _force_armed(monkeypatch, "sol")
    monkeypatch.setattr(settings, "funding_arb_pilot_daily_pnl_floor_usd", -50.0)
    _seed_executions(fake_redis, [-10.0, -10.0, -10.0])
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    result = await g.check("sol", 5.0)
    assert result.allowed is True, (
        f"expected allow at -$30 vs -$50 floor, got {result}"
    )


async def test_g4_pnl_below_floor_denied(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """$60 of losses today, floor=-$50 → deny with gate=daily_pnl."""
    _force_armed(monkeypatch, "sol")
    monkeypatch.setattr(settings, "funding_arb_pilot_daily_pnl_floor_usd", -50.0)
    _seed_executions(fake_redis, [-20.0, -20.0, -20.0])
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    result = await g.check("sol", 5.0)
    assert result.allowed is False
    assert result.gate == GATE_DAILY_PNL


async def test_g4_xrange_failure_treats_day_as_flat(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """XRANGE failure → 0.0 PnL → passes the -$50 floor."""
    _force_armed(monkeypatch, "sol")
    fake_redis.fail_xrange_streams.add(
        settings.funding_arb_executions_stream_key,
    )
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    # All other gates pass → expect allowed
    result = await g.check("sol", 5.0)
    assert result.allowed is True


# ──────────────────────────────────────────────────────────────────────────
# G5 — concurrent
# ──────────────────────────────────────────────────────────────────────────


async def test_g5_one_gmx_position_at_max_denies(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """max_concurrent=1, 1 open GMX position → deny with gate=concurrent."""
    _force_armed(monkeypatch, "sol")
    monkeypatch.setattr(settings, "funding_arb_pilot_max_concurrent", 1)
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_one_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    result = await g.check("sol", 5.0)
    assert result.allowed is False
    assert result.gate == GATE_CONCURRENT


async def test_g5_one_binance_position_at_max_denies(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent count sums both venues; 1 binance + 0 gmx at max=1 → deny."""
    _force_armed(monkeypatch, "sol")
    monkeypatch.setattr(settings, "funding_arb_pilot_max_concurrent", 1)

    async def _one_binance() -> int:
        return 1

    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_one_binance,
    )
    result = await g.check("sol", 5.0)
    assert result.allowed is False
    assert result.gate == GATE_CONCURRENT


# ──────────────────────────────────────────────────────────────────────────
# G6 — cooldown
# ──────────────────────────────────────────────────────────────────────────


async def test_g6_recent_loss_in_cooldown_window_denies(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Last loss 60s ago, cooldown 1800s → deny."""
    _force_armed(monkeypatch, "sol")
    monkeypatch.setattr(settings, "funding_arb_pilot_loss_cooldown_s", 1800)
    # Simulate "60 seconds ago"
    now_ms = 1_700_000_000_000
    monkeypatch.setattr(guard_mod, "_now_ms", lambda: now_ms)
    fake_redis.store[settings.pilot_last_loss_ts_key] = str(now_ms - 60_000)
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    result = await g.check("sol", 5.0)
    assert result.allowed is False
    assert result.gate == GATE_COOLDOWN


async def test_g6_cooldown_elapsed_allowed(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Last loss 2 hours ago, cooldown 30min → allowed."""
    _force_armed(monkeypatch, "sol")
    monkeypatch.setattr(settings, "funding_arb_pilot_loss_cooldown_s", 1800)
    now_ms = 1_700_000_000_000
    monkeypatch.setattr(guard_mod, "_now_ms", lambda: now_ms)
    fake_redis.store[settings.pilot_last_loss_ts_key] = str(
        now_ms - 7_200_000,  # 2hr
    )
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    result = await g.check("sol", 5.0)
    assert result.allowed is True


# ──────────────────────────────────────────────────────────────────────────
# Denial-priority — killswitch wins
# ──────────────────────────────────────────────────────────────────────────


async def test_denial_priority_killswitch_wins_over_all_others(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When EVERY gate would deny, killswitch wins (it's checked first)."""
    # Killswitch SET
    fake_redis.store[settings.pilot_killswitch_key] = "1"
    # NOT armed → would deny on G2
    _force_armed(monkeypatch, "")
    # Above size cap → would deny on G3
    monkeypatch.setattr(settings, "funding_arb_pilot_position_cap_usd", 10.0)
    # Below pnl floor → would deny on G4
    monkeypatch.setattr(settings, "funding_arb_pilot_daily_pnl_floor_usd", -10.0)
    _seed_executions(fake_redis, [-100.0])
    # At max concurrent → would deny on G5
    monkeypatch.setattr(settings, "funding_arb_pilot_max_concurrent", 1)
    # Cooldown active → would deny on G6
    now_ms = 1_700_000_000_000
    monkeypatch.setattr(guard_mod, "_now_ms", lambda: now_ms)
    fake_redis.store[settings.pilot_last_loss_ts_key] = str(now_ms - 60_000)

    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_one_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    result = await g.check("sol", 100.0)
    assert result.allowed is False
    assert result.gate == GATE_KILLSWITCH


async def test_denial_priority_not_armed_wins_over_size_cap(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """G2 (not_armed) is checked before G3 (size_cap)."""
    _force_armed(monkeypatch, "")  # nothing armed
    monkeypatch.setattr(settings, "funding_arb_pilot_position_cap_usd", 10.0)
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    # Over the size cap AND not armed → should fail with not_armed
    result = await g.check("sol", 100.0)
    assert result.allowed is False
    assert result.gate == GATE_NOT_ARMED


# ──────────────────────────────────────────────────────────────────────────
# All-pass path
# ──────────────────────────────────────────────────────────────────────────


async def test_all_gates_pass_returns_allowed(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Killswitch unset, armed, under cap, healthy pnl, no positions, no cooldown."""
    _force_armed(monkeypatch, "sol")
    monkeypatch.setattr(settings, "funding_arb_pilot_position_cap_usd", 10.0)
    monkeypatch.setattr(settings, "funding_arb_pilot_max_concurrent", 1)
    monkeypatch.setattr(settings, "funding_arb_pilot_daily_pnl_floor_usd", -50.0)
    monkeypatch.setattr(settings, "funding_arb_pilot_loss_cooldown_s", 1800)
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    result = await g.check("sol", 5.0, side="long")
    assert result == GuardResult(allowed=True, reason=None, gate=None)


# ──────────────────────────────────────────────────────────────────────────
# Denial XADD — verify guard_blocks stream gets an entry per denial
# ──────────────────────────────────────────────────────────────────────────


async def test_denial_writes_guard_blocks_entry(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every denial XADDs a structured entry to guard_blocks."""
    _force_armed(monkeypatch, "")  # default-deny
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    result = await g.check("sol", 5.0, side="long")
    assert result.allowed is False
    # One XADD to guard_blocks
    assert any(
        s == settings.guard_blocks_stream_key
        for s, _ in fake_redis.xadded
    ), "denial should XADD to guard_blocks"
    stream = fake_redis.streams[settings.guard_blocks_stream_key]
    assert len(stream) == 1
    _, fields = stream[0]
    assert fields["gate"] == GATE_NOT_ARMED
    assert fields["market"] == "sol"
    assert fields["side"] == "long"
    assert "5.0" in fields["notional_usd"]
    assert fields["reason"] != ""


async def test_allow_does_not_write_guard_blocks_entry(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No XADD when the check returns allowed."""
    _force_armed(monkeypatch, "sol")
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_zero_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    result = await g.check("sol", 5.0)
    assert result.allowed is True
    assert settings.guard_blocks_stream_key not in fake_redis.streams


# ──────────────────────────────────────────────────────────────────────────
# GuardState snapshot
# ──────────────────────────────────────────────────────────────────────────


async def test_state_snapshot_reflects_inputs(
    fake_redis: _FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """state() returns a coherent snapshot of all gate inputs."""
    _force_armed(monkeypatch, "sol,btc")
    monkeypatch.setattr(settings, "funding_arb_pilot_position_cap_usd", 10.0)
    monkeypatch.setattr(settings, "funding_arb_pilot_max_concurrent", 1)
    monkeypatch.setattr(settings, "funding_arb_pilot_daily_pnl_floor_usd", -50.0)
    monkeypatch.setattr(settings, "funding_arb_pilot_loss_cooldown_s", 1800)
    _seed_executions(fake_redis, [-5.0, +2.0])
    now_ms = 1_700_000_000_000
    monkeypatch.setattr(guard_mod, "_now_ms", lambda: now_ms)
    fake_redis.store[settings.pilot_last_loss_ts_key] = str(now_ms - 600_000)
    g = PilotGuard(
        redis=fake_redis,
        gmx_position_reader_fn=_one_gmx,
        binance_position_reader_fn=_zero_binance,
    )
    state = await g.state()
    assert state.killswitch_set is False
    assert abs(state.today_pnl_usd - (-3.0)) < 1e-9
    assert state.today_pnl_floor_usd == -50.0
    assert state.open_gmx_positions == 1
    assert state.open_binance_positions == 0
    assert state.max_concurrent == 1
    assert state.armed_markets == {"sol", "btc"}
    assert state.pilot_position_cap_usd == 10.0
    assert state.last_loss_ts_ms == now_ms - 600_000
    # 600s elapsed of a 1800s window → 1200s remaining
    assert state.cooldown_remaining_s == 1200


# ──────────────────────────────────────────────────────────────────────────
# Operator helpers
# ──────────────────────────────────────────────────────────────────────────


async def test_trip_and_reset_killswitch(fake_redis: _FakeRedis) -> None:
    """trip_killswitch SETs the key; reset_killswitch DELs it."""
    await guard_mod.trip_killswitch(reason="manual emergency stop")
    assert fake_redis.store.get(settings.pilot_killswitch_key) == "1"
    await guard_mod.reset_killswitch()
    assert settings.pilot_killswitch_key not in fake_redis.store


async def test_record_loss_sets_last_loss_ts(fake_redis: _FakeRedis) -> None:
    """record_loss writes the unix-ms timestamp to the cooldown key."""
    await guard_mod.record_loss(1_700_000_000_123)
    assert fake_redis.store.get(settings.pilot_last_loss_ts_key) == (
        "1700000000123"
    )
