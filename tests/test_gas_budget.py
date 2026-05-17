"""GMX gas-budget throttle — pure helpers."""
from __future__ import annotations

from gmx_strategies import gas_budget as gb


def test_select_tier_seed():
    t = gb.select_gas_tier(0.0)
    assert t.label == "T0_seed"
    assert t.daily_cap_usd == 50.0
    assert t.weekly_cap_usd == 200.0


def test_select_tier_seed_when_negative():
    t = gb.select_gas_tier(-100.0)
    assert t.label == "T0_seed"


def test_select_tier_t1_at_threshold():
    t = gb.select_gas_tier(300.0)
    assert t.label == "T1_proved"
    assert t.daily_cap_usd == 75.0


def test_select_tier_t2():
    t = gb.select_gas_tier(1500.0)
    assert t.label == "T2_scaling"


def test_select_tier_top():
    t = gb.select_gas_tier(100_000.0)
    assert t.label == "T6_top"
    assert t.daily_cap_usd == 3000.0


def test_is_within_budget_allow():
    tier = gb.select_gas_tier(0)
    ok, reason = gb.is_within_budget(
        daily_spend_usd=10.0,
        weekly_spend_usd=30.0,
        consecutive_reverts=0,
        tier=tier,
    )
    assert ok is True
    assert reason == "ok"


def test_is_within_budget_daily_cap_breached():
    tier = gb.select_gas_tier(0)
    ok, reason = gb.is_within_budget(
        daily_spend_usd=51.0,  # > $50 T0 daily cap
        weekly_spend_usd=51.0,
        consecutive_reverts=0,
        tier=tier,
    )
    assert ok is False
    assert "daily_cap" in reason


def test_is_within_budget_weekly_cap_breached():
    tier = gb.select_gas_tier(0)
    ok, reason = gb.is_within_budget(
        daily_spend_usd=10.0,
        weekly_spend_usd=201.0,  # > $200 T0 weekly cap
        consecutive_reverts=0,
        tier=tier,
    )
    assert ok is False
    assert "weekly_cap" in reason


def test_is_within_budget_revert_breaker():
    tier = gb.select_gas_tier(0)
    ok, reason = gb.is_within_budget(
        daily_spend_usd=10.0,
        weekly_spend_usd=30.0,
        consecutive_reverts=5,  # >= breaker
        tier=tier,
    )
    assert ok is False
    assert "consecutive_reverts" in reason


def test_revert_breaker_priority():
    """Revert breaker is checked FIRST — so a circuit-broken state shows
    the breaker reason even when no budget is breached."""
    tier = gb.select_gas_tier(0)
    ok, reason = gb.is_within_budget(
        daily_spend_usd=49.0,
        weekly_spend_usd=199.0,
        consecutive_reverts=5,
        tier=tier,
    )
    assert ok is False
    assert "consecutive_reverts" in reason


def test_higher_tier_allows_higher_spend():
    """T3 should allow spend that T0 wouldn't."""
    t0 = gb.select_gas_tier(0)
    t3 = gb.select_gas_tier(3000.0)
    spend = 200.0
    ok_t0, _ = gb.is_within_budget(
        daily_spend_usd=spend, weekly_spend_usd=spend,
        consecutive_reverts=0, tier=t0,
    )
    ok_t3, _ = gb.is_within_budget(
        daily_spend_usd=spend, weekly_spend_usd=spend,
        consecutive_reverts=0, tier=t3,
    )
    assert ok_t0 is False
    assert ok_t3 is True


def test_date_key_format():
    k = gb._date_key(1700000000)  # Tue Nov 14 22:13:20 2023 UTC
    assert k == "2023-11-14"


def test_iso_week_key_format():
    k = gb._iso_week_key(1700000000)  # ISO week 46 of 2023
    assert k.startswith("2023-W")
    assert len(k) == 8


def test_gas_ladder_monotonic():
    """Each tier should allow more than the previous."""
    prev = gb.DEFAULT_GAS_LADDER[0]
    for tier in gb.DEFAULT_GAS_LADDER[1:]:
        assert tier.pnl_threshold_usd > prev.pnl_threshold_usd
        assert tier.daily_cap_usd >= prev.daily_cap_usd
        assert tier.weekly_cap_usd >= prev.weekly_cap_usd
        prev = tier
