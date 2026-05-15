"""Tests for execution.py — paper mode + live mode gates."""
from __future__ import annotations

from gmx_strategies.execution import (
    ARBITRUM_LIQUIDATE_GAS_USD,
    GMX_LIQ_FEE_FACTOR,
    build_plan,
    compute_expected_fee_usd,
    compute_expected_net_pnl,
    should_execute,
)
from gmx_strategies.liquidation_trigger import LiquidationTrigger


def _trigger(
    user: str = "0xabc", market: str = "btc",
    distance: float = -0.5, confidence: float = 0.85, reason: str = "trigger",
) -> LiquidationTrigger:
    return LiquidationTrigger(
        user=user, market=market,
        distance_to_liq_pct=distance,
        estimated_fee_usd=0.0,
        confidence=confidence, reason=reason,
    )


def test_compute_expected_fee_basic():
    fee = compute_expected_fee_usd(size_usd=100_000)
    assert fee == 100_000 * GMX_LIQ_FEE_FACTOR
    assert fee == 500.0


def test_compute_expected_fee_zero_size():
    assert compute_expected_fee_usd(size_usd=0) == 0.0
    assert compute_expected_fee_usd(size_usd=-1) == 0.0


def test_compute_expected_fee_custom_factor():
    # Some smaller-cap markets use 1% — verify override works
    fee = compute_expected_fee_usd(size_usd=100_000, liq_fee_factor=0.01)
    assert fee == 1_000.0


def test_compute_expected_net_pnl_subtracts_gas():
    pnl = compute_expected_net_pnl(expected_fee_usd=500, gas_usd=1.5)
    assert abs(pnl - 498.5) < 1e-9


def test_compute_expected_net_pnl_slippage_optional():
    # GMX V2 default slippage = 0 (ADL absorbs the loss)
    pnl = compute_expected_net_pnl(expected_fee_usd=500)
    assert abs(pnl - (500 - ARBITRUM_LIQUIDATE_GAS_USD)) < 1e-9


def test_build_plan_aggregates_correctly():
    plan = build_plan(
        trigger=_trigger(),
        size_usd=200_000, collateral_usd=500, is_long=True,
    )
    assert plan.user == "0xabc"
    assert plan.market == "btc"
    assert plan.is_long is True
    assert plan.size_usd == 200_000
    assert plan.expected_fee_usd == 200_000 * GMX_LIQ_FEE_FACTOR
    assert plan.expected_net_pnl_usd > 0   # 1000 fee - 1.5 gas
    assert plan.confidence == 0.85


def test_should_execute_passes_for_clean_plan():
    plan = build_plan(
        trigger=_trigger(),
        size_usd=200_000, collateral_usd=500, is_long=True,
    )
    ok, reason = should_execute(plan=plan, min_net_profit_usd=50.0)
    assert ok is True
    assert reason == "ready"


def test_should_execute_rejects_low_pnl():
    plan = build_plan(
        trigger=_trigger(),
        size_usd=1_000, collateral_usd=10, is_long=True,
    )
    # Fee: $5 - gas $1.50 = $3.50 net — below $50 threshold
    ok, reason = should_execute(plan=plan, min_net_profit_usd=50.0)
    assert ok is False
    assert "net_pnl_below_threshold" in reason


def test_should_execute_rejects_low_confidence():
    plan = build_plan(
        trigger=_trigger(confidence=0.3),
        size_usd=200_000, collateral_usd=500, is_long=True,
    )
    ok, reason = should_execute(
        plan=plan, min_net_profit_usd=50.0, min_confidence=0.5,
    )
    assert ok is False
    assert "confidence_below_threshold" in reason


def test_should_execute_rejects_non_liquidatable():
    plan = build_plan(
        trigger=_trigger(distance=0.5),  # positive distance = position safe
        size_usd=200_000, collateral_usd=500, is_long=True,
    )
    ok, reason = should_execute(plan=plan, min_net_profit_usd=50.0)
    assert ok is False
    assert "not_yet_liquidatable" in reason


# ─── async paths ──────────────────────────────────────────────────────


class _MockRedis:
    """Records xadd calls for assertion."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def xadd(
        self, stream: str, fields: dict, *,
        maxlen: int | None = None, approximate: bool = False,
    ) -> str:
        self.calls.append((stream, fields))
        return "1234567890-0"


import asyncio  # noqa: E402  — keep top-of-file pure-test imports separate

from gmx_strategies.execution import execute_live, execute_paper  # noqa: E402


def test_execute_paper_records_to_redis():
    plan = build_plan(
        trigger=_trigger(),
        size_usd=200_000, collateral_usd=500, is_long=True,
    )
    r = _MockRedis()
    result = asyncio.run(execute_paper(plan=plan, redis_client=r))
    assert result.accepted is True
    assert result.mode == "paper"
    assert result.tx_hash == ""
    assert len(r.calls) == 1
    stream, fields = r.calls[0]
    assert stream == "gmx:execution:paper_log"
    assert fields["user"] == "0xabc"
    assert fields["market"] == "btc"
    assert fields["mode"] == "paper"


def test_execute_live_refuses_when_disabled():
    plan = build_plan(
        trigger=_trigger(),
        size_usd=200_000, collateral_usd=500, is_long=True,
    )
    r = _MockRedis()
    result = asyncio.run(execute_live(
        plan=plan, redis_client=r,
        settings_live_enabled=False,
        settings_wallet_address="0xabc",
        settings_private_key="0xdef",
        settings_rpc_url="https://arb1.arbitrum.io/rpc",
    ))
    assert result.accepted is False
    assert result.error == "live_not_enabled"


def test_execute_live_refuses_missing_wallet():
    plan = build_plan(
        trigger=_trigger(),
        size_usd=200_000, collateral_usd=500, is_long=True,
    )
    r = _MockRedis()
    result = asyncio.run(execute_live(
        plan=plan, redis_client=r,
        settings_live_enabled=True,
        settings_wallet_address="",
        settings_private_key="",
        settings_rpc_url="https://arb1.arbitrum.io/rpc",
    ))
    assert result.accepted is False
    assert result.error == "wallet_creds_missing"


def test_execute_live_refuses_missing_rpc():
    plan = build_plan(
        trigger=_trigger(),
        size_usd=200_000, collateral_usd=500, is_long=True,
    )
    r = _MockRedis()
    result = asyncio.run(execute_live(
        plan=plan, redis_client=r,
        settings_live_enabled=True,
        settings_wallet_address="0xabc",
        settings_private_key="0xdef",
        settings_rpc_url="",
    ))
    assert result.accepted is False
    assert result.error == "rpc_missing"


def test_execute_live_refuses_sdk_not_wired():
    """Even with all gates passing, live mode refuses until SDK is wired."""
    plan = build_plan(
        trigger=_trigger(),
        size_usd=200_000, collateral_usd=500, is_long=True,
    )
    r = _MockRedis()
    result = asyncio.run(execute_live(
        plan=plan, redis_client=r,
        settings_live_enabled=True,
        settings_wallet_address="0xabc",
        settings_private_key="0xdef" * 10,
        settings_rpc_url="https://arb1.arbitrum.io/rpc",
    ))
    assert result.accepted is False
    assert result.error == "sdk_not_wired"
    # Should still log to Redis even when refusing
    assert len(r.calls) == 1
    stream, _ = r.calls[0]
    assert stream == "gmx:execution:live_log"
