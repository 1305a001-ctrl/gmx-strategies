"""Tests for GMX funding rate arb pure helpers."""
from __future__ import annotations

import pytest

from gmx_strategies.funding_arb import (
    FundingState,
    annualized_yield_pct,
    detect_signal,
    imbalance_ratio,
)


def test_imbalance_ratio_long_skewed():
    state = FundingState("btc", 9_000_000, 1_000_000, 0.0005)
    assert imbalance_ratio(state) == 0.9


def test_imbalance_ratio_balanced():
    state = FundingState("btc", 5_000_000, 5_000_000, 0.0)
    assert imbalance_ratio(state) == 0.5


def test_imbalance_ratio_zero_oi_returns_neutral():
    state = FundingState("btc", 0, 0, 0)
    assert imbalance_ratio(state) == 0.5


def test_annualized_yield_basic():
    # 0.05% per 8hr → 0.05% × 1095 = 54.75% annualized
    assert annualized_yield_pct(0.0005) == pytest.approx(54.75)


def test_annualized_yield_handles_negative():
    # Negative funding (shorts pay longs); we still report positive annual
    assert annualized_yield_pct(-0.0005) == pytest.approx(54.75)


def test_detect_signal_short_gmx_when_longs_pay():
    state = FundingState("btc", 9_000_000, 1_000_000, 0.001)  # longs pay
    sig = detect_signal(state, min_rate=0.0005, max_position_usd=50_000)
    assert sig is not None
    assert sig.direction == "short_gmx_long_cex"


def test_detect_signal_long_gmx_when_shorts_pay():
    state = FundingState("btc", 1_000_000, 9_000_000, -0.001)
    sig = detect_signal(state, min_rate=0.0005, max_position_usd=50_000)
    assert sig is not None
    assert sig.direction == "long_gmx_short_cex"


def test_detect_signal_returns_none_below_threshold():
    state = FundingState("btc", 5_500_000, 4_500_000, 0.0001)
    sig = detect_signal(state, min_rate=0.0005, max_position_usd=50_000)
    assert sig is None
