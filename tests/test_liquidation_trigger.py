"""Tests for GMX liquidation trigger pure helpers."""
from __future__ import annotations

import pytest

from gmx_strategies.liquidation_trigger import (
    GMXPosition,
    detect_trigger,
    distance_to_liq_pct,
    liquidation_price,
)


def _long(*, entry: float = 100_000.0, size: float = 100_000.0,
          collateral: float = 10_000.0) -> GMXPosition:
    return GMXPosition(
        user="0xabc", market="btc", is_long=True,
        size_usd=size, collateral_usd=collateral, entry_price=entry,
        leverage=size/collateral, liquidation_threshold_pct=0.005,
    )


def _short(*, entry: float = 100_000.0, size: float = 100_000.0,
           collateral: float = 10_000.0) -> GMXPosition:
    return GMXPosition(
        user="0xabc", market="btc", is_long=False,
        size_usd=size, collateral_usd=collateral, entry_price=entry,
        leverage=size/collateral, liquidation_threshold_pct=0.005,
    )


def test_liquidation_price_long_position():
    # 10x long with 0.5% threshold:
    # margin_pct = 0.10 - 0.005 = 0.095
    # liq = 100000 * (1 - 0.095) = 90500
    p = _long()
    assert liquidation_price(p) == pytest.approx(90_500.0)


def test_liquidation_price_short_position():
    p = _short()
    assert liquidation_price(p) == pytest.approx(109_500.0)


def test_liquidation_price_zero_size():
    p = GMXPosition(
        user="x", market="btc", is_long=True,
        size_usd=0, collateral_usd=100, entry_price=100,
        leverage=0, liquidation_threshold_pct=0.005,
    )
    assert liquidation_price(p) == 0.0


def test_distance_positive_when_safe_long():
    # long, current 95k, liq 90.5k → distance = 4.5/95 ~ 4.7%
    p = _long()
    d = distance_to_liq_pct(p, 95_000.0)
    assert d == pytest.approx((95_000.0 - 90_500.0) / 95_000.0 * 100.0)
    assert d > 0


def test_distance_negative_when_past_liq_long():
    p = _long()
    d = distance_to_liq_pct(p, 90_000.0)  # below 90.5k liq
    assert d < 0


def test_distance_positive_when_safe_short():
    p = _short()
    d = distance_to_liq_pct(p, 105_000.0)  # below 109.5k liq for short = safe
    assert d > 0


def test_detect_trigger_returns_none_when_far_from_liq():
    p = _long()
    sig = detect_trigger(p, 95_000.0, watch_margin=1.05, estimated_fee_usd=100.0)
    # distance ~4.7% which is > (1.05-1.0)*100 = 5% threshold... actually 4.7 < 5
    # so it should be `watching`, not None.
    assert sig is not None
    assert sig.reason == "watching"


def test_detect_trigger_returns_none_when_well_above_liq():
    p = _long()
    sig = detect_trigger(p, 100_000.0, watch_margin=1.05, estimated_fee_usd=100.0)
    # distance ~9.5% > 5% threshold → no signal at all
    assert sig is None


def test_detect_trigger_fires_when_past_liq():
    p = _long()
    sig = detect_trigger(p, 89_000.0, watch_margin=1.05, estimated_fee_usd=100.0)
    assert sig is not None
    assert sig.reason == "trigger"
    assert sig.distance_to_liq_pct < 0
    assert sig.estimated_fee_usd == 100.0
    assert sig.confidence > 0.5


def test_detect_trigger_fires_for_short_when_price_above_liq():
    p = _short()
    sig = detect_trigger(p, 111_000.0, watch_margin=1.05, estimated_fee_usd=100.0)
    assert sig is not None
    assert sig.reason == "trigger"
