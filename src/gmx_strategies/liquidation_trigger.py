"""GMX V2 liquidation triggering — pure helpers.

The honest edge per the May 2026 strategy doc:
  GMX positions liquidate when (collateral - losses) < (0.4–1%) × position.
  With priority Data Streams, you detect when a position crosses its
  liquidation price BEFORE Keepers process it. Trigger first, earn the
  liquidation fee.

This is the actual Data Streams edge on GMX (NOT funding rate arb,
which is a yield strategy that anyone can do).
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GMXPosition:
    user: str
    market: str           # 'btc', 'eth', 'sol', etc. — matches chainlink alias
    is_long: bool
    size_usd: float
    collateral_usd: float
    entry_price: float
    leverage: float
    liquidation_threshold_pct: float    # GMX V2: typically 0.005 (0.5%)


@dataclass(frozen=True)
class LiquidationTrigger:
    user: str
    market: str
    distance_to_liq_pct: float          # how close (signed; negative = past)
    estimated_fee_usd: float
    confidence: float
    reason: str


def liquidation_price(pos: GMXPosition) -> float:
    """Pure: the price at which this position becomes liquidatable.

    For a long position:
      liq_price = entry × (1 - (collateral/size - threshold))
    For a short:
      liq_price = entry × (1 + (collateral/size - threshold))

    Threshold is the maintenance margin requirement.
    """
    if pos.size_usd <= 0:
        return 0.0
    margin_pct = pos.collateral_usd / pos.size_usd - pos.liquidation_threshold_pct
    if pos.is_long:
        return pos.entry_price * (1.0 - margin_pct)
    return pos.entry_price * (1.0 + margin_pct)


def distance_to_liq_pct(pos: GMXPosition, current_price: float) -> float:
    """Pure: how far is the current price from liq, as a % of current price.

    Positive: position is safe (above liq for longs, below for shorts).
    Negative: position is past liq and could be triggered.
    """
    if current_price <= 0:
        return 0.0
    liq = liquidation_price(pos)
    if pos.is_long:
        return (current_price - liq) / current_price * 100.0
    return (liq - current_price) / current_price * 100.0


def detect_trigger(
    pos: GMXPosition,
    current_price: float,
    *,
    watch_margin: float,
    estimated_fee_usd: float,
) -> LiquidationTrigger | None:
    """Pure: returns a trigger when the position is at-or-past liq.

    `watch_margin`: positions with HF (collateral/size factor) ≥ this
    are skipped (out of the danger zone for now). The tighter the
    margin, the more often we wake to re-check.

    For an actual trigger to fire we need distance < 0 (i.e., past liq
    according to current oracle price); the keeper transaction is
    profitable ASAP.
    """
    distance = distance_to_liq_pct(pos, current_price)
    if distance > 0.0:
        # Not yet at liquidation. Watch the position if it's close.
        if distance < (watch_margin - 1.0) * 100.0:
            return LiquidationTrigger(
                user=pos.user,
                market=pos.market,
                distance_to_liq_pct=distance,
                estimated_fee_usd=0.0,    # not triggerable yet
                confidence=0.0,
                reason="watching",
            )
        return None
    # distance ≤ 0 → liquidatable
    confidence = min(0.95, 0.5 + abs(distance) * 0.05)
    return LiquidationTrigger(
        user=pos.user,
        market=pos.market,
        distance_to_liq_pct=distance,
        estimated_fee_usd=estimated_fee_usd,
        confidence=confidence,
        reason="trigger",
    )


__all__ = [
    "GMXPosition",
    "LiquidationTrigger",
    "detect_trigger",
    "distance_to_liq_pct",
    "liquidation_price",
]
