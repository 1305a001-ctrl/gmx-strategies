"""GMX V2 funding rate arbitrage (delta-neutral) — pure helpers.

Honest framing per the May 2026 doc:
  This strategy does NOT require Data Streams. Anyone can do it.
  Returns of 1–3%/month on capital are realistic and replicable.
  It's a good DeFi yield strategy. We include it because the
  infrastructure overlaps; not because it's a unique edge.

Mechanics:
  When GMX has heavily skewed open interest (e.g., 90% longs), longs
  pay funding to shorts. Take the minority position (short here),
  hedge it on a CEX (long ETH on Binance). Net direction exposure: 0.
  Net income: the funding rate.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FundingState:
    market: str
    longs_oi_usd: float
    shorts_oi_usd: float
    funding_rate_per_8h: float  # signed; positive = longs pay shorts


@dataclass(frozen=True)
class FundingArbSignal:
    market: str
    direction: str  # 'short_gmx_long_cex' or 'long_gmx_short_cex'
    funding_rate_per_8h: float
    annualized_yield_pct: float
    target_position_usd: float


def imbalance_ratio(state: FundingState) -> float:
    """Pure: longs / total. >0.5 = long-skewed, <0.5 = short-skewed."""
    total = state.longs_oi_usd + state.shorts_oi_usd
    if total <= 0:
        return 0.5
    return state.longs_oi_usd / total


def annualized_yield_pct(funding_rate_per_8h: float) -> float:
    """Pure: convert 8h funding rate to annualized %.

    There are 365 × 3 = 1095 funding periods per year. Compounding
    is conservative; we use simple multiplication (matches how most
    GMX users think about funding yield).
    """
    return abs(funding_rate_per_8h) * 1095.0 * 100.0


def detect_signal(
    state: FundingState,
    *,
    min_rate: float,
    max_position_usd: float,
) -> FundingArbSignal | None:
    """Pure: emit a delta-neutral signal when funding rate exceeds threshold.

    Direction: take the minority side (the one that RECEIVES funding).
    """
    if abs(state.funding_rate_per_8h) < min_rate:
        return None
    # Positive rate = longs pay shorts → we want to be SHORT on GMX (receive)
    # Negative rate = shorts pay longs → we want to be LONG on GMX
    if state.funding_rate_per_8h > 0:
        direction = "short_gmx_long_cex"
    else:
        direction = "long_gmx_short_cex"

    return FundingArbSignal(
        market=state.market,
        direction=direction,
        funding_rate_per_8h=state.funding_rate_per_8h,
        annualized_yield_pct=annualized_yield_pct(state.funding_rate_per_8h),
        target_position_usd=max_position_usd,
    )


__all__ = [
    "FundingArbSignal",
    "FundingState",
    "annualized_yield_pct",
    "detect_signal",
    "imbalance_ratio",
]
