"""Gas-budget-aware throttle for GMX live liquidations.

GMX liquidators don't take position-taking risk (ADL absorbs the bad
position; the keeper just collects the fee). So the "capital at risk"
is purely the gas budget — every failed/reverted fire burns ~$0.50-$5
of ETH without recouping anything.

This module enforces:
  1. A daily ceiling on gas spend (default $50/day = ~10-100 fires)
  2. A rolling 7-day ceiling (default $200/week)
  3. A consecutive-revert circuit breaker (default 5 reverts → halt)
  4. A bankroll-tier ladder that scales the daily ceiling as the bot
     proves profitability — matching the bankroll-aware sizing pattern
     on the poly side

Like bankroll-aware sizing, the tier ladder ratchets UP on profit and
ratchets DOWN on drawdown. State is cached in Redis; preflight reads
on every fire.

Tier ladder
───────────
T0: $300 gas budget,  $50/day cap, $200/wk cap (seed)
T1: pnl >= $300       $75/day,  $300/wk     (proved out)
T2: pnl >= $1000      $150/day, $600/wk
T3: pnl >= $3000      $300/day, $1500/wk
T4: pnl >= $8000      $600/day, $3000/wk
T5: pnl >= $20000     $1500/day, $7500/wk
T6: pnl >= $50000     $3000/day, $15000/wk

Each fire records gas_used + effective_gas_price + tx_hash to the
gmx:execution:live_log stream. The budget checker sums recent records
to determine spend over the rolling windows.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass

from gmx_strategies.redis_client import r

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class GasBudgetTier:
    pnl_threshold_usd: float
    daily_cap_usd: float
    weekly_cap_usd: float
    label: str


DEFAULT_GAS_LADDER: tuple[GasBudgetTier, ...] = (
    GasBudgetTier(    0.0,   50.0,   200.0, "T0_seed"),
    GasBudgetTier(  300.0,   75.0,   300.0, "T1_proved"),
    GasBudgetTier( 1000.0,  150.0,   600.0, "T2_scaling"),
    GasBudgetTier( 3000.0,  300.0,  1500.0, "T3_meaningful"),
    GasBudgetTier( 8000.0,  600.0,  3000.0, "T4_real"),
    GasBudgetTier(20000.0, 1500.0,  7500.0, "T5_significant"),
    GasBudgetTier(50000.0, 3000.0, 15000.0, "T6_top"),
)


# Redis state keys
GAS_BUDGET_STATE_KEY = "gmx:gas_budget:state"
CONSECUTIVE_REVERTS_KEY = "gmx:gas_budget:consecutive_reverts"
DAILY_SPEND_KEY = "gmx:gas_budget:daily_spend:{date}"
WEEKLY_SPEND_KEY = "gmx:gas_budget:weekly_spend:{iso_week}"

# Circuit breaker: stop firing after this many consecutive reverts.
# Each successful fire resets the counter.
DEFAULT_REVERT_BREAKER = 5

# How long state stays valid before we fall back to T0.
GAS_STATE_TTL_SEC = 600


def select_gas_tier(
    realized_pnl_usd: float,
    *,
    ladder: tuple[GasBudgetTier, ...] = DEFAULT_GAS_LADDER,
) -> GasBudgetTier:
    """Pure: pick the highest tier whose threshold ≤ realized_pnl_usd."""
    active = ladder[0]
    for tier in ladder:
        if realized_pnl_usd >= tier.pnl_threshold_usd:
            active = tier
        else:
            break
    return active


def _date_key(now_unix: float | None = None) -> str:
    """Pure: UTC date in YYYY-MM-DD for daily spend keying."""
    import datetime
    ts = now_unix if now_unix is not None else time.time()
    return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).strftime("%Y-%m-%d")


def _iso_week_key(now_unix: float | None = None) -> str:
    """Pure: ISO year-week (e.g. '2026-W19') for weekly spend keying."""
    import datetime
    ts = now_unix if now_unix is not None else time.time()
    d = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
    iso = d.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


async def record_gas_spend(usd_cost: float) -> None:
    """Async: increment the rolling spend counters. Called after every
    confirmed liquidation tx (success OR revert — both burn gas)."""
    if usd_cost <= 0:
        return
    pipe = r().pipeline()
    daily_key = DAILY_SPEND_KEY.format(date=_date_key())
    weekly_key = WEEKLY_SPEND_KEY.format(iso_week=_iso_week_key())
    # Atomic increments with TTLs that exceed the lookback window
    pipe.incrbyfloat(daily_key, usd_cost)
    pipe.expire(daily_key, 48 * 3600)
    pipe.incrbyfloat(weekly_key, usd_cost)
    pipe.expire(weekly_key, 14 * 24 * 3600)
    try:
        await pipe.execute()
    except Exception as e:
        log.warning("gas_budget.record_failed err=%s", e)


async def read_daily_spend() -> float:
    """Async: cumulative gas spend in USD for today (UTC)."""
    daily_key = DAILY_SPEND_KEY.format(date=_date_key())
    try:
        raw = await r().get(daily_key)
    except Exception:
        return 0.0
    try:
        return float(raw or 0.0)
    except (TypeError, ValueError):
        return 0.0


async def read_weekly_spend() -> float:
    """Async: cumulative gas spend in USD for this ISO week."""
    weekly_key = WEEKLY_SPEND_KEY.format(iso_week=_iso_week_key())
    try:
        raw = await r().get(weekly_key)
    except Exception:
        return 0.0
    try:
        return float(raw or 0.0)
    except (TypeError, ValueError):
        return 0.0


async def incr_consecutive_reverts() -> int:
    """Async: bump the consecutive-revert counter. Returns new value."""
    try:
        n = await r().incr(CONSECUTIVE_REVERTS_KEY)
        await r().expire(CONSECUTIVE_REVERTS_KEY, 7 * 24 * 3600)
    except Exception as e:
        log.warning("gas_budget.revert_incr_failed err=%s", e)
        return 0
    return int(n)


async def reset_consecutive_reverts() -> None:
    """Async: clear the counter after a successful fire."""
    try:
        await r().delete(CONSECUTIVE_REVERTS_KEY)
    except Exception as e:
        log.debug("gas_budget.revert_reset_failed err=%s", e)


async def read_consecutive_reverts() -> int:
    """Async: current consecutive-revert count."""
    try:
        raw = await r().get(CONSECUTIVE_REVERTS_KEY)
    except Exception:
        return 0
    try:
        return int(raw or 0)
    except (TypeError, ValueError):
        return 0


def is_within_budget(
    *,
    daily_spend_usd: float,
    weekly_spend_usd: float,
    consecutive_reverts: int,
    tier: GasBudgetTier,
    breaker: int = DEFAULT_REVERT_BREAKER,
) -> tuple[bool, str]:
    """Pure: return (allow, reason) given current state.

    All three gates must pass:
      - daily_spend < tier.daily_cap_usd
      - weekly_spend < tier.weekly_cap_usd
      - consecutive_reverts < breaker
    """
    if consecutive_reverts >= breaker:
        return False, f"consecutive_reverts_breaker:{consecutive_reverts}/{breaker}"
    if daily_spend_usd >= tier.daily_cap_usd:
        return False, f"daily_cap_breached:${daily_spend_usd:.2f}/${tier.daily_cap_usd:.2f}"
    if weekly_spend_usd >= tier.weekly_cap_usd:
        return False, f"weekly_cap_breached:${weekly_spend_usd:.2f}/${tier.weekly_cap_usd:.2f}"
    return True, "ok"


async def check_budget_allow(
    *, realized_pnl_usd: float, breaker: int = DEFAULT_REVERT_BREAKER,
) -> tuple[bool, str]:
    """Async: full gate check. Returns (allow, reason)."""
    tier = select_gas_tier(realized_pnl_usd)
    daily = await read_daily_spend()
    weekly = await read_weekly_spend()
    reverts = await read_consecutive_reverts()
    return is_within_budget(
        daily_spend_usd=daily,
        weekly_spend_usd=weekly,
        consecutive_reverts=reverts,
        tier=tier,
        breaker=breaker,
    )


__all__ = [
    "GasBudgetTier",
    "DEFAULT_GAS_LADDER",
    "GAS_BUDGET_STATE_KEY",
    "CONSECUTIVE_REVERTS_KEY",
    "DAILY_SPEND_KEY",
    "WEEKLY_SPEND_KEY",
    "DEFAULT_REVERT_BREAKER",
    "GAS_STATE_TTL_SEC",
    "select_gas_tier",
    "is_within_budget",
    "record_gas_spend",
    "read_daily_spend",
    "read_weekly_spend",
    "incr_consecutive_reverts",
    "reset_consecutive_reverts",
    "read_consecutive_reverts",
    "check_budget_allow",
]
