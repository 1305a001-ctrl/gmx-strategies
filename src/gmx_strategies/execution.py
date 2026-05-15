"""GMX V2 liquidation execution — paper mode + live mode scaffold.

Modes:
  paper (default)  →  Computes the would-have-been outcome and writes to
                      `gmx:execution:paper_log` Redis stream. NO on-chain
                      calls, NO wallet signing. Safe to run unattended.

  live             →  Builds + signs + submits a real `liquidate()` tx
                      via the GMX V2 Python SDK. Gated behind
                      settings.live_enabled AND non-empty wallet creds.

Live mode is INTENTIONALLY scaffolded but the actual SDK integration is
left as a final TODO so the deploy of the paper module can ship without
the wallet-key permission gate. Hard guards prevent live execution
without explicit operator action.

Per-execution Redis log fields (paper mode):
  ts_unix, user, market, is_long, size_usd, collateral_usd,
  distance_to_liq_pct, estimated_fee_usd, fee_paid_usd,
  gas_cost_usd, slippage_usd, net_paper_pnl_usd, decision_at_unix,
  age_at_decision_sec, mode='paper', tx_hash=''
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from gmx_strategies.liquidation_trigger import LiquidationTrigger

log = logging.getLogger(__name__)


# GMX V2 liquidation fee factor — typically 0.5% of position size, paid
# to the liquidator. Some smaller-cap markets use 1%; we use the
# conservative baseline. Calibrate per market when real fills land.
GMX_LIQ_FEE_FACTOR = 0.005

# Per-execution gas assumption (USD) on Arbitrum. Updated as needed
# from observed receipts.
ARBITRUM_LIQUIDATE_GAS_USD = 1.50


@dataclass(frozen=True)
class ExecutionPlan:
    """Pure: what we WOULD execute. Mode-agnostic — both paper + live use
    this struct as the source of truth for what the tx looks like."""
    user: str
    market: str
    is_long: bool
    size_usd: float
    collateral_usd: float
    distance_to_liq_pct: float
    expected_fee_usd: float
    expected_gas_usd: float
    expected_net_pnl_usd: float
    confidence: float
    reason: str


@dataclass(frozen=True)
class ExecutionResult:
    """Outcome of an attempted execution (paper or live)."""
    plan: ExecutionPlan
    accepted: bool
    mode: str               # 'paper' | 'live'
    tx_hash: str            # populated in live mode if accepted
    error: str              # populated on rejection
    decided_at_unix: float


# ─── Pure helpers ──────────────────────────────────────────────────────


def compute_expected_fee_usd(
    *,
    size_usd: float,
    liq_fee_factor: float = GMX_LIQ_FEE_FACTOR,
) -> float:
    """Pure: GMX V2 liquidator fee for a given position size."""
    if size_usd <= 0:
        return 0.0
    return size_usd * liq_fee_factor


def compute_expected_net_pnl(
    *,
    expected_fee_usd: float,
    gas_usd: float = ARBITRUM_LIQUIDATE_GAS_USD,
    slippage_usd: float = 0.0,
) -> float:
    """Pure: net PnL after gas + slippage. GMX V2 liquidators don't take
    slippage (ADL absorbs the position) → slippage default is 0."""
    return expected_fee_usd - gas_usd - slippage_usd


def build_plan(
    *,
    trigger: LiquidationTrigger,
    size_usd: float,
    collateral_usd: float,
    is_long: bool,
    liq_fee_factor: float = GMX_LIQ_FEE_FACTOR,
    gas_usd: float = ARBITRUM_LIQUIDATE_GAS_USD,
) -> ExecutionPlan:
    """Pure: build an ExecutionPlan from a trigger + position details.
    No I/O, no side effects. Caller decides whether to commit it."""
    fee = compute_expected_fee_usd(
        size_usd=size_usd, liq_fee_factor=liq_fee_factor,
    )
    net_pnl = compute_expected_net_pnl(
        expected_fee_usd=fee, gas_usd=gas_usd,
    )
    return ExecutionPlan(
        user=trigger.user,
        market=trigger.market,
        is_long=is_long,
        size_usd=size_usd,
        collateral_usd=collateral_usd,
        distance_to_liq_pct=trigger.distance_to_liq_pct,
        expected_fee_usd=fee,
        expected_gas_usd=gas_usd,
        expected_net_pnl_usd=net_pnl,
        confidence=trigger.confidence,
        reason=trigger.reason,
    )


def should_execute(
    *,
    plan: ExecutionPlan,
    min_net_profit_usd: float,
    min_confidence: float = 0.5,
) -> tuple[bool, str]:
    """Pure: gate the execution decision.

    Returns (ok, reason). Reason is human-readable for the eval log.
    """
    if plan.expected_net_pnl_usd < min_net_profit_usd:
        return False, (
            f"net_pnl_below_threshold "
            f"({plan.expected_net_pnl_usd:.2f} < {min_net_profit_usd:.2f})"
        )
    if plan.confidence < min_confidence:
        return False, (
            f"confidence_below_threshold "
            f"({plan.confidence:.2f} < {min_confidence:.2f})"
        )
    if plan.distance_to_liq_pct >= 0:
        return False, f"not_yet_liquidatable (distance {plan.distance_to_liq_pct:.3f}pp)"
    return True, "ready"


# ─── Async execution paths ─────────────────────────────────────────────


async def execute_paper(
    *,
    plan: ExecutionPlan,
    redis_client: Any,
    log_stream: str = "gmx:execution:paper_log",
    log_stream_maxlen: int = 1_000_000,
) -> ExecutionResult:
    """Simulate the execution + record to Redis. Returns a result with
    accepted=True (paper mode always 'accepts' — see eval_log for would-have-PnL).
    """
    now = time.time()
    record = {
        "ts_unix": int(now),
        "user": plan.user,
        "market": plan.market,
        "is_long": "1" if plan.is_long else "0",
        "size_usd": f"{plan.size_usd:.4f}",
        "collateral_usd": f"{plan.collateral_usd:.4f}",
        "distance_to_liq_pct": f"{plan.distance_to_liq_pct:.4f}",
        "expected_fee_usd": f"{plan.expected_fee_usd:.4f}",
        "expected_gas_usd": f"{plan.expected_gas_usd:.4f}",
        "expected_net_pnl_usd": f"{plan.expected_net_pnl_usd:.4f}",
        "confidence": f"{plan.confidence:.4f}",
        "reason": plan.reason,
        "mode": "paper",
        "tx_hash": "",
    }
    try:
        await redis_client.xadd(
            log_stream, record, maxlen=log_stream_maxlen, approximate=True,
        )
    except Exception as e:
        log.exception("execute_paper.xadd_failed: %s", e)
        return ExecutionResult(
            plan=plan, accepted=False, mode="paper",
            tx_hash="", error=f"redis_xadd_failed: {e}", decided_at_unix=now,
        )
    return ExecutionResult(
        plan=plan, accepted=True, mode="paper",
        tx_hash="", error="", decided_at_unix=now,
    )


async def execute_live(
    *,
    plan: ExecutionPlan,
    redis_client: Any,
    settings_live_enabled: bool,
    settings_wallet_address: str,
    settings_private_key: str,
    settings_rpc_url: str,
    log_stream: str = "gmx:execution:live_log",
    log_stream_maxlen: int = 100_000,
) -> ExecutionResult:
    """SCAFFOLD: would build + sign + submit the real liquidate() tx via
    the GMX V2 Python SDK. NOT YET IMPLEMENTED.

    Three hard gates before any on-chain call:
      1. settings.live_enabled MUST be True (operator flag)
      2. wallet_address + private_key MUST both be non-empty
      3. rpc_url MUST be non-empty

    Until the SDK integration is wired, this function REFUSES live
    execution with reason='sdk_not_wired' so paper-mode soak runs safely.

    To wire live:
      1. pip install gmx-python (or gmx-sdk — verify package name)
      2. Replace the `raise NotImplementedError` below with the real
         build_signed_tx + submit_tx + wait_for_receipt flow
      3. Add receipt parsing → tx_hash + actual_fee_paid + actual_gas
      4. Test on Arbitrum Sepolia FIRST (testnet) before mainnet flip
    """
    now = time.time()

    # Gate 1: operator flag
    if not settings_live_enabled:
        return ExecutionResult(
            plan=plan, accepted=False, mode="live",
            tx_hash="", error="live_not_enabled", decided_at_unix=now,
        )
    # Gate 2: wallet creds
    if not settings_wallet_address or not settings_private_key:
        return ExecutionResult(
            plan=plan, accepted=False, mode="live",
            tx_hash="", error="wallet_creds_missing", decided_at_unix=now,
        )
    # Gate 3: rpc
    if not settings_rpc_url:
        return ExecutionResult(
            plan=plan, accepted=False, mode="live",
            tx_hash="", error="rpc_missing", decided_at_unix=now,
        )

    # Gate 4: SDK not yet wired. Logged + refused.
    log.warning(
        "execute_live.sdk_not_wired user=%s market=%s — refusing (paper-only)",
        plan.user, plan.market,
    )
    record = {
        "ts_unix": int(now),
        "user": plan.user,
        "market": plan.market,
        "is_long": "1" if plan.is_long else "0",
        "size_usd": f"{plan.size_usd:.4f}",
        "expected_net_pnl_usd": f"{plan.expected_net_pnl_usd:.4f}",
        "mode": "live",
        "tx_hash": "",
        "error": "sdk_not_wired",
    }
    try:
        await redis_client.xadd(
            log_stream, record, maxlen=log_stream_maxlen, approximate=True,
        )
    except Exception:
        log.exception("execute_live.xadd_failed")
    return ExecutionResult(
        plan=plan, accepted=False, mode="live",
        tx_hash="", error="sdk_not_wired", decided_at_unix=now,
    )

    # NOTE: real implementation (week 2+) goes here:
    # from gmx_python.market_order import LiquidationCall
    # call = LiquidationCall(
    #     account=plan.user, market_key=plan.market,
    #     collateral_token=..., is_long=plan.is_long,
    # )
    # tx = call.build(wallet_address=settings_wallet_address)
    # signed = call.sign(tx, private_key=settings_private_key)
    # tx_hash = await call.submit(signed, rpc_url=settings_rpc_url)
    # receipt = await call.wait_for_receipt(tx_hash, timeout=60)
    # actual_fee_usd = parse_fee_from_receipt(receipt)
    # actual_gas_usd = parse_gas_from_receipt(receipt)
    # return ExecutionResult(...)


__all__ = [
    "ARBITRUM_LIQUIDATE_GAS_USD",
    "ExecutionPlan",
    "ExecutionResult",
    "GMX_LIQ_FEE_FACTOR",
    "build_plan",
    "compute_expected_fee_usd",
    "compute_expected_net_pnl",
    "execute_live",
    "execute_paper",
    "should_execute",
]
