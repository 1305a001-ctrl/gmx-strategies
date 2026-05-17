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
    chain: str = "arbitrum",
    collateral_token_address: str = "",
    oracle_reports: tuple = (),
    log_stream: str = "gmx:execution:live_log",
    log_stream_maxlen: int = 100_000,
    submit_timeout_sec: float = 60.0,
    # Gas budget gate (2026-05-17 wiring). When True, refuse pre-fire
    # if daily/weekly cap breached or revert breaker tripped. Pass the
    # current realized PnL so the gate picks the active tier.
    gas_budget_enabled: bool = False,
    realized_pnl_usd: float = 0.0,
) -> ExecutionResult:
    """Build + sign + broadcast a real `executeLiquidation` tx via the
    GMX V2 LiquidationHandler. Gated by four hard checks; refuses on any
    failure path with a structured error.

    Caller (liquidation_watcher) must provide:
      - chain                          'arbitrum' | 'avalanche'
      - collateral_token_address       0x... of the collateral token
      - oracle_reports                 tuple[OracleReport, ...] freshly
                                       pulled from chainlink:<alias>:reports
                                       Redis stream (max ~10s stale).

    Four hard gates (all must pass):
      1. settings.live_enabled = True
      2. wallet_address + private_key non-empty
      3. rpc_url non-empty
      4. oracle_reports non-empty (no oracle data → no fresh price → revert)
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
    # Gate 4: oracle reports
    if not oracle_reports:
        return ExecutionResult(
            plan=plan, accepted=False, mode="live",
            tx_hash="", error="oracle_reports_missing", decided_at_unix=now,
        )
    # Gate 5: collateral token must be known (caller supplies)
    if not collateral_token_address:
        return ExecutionResult(
            plan=plan, accepted=False, mode="live",
            tx_hash="", error="collateral_token_missing", decided_at_unix=now,
        )

    # Gate 6: gas budget — refuse if today/this-week's cap is hit OR if
    # consecutive-revert breaker is tripped. Lazy import to avoid the
    # cost when the flag is off.
    if gas_budget_enabled:
        from gmx_strategies.gas_budget import check_budget_allow
        allow, gb_reason = await check_budget_allow(realized_pnl_usd=realized_pnl_usd)
        if not allow:
            return ExecutionResult(
                plan=plan, accepted=False, mode="live",
                tx_hash="", error=f"gas_budget:{gb_reason}", decided_at_unix=now,
            )

    # Lazy imports keep cold-start cheap
    from gmx_strategies.tx_builder import (
        LiquidationTxRequest,
        build_liquidation_tx,
        chain_id_for,
    )
    from gmx_strategies.tx_signer import estimate_gas, sign_tx, submit_and_wait
    from web3 import AsyncHTTPProvider, AsyncWeb3

    cid = chain_id_for(chain)
    if cid == 0:
        return ExecutionResult(
            plan=plan, accepted=False, mode="live",
            tx_hash="", error=f"unknown_chain={chain}", decided_at_unix=now,
        )

    # Fetch nonce. Single RPC round-trip; we don't pre-cache because
    # accuracy matters more than latency at this scale (~10/day live).
    try:
        w3 = AsyncWeb3(AsyncHTTPProvider(settings_rpc_url))
        nonce = await w3.eth.get_transaction_count(
            settings_wallet_address, "pending",
        )
    except Exception as e:
        return ExecutionResult(
            plan=plan, accepted=False, mode="live",
            tx_hash="", error=f"nonce_fetch_failed: {e}", decided_at_unix=now,
        )

    req = LiquidationTxRequest(
        chain=chain,
        account=plan.user,
        market=plan.market,
        collateral_token=collateral_token_address,
        is_long=plan.is_long,
        oracle_reports=oracle_reports,
        nonce=int(nonce),
        chain_id=cid,
        sender_address=settings_wallet_address,
    )

    # Build
    try:
        tx = build_liquidation_tx(req)
    except Exception as e:
        return ExecutionResult(
            plan=plan, accepted=False, mode="live",
            tx_hash="", error=f"build_failed: {e}", decided_at_unix=now,
        )

    # Refine gas via estimate. Bumps the static DEFAULT_GAS_LIMIT down
    # if the estimator says the real consumption will be lower.
    try:
        estimated = await estimate_gas(rpc_url=settings_rpc_url, tx=tx)
        if estimated > 0:
            tx["gas"] = estimated
    except Exception as e:
        # Estimation failure means the tx would revert. Refuse + log
        # without burning gas.
        await _xadd_live_result(
            redis_client, log_stream, log_stream_maxlen,
            plan, now, "", f"estimate_revert: {e}",
        )
        return ExecutionResult(
            plan=plan, accepted=False, mode="live",
            tx_hash="", error=f"estimate_revert: {e}", decided_at_unix=now,
        )

    # Sign
    try:
        signed = sign_tx(tx=tx, private_key=settings_private_key)
    except Exception as e:
        return ExecutionResult(
            plan=plan, accepted=False, mode="live",
            tx_hash="", error=f"sign_failed: {e}", decided_at_unix=now,
        )

    # Submit + wait for receipt
    try:
        receipt = await submit_and_wait(
            rpc_url=settings_rpc_url,
            signed=signed,
            timeout_sec=submit_timeout_sec,
        )
    except Exception as e:
        return ExecutionResult(
            plan=plan, accepted=False, mode="live",
            tx_hash="", error=f"submit_failed: {e}", decided_at_unix=now,
        )

    success = receipt.status == 1
    actual_fee_usd = (
        receipt.gas_used * receipt.effective_gas_price / 10**18 * _ETH_PRICE_USD_FALLBACK
    )
    err = "" if success else f"reverted: {receipt.revert_reason}"

    # Record gas spend + maintain circuit breaker. Every confirmed tx
    # (success OR revert) burns gas; both must increment daily/weekly.
    # Consecutive-revert counter resets on success, increments on revert.
    if gas_budget_enabled:
        from gmx_strategies.gas_budget import (
            incr_consecutive_reverts,
            record_gas_spend,
            reset_consecutive_reverts,
        )
        try:
            await record_gas_spend(actual_fee_usd)
            if success:
                await reset_consecutive_reverts()
            else:
                await incr_consecutive_reverts()
        except Exception:
            log.exception("execute_live.gas_budget_update_failed")

    await _xadd_live_result(
        redis_client, log_stream, log_stream_maxlen,
        plan, now, receipt.tx_hash, err,
        gas_used=receipt.gas_used,
        block_number=receipt.block_number,
        actual_gas_cost_usd=actual_fee_usd,
    )
    return ExecutionResult(
        plan=plan,
        accepted=success,
        mode="live",
        tx_hash=receipt.tx_hash,
        error=err,
        decided_at_unix=now,
    )


# Fallback ETH price for gas cost USD math when we don't have a live
# Chainlink price loaded. The actual gas cost in ETH is exact; the USD
# conversion is for log readability.
_ETH_PRICE_USD_FALLBACK = 3500.0


async def _xadd_live_result(
    redis_client: Any,
    log_stream: str,
    log_stream_maxlen: int,
    plan: ExecutionPlan,
    now: float,
    tx_hash: str,
    error: str,
    *,
    gas_used: int = 0,
    block_number: int = 0,
    actual_gas_cost_usd: float = 0.0,
) -> None:
    record = {
        "ts_unix": int(now),
        "user": plan.user,
        "market": plan.market,
        "is_long": "1" if plan.is_long else "0",
        "size_usd": f"{plan.size_usd:.4f}",
        "expected_net_pnl_usd": f"{plan.expected_net_pnl_usd:.4f}",
        "mode": "live",
        "tx_hash": tx_hash,
        "error": error,
        "gas_used": str(gas_used),
        "block_number": str(block_number),
        "actual_gas_cost_usd": f"{actual_gas_cost_usd:.4f}",
    }
    try:
        await redis_client.xadd(
            log_stream, record, maxlen=log_stream_maxlen, approximate=True,
        )
    except Exception:
        log.exception("execute_live.xadd_failed")


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
