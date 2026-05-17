"""GMX liquidation watcher — paper-mode glue.

One cycle (default 30s):
  1. POST GraphQL to GMX V2 subgraph → list[RawSubgraphPosition]
  2. For each, look up chainlink:<alias>:latest (current oracle price)
  3. Enrich raw → GMXPosition (skip rows with no alias / no price)
  4. detect_trigger(pos, current_price, watch_margin, fee) → trigger | None
  5. XADD `gmx:eval_log` per trigger (paper mode — no order routed)

The keeper / order-execution path is intentionally NOT wired here. That
comes in week 2 once paper mode shows we're catching ≥60% of profitable
liquidations.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any

from gmx_strategies.execution import build_plan, execute_paper, should_execute
from gmx_strategies.gmx_subgraph import (
    MARKET_ADDRESS_TO_ALIAS,
    fetch_open_positions,
    raw_to_gmx_position,
)
from gmx_strategies.liquidation_trigger import detect_trigger

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CycleStats:
    fetched: int
    parsed: int
    no_price: int
    no_alias: int
    triggers: int
    executions_paper: int = 0   # plans that passed should_execute + were XADDed
    executions_rejected: int = 0   # plans rejected by the gate (logged reason only)
    executions_cooldown: int = 0   # plans skipped because a recent fire is on cooldown


def _parse_chainlink_payload(raw: str | None) -> float | None:
    """Pure: chainlink:<alias>:latest payload → mid price.

    The chainlink-streams writer publishes {"price": "<decimal>", ...}
    as JSON. Defensive on parse failure.
    """
    if not raw:
        return None
    try:
        d = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(d, dict):
        return None
    p = d.get("price") or d.get("mid") or d.get("benchmark_price")
    if p is None:
        return None
    try:
        v = float(p)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def build_eval_log_entry(
    *,
    trigger: Any,
    pos: Any,
    current_price: float,
    cycle_unix: int,
) -> dict[str, str]:
    """Pure: build a XADD field dict for the gmx:eval_log stream.

    All values stringified — Redis streams want str fields. JSON-encode
    nested objects so a downstream consumer can decode cleanly.
    """
    return {
        "ts_unix": str(cycle_unix),
        "user": trigger.user,
        "market": trigger.market,
        "is_long": "1" if pos.is_long else "0",
        "size_usd": f"{pos.size_usd:.4f}",
        "collateral_usd": f"{pos.collateral_usd:.4f}",
        "leverage": f"{pos.leverage:.4f}",
        "entry_price": f"{pos.entry_price:.6f}",
        "current_price": f"{current_price:.6f}",
        "distance_to_liq_pct": f"{trigger.distance_to_liq_pct:.4f}",
        "estimated_fee_usd": f"{trigger.estimated_fee_usd:.4f}",
        "confidence": f"{trigger.confidence:.4f}",
        "reason": trigger.reason,
    }


async def run_watch_cycle(
    *,
    httpx_client: Any,
    redis_client: Any,
    subgraph_url: str,
    eval_log_stream: str,
    eval_log_maxlen: int,
    watch_margin: float,
    estimated_fee_usd: float,
    chainlink_key_template: str = "chainlink:{alias}:latest",
    page_size: int = 200,
    max_pages: int = 10,
    alias_map: dict[str, str] | None = None,
    # Paper-execution wiring (off by default to preserve the original
    # "eval-log only" behaviour for existing callers + tests).
    execution_paper_enabled: bool = False,
    execution_paper_log_stream: str = "gmx:execution:paper_log",
    execution_paper_log_maxlen: int = 1_000_000,
    execution_min_net_profit_usd: float = 50.0,
    execution_min_confidence: float = 0.5,
    execution_cooldown_sec: int = 0,   # 0 disables cooldown (preserves existing contract)
    execution_cooldown_key_template: str = "gmx:execution:cooldown:{user}:{market}",
    # G7 (2026-05-17) — on-chain Reader re-check.
    onchain_recheck_enabled: bool = False,
    onchain_recheck_concurrency: int = 8,
    onchain_rpc_url: str = "",
    onchain_chain: str = "arbitrum",
) -> CycleStats:
    """One iteration. Returns stats for logging."""
    raws = await fetch_open_positions(
        httpx_client, subgraph_url,
        page_size=page_size, max_pages=max_pages,
    )
    if not raws:
        return CycleStats(fetched=0, parsed=0, no_price=0, no_alias=0, triggers=0)

    cycle_unix = int(time.time())
    no_price = 0
    no_alias = 0
    triggers = 0
    executions_paper = 0
    executions_rejected = 0
    executions_cooldown = 0
    amap = alias_map or MARKET_ADDRESS_TO_ALIAS

    # G7 (2026-05-17) — on-chain Reader re-check is gated by the
    # caller's flag + an RPC URL. The reader itself ships in onchain.py;
    # the per-cycle wiring is staged so the flag is testable + flippable,
    # but the actual RPC fetch is deferred until the (chain, market_alias)
    # → (collateral_token, is_long) map is populated downstream. For
    # paper-mode, this branch is a no-op log line.
    if onchain_recheck_enabled and onchain_rpc_url:
        log.debug(
            "liq_watcher.onchain_recheck_armed chain=%s concurrency=%d",
            onchain_chain, onchain_recheck_concurrency,
        )

    # Cache chainlink prices by alias so we hit Redis O(unique-markets), not
    # O(positions).
    price_cache: dict[str, float | None] = {}

    for raw in raws:
        alias = amap.get(raw.market_address)
        if alias is None:
            no_alias += 1
            continue
        if alias not in price_cache:
            try:
                payload = await redis_client.get(
                    chainlink_key_template.format(alias=alias),
                )
            except Exception:
                payload = None
            price_cache[alias] = _parse_chainlink_payload(payload)
        price = price_cache[alias]
        if price is None:
            no_price += 1
            continue

        # Prefer the subgraph's entryPrice if available; fall back to the
        # current oracle price (degenerate health calc — won't trigger
        # unless the position is already structurally under-margined).
        effective_entry = raw.entry_price if raw.entry_price else price
        pos = raw_to_gmx_position(
            raw, entry_price=effective_entry, alias_map=amap,
        )
        if pos is None:
            no_alias += 1   # alias map missed somehow — bucket with no_alias
            continue

        trigger = detect_trigger(
            pos, current_price=price,
            watch_margin=watch_margin,
            estimated_fee_usd=estimated_fee_usd,
        )
        if trigger is None or trigger.reason != "trigger":
            continue

        # Eval-log every trigger (the "we saw a candidate" feed).
        try:
            await redis_client.xadd(
                eval_log_stream,
                build_eval_log_entry(
                    trigger=trigger, pos=pos,
                    current_price=price, cycle_unix=cycle_unix,
                ),
                maxlen=eval_log_maxlen,
                approximate=True,
            )
            triggers += 1
        except Exception:
            log.exception("liq_watcher.xadd_failed user=%s market=%s",
                          trigger.user, trigger.market)

        # Paper-execution feed: did this candidate actually pass the
        # profit/confidence gate? Only writes when explicitly enabled by
        # the caller so the existing eval-log-only contract is preserved.
        if execution_paper_enabled:
            # Cooldown gate — without this, the same eligible whale is
            # XADDed every cycle (default 30s) until they actually get
            # liquidated. Real-world a keeper fires once; we model that
            # by suppressing same (user, market) inside the cooldown
            # window. cooldown_sec=0 disables (matches test contract).
            if execution_cooldown_sec > 0:
                cooldown_key = execution_cooldown_key_template.format(
                    user=trigger.user, market=trigger.market,
                )
                try:
                    on_cooldown = await redis_client.get(cooldown_key)
                except Exception:
                    on_cooldown = None
                if on_cooldown:
                    executions_cooldown += 1
                    continue

            plan = build_plan(
                trigger=trigger,
                size_usd=pos.size_usd,
                collateral_usd=pos.collateral_usd,
                is_long=pos.is_long,
            )
            ok, reason = should_execute(
                plan=plan,
                min_net_profit_usd=execution_min_net_profit_usd,
                min_confidence=execution_min_confidence,
            )
            if ok:
                try:
                    await execute_paper(
                        plan=plan,
                        redis_client=redis_client,
                        log_stream=execution_paper_log_stream,
                        log_stream_maxlen=execution_paper_log_maxlen,
                    )
                    executions_paper += 1
                    # Set the cooldown after a successful paper-fire.
                    if execution_cooldown_sec > 0:
                        try:
                            await redis_client.set(
                                cooldown_key,
                                str(cycle_unix),
                                ex=execution_cooldown_sec,
                            )
                        except Exception:
                            log.exception(
                                "liq_watcher.cooldown_set_failed user=%s market=%s",
                                trigger.user, trigger.market,
                            )
                except Exception:
                    log.exception(
                        "liq_watcher.execute_paper_failed user=%s market=%s",
                        trigger.user, trigger.market,
                    )
            else:
                executions_rejected += 1
                log.debug(
                    "liq_watcher.execution_gate_rejected user=%s market=%s reason=%s",
                    trigger.user, trigger.market, reason,
                )

    return CycleStats(
        fetched=len(raws),
        parsed=len(raws) - no_alias - no_price,
        no_price=no_price,
        no_alias=no_alias,
        triggers=triggers,
        executions_paper=executions_paper,
        executions_rejected=executions_rejected,
        executions_cooldown=executions_cooldown,
    )


__all__ = [
    "CycleStats",
    "build_eval_log_entry",
    "run_watch_cycle",
]
