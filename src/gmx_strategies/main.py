"""Entrypoint — GMX strategies loop.

v0.2 SCAFFOLD. Wires the GMX V2 subgraph adapter + chainlink price
join + paper-mode liquidation watcher. Order routing remains TODO
(week 2+).
"""
from __future__ import annotations

import asyncio
import signal

import httpx
import structlog

from gmx_strategies.liquidation_watcher import run_watch_cycle
from gmx_strategies.redis_client import close as close_redis
from gmx_strategies.redis_client import r
from gmx_strategies.settings import settings

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)

log = structlog.get_logger(__name__)


async def _watcher_loop(stop: asyncio.Event) -> None:
    """Forever loop polling subgraph + emitting eval-log triggers."""
    if not settings.gmx_subgraph_url:
        log.warning("gmx_strategies.subgraph_url_empty — watcher idle until configured")
        while not stop.is_set():
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=settings.gmx_subgraph_poll_interval_sec,
                )
            except TimeoutError:
                pass
        return

    async with httpx.AsyncClient(timeout=15.0) as http:
        redis_client = r()
        log.info(
            "gmx_strategies.watcher_starting interval=%ds page_size=%d max_pages=%d",
            settings.gmx_subgraph_poll_interval_sec,
            settings.gmx_subgraph_page_size,
            settings.gmx_subgraph_max_pages,
        )
        while not stop.is_set():
            try:
                stats = await run_watch_cycle(
                    httpx_client=http,
                    redis_client=redis_client,
                    subgraph_url=settings.gmx_subgraph_url,
                    eval_log_stream=settings.paper_log_stream,
                    eval_log_maxlen=settings.paper_log_maxlen,
                    watch_margin=settings.liquidation_watch_margin,
                    estimated_fee_usd=settings.estimated_keeper_fee_usd,
                    chainlink_key_template=settings.chainlink_redis_key_template,
                    page_size=settings.gmx_subgraph_page_size,
                    max_pages=settings.gmx_subgraph_max_pages,
                    execution_paper_enabled=settings.execution_paper_enabled,
                    execution_paper_log_stream=settings.execution_paper_log_stream,
                    execution_paper_log_maxlen=settings.execution_paper_log_maxlen,
                    execution_min_net_profit_usd=settings.execution_min_net_profit_usd,
                    execution_min_confidence=settings.execution_min_confidence,
                )
                log.info(
                    "gmx_strategies.cycle fetched=%d parsed=%d no_price=%d "
                    "no_alias=%d triggers=%d exec_paper=%d exec_rejected=%d",
                    stats.fetched, stats.parsed, stats.no_price,
                    stats.no_alias, stats.triggers,
                    stats.executions_paper, stats.executions_rejected,
                )
            except Exception:
                log.exception("gmx_strategies.cycle_failed")
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=settings.gmx_subgraph_poll_interval_sec,
                )
            except TimeoutError:
                pass


async def main_async() -> int:
    log.info(
        "gmx_strategies.starting chains=%s markets=%s live_enabled=%s",
        settings.chains_enabled,
        settings.monitored_markets,
        settings.live_enabled,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        await _watcher_loop(stop)
    finally:
        await close_redis()
        log.info("gmx_strategies.stopped")
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
