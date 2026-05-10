"""Entrypoint — GMX strategies loop.

v0.1 SCAFFOLD. The pure detection logic is wired and tested
(liquidation_trigger + funding_arb), but the GMX V2 SDK integration
is NOT (TODO week 2+):
  - GMX V2 SDK (TypeScript-first; Python wrapper TBD)
  - Position discovery via Goldsky GMX subgraph
  - Funding rate / OI snapshot from GMX V2 reader contract
  - Keeper bot integration for order execution
"""
from __future__ import annotations

import asyncio
import signal

import structlog

from gmx_strategies.redis_client import close as close_redis
from gmx_strategies.settings import settings

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)

log = structlog.get_logger(__name__)


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
        while not stop.is_set():
            log.info("gmx_strategies.tick gmx_sdk_not_wired_yet")
            try:
                await asyncio.wait_for(stop.wait(), timeout=60.0)
            except TimeoutError:
                pass
    finally:
        await close_redis()
        log.info("gmx_strategies.stopped")
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
