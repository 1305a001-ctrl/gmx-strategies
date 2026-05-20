"""Entrypoint — GMX strategies loop (v0.3 funding-arb runtime, paper mode).

The v0.1 main.py wired a paper-mode liquidation watcher. That code path
was removed in v0.2 after an architecture audit found GMX V2
`LiquidationHandler.executeLiquidation` is permissioned via the
`onlyLiquidationKeeper` Timelock role (non-keeper callers revert). See
`memory/arch_gmx_v2_audit.md`.

v0.3 (this file) wires the funding-arb runtime around the existing pure
helpers in `funding_arb.py`. Both fetchers (GMX V2 funding read, CEX
hedge-leg funding read) are paper-mode placeholders; live web3 / Binance
integration lands in G2/G3.

G7.1 (this file as of the consumer-wiring PR) adds a CONDITIONAL
funding-arb executor task. The consumer is OFF by default
(`funding_arb_consumer_enabled=False`); when flipped, main.py adds
`FundingArbExecutor.run(stop_event)` into the `asyncio.gather`. The
signal-emit runtime continues to run alongside — they're independent
producers + consumers of the same Redis pub/sub channel.

To enable the consumer in production:
  1. Provision the GMX executor key (`/srv/secrets/gmx_executor_key`).
  2. Set `funding_arb_consumer_enabled=true` in .env / env vars.
  3. (Optionally) keep `funding_arb_executor_dry_run=true` for the
     first canary day; flip to false only when ready for real broadcast.
  4. Restart the container — the consumer-enabled gate is read ONCE
     at startup, deliberately, so a config flip isn't acted on mid-flight.
"""

from __future__ import annotations

import asyncio
import logging

from gmx_strategies.funding_arb_runtime import run_funding_arb_runtime
from gmx_strategies.settings import settings

logging.basicConfig(level=getattr(logging, settings.log_level, logging.INFO))
log = logging.getLogger(__name__)


async def _runtime_task() -> None:
    """The legacy paper-mode signal emitter. Always runs."""
    await run_funding_arb_runtime()


async def _consumer_task(stop_event: asyncio.Event) -> None:
    """The G7.1 consumer. Only started when `funding_arb_consumer_enabled`.

    Imported here (not at module top) so the consumer's eth-account /
    web3 import chain doesn't load in the default paper-mode path.
    """
    from gmx_strategies.funding_arb_executor import FundingArbExecutor

    executor = FundingArbExecutor()
    await executor.run(stop_event)


def _build_tasks(stop_event: asyncio.Event) -> list[asyncio.Task[None]]:
    """Pure-ish: assemble the list of long-running tasks to gather.

    Centralizing this lets the test suite assert that the consumer
    task IS / IS NOT present depending on `funding_arb_consumer_enabled`,
    without having to actually invoke `asyncio.gather`.
    """
    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(_runtime_task(), name="funding_arb_runtime"),
    ]
    if settings.funding_arb_consumer_enabled:
        log.info(
            "funding_arb.consumer.enabled dry_run=%s",
            settings.funding_arb_executor_dry_run,
        )
        tasks.append(
            asyncio.create_task(
                _consumer_task(stop_event), name="funding_arb_executor",
            ),
        )
    else:
        log.info(
            "funding_arb.consumer.disabled — set "
            "funding_arb_consumer_enabled=true + restart to enable",
        )
    return tasks


async def _main_async() -> None:
    log.info("funding_arb.v03_runtime_paper_mode_starting")
    stop_event = asyncio.Event()
    tasks = _build_tasks(stop_event)
    try:
        await asyncio.gather(*tasks)
    finally:
        stop_event.set()
        for t in tasks:
            if not t.done():
                t.cancel()
        # Best-effort drain; cancellation is enough — we don't await
        # exceptions from cancelled tasks.
        await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    """Sync entrypoint used by the `gmx-strategies` console script."""
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()
