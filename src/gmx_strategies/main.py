"""Entrypoint — GMX strategies loop (v0.2 stub).

The v0.1 main.py wired a paper-mode liquidation watcher. That code path
was removed in v0.2 after an architecture audit found GMX V2
`LiquidationHandler.executeLiquidation` is permissioned via the
`onlyLiquidationKeeper` Timelock role (non-keeper callers revert).

funding_arb.py retains pure helpers for delta-neutral funding-rate
arbitrage, but the runtime (subgraph polling + CEX hedge leg) is not
yet wired. This stub keeps the entrypoint alive as a no-op until the
v0.3 runtime lands, so deployment scripts and containers don't crash.
"""

from __future__ import annotations

import asyncio
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def _main_async() -> None:
    while True:
        log.info("funding_arb.v02_runtime_not_wired_yet")
        await asyncio.sleep(60)


def main() -> None:
    """Sync entrypoint used by the `gmx-strategies` console script."""
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()
