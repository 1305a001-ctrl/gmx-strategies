"""Entrypoint — GMX strategies loop (v0.3 funding-arb runtime, paper mode).

The v0.1 main.py wired a paper-mode liquidation watcher. That code path
was removed in v0.2 after an architecture audit found GMX V2
`LiquidationHandler.executeLiquidation` is permissioned via the
`onlyLiquidationKeeper` Timelock role (non-keeper callers revert). See
`memory/arch_gmx_v2_audit.md`.

v0.3 (this file) wires the funding-arb runtime around the existing pure
helpers in `funding_arb.py`. Both fetchers (GMX V2 funding read, CEX
hedge-leg funding read) are paper-mode placeholders; live web3 / Binance
integration lands in G2/G3. LIVE_ENABLED gate is untouched and stays
False by default.
"""

from __future__ import annotations

import asyncio
import logging

from gmx_strategies.funding_arb_runtime import run_funding_arb_runtime
from gmx_strategies.settings import settings

logging.basicConfig(level=getattr(logging, settings.log_level, logging.INFO))
log = logging.getLogger(__name__)


async def _main_async() -> None:
    log.info("funding_arb.v03_runtime_paper_mode_starting")
    await run_funding_arb_runtime()


def main() -> None:
    """Sync entrypoint used by the `gmx-strategies` console script."""
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()
