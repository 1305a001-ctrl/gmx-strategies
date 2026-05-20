"""Small argparse CLI for the gmx-strategies package.

Currently the only subcommand is `watchdog`. The CLI is invoked from a cron
on ai-primary (every ~30 minutes) and:

  1. Runs every check in `watchdog.py`.
  2. Prints a one-line per-check summary + final tally to stdout.
  3. With `--emit-alerts`, publishes each non-OK result to the Redis stream
     `settings.trap_alerts_stream` (default `trap_alerts:gmx`) via XADD
     with maxlen=`settings.trap_alerts_maxlen` (approximate).
  4. Exits non-zero (specifically code 2) iff any CRITICAL drift was found.
     This lets the operator wire `cron` → `mail` on non-zero exit, or pipe
     to a healthcheck endpoint.

Manual invocation:
    python -m gmx_strategies.cli watchdog
    python -m gmx_strategies.cli watchdog --emit-alerts
    python -m gmx_strategies.cli watchdog --json

Exit codes:
    0 — all checks OK or WARN only
    1 — CLI usage / argument error (argparse default)
    2 — at least one CRITICAL drift found
    3 — at least one ERROR (watchdog itself couldn't reach a source); the
        operator should investigate the watchdog before trusting "no drift"
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from gmx_strategies.markets import ARBITRUM_MARKETS
from gmx_strategies.redis_client import close as close_redis
from gmx_strategies.redis_client import r as redis_client
from gmx_strategies.watchdog import (
    WatchdogResult,
    check_hyperlend_oracle_source,
    check_markets_alive,
    check_reader_address_drift,
    has_critical,
    publish_alert,
    results_to_json,
    summarize_results,
)

log = logging.getLogger("gmx_strategies.cli")


async def run_all_checks() -> list[WatchdogResult]:
    """Fan out every watchdog check, in order. Returns flat result list.

    Checks are ordered loosely by severity-on-failure:
      1. GMX Reader address (CRITICAL on drift, breaks every market)
      2. HyperLend WHYPE oracle source (CRITICAL on drift, breaks HYPE feed)
      3. GMX markets alive (WARN on disable, one alias each)

    We do NOT short-circuit on the first CRITICAL — the operator may have
    multiple drifts at once (e.g. one redeploy week) and we want the full
    picture in one cron tick rather than two.
    """
    results: list[WatchdogResult] = []
    results.append(await check_reader_address_drift())
    results.append(await check_hyperlend_oracle_source())
    results.extend(await check_markets_alive(markets=ARBITRUM_MARKETS))
    return results


def _print_summary(results: list[WatchdogResult]) -> None:
    """Human-readable per-check + totals, written to stdout."""
    for r in results:
        print(f"[{r.severity:<8}] {r.check_name:<40} {r.message}")
    counts = summarize_results(results)
    print(
        f"summary: CRITICAL={counts['CRITICAL']} WARN={counts['WARN']} "
        f"OK={counts['OK']} ERROR={counts['ERROR']}",
    )


async def _emit_alerts(results: list[WatchdogResult]) -> None:
    """Publish every non-OK result to Redis. Best-effort."""
    non_ok = [r for r in results if r.severity != "OK"]
    if not non_ok:
        return
    rd = redis_client()
    for r in non_ok:
        await publish_alert(rd, r)


async def _watchdog_main(args: argparse.Namespace) -> int:
    """Run all checks, print, optionally emit alerts, compute exit code."""
    results = await run_all_checks()

    if args.json:
        print(results_to_json(results))
    else:
        _print_summary(results)

    if args.emit_alerts:
        try:
            await _emit_alerts(results)
        finally:
            await close_redis()

    if has_critical(results):
        return 2
    # ERROR (watchdog itself broke) is distinct from CRITICAL; we surface as 3
    # so cron can mail-on-error without conflating with confirmed drift.
    if any(r.severity == "ERROR" for r in results):
        return 3
    return 0


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="gmx_strategies.cli",
        description="gmx-strategies operator CLI",
    )
    sub = p.add_subparsers(dest="subcommand", required=True)

    wd = sub.add_parser(
        "watchdog",
        help="Run trap-surface drift checks and exit non-zero on drift.",
    )
    wd.add_argument(
        "--emit-alerts",
        action="store_true",
        help=(
            "Publish each non-OK result to the Redis stream "
            "settings.trap_alerts_stream (XADD with maxlen)."
        ),
    )
    wd.add_argument(
        "--json",
        action="store_true",
        help="Emit results as one JSON line instead of a human-readable summary.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    """Entry point. Returns a process exit code; the module __main__ calls sys.exit."""
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    if args.subcommand == "watchdog":
        return asyncio.run(_watchdog_main(args))
    parser.print_help()
    return 1


if __name__ == "__main__":  # pragma: no cover — module __main__ shim
    sys.exit(main())
