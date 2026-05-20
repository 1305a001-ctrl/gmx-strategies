"""Small argparse CLI for the gmx-strategies package.

Subcommands:
  - `watchdog`      — trap-surface drift checks (cron-driven, see below).
  - `g5_sign_smoke` — G5.2 signer smoke (paper-safe, see below).

`watchdog`:
  Cron on ai-primary (every ~30 minutes). Runs every check in `watchdog.py`:

  1. Runs every check in `watchdog.py`.
  2. Prints a one-line per-check summary + final tally to stdout.
  3. With `--emit-alerts`, publishes each non-OK result to the Redis stream
     `settings.trap_alerts_stream` (default `trap_alerts:gmx`) via XADD
     with maxlen=`settings.trap_alerts_maxlen` (approximate).
  4. Exits non-zero (specifically code 2) iff any CRITICAL drift was found.
     This lets the operator wire `cron` → `mail` on non-zero exit, or pipe
     to a healthcheck endpoint.

`g5_sign_smoke`:
  Operator-invoked one-shot validation that the G5.2 signer module can
  load the configured private key, derive the EOA address, sign a
  synthetic $10 SOL MarketIncrease, and dry-run-simulate it via
  `eth_call`. Never broadcasts (`dry_run=True` is hard-coded).

Manual invocation:
    python -m gmx_strategies.cli watchdog
    python -m gmx_strategies.cli watchdog --emit-alerts
    python -m gmx_strategies.cli watchdog --json
    python -m gmx_strategies.cli g5_sign_smoke

Exit codes:
    0 — all checks OK or WARN only / signer smoke acceptable
    1 — CLI usage / argument error (argparse default)
    2 — at least one CRITICAL drift found / smoke had a critical-fail revert
    3 — at least one ERROR (watchdog itself couldn't reach a source) OR
        signer smoke could not load the executor key; the operator should
        investigate before trusting "no drift" / before retrying the smoke
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Any

from gmx_strategies.markets import ARBITRUM_MARKETS
from gmx_strategies.redis_client import close as close_redis
from gmx_strategies.redis_client import r as redis_client
from gmx_strategies.settings import settings
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


# ──────────────────────────────────────────────────────────────────────────
# g5_sign_smoke — operator-invoked G5.2 signer smoke (paper-safe)
# ──────────────────────────────────────────────────────────────────────────


def _build_smoke_intent_sol_long(account: str) -> Any:
    """Pure: build a synthetic $10 SOL long MarketIncrease intent.

    Used only by `g5_sign_smoke`. Mirrors the smoke-test pattern from
    G5.1 — small $10 size, USDC collateral, alt-band slippage tolerance.
    """
    from gmx_strategies.gmx_order_encoder import OrderIntent
    from gmx_strategies.markets import ARBITRUM_MARKETS

    sol_market = ARBITRUM_MARKETS["sol"]
    return OrderIntent(
        market="sol",
        is_long=True,
        is_increase=True,
        # USDC is the short collateral for SOL longs in the GMX convention
        # we use elsewhere; for this paper-safe smoke we use USDC (6 dec).
        collateral_token=sol_market.short_collateral_token,
        initial_collateral_delta_amount=10_000_000,  # $10 USDC
        size_delta_usd=10 * 10**30,                   # $10 GMX-scaled
        # $150 SOL @ Arbitrum (close to current price; smoke is read-only)
        current_price_1e30=150 * 10**22,
        acceptable_price_band_bps=settings.gmx_default_acceptable_price_band_alts_bps,
        execution_fee_wei=5 * 10**14,                 # 0.0005 ETH
        account=account,
    )


async def _g5_sign_smoke_main(args: argparse.Namespace) -> int:
    """Run the G5.2 signer smoke. Returns the appropriate exit code."""
    # Imported here so the watchdog path doesn't pay the eth-account import
    # cost on every cron tick.
    from gmx_strategies import gmx_signer
    from gmx_strategies.gmx_errors import KNOWN_ACCEPTABLE_BUCKETS, revert_bucket

    # Step 1 — derive EOA from configured key (or fail clearly)
    address = gmx_signer.get_executor_address()
    if address is None:
        print(
            "ERROR: no executor key configured. Set "
            "settings.gmx_executor_key_path (default /srv/secrets/gmx_executor_key) "
            "or the GMX_EXECUTOR_KEY env var.",
        )
        return 3
    print(f"executor_address={address}")

    # Step 2 — build a synthetic $10 SOL MarketIncrease for that address
    intent = _build_smoke_intent_sol_long(address)
    print(
        f"intent: market={intent.market} is_long={intent.is_long} "
        f"is_increase={intent.is_increase} size_usd=$10",
    )

    # Step 3 — sign (this hits Arbitrum mainnet for nonce + gasPrice)
    try:
        signed = await gmx_signer.sign_order(intent)
    except RuntimeError as exc:
        # Should be impossible given Step 1 returned a non-None address —
        # but if the file vanishes mid-flight, surface it cleanly.
        print(f"ERROR: sign_order failed: {exc}")
        return 3
    print(
        f"signed.nonce={signed['nonce']} signed.hash={signed['hash']} "
        f"raw_len={len(signed['raw']) - 2}",
    )

    # Step 4 — dry-run submit (explicit dry_run=True — never broadcasts)
    result = await gmx_signer.submit_signed(signed, intent, dry_run=True)
    if result.dry_run_simulation is None:
        print("ERROR: dry-run simulation returned no result (transport error?)")
        return 3
    sim = result.dry_run_simulation
    if sim.ok:
        print("sim: ok=True (encoding + multicall accepted by mainnet)")
        return 0
    # Revert path — distinguish acceptable (expected for unfunded dummy)
    # from critical-fail (encoding bug indicator).
    if sim.revert_known_acceptable:
        print(
            f"sim: ok=False ACCEPTABLE revert reason={sim.revert_reason_name} "
            f"selector=0x{sim.revert_selector}",
        )
        return 0
    bucket = revert_bucket(sim.revert_selector) if sim.revert_selector else None
    print(
        f"sim: ok=False CRITICAL-FAIL revert "
        f"reason={sim.revert_reason_name or '<unknown>'} "
        f"selector=0x{sim.revert_selector or '<none>'} "
        f"bucket={bucket or '<unmapped>'} "
        f"acceptable_buckets={sorted(KNOWN_ACCEPTABLE_BUCKETS)}",
    )
    return 2


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

    sub.add_parser(
        "g5_sign_smoke",
        help=(
            "G5.2 signer smoke: load key, sign synthetic $10 SOL order, "
            "dry-run-simulate via eth_call. Never broadcasts."
        ),
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
    if args.subcommand == "g5_sign_smoke":
        return asyncio.run(_g5_sign_smoke_main(args))
    parser.print_help()
    return 1


if __name__ == "__main__":  # pragma: no cover — module __main__ shim
    sys.exit(main())
