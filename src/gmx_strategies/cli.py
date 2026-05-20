"""Small argparse CLI for the gmx-strategies package.

Subcommands:
  - `watchdog`          — trap-surface drift checks (cron-driven). See block below.
  - `g5_sign_smoke`     — G5.2 signer smoke (paper-safe; never broadcasts).
  - `g5_position_smoke` — G5.3 position-reader smoke (paper-safe; read-only).
  - `g6_smoke`          — G6.3 Binance Futures testnet/mainnet shakedown.
                          Operator-invoked one-shot validation of the entire
                          G6 read-side stack (auth, exchangeInfo, funding,
                          position mode, account balance, position info)
                          against the configured base URL (typically
                          demo-fapi.binance.com).
  - `g6_smoke`          — G6.3 Binance Futures testnet/mainnet shakedown.
                          Operator-invoked one-shot validation of the entire G6
                          read-side stack (auth, exchangeInfo, funding, position
                          mode, account balance, position info) against the
                          configured base URL (typically demo-fapi.binance.com).
  - `g7_guard_status`   — G7.3 pilot-guard status snapshot. Prints the
                          GuardState (killswitch / pnl / positions / cooldown
                          / armed markets) + runs PilotGuard.check() at the
                          pilot cap for each monitored market. Read-only,
                          always exits 0.
  - `g6_dry_run_order`  — G6.4 order-placement dry-run. Constructs a $5 SOLUSDT
                          BUY MARKET order at the current Binance mark price
                          and runs `place_market_order(dry_run=True)`. NEVER
                          broadcasts. Operator-invoked validation that the
                          full order-construction surface (exchangeInfo
                          rounding + funding mark-price read + client-order-id
                          generation) produces a valid signed-params payload.

`watchdog`:
  Cron on ai-primary (every ~30 minutes). Runs every check in `watchdog.py`,
  prints a one-line per-check summary + final tally, with `--emit-alerts`
  publishes each non-OK result to the Redis stream
  `settings.trap_alerts_stream` (default `trap_alerts:gmx`) via XADD
  with maxlen=`settings.trap_alerts_maxlen` (approximate). Exits non-zero
  (code 2) iff any CRITICAL drift was found.

`g6_smoke`:
  Operator-invoked validation that the API key / IP allowlist / position
  mode / exchange filters / funding-rate path all work end-to-end BEFORE
  any G6.4 order-placement work. PAPER-SAFE — read-only signed endpoints
  + public reads. NO order placement. NO `marginType`/`leverage` flips.
  See `_g6_smoke_main` for the exact check sequence + exit code map.

`g5_sign_smoke`:
  Operator-invoked one-shot validation that the G5.2 signer module can
  load the configured private key, derive the EOA address, sign a
  synthetic $10 SOL MarketIncrease, and dry-run-simulate it via
  `eth_call`. Never broadcasts (`dry_run=True` is hard-coded).

`g5_position_smoke`:
  Operator-invoked one-shot validation that the G5.3 position reader can
  reach the GMX V2 Reader on Arbitrum and decode `Position.Props[]`. Reads
  positions for the canonical empty address `0x0000…0001` (expects an
  empty list); if a configured executor key is present, also reads + prints
  the operator's own positions. NEVER broadcasts; read-only `eth_call` only.

Manual invocation:
    python -m gmx_strategies.cli watchdog
    python -m gmx_strategies.cli watchdog --emit-alerts
    python -m gmx_strategies.cli watchdog --json
    python -m gmx_strategies.cli g5_sign_smoke
    python -m gmx_strategies.cli g5_position_smoke
    python -m gmx_strategies.cli g6_smoke
    python -m gmx_strategies.cli g6_smoke --force-refresh-exchange-info
    python -m gmx_strategies.cli g6_dry_run_order
    python -m gmx_strategies.cli g7_guard_status

Exit codes:
    0 — all checks OK or WARN only / signer smoke acceptable /
        position smoke OK
        g6_dry_run_order: dry_run_request constructed cleanly
    1 — CLI usage / argument error (argparse default)
    2 — watchdog: at least one CRITICAL drift found;
        g5_sign_smoke: critical-fail revert;
        g5_position_smoke: decoder failed (unexpected non-empty / malformed);
        g6_smoke: at least one functional check failed (hedge mode, missing
        market filter, etc.);
        g6_dry_run_order: a pre-flight check failed (below min_notional,
        bad symbol, lot-step underflow, etc.)
    3 — watchdog: at least one ERROR (watchdog itself couldn't reach a source);
        g5_sign_smoke: could not load the executor key;
        g5_position_smoke: RPC unreachable;
        g6_smoke: credentials not configured (BINANCE_API_KEY/SECRET unset).
        g6_smoke: credentials not configured (BINANCE_API_KEY/SECRET unset);
        g6_dry_run_order: exchange_info or funding read failed (no mark price
        available to size against).
    4 — g6_smoke only: API down / unreachable (every signed read returned
        None — could be auth failure, IP-allowlist miss, network outage).
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


# ──────────────────────────────────────────────────────────────────────────
# g5_position_smoke — operator-invoked G5.3 position-reader smoke
# ──────────────────────────────────────────────────────────────────────────
#
# Paper-safe — read-only `eth_call` against GMX V2 Reader on Arbitrum.
# NEVER broadcasts.
#
# Sequence:
#   1. Read positions for the canonical empty address `0x000…0001`.
#      Should return an empty list. If decode fails or the read raises,
#      something is structurally wrong with the ABI shape or RPC.
#   2. If an executor key is configured (via `gmx_signer.get_executor_address`),
#      also read + print positions for that address. Either an empty list
#      or any number of real positions is acceptable — the smoke validates
#      the READER, not the trader's portfolio.
#
# Exit codes:
#   0 — both reads succeeded (regardless of position count)
#   2 — decoder failed on a non-empty / malformed response
#   3 — RPC unreachable (transport-layer failure on the canonical empty read)


_CANONICAL_EMPTY_ADDRESS = "0x0000000000000000000000000000000000000001"


async def _g5_position_smoke_main(args: argparse.Namespace) -> int:
    """Run the G5.3 position-reader smoke. Returns the appropriate exit code."""
    import httpx

    from gmx_strategies import gmx_position_reader

    print(
        f"G5.3 position-reader smoke — read-only on-chain query\n"
        f"  rpc_url: {settings.arbitrum_rpc_url}\n"
        f"  reader:  {settings.gmx_reader_address_arbitrum}",
    )
    print()

    # Step 1 — canonical empty address. We use a single shared client so
    # the smoke can distinguish "no positions" (empty list AND no error)
    # from "RPC unreachable" (empty list AND transport error). The reader's
    # public surface intentionally returns an empty list on both; the smoke
    # is the place where the operator wants to see the distinction.
    timeout = httpx.Timeout(settings.gmx_reader_timeout_s)
    async with httpx.AsyncClient(timeout=timeout) as client:
        # Quick connectivity probe — pass through eth_chainId for a cheap
        # signal that the RPC itself is reachable. If this fails we exit 3
        # rather than 2, because the user's first action is different
        # (debug network) than for a decode failure.
        try:
            probe = await client.post(
                settings.arbitrum_rpc_url,
                json={"jsonrpc": "2.0", "id": 0, "method": "eth_chainId", "params": []},
            )
            if probe.status_code != 200:
                print(
                    f"RPC unreachable: eth_chainId returned HTTP {probe.status_code}",
                )
                return 3
            probe_body = probe.json()
            if not isinstance(probe_body, dict) or "result" not in probe_body:
                print(f"RPC unreachable: malformed response {probe_body!r}")
                return 3
            chain_id_hex = probe_body.get("result")
            if not isinstance(chain_id_hex, str):
                print(f"RPC unreachable: bad chainId={chain_id_hex!r}")
                return 3
            chain_id = int(chain_id_hex, 16)
            print(f"connectivity: eth_chainId={chain_id}")
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as exc:
            print(f"RPC unreachable: {exc.__class__.__name__}: {exc}")
            return 3

        # Step 1: read for canonical empty address
        positions_empty = await gmx_position_reader.fetch_account_positions(
            _CANONICAL_EMPTY_ADDRESS,
            client=client,
        )
        if positions_empty:
            # Unexpected — the canonical empty address shouldn't have any
            # positions. Either GMX state changed or the decoder is wrong.
            print(
                f"FAIL: canonical empty address has "
                f"{len(positions_empty)} positions (expected 0); decoder may "
                f"be misinterpreting bytes",
            )
            for p in positions_empty:
                print(f"  unexpected: market={p.market_alias} {p}")
            return 2
        print(
            f"empty_address={_CANONICAL_EMPTY_ADDRESS}: 0 positions "
            f"(expected; decode OK)",
        )

        # Step 2: if a key is loaded, read the executor's positions too. We
        # do NOT require it — most dev shells run the smoke without a key.
        # When present, this exercises the same code path against a real
        # account (which may or may not have positions; either is fine).
        from gmx_strategies import gmx_signer

        executor_address = gmx_signer.get_executor_address()
        if executor_address is None:
            print(
                "executor_address=<none>: skipping operator-account read "
                "(no key configured)",
            )
        else:
            print(f"executor_address={executor_address}: reading positions")
            executor_positions = await gmx_position_reader.fetch_account_positions(
                executor_address,
                client=client,
            )
            print(
                f"executor positions: {len(executor_positions)}",
            )
            for p in executor_positions:
                print(
                    f"  market={p.market_alias or '<unknown>'} "
                    f"is_long={p.is_long} "
                    f"size=${p.size_in_usd_float:.2f} "
                    f"collateral_token={p.collateral_token}",
                )

    print()
    print("exit_code: 0 (position reader smoke OK)")
    return 0


# ──────────────────────────────────────────────────────────────────────────
# g6_smoke — G6.3 Binance Futures testnet/mainnet shakedown CLI
# ──────────────────────────────────────────────────────────────────────────
#
# The smoke runs each read-only check in order and prints one line per
# check with a clear pass / fail / warn marker. At the end it tallies and
# exits with one of:
#   0 — every check passed
#   2 — at least one functional check failed (e.g. hedge mode, missing
#       market, funding-rate parse failed)
#   3 — credentials not configured (api_key OR api_secret unset)
#   4 — API down / unreachable (every signed read returned None — could be
#       wrong base URL, bad IP-allowlist, clock drift, or testnet outage)
#
# The expected MARKETS list is the G6 hedge-leg basket. Mirrored from
# binance_funding.BINANCE_SYMBOL_BY_ALIAS so they stay in lockstep.

# Markers — ASCII so they render in any terminal / log forwarder. Operators
# scan the column visually; emoji-style markers caused some terminals to
# misalign and obscured the pass/fail signal in early G5 smoke runs.
# ruff S105 false-positive: `_PASS = "[PASS]"` is a UI tag, not a password.
_PASS = "[PASS]"  # noqa: S105
_FAIL = "[FAIL]"  # noqa: S105
_WARN = "[WARN]"  # noqa: S105

# The 5 markets G6 cares about, as Binance USDT-M perp symbols. Kept here
# rather than imported from binance_funding's reverse map because the
# smoke wants the symbol-set as a fixed expectation: "did exchangeInfo
# return ALL 5?" The funding module's map is the source of truth; this
# list is its mirror. If they drift, update both.
_EXPECTED_MARKETS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT")


def _format_check(marker: str, name: str, detail: str) -> str:
    """Format one check line. Kept private so tests can match exact strings."""
    return f"{marker} {name}: {detail}"


async def _g6_smoke_main(args: argparse.Namespace) -> int:
    """Run the G6.3 testnet/mainnet smoke and return an exit code.

    Sequence (audit `arch_binance_executor_audit.md`):
      AUTH-1   credentials configured (settings.binance_api_key/secret)
      AUTH-2   base URL configured (warn if mainnet)
      READ-1   position mode (signed) — must be one-way
      READ-2   account balance (signed)
      READ-3   USDT free margin (signed) — should be ~10000 USDT-T on testnet
      READ-4   position information (signed)
      PUBLIC-1 exchange info — verify all 5 G6 markets present
      PUBLIC-2 funding rates — verify all 5 markets return a parseable rate
      CONSISTENCY-1 position-mode startup gate (assert_one_way_position_mode)

    No order placement. No state-changing calls. Read-only.
    """
    # Import the read modules lazily so the CLI `--help` path stays cheap;
    # `settings` is already loaded at module import for both subcommands.
    from gmx_strategies import (
        binance_account,
        binance_exchange_info,
        binance_funding,
        binance_startup_check,
    )

    # Track each check's outcome. Aggregated at the end for the summary
    # line + exit-code computation. Counts mirror the watchdog summary
    # format so an operator who knows one knows the other.
    n_pass = 0
    n_fail = 0
    n_warn = 0
    # Track signed-read failures separately. If EVERY signed read returns
    # None we exit 4 (API down) rather than 2 (functional fail) — the
    # operator's first action is different (debug network/IP vs. fix
    # account config).
    signed_total = 0
    signed_none = 0

    print("G6.3 Binance Futures smoke — read-only validation")
    print(f"  base_url: {settings.binance_fapi_base_url}")
    print(f"  recv_window_ms: {settings.binance_recv_window_ms}")
    print()

    # ── AUTH-1: credentials configured ──────────────────────────────────
    # Done BEFORE any HTTP call. If the key/secret is unset every signed
    # call returns None — but the operator's recovery is "go set the
    # creds", not "debug the network". Distinguish the two upfront.
    api_key_set = bool(settings.binance_api_key)
    api_secret_set = bool(settings.binance_api_secret)
    if api_key_set and api_secret_set:
        print(_format_check(_PASS, "AUTH-1 credentials configured",
                            "api_key + api_secret both set"))
        n_pass += 1
    else:
        print(_format_check(_FAIL, "AUTH-1 credentials configured",
                            f"api_key_set={api_key_set} api_secret_set={api_secret_set} "
                            "— set BINANCE_API_KEY / BINANCE_API_SECRET env or "
                            "/srv/secrets/binance_api_{key,secret} files"))
        n_fail += 1
        # Early-out: nothing else works without creds. Print a summary
        # and exit 3 so operators don't wade through 8 redundant FAILs.
        print()
        print(f"summary: PASS={n_pass} FAIL={n_fail} WARN={n_warn}")
        print("exit_code: 3 (credentials not configured)")
        return 3

    # ── AUTH-2: base URL configured ─────────────────────────────────────
    # Mainnet is a footgun for a SHAKEDOWN run — the audit's
    # CONDITIONAL GO requires testnet validation first. We warn but
    # don't fail; operators do occasionally use this as a mainnet
    # connectivity check.
    base = settings.binance_fapi_base_url
    if "demo" in base or "testnet" in base:
        print(_format_check(_PASS, "AUTH-2 base URL configured",
                            f"testnet detected ({base})"))
        n_pass += 1
    else:
        print(_format_check(_WARN, "AUTH-2 base URL configured",
                            f"mainnet detected ({base}) — testnet recommended "
                            "for first shakedown"))
        n_warn += 1

    # ── READ-1: position mode (signed) ──────────────────────────────────
    # The audit's H3 finding. Must be one-way; hedge mode rejects every
    # order with -4061. None means the auth path itself didn't return —
    # the operator needs to debug creds, IP allowlist, or clock drift.
    signed_total += 1
    mode = await binance_account.fetch_position_mode()
    if mode is False:
        print(_format_check(_PASS, "READ-1 position mode (signed)",
                            "one-way (dualSidePosition=false)"))
        n_pass += 1
    elif mode is True:
        print(_format_check(_FAIL, "READ-1 position mode (signed)",
                            "HEDGE mode detected — flip to one-way in Binance "
                            "UI (Preferences → Position Mode) before G6.4"))
        n_fail += 1
    else:
        # mode is None → auth/network failure path
        print(_format_check(_FAIL, "READ-1 position mode (signed)",
                            "signed read returned None — check API key, IP "
                            "allowlist, base URL, clock drift"))
        n_fail += 1
        signed_none += 1

    # ── READ-2: account balance (signed) ────────────────────────────────
    # Returns a list of per-asset dicts. We only print the USDT one to
    # avoid spamming a 20+ line output with stablecoins the operator
    # doesn't care about.
    signed_total += 1
    balances = await binance_account.fetch_account_balance()
    if balances is None:
        print(_format_check(_FAIL, "READ-2 account balance (signed)",
                            "signed read returned None"))
        n_fail += 1
        signed_none += 1
    elif isinstance(balances, list) and len(balances) > 0:
        usdt_summary = "no USDT entry"
        for entry in balances:
            if isinstance(entry, dict) and entry.get("asset") == "USDT":
                bal = entry.get("balance", "?")
                avail = entry.get("availableBalance", "?")
                usdt_summary = f"USDT balance={bal} available={avail}"
                break
        print(_format_check(_PASS, "READ-2 account balance (signed)",
                            f"n_assets={len(balances)} {usdt_summary}"))
        n_pass += 1
    else:
        # Empty list — odd but not a hard fail; the account has no asset
        # balances which is possible on a fresh testnet account that's
        # never deposited.
        print(_format_check(_WARN, "READ-2 account balance (signed)",
                            "empty balance list — fresh account?"))
        n_warn += 1

    # ── READ-3: USDT free margin (signed) ───────────────────────────────
    # Convenience helper that float-coerces the USDT availableBalance. On
    # testnet a freshly-issued key typically returns ~10000 USDT-T.
    signed_total += 1
    free_margin = await binance_account.fetch_usdt_free_margin()
    if free_margin is None:
        print(_format_check(_FAIL, "READ-3 USDT free margin (signed)",
                            "returned None — no USDT entry or parse failure"))
        n_fail += 1
        signed_none += 1
    else:
        print(_format_check(_PASS, "READ-3 USDT free margin (signed)",
                            f"available={free_margin:.4f} USDT"))
        n_pass += 1

    # ── READ-4: position information (signed) ───────────────────────────
    # Returns a list of all position entries. We count non-zero ones to
    # let the operator confirm whether the testnet sandbox is empty (the
    # expected state for a fresh shakedown) or has leftover positions
    # from a prior experiment.
    signed_total += 1
    positions = await binance_account.fetch_position_information()
    if positions is None:
        print(_format_check(_FAIL, "READ-4 position information (signed)",
                            "signed read returned None"))
        n_fail += 1
        signed_none += 1
    else:
        n_open = 0
        for entry in positions:
            if not isinstance(entry, dict):
                continue
            amt_raw = entry.get("positionAmt", "0")
            try:
                amt = float(amt_raw)
            except (ValueError, TypeError):
                continue
            if amt != 0.0:
                n_open += 1
        print(_format_check(_PASS, "READ-4 position information (signed)",
                            f"n_entries={len(positions)} n_open={n_open}"))
        n_pass += 1

    # ── PUBLIC-1: exchange info ─────────────────────────────────────────
    # G6.1's reader. Verify all 5 markets we care about parse correctly.
    # We use fetch_exchange_info (bypassing the TTL cache) so the smoke
    # always reflects the LIVE state, not whatever a prior process
    # warmed. Force-refresh flag is offered as a no-op alias for symmetry
    # with the cached path — see CLI help text.
    if args.force_refresh_exchange_info:
        binance_exchange_info._reset_cache_for_testing()  # noqa: SLF001
    info = await binance_exchange_info.fetch_exchange_info()
    if not info:
        print(_format_check(_FAIL, "PUBLIC-1 exchange info",
                            "empty dict — public endpoint outage or "
                            "wrong base URL"))
        n_fail += 1
    else:
        missing = [m for m in _EXPECTED_MARKETS if m not in info]
        if missing:
            print(_format_check(_FAIL, "PUBLIC-1 exchange info",
                                f"missing markets: {','.join(missing)} "
                                f"(got {len(info)} total)"))
            n_fail += 1
        else:
            # Print each target market's min_notional + lot_step so the
            # operator can spot the BTC $50 min vs the $10/trade cap
            # without having to grep separately.
            print(_format_check(_PASS, "PUBLIC-1 exchange info",
                                f"{len(info)} symbols, all 5 G6 markets present"))
            for sym in _EXPECTED_MARKETS:
                s = info[sym]
                print(f"        {sym}: lot_step={s.lot_step} "
                      f"lot_min={s.lot_min} min_notional=${s.min_notional}")
            n_pass += 1

    # ── PUBLIC-2: funding rates ─────────────────────────────────────────
    # G3's reader. Batched call; we expect all 5 aliases back.
    fundings = await binance_funding.fetch_all_cex_fundings()
    if not fundings:
        print(_format_check(_FAIL, "PUBLIC-2 funding rates",
                            "empty dict — public endpoint outage or wrong "
                            "base URL"))
        n_fail += 1
    else:
        expected_aliases = list(binance_funding.BINANCE_SYMBOL_BY_ALIAS.keys())
        missing_aliases = [a for a in expected_aliases if a not in fundings]
        if missing_aliases:
            print(_format_check(_FAIL, "PUBLIC-2 funding rates",
                                f"missing rates: {','.join(missing_aliases)}"))
            n_fail += 1
        else:
            print(_format_check(_PASS, "PUBLIC-2 funding rates",
                                f"{len(fundings)} rates parsed"))
            for alias in expected_aliases:
                rate = fundings[alias]
                # Show per-8h fraction + the annualized equivalent so the
                # operator can sanity-check the magnitude at a glance.
                ann_pct = rate * 3 * 365 * 100
                print(f"        {alias}: {rate:+.6f}/8h ({ann_pct:+.2f}%/yr)")
            n_pass += 1

    # ── CONSISTENCY-1: position-mode startup gate ───────────────────────
    # binance_startup_check.assert_one_way_position_mode IS the gate that
    # G6.4's executor boot will call. Smoke runs it as the LAST step so
    # if it raises (hedge or unknown), the operator has already seen the
    # earlier READ-1 result and the gate's exception is just a confirmation.
    try:
        await binance_startup_check.assert_one_way_position_mode()
        print(_format_check(_PASS, "CONSISTENCY-1 position-mode gate",
                            "assert_one_way_position_mode() passed"))
        n_pass += 1
    except RuntimeError as exc:
        # Don't include the api key/secret in the log even on failure —
        # the exception message itself is safe (no secrets), but we
        # explicitly defend the boundary.
        print(_format_check(_FAIL, "CONSISTENCY-1 position-mode gate",
                            f"raised RuntimeError: {exc}"))
        n_fail += 1

    # ── Summary + exit code ─────────────────────────────────────────────
    print()
    print(f"summary: PASS={n_pass} FAIL={n_fail} WARN={n_warn}")

    # Distinguish "API totally down" from "API works but some checks
    # fail". If every signed read came back None, the auth/network path
    # itself is broken — operator should debug creds + IP + clock first.
    if signed_total > 0 and signed_none == signed_total:
        print("exit_code: 4 (every signed read returned None — API unreachable)")
        return 4
    if n_fail > 0:
        print(f"exit_code: 2 ({n_fail} functional check(s) failed)")
        return 2
    print("exit_code: 0 (all checks passed)")
    return 0


# ──────────────────────────────────────────────────────────────────────────
# g6_dry_run_order — G6.4 order-placement dry-run smoke
# ──────────────────────────────────────────────────────────────────────────
#
# Constructs the canonical $5 SOLUSDT BUY MARKET order described in the
# README's "G6.4 — Order placement" section. NEVER broadcasts — calls
# `place_market_order(dry_run=True)` regardless of any flag. Even with
# `settings.live_binance_enabled=True` this CLI cannot broadcast (the
# `dry_run=True` argument is hard-coded).
#
# The smoke is the operator's last sanity check before wiring G6.4 into
# the funding-arb runtime (G7.1): does the full path — exchangeInfo
# round-trip + funding mark-price read + lot_step rounding + min_notional
# check + client_order_id generation — produce a clean signed-params dict?
#
# Sequence:
#   1. Fetch exchange_info for SOLUSDT (cached).
#   2. Fetch SOL mark price via `binance_funding.fetch_all_cex_fundings`'s
#      cousin — actually the audit recommends the markPrice from
#      `/fapi/v1/premiumIndex` directly. We reuse `fetch_all_cex_fundings`
#      which already hits the batched endpoint, but we extend it by also
#      reading a single-symbol mark price. For G6.4 we do a minimal
#      single-symbol read via httpx (kept here to avoid polluting the
#      funding module with executor-specific helpers).
#   3. Call `place_market_order("SOLUSDT", "BUY", 5.0, mark_price=...,
#      dry_run=True)`.
#   4. Print OrderResult.dry_run_request verbatim.
#   5. Exit code per the OrderResult shape.
#
# Hard constraint: this CLI ALWAYS passes `dry_run=True`. No `--force-live`
# flag exists, on purpose. The operator who wants to live-broadcast must
# wire G6.4 into a runtime that explicitly opts in (G7.1).


async def _fetch_solusdt_mark_price() -> float | None:
    """Fetch SOL mark price from Binance public /fapi/v1/premiumIndex.

    Returns the float on success, None on any failure. Kept inline in the
    CLI rather than added to `binance_funding.py` because it's an
    executor-time helper, not part of the funding-arb signal pipeline.
    """
    # Imported here to avoid pulling httpx into the watchdog import path.
    import httpx as _httpx  # local alias keeps the global import surface clean

    from gmx_strategies.settings import settings as _settings

    url = f"{_settings.binance_fapi_base_url}/fapi/v1/premiumIndex"
    timeout = _httpx.Timeout(_settings.binance_funding_timeout_s)
    try:
        async with _httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url, params={"symbol": "SOLUSDT"})
    except (_httpx.HTTPError, _httpx.TimeoutException) as exc:
        log.warning("g6_dry_run_order.mark_price.http_error err=%s", exc)
        return None
    if resp.status_code != 200:
        log.warning("g6_dry_run_order.mark_price.bad_status status=%d", resp.status_code)
        return None
    try:
        body = resp.json()
    except (ValueError, TypeError):
        return None
    if not isinstance(body, dict):
        return None
    raw = body.get("markPrice")
    if not isinstance(raw, (str, int, float)):
        return None
    try:
        value = float(raw)
    except (ValueError, TypeError):
        return None
    if value != value or value in (float("inf"), float("-inf")):
        return None
    return value


async def _g6_dry_run_order_main(args: argparse.Namespace) -> int:  # noqa: ARG001
    """Run the G6.4 order-placement dry-run smoke. NEVER broadcasts."""
    from gmx_strategies import binance_exchange_info, binance_order

    print("G6.4 Binance Futures order placement — DRY-RUN smoke")
    print(f"  base_url: {settings.binance_fapi_base_url}")
    print(
        f"  live_binance_enabled: {settings.live_binance_enabled} "
        "(gate — must be True to broadcast, but this CLI is dry_run-only)",
    )
    print()

    # Step 1 — fetch exchange_info (verifies the cache layer is reachable)
    info_map = await binance_exchange_info.get_cached_exchange_info()
    if not info_map:
        print(
            "ERROR: get_cached_exchange_info returned empty — Binance "
            "public endpoint down or wrong base_url.",
        )
        return 3
    if "SOLUSDT" not in info_map:
        print("ERROR: SOLUSDT missing from exchange_info — Binance unexpectedly delisted SOL?")
        return 3
    sol_info = info_map["SOLUSDT"]
    print(
        f"exchange_info OK: SOLUSDT lot_step={sol_info.lot_step} "
        f"lot_min={sol_info.lot_min} min_notional=${sol_info.min_notional}",
    )

    # Step 2 — fetch SOL mark price (public, no auth)
    mark_price = await _fetch_solusdt_mark_price()
    if mark_price is None:
        print("ERROR: could not read SOLUSDT mark price from /fapi/v1/premiumIndex.")
        return 3
    print(f"mark_price OK: SOLUSDT markPrice=${mark_price:.4f}")

    # Step 3 — DRY-RUN place_market_order. ALWAYS dry_run=True.
    #
    # Notional sized to $6 (not $5) to clear the $5 min_notional with
    # lot-step rounding headroom across the realistic SOL price range
    # ($50-$300). At $5 exactly, lot-step rounding (0.01 SOL) shaves the
    # notional below the $5 min when SOL trades > ~$50. The spec's
    # "approximately $5" intent is preserved; the +$1 headroom is the
    # difference between "demonstrate construction" and "demonstrate the
    # min_notional guard" — we want the first.
    print()
    print(
        "Calling place_market_order('SOLUSDT', 'BUY', notional_usd=$6, "
        "dry_run=True) ...",
    )
    result = await binance_order.place_market_order(
        "SOLUSDT", "BUY", 6.0,
        mark_price=mark_price,
        dry_run=True,
    )

    # Step 4 — print the OrderResult outcome
    print()
    print("OrderResult:")
    print(f"  submitted={result.submitted}")
    print(f"  client_order_id={result.client_order_id}")
    print(f"  error_code={result.error_code}")
    print(f"  error_msg={result.error_msg}")
    print(f"  gate_blocked={result.gate_blocked}")
    print()
    print("dry_run_request (the params that WOULD have been signed and POSTed):")
    if result.dry_run_request is None:
        print("  <None — pre-flight rejection; see error_msg above>")
    else:
        for k, v in sorted(result.dry_run_request.items()):
            print(f"  {k}: {v!r}")

    # Step 5 — exit code
    if result.dry_run_request is None or result.error_code is not None:
        # Pre-flight rejection — most likely below_min_notional ($5 SOL
        # passes SOL's $5 min, so this only fires if the operator's basket
        # caps or Binance's filters shifted).
        return 2
    return 0


# ──────────────────────────────────────────────────────────────────────────
# g7_guard_status — G7.3 pilot-guard status snapshot
# ──────────────────────────────────────────────────────────────────────────
#
# Paper-safe / read-only. Prints the GuardState snapshot as a clean table
# (every field on its own line) then runs `guard.check()` against each
# monitored market at `pilot_position_cap_usd` so the operator can see
# at a glance which markets would be allowed RIGHT NOW.
#
# ALWAYS exits 0 — informational only. Operational gates are enforced at
# the call site in G7.1, not here.
#
# Operator flow to take a market live (mirrored in the README):
#   1. flip `live_gmx_enabled=True` AND `live_binance_enabled=True`
#   2. set `funding_arb_armed_markets_csv=sol` (or whatever pilot market)
#   3. run `python -m gmx_strategies.cli g7_guard_status` to confirm
#   4. start the consumer (G7.1, next PR)


async def _g7_guard_status_main(args: argparse.Namespace) -> int:  # noqa: ARG001
    """Print the live pilot-guard snapshot + per-market check results."""
    from gmx_strategies import pilot_guard

    print("G7.3 pilot-guard status — read-only snapshot")
    print(f"  redis_url: {settings.redis_url}")
    print(
        f"  live_gmx_enabled: {settings.live_gmx_enabled} "
        f"live_binance_enabled: {settings.live_binance_enabled}",
    )
    print()

    guard = pilot_guard.PilotGuard()
    state = await guard.state()

    # State table — one field per line, fixed-width for grep-friendliness.
    print("GuardState:")
    print(f"  killswitch_set:           {state.killswitch_set}")
    print(
        f"  today_pnl_usd:            ${state.today_pnl_usd:.2f} "
        f"(floor ${state.today_pnl_floor_usd:.2f})",
    )
    print(f"  open_gmx_positions:       {state.open_gmx_positions}")
    print(f"  open_binance_positions:   {state.open_binance_positions}")
    print(f"  max_concurrent:           {state.max_concurrent}")
    armed_list = sorted(state.armed_markets) or ["<none — DEFAULT-DENY>"]
    print(f"  armed_markets:            {armed_list}")
    print(f"  pilot_position_cap_usd:   ${state.pilot_position_cap_usd:.2f}")
    print(f"  last_loss_ts_ms:          {state.last_loss_ts_ms}")
    print(f"  cooldown_remaining_s:     {state.cooldown_remaining_s}")
    print()

    # Per-market check at the pilot cap. The cap-sized check is the most
    # operator-actionable signal: "if I tried to open a pilot-sized
    # position in this market RIGHT NOW, would the guard allow it?"
    print(
        f"Per-market check at notional=${state.pilot_position_cap_usd:.2f}:",
    )
    cap = state.pilot_position_cap_usd
    markets = [m.strip() for m in settings.monitored_markets.split(",") if m.strip()]
    if not markets:
        print("  <no monitored_markets configured>", file=sys.stderr)
    for market in markets:
        result = await guard.check(market, cap)
        marker = "[ALLOW]" if result.allowed else "[DENY ]"
        if result.allowed:
            print(f"  {marker} {market:<5} allowed")
        else:
            print(f"  {marker} {market:<5} gate={result.gate} reason={result.reason}")

    # Operator-friendly nudge if armed_markets is empty — the most common
    # "why isn't anything allowed?" question. Stderr so it's distinct
    # from the table.
    if not state.armed_markets:
        print(
            "\nNOTE: funding_arb_armed_markets_csv is empty — DEFAULT-DENY "
            "is in effect. To arm a market, e.g. SOL:\n"
            "  export FUNDING_ARB_ARMED_MARKETS_CSV=sol\n"
            "Or add to your .env. Then re-run this command to confirm.",
            file=sys.stderr,
        )

    return 0


# ──────────────────────────────────────────────────────────────────────────
# g7_consumer_smoke — G7.1 funding-arb consumer one-shot smoke
# ──────────────────────────────────────────────────────────────────────────
#
# Paper-safe — invokes `FundingArbExecutor.handle_signal` ONCE on a synthetic
# SOL short_gmx_long_cex signal, with dry_run hard-coded True for both legs
# (regardless of `settings.funding_arb_executor_dry_run`). The CLI itself
# patches the dry_run flag for the duration of the call — see notes inline.
#
# What the smoke validates:
#   - Synthetic signal payload parses through the consumer pipeline.
#   - PilotGuard.check() is called once; result is included in the record.
#   - If the operator has armed SOL, the GMX + Binance dry-run legs both
#     attempt their respective construction paths.
#   - The ExecutionRecord is XADD'd to `funding_arb:executions` (visible
#     via `XREVRANGE funding_arb:executions + - COUNT 1`).
#
# Exit codes:
#   0 — handle_signal completed without raising (any outcome is fine —
#       guard denial is success-shaped here)
#   2 — handle_signal raised an unexpected exception


async def _g7_consumer_smoke_main(args: argparse.Namespace) -> int:  # noqa: ARG001
    """Run the G7.1 consumer smoke — never broadcasts."""
    from gmx_strategies import funding_arb_executor

    # HARD-CODE dry_run=True for the smoke regardless of env settings.
    # We patch the settings field locally for the duration of this call,
    # then restore it — both for safety and so the operator sees the
    # actual setting reflected in the printed record.
    original_dry_run = settings.funding_arb_executor_dry_run
    settings.funding_arb_executor_dry_run = True

    print("G7.1 funding-arb consumer smoke — DRY-RUN ONLY")
    print(f"  signals_channel: {settings.funding_arb_signals_channel}")
    print(
        f"  consumer_enabled (setting): {settings.funding_arb_consumer_enabled} "
        "(this CLI runs the executor once regardless)",
    )
    print(
        f"  executor_dry_run (forced True for smoke): "
        f"original={original_dry_run}, smoke_uses=True",
    )
    print(
        f"  live_gmx_enabled: {settings.live_gmx_enabled} "
        f"live_binance_enabled: {settings.live_binance_enabled}",
    )
    print()

    # Synthetic SOL signal — matches the funding_arb_runtime emit shape
    # (short_gmx_long_cex direction at $10 notional). Picking SOL so the
    # alt-band slippage tolerance + the SOLUSDT min-notional ($5) make
    # the construction succeed when the operator has armed SOL.
    signal: dict[str, Any] = {
        "ts": 0,
        "market": "sol",
        "direction": "short_gmx_long_cex",
        "funding_rate_per_8h": 0.001,
        "annualized_yield_pct": 109.5,
        "target_position_usd": 10.0,
        "cex_rate_per_8h": 0.0,
        "net_rate_per_8h": 0.001,
        "cex_source": "mock",
        "mode": "paper",
    }
    print("synthetic signal:")
    for k, v in signal.items():
        print(f"  {k}: {v!r}")
    print()

    executor = funding_arb_executor.FundingArbExecutor()
    try:
        record = await executor.handle_signal(signal)
    except Exception as exc:  # noqa: BLE001 — surface as exit code 2
        print(f"ERROR: handle_signal raised: {exc.__class__.__name__}: {exc}")
        settings.funding_arb_executor_dry_run = original_dry_run
        return 2
    finally:
        # Restore the setting whether or not we raised.
        settings.funding_arb_executor_dry_run = original_dry_run

    # Pretty-print the record. Avoid dumping any sub-result that might
    # carry sensitive fields; only the cardinal fields go to stdout.
    print("ExecutionRecord:")
    print(f"  ts_ms:               {record.ts_ms}")
    print(f"  market:              {record.market}")
    print(f"  direction:           {record.direction}")
    print(f"  notional_usd_target: ${record.notional_usd_target:.2f}")
    print(f"  success_both_legs:   {record.success_both_legs}")
    print(f"  realized_pnl_usd:    ${record.realized_pnl_usd:.4f}")
    print(f"  gmx_tx_hash:         {record.gmx_tx_hash}")
    print(f"  binance_order_id:    {record.binance_order_id}")
    print(f"  guard_block:         {record.guard_block}")
    print(f"  reconcile_block:     {record.reconcile_block}")
    print(f"  error:               {record.error}")
    # gmx_result / binance_result kept brief — print the keys only so a
    # secrets-bearing field can't sneak into a log paste.
    gmx_keys = list(record.gmx_result.keys()) if record.gmx_result else None
    bnc_keys = list(record.binance_result.keys()) if record.binance_result else None
    print(f"  gmx_result keys:     {gmx_keys}")
    print(f"  binance_result keys: {bnc_keys}")
    print()
    print("exit_code: 0 (handle_signal completed)")
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

    sub.add_parser(
        "g5_sign_smoke",
        help=(
            "G5.2 signer smoke: load key, sign synthetic $10 SOL order, "
            "dry-run-simulate via eth_call. Never broadcasts."
        ),
    )

    sub.add_parser(
        "g5_position_smoke",
        help=(
            "G5.3 position-reader smoke: read on-chain positions for the "
            "canonical empty address (and the executor address if a key is "
            "configured). Read-only eth_call against GMX V2 Reader."
        ),
    )

    smoke = sub.add_parser(
        "g6_smoke",
        help=(
            "G6.3 testnet/mainnet shakedown. Runs every read-only Binance "
            "Futures check end-to-end. Paper-safe — no order placement."
        ),
    )
    smoke.add_argument(
        "--force-refresh-exchange-info",
        action="store_true",
        help=(
            "Reset the binance_exchange_info module-level TTL cache before "
            "running PUBLIC-1. Useful if you just bumped filter values in "
            "the Binance UI and want to verify they propagate."
        ),
    )

    sub.add_parser(
        "g6_dry_run_order",
        help=(
            "G6.4 order-placement dry-run: constructs a $5 SOLUSDT BUY MARKET "
            "order at the current mark price and runs place_market_order with "
            "dry_run=True. NEVER broadcasts. Prints the would-be signed params."
        ),
    )

    sub.add_parser(
        "g7_guard_status",
        help=(
            "G7.3 pilot-guard status snapshot. Prints every gate input + "
            "runs PilotGuard.check() for each monitored market at the pilot "
            "position cap. Read-only; never broadcasts. Always exits 0."
        ),
    )

    sub.add_parser(
        "g7_consumer_smoke",
        help=(
            "G7.1 funding-arb consumer smoke. Constructs a synthetic SOL "
            "short_gmx_long_cex signal at $10 and runs "
            "FundingArbExecutor.handle_signal ONCE with dry_run hard-coded "
            "True. NEVER broadcasts. Prints the ExecutionRecord."
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
    if args.subcommand == "g5_position_smoke":
        return asyncio.run(_g5_position_smoke_main(args))
    if args.subcommand == "g6_smoke":
        return asyncio.run(_g6_smoke_main(args))
    if args.subcommand == "g6_dry_run_order":
        return asyncio.run(_g6_dry_run_order_main(args))
    if args.subcommand == "g7_guard_status":
        return asyncio.run(_g7_guard_status_main(args))
    if args.subcommand == "g7_consumer_smoke":
        return asyncio.run(_g7_consumer_smoke_main(args))
    parser.print_help()
    return 1


if __name__ == "__main__":  # pragma: no cover — module __main__ shim
    sys.exit(main())
