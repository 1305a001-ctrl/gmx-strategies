"""Funding-arb runtime — paper-mode loop wiring around pure helpers.

v0.3 scaffold (G1) + G2 live-reader switch + G3 Binance CEX leg.

What this module does:
  - Iterates monitored Arbitrum GMX V2 markets (filtered against markets.py).
  - For each market, fetches a FundingState via either:
      * `fetch_gmx_funding_mock` (default, settings.gmx_funding_source="mock")
      * `gmx_reader.fetch_gmx_funding_live` (settings.gmx_funding_source="live")
  - For each market, fetches the CEX (Binance) hedge funding rate via either:
      * `fetch_cex_funding` zero-stub (default, settings.binance_funding_source="mock")
      * `binance_funding.fetch_cex_funding_live` (settings.binance_funding_source="live")
      * `binance_funding.fetch_all_cex_fundings` (one batched call per sweep, when
         settings.binance_funding_batch=True — much cheaper at 60s cadence).
  - Runs the pure `detect_signal()` helper. NOTE: detect_signal is intentionally
    venue-agnostic — it takes ONLY the GMX FundingState. The CEX rate composes
    into the EMIT payload (as `cex_rate_per_8h` + `net_rate_per_8h`), not the
    trigger. Reason: keep the pure helper clean; net-rate-based detection is a
    separate decision for later.
  - On a hit, emits the signal to Redis pub/sub channel `funding_arb:signals`
    AND XADD-s it to `funding_arb:eval_log`. mode="paper" is hard-coded.
  - Sleeps `funding_arb_poll_interval_s` between sweeps.

What this module deliberately does NOT do:
  - No order placement. No live execution. LIVE_ENABLED gate untouched.

Injection points for later phases:
  Pass custom `gmx_fetcher` / `cex_fetcher` callables to `run_funding_arb_runtime`.
  This is how tests drive the loop and how the live fetchers swap in without
  touching the loop body. The default fetchers are picked by `_default_*_fetcher()`
  at call-time from settings.

Error handling:
  A fetcher raising for one market is logged and skipped — never kills the
  loop. The next market in the sweep proceeds normally. The live GMX reader's
  `_fetch_gmx_funding_live_wrapper` converts a `None` (any failure inside
  `fetch_gmx_funding_live`) to a raise so the existing per-market try/except
  handles it uniformly. The CEX leg is more forgiving: a None/failure does
  NOT block the signal — net_rate falls back to gmx_rate (cex=0) and the
  payload's `cex_source` becomes "mock_fallback".
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Final

from gmx_strategies.binance_funding import (
    fetch_all_cex_fundings,
    fetch_cex_funding_live,
)
from gmx_strategies.funding_arb import FundingState, detect_signal
from gmx_strategies.gmx_reader import fetch_gmx_funding_live
from gmx_strategies.markets import ARBITRUM_MARKETS
from gmx_strategies.redis_client import r
from gmx_strategies.settings import settings

log = logging.getLogger(__name__)

# Aliases of operator's live Chainlink Data Streams feeds that ALSO have a
# GMX V2 perp on Arbitrum. BNB + HYPE are excluded — no GMX market. SOL/DOGE/
# XRP are intended targets; we filter against ARBITRUM_MARKETS below so that
# missing entries silently drop without crashing the loop. The PR notes any
# gaps so they can be filled when addresses are verified.
_INTENDED_MARKETS: Final[tuple[str, ...]] = ("btc", "eth", "sol", "doge", "xrp")

GmxFetcher = Callable[[str], Awaitable[FundingState]]
# CexFetcher returns float (mock path, never None) OR float | None (live path,
# None on transient failure → falls back to 0.0 with cex_source="mock_fallback").
# The Union keeps back-compat with the G1 mock signature.
CexFetcher = Callable[[str], Awaitable[float | None]]


async def fetch_gmx_funding_mock(market: str) -> FundingState:
    """Paper-mode placeholder for the GMX V2 funding-rate read.

    Returns a deterministic mock FundingState. G2 wired the live reader
    in `gmx_reader.fetch_gmx_funding_live`; this mock remains as the
    default (settings.gmx_funding_source == "mock") so the operator must
    explicitly opt into live reads.

    The mock biases btc/eth toward long-skew + small positive funding so the
    runtime exercises the emit path under default thresholds; sol/doge/xrp
    are kept below threshold so the runtime exercises the no-emit path too.
    """
    presets: dict[str, FundingState] = {
        "btc": FundingState(
            market="btc",
            longs_oi_usd=80_000_000.0,
            shorts_oi_usd=20_000_000.0,
            funding_rate_per_8h=0.0008,
        ),
        "eth": FundingState(
            market="eth",
            longs_oi_usd=60_000_000.0,
            shorts_oi_usd=40_000_000.0,
            funding_rate_per_8h=0.0006,
        ),
        "sol": FundingState(
            market="sol",
            longs_oi_usd=5_000_000.0,
            shorts_oi_usd=4_800_000.0,
            funding_rate_per_8h=0.0001,
        ),
        "doge": FundingState(
            market="doge",
            longs_oi_usd=2_500_000.0,
            shorts_oi_usd=2_400_000.0,
            funding_rate_per_8h=0.0001,
        ),
        "xrp": FundingState(
            market="xrp",
            longs_oi_usd=3_000_000.0,
            shorts_oi_usd=2_900_000.0,
            funding_rate_per_8h=0.0001,
        ),
    }
    if market not in presets:
        return FundingState(
            market=market,
            longs_oi_usd=1_000_000.0,
            shorts_oi_usd=1_000_000.0,
            funding_rate_per_8h=0.0,
        )
    return presets[market]


async def _fetch_gmx_funding_live_wrapper(market: str) -> FundingState:
    """Adapter: convert `fetch_gmx_funding_live` (returns FundingState | None)
    into the strict `GmxFetcher` shape (returns FundingState, raises on None).

    `_process_market` catches Exception and skips the market — so raising
    here on a None result preserves the loop-survival contract.
    """
    state = await fetch_gmx_funding_live(market, chain="arbitrum")
    if state is None:
        raise RuntimeError(f"live GMX reader returned None for market={market}")
    return state


# Back-compat: existing tests patch `fetch_gmx_funding`; keep the symbol
# pointing at the default fetcher (which the tests have always assumed is
# the mock path). The live path is opted-in via settings.gmx_funding_source.
fetch_gmx_funding = fetch_gmx_funding_mock


def _default_gmx_fetcher() -> GmxFetcher:
    """Pick the default fetcher per settings — runs at call-time, not import-time,
    so env overrides for `gmx_funding_source` are respected on each call.
    """
    if settings.gmx_funding_source == "live":
        return _fetch_gmx_funding_live_wrapper
    # The module attribute `fetch_gmx_funding` is what existing tests patch,
    # so we read it dynamically rather than capturing the function object
    # at definition time.
    import sys

    mod = sys.modules[__name__]
    return mod.fetch_gmx_funding  # type: ignore[no-any-return]


async def fetch_cex_funding(symbol: str) -> float:
    """Paper-mode placeholder for the CEX (Binance) perp funding read.

    Returns a constant 0.0 — keeps `net_rate == gmx_rate` for mock-mode emits.
    The live path (`binance_funding.fetch_cex_funding_live`) is opted in via
    `settings.binance_funding_source = "live"`.

    Signature is the legacy single-market shape kept for the existing
    `cex_fetcher` injection point in tests.
    """
    _ = symbol
    return 0.0


def _default_cex_fetcher() -> CexFetcher:
    """Pick the default CEX fetcher per settings — runs at call-time so env
    overrides for `binance_funding_source` are respected on each call.

    When `binance_funding_source == "live"` AND `binance_funding_batch == False`,
    the per-market live fetcher is returned. The batched path is handled
    separately in `run_funding_arb_runtime` because it makes ONE HTTP call
    per sweep, not per market — it doesn't fit the per-market fetcher shape.

    For tests that patch `fetch_cex_funding` at module level (back-compat with
    G1 tests), we read it dynamically through `sys.modules` rather than
    capturing the function object at definition time.
    """
    if settings.binance_funding_source == "live" and not settings.binance_funding_batch:
        return fetch_cex_funding_live
    import sys

    mod = sys.modules[__name__]
    return mod.fetch_cex_funding  # type: ignore[no-any-return]


def _signal_payload(
    *,
    market: str,
    direction: str,
    funding_rate_per_8h: float,
    annualized_yield_pct: float,
    target_position_usd: float,
    cex_rate_per_8h: float,
    net_rate_per_8h: float,
    cex_source: str,
) -> dict[str, object]:
    """Build the JSON-serializable emit payload.

    G3 extends the G2 payload with 3 fields:
      - `cex_rate_per_8h`: Binance funding rate (GMX convention, same sign).
      - `net_rate_per_8h`: gmx_rate - cex_rate. THIS is the actual edge —
        downstream consumers should prefer this over `funding_rate_per_8h`
        for sizing/PnL projections.
      - `cex_source`: "mock" | "binance" | "mock_fallback" (live attempted
        but failed, treated as 0.0).

    The existing field `funding_rate_per_8h` continues to mean the GMX-side
    rate — unchanged from G1/G2 so downstream consumers don't break.
    """
    return {
        "ts": int(time.time()),
        "market": market,
        "direction": direction,
        "funding_rate_per_8h": funding_rate_per_8h,
        "annualized_yield_pct": annualized_yield_pct,
        "target_position_usd": target_position_usd,
        "cex_rate_per_8h": cex_rate_per_8h,
        "net_rate_per_8h": net_rate_per_8h,
        "cex_source": cex_source,
        "mode": "paper",
    }


async def _emit_signal(payload: dict[str, object]) -> None:
    """Publish to pub/sub + XADD to eval-log stream. Best-effort; logs on fail."""
    redis = r()
    body = json.dumps(payload)
    try:
        await redis.publish(settings.funding_arb_signals_channel, body)
    except Exception as exc:  # noqa: BLE001 — emit failures must not kill loop
        log.warning("funding_arb.emit.publish_failed market=%s err=%s",
                    payload.get("market"), exc)
    try:
        # All XADD values must be strings; stringify primitives. The exact
        # key/value union here matches redis-py's xadd signature so mypy
        # strict mode passes.
        stream_fields: dict[
            bytes | bytearray | memoryview[int] | str | int | float,
            bytes | bytearray | memoryview[int] | str | int | float,
        ] = {k: str(v) for k, v in payload.items()}
        await redis.xadd(
            settings.funding_arb_eval_log_stream,
            stream_fields,
            maxlen=settings.funding_arb_eval_log_maxlen,
            approximate=True,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("funding_arb.emit.xadd_failed market=%s err=%s",
                    payload.get("market"), exc)


async def _process_market(
    market: str,
    *,
    gmx_fetcher: GmxFetcher,
    cex_fetcher: CexFetcher,
    cex_cache: dict[str, float] | None = None,
) -> bool:
    """One market poll. Returns True iff a signal was emitted.

    GMX-fetcher exceptions kill THIS market's signal (loop survives, next
    market proceeds normally — the funding-arb edge needs GMX data).

    CEX-fetcher exceptions and `None` returns are RECOVERABLE: log the
    failure, treat cex_rate as 0.0 (so net_rate falls back to gmx_rate),
    set cex_source="mock_fallback", and STILL ship the signal. Downstream
    consumers can decide whether to act on a signal with no hedge data.

    Args:
        cex_cache: when present, the CEX rate is read from this dict (the
            batched-mode path populates it once per sweep) instead of
            calling `cex_fetcher`. Missing keys fall back to mock_fallback.
    """
    # ---- GMX leg: hard dependency for signal generation ----
    try:
        state = await gmx_fetcher(market)
    except Exception as exc:  # noqa: BLE001
        log.warning("funding_arb.gmx_fetch_failed market=%s err=%s", market, exc)
        return False

    # ---- CEX leg: optional; failure does NOT block the signal ----
    cex_source: str
    cex_rate: float
    if cex_cache is not None:
        # Batched path — the cache is pre-populated for this sweep.
        cached = cex_cache.get(market)
        if cached is None:
            log.warning(
                "funding_arb.cex_cache_miss market=%s — emitting with mock_fallback",
                market,
            )
            cex_rate = 0.0
            cex_source = "mock_fallback"
        else:
            cex_rate = cached
            cex_source = "binance"
    else:
        # Per-market path — call the injected fetcher.
        try:
            result = await cex_fetcher(market)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "funding_arb.cex_fetch_failed market=%s err=%s — emitting with mock_fallback",
                market, exc,
            )
            result = None
        if result is None:
            cex_rate = 0.0
            # Distinguish "we never tried live" (mock path) vs "live attempted
            # and failed". The mock stub returns 0.0 (not None), so a None here
            # is always from the live path failing.
            cex_source = "mock_fallback" if (
                settings.binance_funding_source == "live"
            ) else "mock"
        else:
            cex_rate = float(result)
            cex_source = (
                "binance" if settings.binance_funding_source == "live" else "mock"
            )

    # Signal detection is GMX-only (pure helper stays venue-agnostic).
    signal = detect_signal(
        state,
        min_rate=settings.funding_arb_min_rate,
        max_position_usd=settings.funding_arb_max_position_usd,
    )
    if signal is None:
        log.debug(
            "funding_arb.no_signal market=%s gmx_rate=%s cex_rate=%s",
            market, state.funding_rate_per_8h, cex_rate,
        )
        return False

    net_rate = signal.funding_rate_per_8h - cex_rate
    payload = _signal_payload(
        market=signal.market,
        direction=signal.direction,
        funding_rate_per_8h=signal.funding_rate_per_8h,
        annualized_yield_pct=signal.annualized_yield_pct,
        target_position_usd=signal.target_position_usd,
        cex_rate_per_8h=cex_rate,
        net_rate_per_8h=net_rate,
        cex_source=cex_source,
    )
    await _emit_signal(payload)
    log.info(
        "funding_arb.signal_emitted market=%s direction=%s annualized=%.2f%% "
        "gmx_rate=%.6f cex_rate=%.6f net_rate=%.6f cex_source=%s",
        signal.market, signal.direction, signal.annualized_yield_pct,
        signal.funding_rate_per_8h, cex_rate, net_rate, cex_source,
    )
    return True


def _resolve_markets() -> list[str]:
    """Filter intended markets against ARBITRUM_MARKETS — drop unknowns."""
    resolved: list[str] = []
    skipped: list[str] = []
    for alias in _INTENDED_MARKETS:
        if alias in ARBITRUM_MARKETS:
            resolved.append(alias)
        else:
            skipped.append(alias)
    if skipped:
        log.warning(
            "funding_arb.markets_skipped aliases=%s reason=no_market_address",
            ",".join(skipped),
        )
    return resolved


async def run_funding_arb_runtime(
    *,
    gmx_fetcher: GmxFetcher | None = None,
    cex_fetcher: CexFetcher | None = None,
    iterations: int | None = None,
) -> None:
    """Main async loop. Paper mode only.

    Args:
        gmx_fetcher: override the GMX funding fetcher (tests).
        cex_fetcher: override the CEX funding fetcher (tests). When None,
            the default is picked from settings — see `_default_cex_fetcher`.
        iterations: when set, run exactly N sweeps then return (test hook).
            None = run forever.

    Batched CEX path (default):
      When `settings.binance_funding_source == "live"` AND
      `settings.binance_funding_batch == True` AND no explicit `cex_fetcher`
      override is passed, we call `fetch_all_cex_fundings()` ONCE per sweep
      and dispatch the cached rates into `_process_market`. This is 5× fewer
      HTTP calls per sweep compared to per-market fetches.

      An injected `cex_fetcher` overrides this — tests can still drive the
      per-market path even when settings would otherwise pick the batched one.
    """
    gmx = gmx_fetcher or _default_gmx_fetcher()
    use_batched_cex = (
        cex_fetcher is None
        and settings.binance_funding_source == "live"
        and settings.binance_funding_batch
    )
    cex = cex_fetcher or _default_cex_fetcher()
    markets = _resolve_markets()
    log.info(
        "funding_arb.runtime_start markets=%s poll_s=%s min_rate=%s "
        "gmx_source=%s cex_source=%s batched_cex=%s",
        ",".join(markets), settings.funding_arb_poll_interval_s,
        settings.funding_arb_min_rate,
        settings.gmx_funding_source, settings.binance_funding_source,
        use_batched_cex,
    )

    sweep = 0
    while iterations is None or sweep < iterations:
        # In batched-CEX mode, fetch all CEX rates ONCE per sweep, then
        # dispatch the cache to _process_market. The cache is None for the
        # per-market path (the legacy default + the live single-market path).
        cex_cache: dict[str, float] | None = None
        if use_batched_cex:
            try:
                cex_cache = await fetch_all_cex_fundings()
            except Exception as exc:  # noqa: BLE001
                # Batched fetch failures fall back to empty cache → every
                # market in this sweep emits with cex_source="mock_fallback".
                log.warning(
                    "funding_arb.batched_cex_failed err=%s — sweep proceeds with empty cache",
                    exc,
                )
                cex_cache = {}

        polled = 0
        emitted = 0
        errors = 0
        for market in markets:
            try:
                fired = await _process_market(
                    market,
                    gmx_fetcher=gmx,
                    cex_fetcher=cex,
                    cex_cache=cex_cache,
                )
            except Exception as exc:  # noqa: BLE001
                # Defensive: _process_market already catches fetch errors;
                # this catches anything genuinely unexpected (e.g. redis emit
                # raising a non-Exception class), so the sweep stays alive.
                log.warning(
                    "funding_arb.sweep_market_error market=%s err=%s", market, exc,
                )
                errors += 1
                continue
            polled += 1
            if fired:
                emitted += 1
        log.info(
            "funding_arb.sweep_done sweep=%s polled=%s emitted=%s errors=%s",
            sweep, polled, emitted, errors,
        )
        sweep += 1
        if iterations is not None and sweep >= iterations:
            break
        await asyncio.sleep(settings.funding_arb_poll_interval_s)


__all__ = [
    "CexFetcher",
    "GmxFetcher",
    "fetch_cex_funding",
    "fetch_gmx_funding",
    "fetch_gmx_funding_mock",
    "fetch_all_cex_fundings",
    "fetch_cex_funding_live",
    "run_funding_arb_runtime",
]
