"""Funding-arb runtime — paper-mode loop wiring around pure helpers.

v0.3 scaffold (G1) + G2 live-reader switch.

What this module does:
  - Iterates monitored Arbitrum GMX V2 markets (filtered against markets.py).
  - For each market, fetches a FundingState via either:
      * `fetch_gmx_funding_mock` (default, settings.gmx_funding_source="mock")
      * `gmx_reader.fetch_gmx_funding_live` (settings.gmx_funding_source="live")
  - Fetches the would-be hedge venue funding via `fetch_cex_funding` (paper stub).
  - Runs the pure `detect_signal()` helper.
  - On a hit, emits the signal to Redis pub/sub channel `funding_arb:signals`
    AND XADD-s it to `funding_arb:eval_log`. mode="paper" is hard-coded.
  - Sleeps `funding_arb_poll_interval_s` between sweeps.

What this module deliberately does NOT do (G3+ territory):
  - No live Binance API. `fetch_cex_funding` returns a mocked constant.
  - No order placement. No live execution. LIVE_ENABLED gate untouched.

Injection points for later phases:
  Pass custom `gmx_fetcher` / `cex_fetcher` callables to `run_funding_arb_runtime`.
  This is how tests drive the loop and how G3 will swap in real fetchers
  without touching the loop body. The default `gmx_fetcher` is picked by
  `_default_gmx_fetcher()` at call-time from settings.

Error handling:
  A fetcher raising for one market is logged and skipped — never kills the
  loop. The next market in the sweep proceeds normally. The live reader's
  `_fetch_gmx_funding_live_wrapper` converts a `None` (any failure inside
  `fetch_gmx_funding_live`) to a raise so the existing per-market try/except
  handles it uniformly.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Final

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
CexFetcher = Callable[[str], Awaitable[float]]


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

    Returns a mock 8h funding rate as a float. G3 will replace this with a
    Binance premiumIndex call. Signature is the live shape: symbol -> rate.
    """
    return 0.0


def _signal_payload(
    *,
    market: str,
    direction: str,
    funding_rate_per_8h: float,
    annualized_yield_pct: float,
    target_position_usd: float,
) -> dict[str, object]:
    """Build the JSON-serializable emit payload (shape locked for G2/G3)."""
    return {
        "ts": int(time.time()),
        "market": market,
        "direction": direction,
        "funding_rate_per_8h": funding_rate_per_8h,
        "annualized_yield_pct": annualized_yield_pct,
        "target_position_usd": target_position_usd,
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
) -> bool:
    """One market poll. Returns True iff a signal was emitted.

    Exceptions from EITHER fetcher are caught here so a single bad market
    does not kill the sweep. The next market continues normally.
    """
    try:
        state = await gmx_fetcher(market)
        # The CEX leg is fetched even though detect_signal doesn't consume it
        # directly — it's logged for parity with G3 wiring + so a broken CEX
        # fetcher surfaces here, in the per-market try, not at sweep level.
        cex_rate = await cex_fetcher(market)
    except Exception as exc:  # noqa: BLE001 — graceful per-market degradation
        log.warning("funding_arb.fetch_failed market=%s err=%s", market, exc)
        return False

    signal = detect_signal(
        state,
        min_rate=settings.funding_arb_min_rate,
        max_position_usd=settings.funding_arb_max_position_usd,
    )
    if signal is None:
        log.debug(
            "funding_arb.no_signal market=%s rate_per_8h=%s cex_rate=%s",
            market, state.funding_rate_per_8h, cex_rate,
        )
        return False

    payload = _signal_payload(
        market=signal.market,
        direction=signal.direction,
        funding_rate_per_8h=signal.funding_rate_per_8h,
        annualized_yield_pct=signal.annualized_yield_pct,
        target_position_usd=signal.target_position_usd,
    )
    await _emit_signal(payload)
    log.info(
        "funding_arb.signal_emitted market=%s direction=%s annualized=%.2f%% cex_rate=%s",
        signal.market, signal.direction, signal.annualized_yield_pct, cex_rate,
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
        gmx_fetcher: override the GMX funding fetcher (tests / future G2).
        cex_fetcher: override the CEX funding fetcher (tests / future G3).
        iterations: when set, run exactly N sweeps then return (test hook).
            None = run forever.
    """
    gmx = gmx_fetcher or _default_gmx_fetcher()
    cex = cex_fetcher or fetch_cex_funding
    markets = _resolve_markets()
    log.info(
        "funding_arb.runtime_start markets=%s poll_s=%s min_rate=%s",
        ",".join(markets), settings.funding_arb_poll_interval_s,
        settings.funding_arb_min_rate,
    )

    sweep = 0
    while iterations is None or sweep < iterations:
        polled = 0
        emitted = 0
        errors = 0
        for market in markets:
            try:
                fired = await _process_market(
                    market, gmx_fetcher=gmx, cex_fetcher=cex,
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
    "run_funding_arb_runtime",
]
