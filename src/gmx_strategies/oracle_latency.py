"""Oracle-latency measurement — how much head-start does Chainlink Data
Streams give us vs the on-chain Aave/GMX oracle?

The thesis behind the GMX liquidator + chainlink-lag strategies is that
Chainlink Data Streams delivers oracle updates BEFORE the on-chain price
oracle reflects them. The latency window is our edge — bigger window =
more time to act before the rest of the market sees the new price.

This module measures the lead time in two ways:
  1. Streams-to-chain gap: time between Data Streams report timestamp
     and the on-chain Aggregator's `latestRoundData()` updatedAt
  2. Streams-to-mid-price gap: time between Data Streams report and
     when the cross-venue mid (Binance/Bybit) reflects the same price

Persists per-asset latency stats to Redis stream `oracle:latency:samples`
and aggregates p50/p95/p99 into `oracle:latency:summary` daily.

Pure helpers (parsing + percentile math) tested without I/O. Async
fetchers test against mocked Redis. Live calibration requires the
chainlink:reports stream to be populating + on-chain Aggregator addresses
configured per asset (settings.chainlink_aggregator_addresses).
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class LatencySample:
    """One measurement of streams-to-chain lead time."""
    asset: str
    streams_ts_unix: float
    onchain_ts_unix: float
    lead_time_sec: float
    streams_price: float
    onchain_price: float
    price_delta_pct: float


@dataclass(frozen=True)
class LatencySummary:
    """Aggregated percentile snapshot for one asset over a window."""
    asset: str
    n_samples: int
    p50_lead_sec: float
    p95_lead_sec: float
    p99_lead_sec: float
    median_price_delta_pct: float


SAMPLES_STREAM = "oracle:latency:samples"
SAMPLES_STREAM_MAXLEN = 1_000_000
SUMMARY_KEY = "oracle:latency:summary:{asset}"
SUMMARY_TTL_SEC = 7 * 24 * 3600


def compute_lead_time(
    *, streams_ts_unix: float, onchain_ts_unix: float,
) -> float:
    """Pure: streams should be FASTER than on-chain — so streams_ts
    is typically EARLIER (smaller). Lead time = onchain - streams
    when streams arrives first; negative if on-chain was ahead
    (unusual; indicates we lost the race)."""
    return onchain_ts_unix - streams_ts_unix


def compute_price_delta_pct(
    *, streams_price: float, onchain_price: float,
) -> float:
    """Pure: |streams - onchain| / onchain × 100. Returns 0.0 on
    degenerate onchain_price."""
    if onchain_price <= 0:
        return 0.0
    return abs(streams_price - onchain_price) / onchain_price * 100.0


def percentile(values: list[float], p: float) -> float:
    """Pure: nearest-rank percentile (no interpolation). Empty → 0.0."""
    if not values:
        return 0.0
    if p <= 0:
        return min(values)
    if p >= 100:
        return max(values)
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    # nearest-rank: ceil(p/100 * N)
    rank = int(p / 100.0 * n)
    if rank >= n:
        rank = n - 1
    return sorted_vals[rank]


def aggregate(samples: list[LatencySample]) -> LatencySummary | None:
    """Pure: build a LatencySummary from a list of samples. None when empty."""
    if not samples:
        return None
    leads = [s.lead_time_sec for s in samples]
    deltas = [s.price_delta_pct for s in samples]
    return LatencySummary(
        asset=samples[0].asset,
        n_samples=len(samples),
        p50_lead_sec=percentile(leads, 50),
        p95_lead_sec=percentile(leads, 95),
        p99_lead_sec=percentile(leads, 99),
        median_price_delta_pct=percentile(deltas, 50),
    )


def build_sample(
    *,
    asset: str,
    streams_payload: dict,
    onchain_payload: dict,
) -> LatencySample | None:
    """Pure: build a LatencySample from decoded streams + on-chain payloads.

    streams_payload expects: {price, ts_unix, ...}
    onchain_payload expects: {price, updated_at_unix, ...}

    Returns None when either side is missing required fields or zero.
    """
    try:
        s_ts = float(streams_payload.get("ts_unix") or streams_payload.get("timestamp") or 0)
        s_price = float(streams_payload.get("price") or 0)
    except (TypeError, ValueError):
        return None
    try:
        o_ts = float(onchain_payload.get("updated_at_unix") or onchain_payload.get("ts_unix") or 0)
        o_price = float(onchain_payload.get("price") or onchain_payload.get("answer") or 0)
    except (TypeError, ValueError):
        return None
    if s_ts <= 0 or o_ts <= 0 or s_price <= 0 or o_price <= 0:
        return None

    return LatencySample(
        asset=asset,
        streams_ts_unix=s_ts,
        onchain_ts_unix=o_ts,
        lead_time_sec=compute_lead_time(
            streams_ts_unix=s_ts, onchain_ts_unix=o_ts,
        ),
        streams_price=s_price,
        onchain_price=o_price,
        price_delta_pct=compute_price_delta_pct(
            streams_price=s_price, onchain_price=o_price,
        ),
    )


def summary_to_dict(summary: LatencySummary) -> dict[str, float | int | str]:
    """Pure: JSON-friendly dict for Redis SET."""
    return {
        "asset": summary.asset,
        "n_samples": summary.n_samples,
        "p50_lead_sec": summary.p50_lead_sec,
        "p95_lead_sec": summary.p95_lead_sec,
        "p99_lead_sec": summary.p99_lead_sec,
        "median_price_delta_pct": summary.median_price_delta_pct,
        "computed_at_unix": time.time(),
    }


# Async wrappers — lazy-imported Redis when called


async def record_sample(sample: LatencySample) -> None:
    """Async: XADD a sample to the rolling stream."""
    from gmx_strategies.redis_client import r
    try:
        await r().xadd(
            SAMPLES_STREAM,
            {
                "ts_unix": str(int(time.time())),
                "asset": sample.asset,
                "streams_ts": f"{sample.streams_ts_unix:.3f}",
                "onchain_ts": f"{sample.onchain_ts_unix:.3f}",
                "lead_sec": f"{sample.lead_time_sec:.3f}",
                "streams_price": f"{sample.streams_price:.6f}",
                "onchain_price": f"{sample.onchain_price:.6f}",
                "delta_pct": f"{sample.price_delta_pct:.4f}",
            },
            maxlen=SAMPLES_STREAM_MAXLEN,
            approximate=True,
        )
    except Exception:
        log.exception("oracle_latency.record_failed asset=%s", sample.asset)


async def persist_summary(summary: LatencySummary) -> None:
    """Async: SET the per-asset summary."""
    from gmx_strategies.redis_client import r
    try:
        await r().set(
            SUMMARY_KEY.format(asset=summary.asset),
            json.dumps(summary_to_dict(summary)),
            ex=SUMMARY_TTL_SEC,
        )
    except Exception:
        log.exception("oracle_latency.summary_failed asset=%s", summary.asset)


__all__ = [
    "LatencySample",
    "LatencySummary",
    "SAMPLES_STREAM",
    "SUMMARY_KEY",
    "compute_lead_time",
    "compute_price_delta_pct",
    "percentile",
    "aggregate",
    "build_sample",
    "summary_to_dict",
    "record_sample",
    "persist_summary",
]
