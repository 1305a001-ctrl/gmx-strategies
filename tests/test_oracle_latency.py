"""Oracle latency telemetry — pure helper tests."""
from __future__ import annotations

from gmx_strategies import oracle_latency as ol


def test_compute_lead_time_streams_first():
    """When streams arrives BEFORE on-chain, lead = onchain - streams > 0."""
    lead = ol.compute_lead_time(streams_ts_unix=100.0, onchain_ts_unix=102.5)
    assert lead == 2.5


def test_compute_lead_time_streams_late():
    """When streams arrives AFTER on-chain, lead is negative (we lost race)."""
    lead = ol.compute_lead_time(streams_ts_unix=110.0, onchain_ts_unix=100.0)
    assert lead == -10.0


def test_price_delta_pct_close():
    delta = ol.compute_price_delta_pct(
        streams_price=100.0, onchain_price=100.05,
    )
    assert abs(delta - 0.05) < 1e-9


def test_price_delta_pct_zero_onchain():
    """Degenerate onchain_price → 0.0 not infinity."""
    assert ol.compute_price_delta_pct(streams_price=100.0, onchain_price=0.0) == 0.0


def test_percentile_p50_p95():
    values = list(range(1, 101))   # 1..100
    assert ol.percentile(values, 50) >= 50    # roughly median
    assert ol.percentile(values, 95) >= 95


def test_percentile_empty_returns_zero():
    assert ol.percentile([], 50) == 0.0


def test_percentile_clamps():
    values = [1.0, 2.0, 3.0]
    assert ol.percentile(values, -10) == 1.0   # min
    assert ol.percentile(values, 110) == 3.0   # max


def test_aggregate_empty_returns_none():
    assert ol.aggregate([]) is None


def test_aggregate_summary_shape():
    samples = [
        ol.LatencySample(
            asset="btc",
            streams_ts_unix=100.0 + i,
            onchain_ts_unix=102.0 + i,
            lead_time_sec=2.0,
            streams_price=110000.0,
            onchain_price=110000.05,
            price_delta_pct=0.00005,
        )
        for i in range(20)
    ]
    summary = ol.aggregate(samples)
    assert summary is not None
    assert summary.asset == "btc"
    assert summary.n_samples == 20
    assert summary.p50_lead_sec == 2.0
    assert summary.p95_lead_sec == 2.0


def test_build_sample_happy_path():
    s = ol.build_sample(
        asset="btc",
        streams_payload={"price": 100000.0, "ts_unix": 1700000000.0},
        onchain_payload={"price": 100010.0, "updated_at_unix": 1700000002.5},
    )
    assert s is not None
    assert s.lead_time_sec == 2.5
    assert s.streams_price == 100000.0


def test_build_sample_missing_fields():
    s = ol.build_sample(
        asset="btc",
        streams_payload={},
        onchain_payload={"price": 100.0, "updated_at_unix": 1700000000.0},
    )
    assert s is None


def test_build_sample_zero_price():
    s = ol.build_sample(
        asset="btc",
        streams_payload={"price": 0.0, "ts_unix": 1700000000.0},
        onchain_payload={"price": 100.0, "updated_at_unix": 1700000000.0},
    )
    assert s is None


def test_summary_to_dict_serializable():
    """Output dict should be JSON-serializable (no non-stringable types)."""
    import json
    summary = ol.LatencySummary(
        asset="eth", n_samples=10,
        p50_lead_sec=1.5, p95_lead_sec=3.0, p99_lead_sec=5.0,
        median_price_delta_pct=0.005,
    )
    d = ol.summary_to_dict(summary)
    encoded = json.dumps(d)
    decoded = json.loads(encoded)
    assert decoded["asset"] == "eth"
    assert decoded["n_samples"] == 10
