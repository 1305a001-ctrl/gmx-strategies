"""Tests for the trap-surface watchdog (watchdog.py + cli.py).

We avoid all real network — every check is exercised against a stub
httpx.AsyncClient injected via the function's `client` parameter. The
package's other suites have shown this pattern works without needing
respx (which is not in dev deps and we cannot add).

Coverage:
  - check_reader_address_drift: OK / CRITICAL / ERROR paths.
  - check_markets_alive: OK / WARN-on-disable / ERROR paths.
  - check_hyperlend_oracle_source: OK / CRITICAL / ERROR paths.
  - has_critical / summarize_results / publish_alert helpers.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
from eth_abi import encode

from gmx_strategies import watchdog
from gmx_strategies.markets import GMXMarket

# ──────────────────────────────────────────────────────────────────────────
# Stub plumbing — minimal httpx.AsyncClient surface needed by watchdog.py
# ──────────────────────────────────────────────────────────────────────────


class _StubResponse:
    """Mimics the slice of httpx.Response that watchdog uses."""

    def __init__(self, *, status_code: int = 200, json_body: Any = None) -> None:
        self.status_code = status_code
        self._json_body = json_body

    def json(self) -> Any:
        return self._json_body


class _StubClient:
    """Mimics httpx.AsyncClient with .get() and .post() recorded.

    Returns the responses pushed via `set_get_response` / `set_post_responses`.
    Multiple POSTs can be queued in `_post_queue` (popped FIFO).
    """

    def __init__(self) -> None:
        self._get_response: _StubResponse | None = None
        self._get_exc: Exception | None = None
        self._post_queue: list[_StubResponse | Exception] = []
        self.gets: list[tuple[str, Any]] = []
        self.posts: list[tuple[str, Any]] = []

    def set_get_response(
        self, response: _StubResponse | None = None, *, exc: Exception | None = None,
    ) -> None:
        self._get_response = response
        self._get_exc = exc

    def push_post_response(self, response: _StubResponse | Exception) -> None:
        self._post_queue.append(response)

    async def get(self, url: str, *args: Any, **kwargs: Any) -> _StubResponse:
        self.gets.append((url, kwargs))
        if self._get_exc is not None:
            raise self._get_exc
        assert self._get_response is not None, "set_get_response not called"
        return self._get_response

    async def post(
        self, url: str, *, json: Any = None, **kwargs: Any,
    ) -> _StubResponse:
        self.posts.append((url, json))
        if not self._post_queue:
            raise AssertionError("post called more times than responses queued")
        nxt = self._post_queue.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt

    async def aclose(self) -> None:
        return None


# Helper: address-typed eth_call response.
def _encode_address_result(addr: str) -> str:
    return "0x" + encode(["address"], [addr]).hex()


# Helper: Market.Props eth_call response (4-tuple of addresses).
def _encode_market_props(
    market_token: str, index_token: str, long_token: str, short_token: str,
) -> str:
    body = encode(
        ["(address,address,address,address)"],
        [(market_token, index_token, long_token, short_token)],
    )
    return "0x" + body.hex()


_ETH_ZERO = "0x0000000000000000000000000000000000000000"


# ──────────────────────────────────────────────────────────────────────────
# check_reader_address_drift
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reader_address_drift_ok_when_match() -> None:
    """GitHub returns the same address as settings → severity=OK."""
    expected = "0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789"
    client = _StubClient()
    client.set_get_response(_StubResponse(json_body={"address": expected}))
    result = await watchdog.check_reader_address_drift(
        expected=expected,
        github_url="https://example.invalid/Reader.json",
        client=client,  # type: ignore[arg-type]
    )
    assert result.severity == "OK"
    assert result.status == "ok"
    assert result.observed == expected
    # Sanity: the HTTP call actually fired with the right URL.
    assert client.gets[0][0] == "https://example.invalid/Reader.json"


@pytest.mark.asyncio
async def test_reader_address_drift_critical_on_mismatch() -> None:
    """GitHub returns a different address → severity=CRITICAL, status=drift."""
    expected = "0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789"
    canonical = "0xDEADBEEFcafEBabE000000000000000000000001"
    client = _StubClient()
    client.set_get_response(_StubResponse(json_body={"address": canonical}))
    result = await watchdog.check_reader_address_drift(
        expected=expected,
        github_url="https://example.invalid/Reader.json",
        client=client,  # type: ignore[arg-type]
    )
    assert result.severity == "CRITICAL"
    assert result.status == "drift"
    assert result.is_drift()
    assert result.observed == canonical
    # Case-insensitive equality: confirm we don't trip on EIP-55 vs lowercase.
    client2 = _StubClient()
    client2.set_get_response(_StubResponse(json_body={"address": expected.lower()}))
    r2 = await watchdog.check_reader_address_drift(
        expected=expected,
        github_url="https://example.invalid/Reader.json",
        client=client2,  # type: ignore[arg-type]
    )
    assert r2.severity == "OK"


@pytest.mark.asyncio
async def test_reader_address_drift_http_error_returns_unreachable() -> None:
    """HTTP failure → severity=ERROR + status=unreachable, NOT OK."""
    client = _StubClient()
    client.set_get_response(exc=httpx.ConnectError("conn refused"))
    result = await watchdog.check_reader_address_drift(
        expected="0xWHATEVER",
        github_url="https://example.invalid/Reader.json",
        client=client,  # type: ignore[arg-type]
    )
    assert result.severity == "ERROR"
    assert result.status == "unreachable"
    assert not result.is_drift()


@pytest.mark.asyncio
async def test_reader_address_drift_bad_json_shape_returns_unreachable() -> None:
    """GitHub returns a JSON body but without an `address` string → ERROR."""
    client = _StubClient()
    client.set_get_response(_StubResponse(json_body={"not_address": "0x123"}))
    result = await watchdog.check_reader_address_drift(
        expected="0xWHATEVER",
        github_url="https://example.invalid/Reader.json",
        client=client,  # type: ignore[arg-type]
    )
    assert result.severity == "ERROR"
    assert result.status == "unreachable"


@pytest.mark.asyncio
async def test_reader_address_drift_non_200_returns_unreachable() -> None:
    """GitHub 503 → severity=ERROR."""
    client = _StubClient()
    client.set_get_response(_StubResponse(status_code=503, json_body={}))
    result = await watchdog.check_reader_address_drift(
        expected="0xWHATEVER",
        github_url="https://example.invalid/Reader.json",
        client=client,  # type: ignore[arg-type]
    )
    assert result.severity == "ERROR"


# ──────────────────────────────────────────────────────────────────────────
# check_markets_alive
# ──────────────────────────────────────────────────────────────────────────


_FAKE_MARKETS = {
    "btc": GMXMarket(
        alias="btc",
        chain="arbitrum",
        market_address="0x47c031236e19d024b42f8AE6780E44A573170703",
        long_collateral_token="0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
        short_collateral_token="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
    ),
    "wsteth_delisted": GMXMarket(
        alias="wsteth_delisted",
        chain="arbitrum",
        market_address="0x0Cf1fb4d1FF67A3D8Ca92c9d6643F8F9be8e03E4",
        long_collateral_token="0x5979D7b546E38E414F7E9822514be443A4800529",
        short_collateral_token="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
    ),
}


@pytest.mark.asyncio
async def test_markets_alive_returns_one_result_per_market_ok_path() -> None:
    """Both markets return non-zero Market.Props → both OK."""
    client = _StubClient()
    # First market response (btc): non-zero indexToken.
    client.push_post_response(_StubResponse(json_body={
        "jsonrpc": "2.0", "id": 1,
        "result": _encode_market_props(
            "0x47c031236e19d024b42f8AE6780E44A573170703",
            "0x47904963fc8b2340414262125af798b9655e58cd",
            "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        ),
    }))
    # Second market response (wsteth_delisted): also alive in this test
    client.push_post_response(_StubResponse(json_body={
        "jsonrpc": "2.0", "id": 1,
        "result": _encode_market_props(
            "0x0Cf1fb4d1FF67A3D8Ca92c9d6643F8F9be8e03E4",
            "0x5979D7b546E38E414F7E9822514be443A4800529",
            "0x5979D7b546E38E414F7E9822514be443A4800529",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        ),
    }))
    results = await watchdog.check_markets_alive(
        markets=_FAKE_MARKETS, client=client,  # type: ignore[arg-type]
    )
    assert len(results) == 2
    assert all(r.severity == "OK" for r in results)
    assert {r.status for r in results} == {"alive"}


@pytest.mark.asyncio
async def test_markets_alive_detects_disabled_zero_struct() -> None:
    """A market that returns zero-struct (delisted pattern) → severity=WARN."""
    client = _StubClient()
    # btc — alive
    client.push_post_response(_StubResponse(json_body={
        "jsonrpc": "2.0", "id": 1,
        "result": _encode_market_props(
            "0x47c031236e19d024b42f8AE6780E44A573170703",
            "0x47904963fc8b2340414262125af798b9655e58cd",
            "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        ),
    }))
    # wsteth_delisted — zero struct (the actual production failure pattern)
    client.push_post_response(_StubResponse(json_body={
        "jsonrpc": "2.0", "id": 1,
        "result": _encode_market_props(_ETH_ZERO, _ETH_ZERO, _ETH_ZERO, _ETH_ZERO),
    }))
    results = await watchdog.check_markets_alive(
        markets=_FAKE_MARKETS, client=client,  # type: ignore[arg-type]
    )
    alive = [r for r in results if r.severity == "OK"]
    disabled = [r for r in results if r.severity == "WARN"]
    assert len(alive) == 1
    assert len(disabled) == 1
    assert disabled[0].check_name == "gmx_market_alive.wsteth_delisted"
    assert disabled[0].status == "disabled"


@pytest.mark.asyncio
async def test_markets_alive_rpc_error_yields_error_severity() -> None:
    """RPC throws → severity=ERROR for that market, others still checked."""
    client = _StubClient()
    # First market: connect error
    client.push_post_response(httpx.ConnectError("conn refused"))
    # Second market: alive
    client.push_post_response(_StubResponse(json_body={
        "jsonrpc": "2.0", "id": 1,
        "result": _encode_market_props(
            "0x0Cf1fb4d1FF67A3D8Ca92c9d6643F8F9be8e03E4",
            "0x5979D7b546E38E414F7E9822514be443A4800529",
            "0x5979D7b546E38E414F7E9822514be443A4800529",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        ),
    }))
    results = await watchdog.check_markets_alive(
        markets=_FAKE_MARKETS, client=client,  # type: ignore[arg-type]
    )
    error_results = [r for r in results if r.severity == "ERROR"]
    ok_results = [r for r in results if r.severity == "OK"]
    assert len(error_results) == 1
    assert len(ok_results) == 1
    assert error_results[0].status == "unreachable"


# ──────────────────────────────────────────────────────────────────────────
# check_hyperlend_oracle_source
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_oracle_source_ok_when_expected() -> None:
    """Oracle.getSourceOfAsset returns expected → severity=OK."""
    expected = "0x40EA33eA76Fbe35e9FB422eDd175b8c8D84A63Cc"
    client = _StubClient()
    client.push_post_response(_StubResponse(json_body={
        "jsonrpc": "2.0", "id": 1,
        "result": _encode_address_result(expected),
    }))
    result = await watchdog.check_hyperlend_oracle_source(
        expected_source=expected,
        client=client,  # type: ignore[arg-type]
    )
    assert result.severity == "OK"
    assert result.observed is not None and result.observed.lower() == expected.lower()


@pytest.mark.asyncio
async def test_oracle_source_drift_critical_when_rotated() -> None:
    """Source rotated → severity=CRITICAL, status=drift."""
    expected = "0x40EA33eA76Fbe35e9FB422eDd175b8c8D84A63Cc"
    # Simulate a governance rotation onto the kHYPE composite source (the
    # specific contamination case from arch_hyperevm_lending_audit.md).
    rotated = "0x6dcFA746f7b11918eF3522c92e6429CA589C3875"
    client = _StubClient()
    client.push_post_response(_StubResponse(json_body={
        "jsonrpc": "2.0", "id": 1,
        "result": _encode_address_result(rotated),
    }))
    result = await watchdog.check_hyperlend_oracle_source(
        expected_source=expected,
        client=client,  # type: ignore[arg-type]
    )
    assert result.severity == "CRITICAL"
    assert result.status == "drift"
    assert result.is_drift()
    assert result.observed is not None and result.observed.lower() == rotated.lower()


@pytest.mark.asyncio
async def test_oracle_source_rpc_error_unreachable() -> None:
    """RPC failure → severity=ERROR, NOT OK."""
    client = _StubClient()
    client.push_post_response(httpx.TimeoutException("timed out"))
    result = await watchdog.check_hyperlend_oracle_source(
        expected_source="0x40EA33eA76Fbe35e9FB422eDd175b8c8D84A63Cc",
        client=client,  # type: ignore[arg-type]
    )
    assert result.severity == "ERROR"
    assert result.status == "unreachable"


@pytest.mark.asyncio
async def test_oracle_source_rpc_returns_revert_unreachable() -> None:
    """`result=0x` (typical revert) → severity=ERROR (decode failure)."""
    client = _StubClient()
    client.push_post_response(_StubResponse(json_body={
        "jsonrpc": "2.0", "id": 1, "result": "0x",
    }))
    result = await watchdog.check_hyperlend_oracle_source(
        expected_source="0x40EA33eA76Fbe35e9FB422eDd175b8c8D84A63Cc",
        client=client,  # type: ignore[arg-type]
    )
    assert result.severity == "ERROR"


# ──────────────────────────────────────────────────────────────────────────
# Aggregation helpers
# ──────────────────────────────────────────────────────────────────────────


def test_has_critical_true_when_any_drift() -> None:
    res = [
        watchdog.WatchdogResult("a", "OK", "ok", None, None, "fine"),
        watchdog.WatchdogResult("b", "CRITICAL", "drift", "x", "y", "drift!"),
        watchdog.WatchdogResult("c", "WARN", "disabled", None, None, "warn"),
    ]
    assert watchdog.has_critical(res) is True


def test_has_critical_false_when_no_drift() -> None:
    res = [
        watchdog.WatchdogResult("a", "OK", "ok", None, None, "fine"),
        watchdog.WatchdogResult("b", "WARN", "disabled", None, None, "warn"),
        watchdog.WatchdogResult("c", "ERROR", "unreachable", None, None, "err"),
    ]
    assert watchdog.has_critical(res) is False


def test_summarize_results_bucketed_correctly() -> None:
    res = [
        watchdog.WatchdogResult("a", "OK", "ok", None, None, ""),
        watchdog.WatchdogResult("b", "OK", "ok", None, None, ""),
        watchdog.WatchdogResult("c", "WARN", "disabled", None, None, ""),
        watchdog.WatchdogResult("d", "CRITICAL", "drift", None, None, ""),
        watchdog.WatchdogResult("e", "ERROR", "unreachable", None, None, ""),
    ]
    counts = watchdog.summarize_results(res)
    assert counts == {"OK": 2, "WARN": 1, "CRITICAL": 1, "ERROR": 1}


@pytest.mark.asyncio
async def test_publish_alert_calls_xadd_with_expected_args() -> None:
    """publish_alert XADDs to the configured stream with maxlen approx."""
    redis_stub = AsyncMock()
    result = watchdog.WatchdogResult(
        check_name="gmx_reader_address_drift",
        severity="CRITICAL",
        status="drift",
        expected="0xAAA",
        observed="0xBBB",
        message="drift!",
    )
    await watchdog.publish_alert(redis_stub, result)
    redis_stub.xadd.assert_awaited_once()
    args, kwargs = redis_stub.xadd.call_args
    # First positional arg: the stream name; second: the fields dict.
    assert args[0] == "trap_alerts:gmx"
    assert args[1]["check_name"] == "gmx_reader_address_drift"
    assert args[1]["severity"] == "CRITICAL"
    assert kwargs.get("approximate") is True


@pytest.mark.asyncio
async def test_publish_alert_swallows_redis_errors() -> None:
    """A Redis outage MUST NOT raise — the cron continues to the next tick."""

    class _BadRedis:
        async def xadd(self, *args: Any, **kwargs: Any) -> None:
            raise ConnectionError("redis down")

    result = watchdog.WatchdogResult(
        check_name="x", severity="WARN", status="x", expected=None, observed=None,
        message="x",
    )
    # Must not raise
    await watchdog.publish_alert(_BadRedis(), result)


def test_results_to_json_roundtrip() -> None:
    """JSON line shape carries every field, in order."""
    import json as _json
    res = [
        watchdog.WatchdogResult("a", "OK", "ok", "x", "x", "m"),
        watchdog.WatchdogResult("b", "CRITICAL", "drift", "x", "y", "m2"),
    ]
    line = watchdog.results_to_json(res)
    parsed = _json.loads(line)
    assert parsed[0]["check_name"] == "a"
    assert parsed[1]["severity"] == "CRITICAL"
    assert parsed[1]["observed"] == "y"
