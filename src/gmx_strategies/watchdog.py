"""Trap-surface watchdog — drift detection for external state we depend on.

The funding-arb runtime reads from THREE external surfaces that can change
out from under us without warning:

  1. GMX V2 Reader contract on Arbitrum — redeployed at least once during
     this project's life (memory/arch_gmx_v2_audit.md addendum 2026-05-20).
     A silently-stale Reader address means every live call hits an orphaned
     contract that decodes wrong → garbage funding rates → bad signals.

  2. GMX V2 markets being disabled / delisted — `wsteth` was a live market
     until GMX delisted it (Reader.getMarket() now returns zero-struct).
     The runtime handles None gracefully but the operator should KNOW.

  3. HyperLend Oracle source for WHYPE — Aave-V3-style oracles expose
     `setSourceOfAsset` (governance-callable). A rotation away from the
     audited source contract would silently flip OCDE's HYPE divergence
     loop onto a different feed (composite kHYPE vs single-source HYPE
     would contaminate the divergence signal with staking-ratio drift).

This module is PURE, async, read-only, and uses ONLY pinned deps
(httpx + web3 + redis). It is invoked from `gmx_strategies.cli watchdog`
on a cron — NOT from the funding-arb runtime hot path. Each check returns
a `WatchdogResult` regardless of pass/fail; the CLI aggregates and
optionally publishes drift findings to a Redis stream.

Adding a new check:
  - Write `async def check_<thing>(*, ...) -> WatchdogResult` or
    `list[WatchdogResult]` for fan-out checks.
  - Register it in `cli.run_all_checks()`.
  - Add a unit test in `tests/test_watchdog.py` that mocks the network call.

Severity guide:
  - CRITICAL: the runtime is reading a wrong source RIGHT NOW. Operator
    should halt the strategy + redeploy with corrected settings.
  - WARN: a configured-but-currently-skipped path (e.g. disabled market).
    No bleed, but a future market re-enable would silently un-skip.
  - OK: drift check passed — observed == expected.
  - ERROR: the watchdog itself failed (HTTP/decode). Operator should
    investigate the watchdog, not conclude the production source is fine.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Literal

import httpx
from eth_abi import decode, encode  # type: ignore[attr-defined]
from web3 import Web3

from gmx_strategies.markets import GMXMarket
from gmx_strategies.settings import settings

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────
# Public result shape
# ──────────────────────────────────────────────────────────────────────────


Severity = Literal["CRITICAL", "WARN", "OK", "ERROR"]


@dataclass(frozen=True)
class WatchdogResult:
    """Outcome of one watchdog check.

    Frozen so a downstream alerting layer can hash/dedupe by (check_name,
    severity, expected, observed) without worrying about mutation.

    Attributes:
        check_name: stable identifier, e.g. "gmx_reader_address_drift".
        severity:   "CRITICAL" | "WARN" | "OK" | "ERROR".
        status:     "drift" | "alive" | "disabled" | "unreachable" | "ok".
        expected:   the stringified expected value (or `None` if N/A).
        observed:   the stringified observed value (or `None` on lookup fail).
        message:    human-readable one-liner suitable for logs / alerts.
    """

    check_name: str
    severity: Severity
    status: str
    expected: str | None
    observed: str | None
    message: str

    def is_drift(self) -> bool:
        """Convenience: True when this is a CRITICAL drift result."""
        return self.severity == "CRITICAL"


# ──────────────────────────────────────────────────────────────────────────
# Internal HTTP / RPC helpers (best-effort, never raise)
# ──────────────────────────────────────────────────────────────────────────


async def _http_get_json(
    url: str, *, timeout_s: float, client: httpx.AsyncClient | None = None,
) -> Any | None:
    """Anonymous HTTPS GET → parsed JSON or None on any failure.

    Used for the GitHub raw fetch of the GMX Reader address. Best-effort:
    HTTP error, non-200, malformed JSON all return None and log WARN.
    """
    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=timeout_s)
    try:
        try:
            resp = await client.get(url)
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            log.warning("watchdog.http_error url=%s err=%s", url, exc)
            return None
        if resp.status_code != 200:
            log.warning(
                "watchdog.http_bad_status url=%s status=%d", url, resp.status_code,
            )
            return None
        try:
            return resp.json()
        except (ValueError, TypeError):
            log.warning("watchdog.http_bad_json url=%s", url)
            return None
    finally:
        if owns_client:
            await client.aclose()


async def _eth_call(
    *,
    rpc_url: str,
    to_address: str,
    data: str,
    timeout_s: float,
    client: httpx.AsyncClient | None = None,
) -> str | None:
    """Best-effort eth_call. Returns the hex result or None on any failure.

    Mirrors the pattern in `gmx_reader._eth_call` so future readers of this
    file can spot the deliberate similarity (and refactor into a shared
    helper if a fourth user emerges).
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [{"to": to_address, "data": data}, "latest"],
    }
    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=timeout_s)
    try:
        try:
            resp = await client.post(rpc_url, json=payload)
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            log.warning("watchdog.rpc_http_error rpc=%s err=%s", rpc_url, exc)
            return None
        if resp.status_code != 200:
            log.warning(
                "watchdog.rpc_bad_status rpc=%s status=%d", rpc_url, resp.status_code,
            )
            return None
        try:
            body = resp.json()
        except (ValueError, TypeError):
            log.warning("watchdog.rpc_bad_json rpc=%s", rpc_url)
            return None
        if not isinstance(body, dict):
            log.warning("watchdog.rpc_bad_body_shape rpc=%s", rpc_url)
            return None
        if "error" in body:
            log.warning("watchdog.rpc_error rpc=%s err=%s", rpc_url, body["error"])
            return None
        result = body.get("result")
        if not isinstance(result, str):
            log.warning("watchdog.rpc_missing_result rpc=%s", rpc_url)
            return None
        return result
    finally:
        if owns_client:
            await client.aclose()


def _address_eq(a: str | None, b: str | None) -> bool:
    """Case-insensitive Ethereum address equality. None never equals."""
    if a is None or b is None:
        return False
    return a.lower() == b.lower()


def _decode_address(result_hex: str) -> str | None:
    """Decode a single-address eth_call return blob.

    Aave-V3 Oracle.getSourceOfAsset returns a single `address` (right-padded
    to 32 bytes by ABI rules). We slice the last 20 bytes and EIP-55 it
    via Web3.to_checksum_address.
    """
    if not isinstance(result_hex, str) or not result_hex.startswith("0x"):
        return None
    try:
        body = bytes.fromhex(result_hex[2:])
    except ValueError:
        return None
    if len(body) != 32:
        return None
    try:
        (addr,) = decode(["address"], body)
        return Web3.to_checksum_address(addr)
    except Exception as exc:  # noqa: BLE001
        log.warning("watchdog.decode_address_failed err=%s", exc)
        return None


# Precomputed selectors used by the on-chain checks.
_SEL_GET_SOURCE_OF_ASSET = "0x" + Web3.keccak(text="getSourceOfAsset(address)")[:4].hex()
_SEL_GET_MARKET = "0x" + Web3.keccak(text="getMarket(address,address)")[:4].hex()


# ──────────────────────────────────────────────────────────────────────────
# Check 1: GMX V2 Reader redeploy
# ──────────────────────────────────────────────────────────────────────────


async def check_reader_address_drift(
    *,
    expected: str | None = None,
    github_url: str | None = None,
    timeout_s: float | None = None,
    client: httpx.AsyncClient | None = None,
) -> WatchdogResult:
    """Pull the canonical Reader address from gmx-synthetics and compare.

    The check is CRITICAL — a stale Reader address means every live call
    in the funding-arb runtime hits an orphaned contract that decodes wrong.
    We've already eaten one such redeploy mid-project (2026-05-18 →
    2026-05-20); this guards against the next one.

    Returns WatchdogResult with:
      - severity=OK + status="ok" when expected == observed (case-insensitive)
      - severity=CRITICAL + status="drift" when they differ
      - severity=ERROR + status="unreachable" when GitHub can't be reached
        or the JSON shape is wrong (we cannot say "no drift" if we never
        actually checked)
    """
    expected = expected or settings.gmx_reader_address_arbitrum
    github_url = github_url or settings.gmx_reader_github_url
    timeout_s = timeout_s or settings.watchdog_http_timeout_s

    body = await _http_get_json(github_url, timeout_s=timeout_s, client=client)
    if not isinstance(body, dict):
        return WatchdogResult(
            check_name="gmx_reader_address_drift",
            severity="ERROR",
            status="unreachable",
            expected=expected,
            observed=None,
            message=(
                f"Could not fetch Reader address from {github_url}; cannot say "
                "whether drift has occurred. Investigate the watchdog."
            ),
        )
    observed = body.get("address")
    if not isinstance(observed, str):
        return WatchdogResult(
            check_name="gmx_reader_address_drift",
            severity="ERROR",
            status="unreachable",
            expected=expected,
            observed=None,
            message=(
                f"Reader.json at {github_url} did not contain a string `address` "
                "field; cannot say whether drift has occurred."
            ),
        )

    if _address_eq(expected, observed):
        return WatchdogResult(
            check_name="gmx_reader_address_drift",
            severity="OK",
            status="ok",
            expected=expected,
            observed=observed,
            message="GMX Reader address matches gmx-synthetics canonical.",
        )
    return WatchdogResult(
        check_name="gmx_reader_address_drift",
        severity="CRITICAL",
        status="drift",
        expected=expected,
        observed=observed,
        message=(
            f"GMX Reader address drift: settings={expected} but "
            f"gmx-synthetics canonical={observed}. Update "
            "settings.gmx_reader_address_arbitrum and redeploy."
        ),
    )


# ──────────────────────────────────────────────────────────────────────────
# Check 2: GMX V2 market disabled / delisted
# ──────────────────────────────────────────────────────────────────────────


_ETH_ZERO = "0x0000000000000000000000000000000000000000"


async def check_markets_alive(
    *,
    markets: dict[str, GMXMarket],
    reader: str | None = None,
    datastore: str | None = None,
    rpc_url: str | None = None,
    timeout_s: float | None = None,
    client: httpx.AsyncClient | None = None,
) -> list[WatchdogResult]:
    """For each market, call Reader.getMarket(DataStore, marketAddress).

    Returns one WatchdogResult per market. Three outcomes per market:
      - severity=OK + status="alive": getMarket returned non-zero Market.Props
        (indexToken != 0x0).
      - severity=WARN + status="disabled": getMarket returned a zero-struct
        (the wsteth pattern — market has been delisted by GMX).
      - severity=ERROR + status="unreachable": RPC failure / decode error.

    We don't also call getMarketInfo here — that's a heavier call (needs
    MarketPrices) and the cheap getMarket call already catches the
    delist case. If we want to catch transient `isDisabled=true` (without a
    delist) we'd need MarketPrices; punt for now and add in a follow-up.
    """
    reader = reader or settings.gmx_reader_address_arbitrum
    datastore = datastore or settings.gmx_datastore_address_arbitrum
    rpc_url = rpc_url or settings.arbitrum_rpc_url
    timeout_s = timeout_s or settings.watchdog_http_timeout_s

    results: list[WatchdogResult] = []
    # Share one client across all market checks to amortize TLS handshake.
    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=timeout_s)
    try:
        for alias, market in markets.items():
            data = _SEL_GET_MARKET + encode(
                ["address", "address"], [datastore, market.market_address],
            ).hex()
            result_hex = await _eth_call(
                rpc_url=rpc_url,
                to_address=reader,
                data=data,
                timeout_s=timeout_s,
                client=client,
            )
            if result_hex is None:
                results.append(WatchdogResult(
                    check_name=f"gmx_market_alive.{alias}",
                    severity="ERROR",
                    status="unreachable",
                    expected=market.market_address,
                    observed=None,
                    message=(
                        f"RPC call Reader.getMarket failed for market={alias}; "
                        "cannot verify alive."
                    ),
                ))
                continue
            try:
                body = bytes.fromhex(result_hex[2:])
                (market_props,) = decode(
                    ["(address,address,address,address)"], body,
                )
                _market_token, index_token, _long_token, _short_token = market_props
            except Exception as exc:  # noqa: BLE001
                results.append(WatchdogResult(
                    check_name=f"gmx_market_alive.{alias}",
                    severity="ERROR",
                    status="unreachable",
                    expected=market.market_address,
                    observed=None,
                    message=(
                        f"Could not decode Reader.getMarket response for "
                        f"market={alias}: {exc}"
                    ),
                ))
                continue
            if _address_eq(index_token, _ETH_ZERO):
                # Delist pattern — wsteth before we removed it.
                results.append(WatchdogResult(
                    check_name=f"gmx_market_alive.{alias}",
                    severity="WARN",
                    status="disabled",
                    expected=market.market_address,
                    observed=_ETH_ZERO,
                    message=(
                        f"GMX market={alias} returned zero-struct from "
                        "Reader.getMarket — likely delisted. Remove from "
                        "ARBITRUM_MARKETS or expect silent skips."
                    ),
                ))
                continue
            results.append(WatchdogResult(
                check_name=f"gmx_market_alive.{alias}",
                severity="OK",
                status="alive",
                expected=market.market_address,
                observed=index_token,
                message=f"GMX market={alias} alive (indexToken={index_token}).",
            ))
    finally:
        if owns_client:
            await client.aclose()
    return results


# ──────────────────────────────────────────────────────────────────────────
# Check 3: HyperLend Oracle source rotation
# ──────────────────────────────────────────────────────────────────────────


async def check_hyperlend_oracle_source(
    *,
    expected_source: str | None = None,
    oracle: str | None = None,
    whype_token: str | None = None,
    rpc_url: str | None = None,
    timeout_s: float | None = None,
    client: httpx.AsyncClient | None = None,
) -> WatchdogResult:
    """Verify HyperLend's WHYPE price source is still the expected RedStone feed.

    HyperLend uses the Aave-V3 Oracle pattern: `getSourceOfAsset(token)`
    returns the per-token aggregator address. Governance can rotate via
    `setSourceOfAsset`. A silent rotation onto e.g. the kHYPE composite
    source would contaminate OCDE's HYPE-divergence signal with the
    kHYPE-vs-HYPE staking ratio drift (memory/arch_hyperevm_lending_audit.md).

    Returns WatchdogResult with:
      - severity=OK + status="ok" when observed == expected
      - severity=CRITICAL + status="drift" when the source has rotated
      - severity=ERROR + status="unreachable" when the RPC fails
    """
    expected_source = expected_source or settings.expected_hyperlend_whype_source
    oracle = oracle or settings.hyperlend_oracle_address
    whype_token = whype_token or settings.hyperlend_whype_token
    rpc_url = rpc_url or settings.hyperevm_rpc_url
    timeout_s = timeout_s or settings.watchdog_http_timeout_s

    data = _SEL_GET_SOURCE_OF_ASSET + encode(["address"], [whype_token]).hex()
    result_hex = await _eth_call(
        rpc_url=rpc_url,
        to_address=oracle,
        data=data,
        timeout_s=timeout_s,
        client=client,
    )
    if result_hex is None:
        return WatchdogResult(
            check_name="hyperlend_whype_source_drift",
            severity="ERROR",
            status="unreachable",
            expected=expected_source,
            observed=None,
            message=(
                "RPC call Oracle.getSourceOfAsset(WHYPE) failed; cannot verify "
                "source. Investigate the watchdog."
            ),
        )
    observed = _decode_address(result_hex)
    if observed is None:
        return WatchdogResult(
            check_name="hyperlend_whype_source_drift",
            severity="ERROR",
            status="unreachable",
            expected=expected_source,
            observed=None,
            message=(
                f"Could not decode address from Oracle.getSourceOfAsset(WHYPE) "
                f"result={result_hex}"
            ),
        )
    if _address_eq(expected_source, observed):
        return WatchdogResult(
            check_name="hyperlend_whype_source_drift",
            severity="OK",
            status="ok",
            expected=expected_source,
            observed=observed,
            message="HyperLend WHYPE source matches expected RedStone feed.",
        )
    return WatchdogResult(
        check_name="hyperlend_whype_source_drift",
        severity="CRITICAL",
        status="drift",
        expected=expected_source,
        observed=observed,
        message=(
            f"HyperLend WHYPE price source has rotated: expected={expected_source} "
            f"but observed={observed}. OCDE divergence signal is now reading the "
            "wrong feed — update settings.expected_hyperlend_whype_source + "
            "ocde settings.redstone_hype_source after manual verification."
        ),
    )


# ──────────────────────────────────────────────────────────────────────────
# Redis publish helper (used by cli.py)
# ──────────────────────────────────────────────────────────────────────────


async def publish_alert(redis: Any, result: WatchdogResult) -> None:
    """XADD one result to settings.trap_alerts_stream with maxlen approx.

    `redis` must be the package's async client (decode_responses=True).
    Failures are logged and swallowed — alerting is best-effort; we do not
    want a Redis outage to crash the watchdog and miss the next cron tick.
    """
    fields: dict[str, str] = {
        "check_name": result.check_name,
        "severity": result.severity,
        "status": result.status,
        "expected": "" if result.expected is None else result.expected,
        "observed": "" if result.observed is None else result.observed,
        "message": result.message,
    }
    try:
        await redis.xadd(
            settings.trap_alerts_stream,
            fields,
            maxlen=settings.trap_alerts_maxlen,
            approximate=True,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "watchdog.publish_failed stream=%s err=%s",
            settings.trap_alerts_stream, exc,
        )


def summarize_results(results: list[WatchdogResult]) -> dict[str, int]:
    """Bucket-count results by severity for one-line stdout summary.

    Returns a dict with keys CRITICAL, WARN, OK, ERROR — missing buckets
    are zero so callers can format without KeyError.
    """
    counts = {"CRITICAL": 0, "WARN": 0, "OK": 0, "ERROR": 0}
    for r in results:
        counts[r.severity] = counts.get(r.severity, 0) + 1
    return counts


def has_critical(results: list[WatchdogResult]) -> bool:
    """True iff any result has severity=CRITICAL (used for CLI exit code)."""
    return any(r.severity == "CRITICAL" for r in results)


__all__ = [
    "Severity",
    "WatchdogResult",
    "check_hyperlend_oracle_source",
    "check_markets_alive",
    "check_reader_address_drift",
    "has_critical",
    "publish_alert",
    "summarize_results",
]


def _to_dict_for_export(r: WatchdogResult) -> dict[str, Any]:
    """JSON-friendly dict — convenience for cli.py logs / future telemetry."""
    return {
        "check_name": r.check_name,
        "severity": r.severity,
        "status": r.status,
        "expected": r.expected,
        "observed": r.observed,
        "message": r.message,
    }


def results_to_json(results: list[WatchdogResult]) -> str:
    """Serialize a list of results to a single JSON line. Used by the CLI."""
    return json.dumps([_to_dict_for_export(r) for r in results])
