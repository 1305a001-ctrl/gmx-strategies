"""Build OracleReport tuples from the chainlink-streams Redis feed.

GMX V2's executeLiquidation contract REQUIRES fresh oracle price reports
inline with the tx. The chainlink-streams Go service publishes signed
Chainlink Data Streams reports to Redis at `chainlink:<alias>:reports`
(stream) and `chainlink:<alias>:latest` (key with the most recent JSON
payload containing report_blob).

This module produces the OracleReport tuple expected by tx_builder, given
a list of asset aliases involved in a liquidation. Staleness gate: any
report older than `max_age_sec` (default 30s) is REFUSED — keepers that
submit stale oracle reports get reverted by the contract.

For a given (chain, market_alias, is_long) liquidation, the required
reports are: index_asset price + collateral_token price (when distinct).
"""
from __future__ import annotations

import json
import logging
import time

from gmx_strategies.markets import market_for
from gmx_strategies.redis_client import r
from gmx_strategies.tx_builder import OracleReport

log = logging.getLogger(__name__)


# Chainlink Verifier Proxy addresses per chain. Required as the `provider`
# field in OracleReport — the contract uses this to validate report origin.
# Source: https://docs.chain.link/data-streams/crypto-streams
VERIFIER_PROXY_BY_CHAIN: dict[str, str] = {
    "arbitrum":  "0x478Aa2aC9F6D65F84e09D9185d126c3a17c2a93C",
    "avalanche": "0x79BAa790f2A45A552c0C1Be8C3a26d0CCb3a1bA1",
}


DEFAULT_REPORT_MAX_AGE_SEC = 30.0


def _decode_blob_from_payload(payload: str | None) -> tuple[bytes, float] | None:
    """Pure: decode `{report_blob, ts_unix}` from a chainlink:<alias>:latest payload.

    Returns (blob_bytes, ts_unix) or None on parse failure / missing fields.
    """
    if not payload:
        return None
    try:
        d = json.loads(payload)
    except (TypeError, ValueError):
        return None
    if not isinstance(d, dict):
        return None
    blob_hex = d.get("report_blob") or d.get("blob")
    if not isinstance(blob_hex, str) or not blob_hex:
        return None
    blob_hex = blob_hex[2:] if blob_hex.startswith("0x") else blob_hex
    try:
        blob = bytes.fromhex(blob_hex)
    except ValueError:
        return None
    if not blob:
        return None
    ts_raw = d.get("ts_unix") or d.get("timestamp") or d.get("observation_ts")
    try:
        ts = float(ts_raw) if ts_raw is not None else 0.0
    except (TypeError, ValueError):
        ts = 0.0
    return blob, ts


def alias_token_address(chain: str, alias: str) -> str:
    """Pure: the token address for an index alias on a given chain.

    For GMX's OracleUtils, the report's `token` field is the underlying
    asset's contract address. We pull from the market's long-collateral
    field as the canonical address for that alias.
    """
    m = market_for(chain, alias)
    return m.long_collateral_token if m else ""


def required_aliases_for(
    chain: str, market_alias: str, is_long: bool,
) -> tuple[str, ...]:
    """Pure: which aliases need fresh oracle reports for this liquidation.

    GMX V2 needs the INDEX price (the market asset) and the COLLATERAL
    price (USDC for short side; same as index for long-collateral cases).
    """
    m = market_for(chain, market_alias)
    if m is None:
        return ()
    aliases: list[str] = [market_alias]
    # Short positions use USDC collateral → need USDC price too
    if not is_long:
        if m.short_collateral_token != m.long_collateral_token:
            aliases.append("usdc")
    return tuple(aliases)


async def fetch_oracle_reports(
    *,
    chain: str,
    market_alias: str,
    is_long: bool,
    max_age_sec: float = DEFAULT_REPORT_MAX_AGE_SEC,
) -> tuple[OracleReport, ...]:
    """Async: build the OracleReport tuple needed for liquidating one
    (chain, market_alias, is_long) position.

    Returns empty tuple if ANY required report is missing or stale —
    caller refuses the live-fire on empty tuple (execute_live Gate 4).
    """
    provider = VERIFIER_PROXY_BY_CHAIN.get(chain, "")
    if not provider:
        log.warning("oracle_reports.no_verifier_proxy chain=%s", chain)
        return ()

    aliases = required_aliases_for(chain, market_alias, is_long)
    if not aliases:
        log.warning(
            "oracle_reports.no_market chain=%s alias=%s",
            chain, market_alias,
        )
        return ()

    now = time.time()
    reports: list[OracleReport] = []
    for alias in aliases:
        try:
            payload = await r().get(f"chainlink:{alias}:latest")
        except Exception as e:
            log.warning("oracle_reports.redis_failed alias=%s err=%s", alias, e)
            return ()
        decoded = _decode_blob_from_payload(payload)
        if decoded is None:
            log.warning(
                "oracle_reports.decode_failed alias=%s", alias,
            )
            return ()
        blob, ts = decoded
        if ts <= 0 or (now - ts) > max_age_sec:
            log.warning(
                "oracle_reports.stale alias=%s age_sec=%.1f max=%.1f",
                alias, now - ts, max_age_sec,
            )
            return ()
        token_addr = alias_token_address(chain, alias)
        if not token_addr:
            log.warning("oracle_reports.no_token_addr alias=%s", alias)
            return ()
        reports.append(OracleReport(
            token=token_addr,
            provider=provider,
            data=blob,
        ))

    return tuple(reports)


__all__ = [
    "VERIFIER_PROXY_BY_CHAIN",
    "DEFAULT_REPORT_MAX_AGE_SEC",
    "alias_token_address",
    "required_aliases_for",
    "fetch_oracle_reports",
]
