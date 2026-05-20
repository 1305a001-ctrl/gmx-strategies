"""Binance USDT-M Futures read-only account state (G6.2 — CEX hedge leg).

Signed-endpoint readers for the account-state surface G6.3+ will rely on:
  - position mode (one-way vs hedge) — startup gate, see binance_startup_check.py
  - account balances (per-asset, USDT free margin)
  - position information per-symbol (entry, mark, unrealized PnL, side)

All functions are READ-ONLY. No POST. No order placement. No margin/leverage
changes. Those land in G6.3+ once testnet shakedown passes (per the audit).

Pattern (matches binance_funding.py + binance_exchange_info.py):
  - Hand-rolled httpx via the binance_auth.signed_get wrapper.
  - Best-effort error handling — return None on ANY failure, never raise.
  - Caller treats None as "auth issue or API down" and decides.

Required scopes on the API key (per the audit §1):
  - `enableReading` (always granted)
  - `enableFutures`
That's it. These reads do NOT require `enableWithdrawals` or
`enableSpotAndMarginTrading`. The README "G6 — Binance auth setup" section
walks through key creation with the minimum-scope guardrail.

Endpoints (verified 2026-05-20 against
https://developers.binance.com/docs/derivatives/usds-margined-futures):
  - `GET /fapi/v1/positionSide/dual` — returns `{"dualSidePosition": bool}`.
    True = hedge mode (separate LONG + SHORT books per symbol). False =
    one-way (one net position per symbol). G6 REFUSES TO RUN IF TRUE
    (audit H3). The refusal logic lives in binance_startup_check.py;
    this module just READS.
  - `GET /fapi/v2/balance` — returns a list of per-asset balance dicts.
    We use `availableBalance` for sizing decisions (free margin available
    to open new positions). Weight 5.
  - `GET /fapi/v2/positionRisk` — returns a list of per-symbol position
    dicts. Filterable by `?symbol=BTCUSDT`. `positionAmt > 0` long,
    `< 0` short, `== 0` flat. Weight 5.

Decimal handling:
  Binance returns numeric fields as STRINGS (`"123.45"`). We coerce with
  float() at the convenience-helper boundary (`fetch_usdt_free_margin`).
  The list-returning helpers preserve the raw shape so callers can do
  Decimal arithmetic if they need it for order sizing — float is safe
  for read-only display + threshold comparisons; not safe for the
  step-size rounding (that lives in binance_exchange_info.py).
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from gmx_strategies.binance_auth import signed_get

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────
# Position mode (one-way vs hedge) — audit H3
# ──────────────────────────────────────────────────────────────────────────


async def fetch_position_mode(
    client: httpx.AsyncClient | None = None,
) -> bool | None:
    """Read the account's position-mode setting.

    Returns:
        True   — account is in HEDGE mode (`dualSidePosition=true`).
                 G6 MUST refuse to run in this state; every order without
                 `positionSide` fails -4061 (POSITION_SIDE_NOT_MATCH).
        False  — account is in ONE-WAY mode. G6 can proceed.
        None   — read failed (auth issue, API down, malformed response).
                 Caller treats as "unknown — don't proceed".

    The refusal logic itself lives in `binance_startup_check.py` —
    this is a pure read.

    Endpoint: GET /fapi/v1/positionSide/dual (signed, weight 30).
    """
    body = await signed_get("/fapi/v1/positionSide/dual", {}, client=client)
    if not isinstance(body, dict):
        log.warning(
            "binance_account.position_mode.bad_shape body_type=%s",
            type(body).__name__ if body is not None else "None",
        )
        return None
    raw = body.get("dualSidePosition")
    if not isinstance(raw, bool):
        log.warning("binance_account.position_mode.bad_field raw=%r", raw)
        return None
    log.info(
        "binance_account.position_mode mode=%s",
        "hedge" if raw else "one-way",
    )
    return raw


# ──────────────────────────────────────────────────────────────────────────
# Account balances
# ──────────────────────────────────────────────────────────────────────────


async def fetch_account_balance(
    client: httpx.AsyncClient | None = None,
) -> list[dict[str, Any]] | None:
    """Read per-asset Futures balances.

    Returns the full list of asset balance dicts (USDT, BUSD, etc.) on
    success, None on failure. The list shape matches Binance's response
    format — per-asset dict with `asset`, `balance`, `availableBalance`,
    `crossWalletBalance`, `crossUnPnl`, `maxWithdrawAmount`, etc.

    Endpoint: GET /fapi/v2/balance (signed, weight 5).
    """
    body = await signed_get("/fapi/v2/balance", {}, client=client)
    if not isinstance(body, list):
        log.warning(
            "binance_account.balance.bad_shape body_type=%s",
            type(body).__name__ if body is not None else "None",
        )
        return None
    # Sanity: each entry should be a dict with at least an `asset` key.
    # We don't drop bad entries — pass through to the caller and let
    # `fetch_usdt_free_margin` filter; surfacing the full payload at this
    # layer keeps the function reusable for other assets (BNB, BUSD).
    log.info("binance_account.balance.read_ok n_assets=%d", len(body))
    return body


async def fetch_usdt_free_margin(
    client: httpx.AsyncClient | None = None,
) -> float | None:
    """Convenience: pull USDT `availableBalance` as a float.

    For G6's sizing-pre-order check: `availableBalance` is the free margin
    available to open NEW positions. Subtracts margin already committed
    to open positions but ADDS unrealized PnL (audit M2: a -$5 uPnL
    reduces availableBalance by $5; callers should hold back buffer).

    Returns None on:
      - Auth failure (propagated from `fetch_account_balance`).
      - No `USDT` entry in the balance list.
      - `availableBalance` field missing or non-numeric.
    """
    balances = await fetch_account_balance(client=client)
    if balances is None:
        return None
    for entry in balances:
        if not isinstance(entry, dict):
            continue
        if entry.get("asset") != "USDT":
            continue
        raw = entry.get("availableBalance")
        if not isinstance(raw, (str, int, float)):
            log.warning(
                "binance_account.usdt_free_margin.bad_field raw_type=%s",
                type(raw).__name__,
            )
            return None
        try:
            value = float(raw)
        except (ValueError, TypeError):
            log.warning("binance_account.usdt_free_margin.bad_value raw=%r", raw)
            return None
        # NaN / inf guard.
        if value != value or value in (float("inf"), float("-inf")):
            log.warning("binance_account.usdt_free_margin.nonfinite raw=%r", raw)
            return None
        log.info("binance_account.usdt_free_margin available=%.4f", value)
        return value
    log.warning("binance_account.usdt_free_margin.no_usdt_entry")
    return None


# ──────────────────────────────────────────────────────────────────────────
# Position information
# ──────────────────────────────────────────────────────────────────────────


async def fetch_position_information(
    symbol: str | None = None,
    *,
    client: httpx.AsyncClient | None = None,
) -> list[dict[str, Any]] | None:
    """Read open-position information.

    When `symbol` is None: returns the full list of positions across the
    account (one entry per symbol/positionSide combination — in one-way
    mode that's one per active symbol; in hedge mode that's two per
    active symbol).

    When `symbol` is provided: filters to that one symbol via the
    `?symbol=` query param (server-side filter — cheaper than fetching all
    and filtering locally).

    Each entry includes (audit §5):
      - `symbol` (str), `positionAmt` (str, signed), `entryPrice` (str),
        `markPrice` (str), `unRealizedProfit` (str), `liquidationPrice` (str),
        `leverage` (str), `marginType` (str, lowercase: "isolated"|"cross"),
        `isolatedMargin` (str), `positionSide` (str: "BOTH"|"LONG"|"SHORT"),
        `notional` (str), `updateTime` (int ms).

    Returns None on auth/HTTP/parse failure. Returns `[]` (empty list)
    if the account simply has no open positions — that's a SUCCESSFUL
    read, not a failure.

    Endpoint: GET /fapi/v2/positionRisk (signed, weight 5).
    """
    params: dict[str, str | int | float] = {}
    if symbol is not None:
        params["symbol"] = symbol
    body = await signed_get("/fapi/v2/positionRisk", params, client=client)
    if not isinstance(body, list):
        log.warning(
            "binance_account.position_info.bad_shape body_type=%s symbol=%s",
            type(body).__name__ if body is not None else "None",
            symbol,
        )
        return None
    log.info(
        "binance_account.position_info.read_ok n_positions=%d symbol=%s",
        len(body), symbol or "<all>",
    )
    return body


__all__ = [
    "fetch_account_balance",
    "fetch_position_information",
    "fetch_position_mode",
    "fetch_usdt_free_margin",
]
