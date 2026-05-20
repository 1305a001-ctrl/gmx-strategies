"""Binance Futures startup gates (G6.2 — refuse-to-run guardrails).

The audit (`memory/arch_binance_executor_audit.md`) flagged THREE HIGH-
severity findings that must be checked at G6 boot:

  H1 (handled by G6.1: binance_exchange_info.py)
     MIN_NOTIONAL filter must be honored per-symbol or BTC orders silently
     reject -4164. G6.1's `passes_min_notional` helper is the gate.

  H2 (handled by G6.3+: future binance_margin.py)
     `marginType` + `leverage` must be set BEFORE first position per symbol.
     Idempotent setup logic lives in G6.3.

  H3 (handled by THIS module)
     If the account is in hedge mode (`dualSidePosition=true`), every G6
     order without `positionSide` fails -4061 (POSITION_SIDE_NOT_MATCH).
     This is silent until the first order is placed. G6 MUST read
     `/fapi/v1/positionSide/dual` at startup and REFUSE TO RUN if hedge.

This module IS the H3 gate. It's the ONE place in the package that's
allowed to RAISE — that's its job: stop the executor cold rather than
quietly trade into rejection loops.

WIRING:
  This function is DEFINED here but NOT WIRED to any runtime startup
  path in this PR (G6.2). Wiring lands in G6.4 when the order-placement
  executor stands up. The G6.4 PR will call this in the executor's boot
  sequence — between credential load and the per-symbol margin/leverage
  setup loop.

OPERATOR FLIP PROCEDURE:
  If `assert_one_way_position_mode` raises with the hedge-mode message,
  the operator must flip the account back to one-way mode via the
  Binance UI (NOT the API — see README "G6 — Binance auth setup" for
  why the API path is intentionally avoided for this setting). Steps:
    1. Log into Binance Futures UI.
    2. Top-right user icon → Preferences → Position Mode.
    3. Select "One-Way Mode" (NOT "Hedge Mode").
    4. Confirm. Note: position-mode flips are only accepted when there
       are no open positions on the account.
    5. Re-run G6 — `assert_one_way_position_mode` will now pass.

NO TIMING SUBSCRIPTION:
  The gate does NOT poll. The audit's threat model is operator-mistake
  at boot (e.g. the operator manually flipped to hedge mode for some
  one-off experiment and forgot to flip back). A boot-time read is
  sufficient because position-mode is not changed in the hot path —
  G6 itself never POSTs to /positionSide/dual.
"""

from __future__ import annotations

import logging

from gmx_strategies.binance_account import fetch_position_mode

log = logging.getLogger(__name__)


# Exception messages are public; the operator reads them in alerts /
# logs / on-call runbooks. Wording is exact so the recovery procedure
# (UI flip) is unambiguous from the message alone.
_HEDGE_MODE_MESSAGE = (
    "BINANCE: account is in HEDGE mode. Switch to ONE-WAY in the UI "
    "before running G6 executor. Every order without positionSide will "
    "fail -4061."
)
_UNKNOWN_MODE_MESSAGE = (
    "BINANCE: cannot verify position mode — auth issue or API down"
)


async def assert_one_way_position_mode() -> None:
    """Boot-time guardrail: refuse to run if account is in HEDGE mode.

    Calls `fetch_position_mode()` via the binance_auth signed_get path.
    Behavior:
      - Returns `True` (HEDGE) → raise `RuntimeError(_HEDGE_MODE_MESSAGE)`.
      - Returns `None`  (unknown — auth gap, network, malformed) →
        raise `RuntimeError(_UNKNOWN_MODE_MESSAGE)`.
      - Returns `False` (ONE-WAY) → log + pass silently.

    Raises:
        RuntimeError: with one of the two messages above when the mode
            cannot be confirmed as one-way.
    """
    mode = await fetch_position_mode()
    if mode is True:
        # Hedge mode — every G6 order would reject -4061. HARD STOP.
        log.error("binance_startup_check.hedge_mode_detected")
        raise RuntimeError(_HEDGE_MODE_MESSAGE)
    if mode is None:
        # Unknown — auth issue, network, malformed response. HARD STOP
        # so we don't trade blind. This is the "fail-loud rather than
        # fail-quiet" principle from the audit.
        log.error("binance_startup_check.position_mode_unknown")
        raise RuntimeError(_UNKNOWN_MODE_MESSAGE)
    # False — one-way mode. Confirmed safe.
    log.info("binance_startup_check.position_mode_ok mode=one-way")


__all__ = [
    "assert_one_way_position_mode",
]
