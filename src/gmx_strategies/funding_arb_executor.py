"""G7.1 — funding-arb consumer (signal → executor).

The runtime module that closes the loop between the funding-arb signal
pipeline (G2/G3, `funding_arb_runtime.py`) and the executor stack (G5.x +
G6.x), gated by G7.3 `PilotGuard`. After this module lands, every piece
of the GMX-lane build exists and is wired — but consumer-disabled by
default, dry-run-on, default-deny.

WHAT THIS DOES (the per-signal pipeline — see `handle_signal`):

  1. Receive a JSON-decoded signal dict from `funding_arb:signals`
     (pub/sub). Shape per `funding_arb_runtime._signal_payload`:
       { ts, market, direction, funding_rate_per_8h, annualized_yield_pct,
         target_position_usd, cex_rate_per_8h, net_rate_per_8h,
         cex_source, mode }
  2. Call `PilotGuard.check(market, notional_usd)`. If denied: log,
     XADD an ExecutionRecord-with-guard_block to `funding_arb:executions`
     so the audit trail is complete + the daily_pnl gate sees it, then
     return. The guard already XADDs to `funding_arb:guard_blocks` on
     check failure — we don't duplicate.
  3. Fetch the executor's current GMX positions
     (`gmx_position_reader.fetch_account_positions`). A read failure
     returns an empty list — we fall through to step 4 with no known
     positions (reconcile will PROCEED since there's nothing to merge
     against).
  4. Build an `OrderIntent` for the GMX leg from the signal direction:
       - "short_gmx_long_cex" → GMX is_long=False; Binance side="BUY"
       - "long_gmx_short_cex" → GMX is_long=True;  Binance side="SELL"
     Collateral defaults to USDC (the short-collateral token from
     `markets.ARBITRUM_MARKETS[<alias>]`) — both legs use stable
     collateral so funding-arb PnL doesn't drift on index-token vol.
  5. Reconcile: `reconcile_intent(intent, positions)`. ABORT → log,
     XADD ExecutionRecord-with-reconcile_block, return.
  6. PARALLEL via `asyncio.gather`:
       - GMX leg: `sign_order` → `submit_signed(dry_run=...)`
       - Binance leg: `place_market_order(dry_run=...)`
     Each respects its own gate stack (G5.2 / G6.4) — we pass
     `dry_run=settings.funding_arb_executor_dry_run` so the operator
     can override per-process.
  7. Collect both OrderResults. Build an ExecutionRecord. XADD it to
     `funding_arb:executions`. The PilotGuard.G4 daily_pnl gate reads
     from this stream — every signal MUST land here, even denied ones,
     so the audit trail is complete.
  8. If either leg returns a loss-shaped error, call
     `pilot_guard.record_loss(now_ms)` so G6 cooldown kicks in.

THE 5-GATE LADDER (consumer → broadcast):

  Required for ANY broadcast (all five must be True):
    1. `funding_arb_consumer_enabled` — hard gate ABOVE the per-venue
       live_*_enabled flags. Default False → consumer never runs.
    2. `live_gmx_enabled` AND/OR `live_binance_enabled` per leg — each
       leg's own gate stack. Default False.
    3. `PilotGuard.check(market, notional_usd).allowed == True` — the
       6-gate killswitch / armed-markets / size / pnl / concurrent /
       cooldown stack. Default-deny.
    4. `reconcile_intent(intent, positions).action != "ABORT"` —
       on-chain state must not contradict the intent.
    5. `dry_run=False` — operator explicit opt-in PER PROCESS via
       `settings.funding_arb_executor_dry_run=False`. Default True.

  ALL FIVE → broadcast. ANY missing → simulate / refuse / log only.

WHAT THIS IS NOT:

  - No settlement loop. Realized PnL is initialized to 0.0 on every
    ExecutionRecord — G7.2 (a future PR) will add the position-tracker
    that updates PnL when positions close. For now the daily_pnl gate
    reads what we write (0.0 per fill), so it never trips spuriously
    on a fresh fill.
  - No exits / TP / SL. The consumer only OPENS. Closing logic is
    out-of-scope for G7.1.
  - No retry. If a leg fails mid-flight (network, partial submit), the
    operator MUST reconcile manually. The audit-§12 reconciliation
    pattern (`get_order_status` by client_order_id) is available
    on the Binance side; on GMX, the position reader can confirm
    on-chain state.

INTEGRATION (main.py):

  The runtime keeps doing what it does today (paper signal emit). When
  `settings.funding_arb_consumer_enabled=True`, main.py ALSO starts a
  `FundingArbExecutor.run(stop_event)` task in the same `asyncio.gather`.
  Restart required after toggling — the consumer-enabled gate is read
  ONCE at startup, not per-signal. That's intentional: the operator
  flips it as a deliberate decision, not a per-tick experiment.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from typing import Any

from gmx_strategies import (
    binance_order,
    gmx_position_reader,
    gmx_signer,
    pilot_guard,
)
from gmx_strategies.binance_funding import BINANCE_SYMBOL_BY_ALIAS
from gmx_strategies.gmx_order_encoder import OrderIntent
from gmx_strategies.markets import ARBITRUM_MARKETS
from gmx_strategies.redis_client import r as default_redis_factory
from gmx_strategies.settings import settings

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────
# Defaults — sizing constants for OrderIntent construction
# ──────────────────────────────────────────────────────────────────────────

# Acceptable price-band bps per market class. Mirrors
# `gmx_default_acceptable_price_band_{majors,alts}_bps` from settings —
# the consumer picks the right band at intent-construction time.
_MAJORS = frozenset({"btc", "eth"})

# Initial collateral delta amount (raw USDC units, 6 decimals) for the
# GMX leg. Sized to roughly match `target_position_usd` — for a $10
# position with 1x leverage we send $10 USDC = 10_000_000.
# (For real funding-arb the operator would also stack leverage; G7.1
# keeps 1x for the pilot and lets G7.4 add a leverage knob.)
_USDC_UNITS_PER_USD = 1_000_000  # 6-decimal USDC raw units per $1

# GMX-scaled USD multiplier — 30-decimal fixed point.
_GMX_USD_SCALE = 10**30

# Execution fee (wei) for the keeper. 0.0005 ETH = 5*10^14 wei. Matches
# the smoke-test default in `cli._build_smoke_intent_sol_long`. The
# operator can override via `_default_execution_fee_wei` future-style;
# G7.1 keeps it constant for the pilot.
_DEFAULT_EXECUTION_FEE_WEI = 5 * 10**14


# ──────────────────────────────────────────────────────────────────────────
# Types — DI shims so tests can swap real-network calls
# ──────────────────────────────────────────────────────────────────────────

# Position fetcher: (account_address) -> list[Position].
GmxPositionReaderFn = Callable[[str], Awaitable[list[Any]]]
# Sign function: (intent) -> signed_tx dict.
GmxSignFn = Callable[[OrderIntent], Awaitable[dict[str, Any]]]
# Submit function: (signed_tx, intent, *, dry_run) -> SendResult.
GmxSubmitFn = Callable[..., Awaitable[Any]]
# Binance order function: (symbol, side, notional_usd, *, mark_price,
# dry_run) -> OrderResult.
BinanceOrderFn = Callable[..., Awaitable[Any]]


# ──────────────────────────────────────────────────────────────────────────
# Frozen dataclass — the public record type
# ──────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ExecutionRecord:
    """One funding-arb signal's end-to-end outcome.

    Always XADD'd to `funding_arb:executions` — even on guard denial /
    reconcile abort, because the daily_pnl gate (and any future audit
    consumer) needs the complete trail.

    `realized_pnl_usd` is 0.0 on every fresh fill — G7.2 (future)
    will populate it when positions close. The daily_pnl gate sums
    `realized_pnl_usd` across today's entries; initializing to 0.0
    keeps the gate honest until settlement-tracking ships.
    """

    ts_ms: int
    market: str
    direction: str
    notional_usd_target: float
    gmx_result: dict[str, Any] | None
    binance_result: dict[str, Any] | None
    guard_block: dict[str, Any] | None
    reconcile_block: dict[str, Any] | None
    success_both_legs: bool
    error: str | None
    # Fields below are convenience for downstream PnL accounting; the
    # daily_pnl gate reads `realized_pnl_usd` directly.
    realized_pnl_usd: float = 0.0
    gmx_tx_hash: str | None = None
    binance_order_id: int | None = None


# ──────────────────────────────────────────────────────────────────────────
# Helpers — pure
# ──────────────────────────────────────────────────────────────────────────


def _now_ms() -> int:
    """Current unix epoch in milliseconds. Carved out for monkeypatching."""
    return int(time.time() * 1000)


def _direction_to_gmx_is_long(direction: str) -> bool:
    """Pure: map signal direction → GMX is_long.

    "short_gmx_long_cex" → False (short on GMX)
    "long_gmx_short_cex" → True  (long on GMX)
    """
    if direction == "short_gmx_long_cex":
        return False
    if direction == "long_gmx_short_cex":
        return True
    raise ValueError(f"unknown direction: {direction!r}")


def _direction_to_binance_side(direction: str) -> str:
    """Pure: map signal direction → Binance side.

    The CEX leg HEDGES the GMX leg, so it takes the opposite side:
      "short_gmx_long_cex" → "BUY"  (long on CEX)
      "long_gmx_short_cex" → "SELL" (short on CEX)
    """
    if direction == "short_gmx_long_cex":
        return "BUY"
    if direction == "long_gmx_short_cex":
        return "SELL"
    raise ValueError(f"unknown direction: {direction!r}")


def _acceptable_band_bps_for(market: str) -> int:
    """Pure: pick the right slippage band for the market class."""
    if market in _MAJORS:
        return settings.gmx_default_acceptable_price_band_majors_bps
    return settings.gmx_default_acceptable_price_band_alts_bps


def _serialize_result(obj: Any) -> dict[str, Any] | None:
    """Best-effort dataclass-or-dict → JSON-safe dict. None on failure."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj
    # Frozen dataclasses (SendResult, OrderResult) — use asdict.
    try:
        return asdict(obj)
    except TypeError:
        # Not a dataclass — fall back to vars() or string-coerce.
        if hasattr(obj, "__dict__"):
            return dict(vars(obj))
        return {"repr": repr(obj)}


def _record_to_stream_fields(
    record: ExecutionRecord,
) -> dict[
    bytes | bytearray | memoryview[int] | str | int | float,
    bytes | bytearray | memoryview[int] | str | int | float,
]:
    """Convert an ExecutionRecord → XADD-safe field dict.

    Redis stream fields must be string/bytes/int/float — nested dicts
    are JSON-encoded into a single string. Caller responsibility to
    serialize the gmx/binance/guard sub-results before this is invoked
    is the easier path; we re-encode them here for robustness.
    """
    fields: dict[
        bytes | bytearray | memoryview[int] | str | int | float,
        bytes | bytearray | memoryview[int] | str | int | float,
    ] = {
        "ts_ms": str(record.ts_ms),
        "market": record.market,
        "direction": record.direction,
        "notional_usd_target": f"{record.notional_usd_target:.6f}",
        "success_both_legs": "1" if record.success_both_legs else "0",
        "realized_pnl_usd": f"{record.realized_pnl_usd:.6f}",
    }
    if record.error is not None:
        fields["error"] = record.error
    if record.gmx_tx_hash is not None:
        fields["gmx_tx_hash"] = record.gmx_tx_hash
    if record.binance_order_id is not None:
        fields["binance_order_id"] = str(record.binance_order_id)
    if record.gmx_result is not None:
        try:
            fields["gmx_result_json"] = json.dumps(record.gmx_result, default=str)
        except (TypeError, ValueError):
            fields["gmx_result_json"] = repr(record.gmx_result)
    if record.binance_result is not None:
        try:
            fields["binance_result_json"] = json.dumps(
                record.binance_result, default=str,
            )
        except (TypeError, ValueError):
            fields["binance_result_json"] = repr(record.binance_result)
    if record.guard_block is not None:
        try:
            fields["guard_block_json"] = json.dumps(record.guard_block, default=str)
        except (TypeError, ValueError):
            fields["guard_block_json"] = repr(record.guard_block)
    if record.reconcile_block is not None:
        try:
            fields["reconcile_block_json"] = json.dumps(
                record.reconcile_block, default=str,
            )
        except (TypeError, ValueError):
            fields["reconcile_block_json"] = repr(record.reconcile_block)
    return fields


def _is_loss_shaped_gmx_result(send_result: Any) -> bool:
    """Pure: heuristic — does this GMX SendResult look like a loss/failure?

    Conservative — we count as "loss" any submitted-True with status=0
    (on-chain revert) OR any SendResult with a non-None `error` field
    AND `submitted=False` (gate-fail / RPC error mid-broadcast). Pure
    dry-run simulation results (submitted=False, error=None) are NOT
    losses — they're paper.
    """
    if send_result is None:
        return False
    # SendResult is a frozen dataclass with .submitted .status .error
    submitted = getattr(send_result, "submitted", None)
    status = getattr(send_result, "status", None)
    error = getattr(send_result, "error", None)
    if submitted is True and status == 0:
        return True
    if submitted is False and error is not None and "live_gmx_disabled" not in str(error):
        # The dry_run gate-fail and the live-disabled gate-fail are not
        # losses — they're refusals. Anything else from a non-dry path is.
        # We can't tell from the result alone whether it was dry_run; the
        # caller already opted for `dry_run=False` if we're checking
        # here. Treat as a loss-shaped failure.
        return True
    return False


def _is_loss_shaped_binance_result(order_result: Any) -> bool:
    """Pure: heuristic — does this Binance OrderResult look like a loss/failure?

    A non-None `error_code` other than -1 (local pre-flight) and not a
    gate_blocked → real Binance reject. Counts as loss for cooldown
    purposes (we don't want to keep retrying a $5-too-small order
    every poll cycle).
    """
    if order_result is None:
        return False
    error_code = getattr(order_result, "error_code", None)
    gate_blocked = getattr(order_result, "gate_blocked", None)
    if gate_blocked is not None:
        return False  # gate-fail isn't a loss
    if error_code is not None and error_code != -1:
        return True
    return False


# ──────────────────────────────────────────────────────────────────────────
# FundingArbExecutor — the main consumer class
# ──────────────────────────────────────────────────────────────────────────


class FundingArbExecutor:
    """Per-signal pipeline: subscribe → guard → reconcile → broadcast → log.

    Construct once per process. Inject test doubles via the constructor
    kwargs; defaults wire to the real modules.
    """

    def __init__(
        self,
        *,
        guard: pilot_guard.PilotGuard | None = None,
        gmx_position_reader_fn: GmxPositionReaderFn | None = None,
        gmx_sign_fn: GmxSignFn | None = None,
        gmx_submit_fn: GmxSubmitFn | None = None,
        binance_order_fn: BinanceOrderFn | None = None,
        redis_client: Any | None = None,
        mark_price_fn: Callable[[str], Awaitable[float | None]] | None = None,
    ) -> None:
        self._guard = guard or pilot_guard.PilotGuard()
        self._gmx_position_reader_fn = (
            gmx_position_reader_fn or gmx_position_reader.fetch_account_positions
        )
        self._gmx_sign_fn = gmx_sign_fn or gmx_signer.sign_order
        self._gmx_submit_fn = gmx_submit_fn or gmx_signer.submit_signed
        self._binance_order_fn = binance_order_fn or binance_order.place_market_order
        self._redis_override = redis_client
        # Mark-price fetcher for the Binance leg sizing. Default returns
        # None — the live build can plug in `/fapi/v1/premiumIndex` here.
        # When None, we fall back to a synthetic placeholder (1.0) so the
        # pre-flight lot-min check still runs through the code path.
        self._mark_price_fn = mark_price_fn or _default_mark_price_fn

    def _redis(self) -> Any:
        if self._redis_override is not None:
            return self._redis_override
        return default_redis_factory()

    # ── Per-signal pipeline ──────────────────────────────────────────────

    async def handle_signal(self, signal: dict[str, Any]) -> ExecutionRecord:
        """One signal → one ExecutionRecord. Always returns; never raises.

        See module docstring for the step-by-step. Exceptions inside
        the pipeline are caught and surfaced as `error` on the record;
        the per-signal contract is "always log, never crash the loop".
        """
        ts_ms = _now_ms()
        market = str(signal.get("market", "")).lower()
        direction = str(signal.get("direction", ""))
        # `target_position_usd` on the payload, falling back to the
        # consumer default. The pilot guard's size_cap still applies.
        notional_usd = float(
            signal.get(
                "target_position_usd",
                settings.funding_arb_target_position_usd,
            )
        )

        try:
            return await self._handle_signal_inner(
                ts_ms=ts_ms,
                market=market,
                direction=direction,
                notional_usd=notional_usd,
            )
        except Exception as exc:  # noqa: BLE001 — per-signal failures must not kill loop
            log.exception(
                "funding_arb.consumer.handle_signal_failed market=%s err=%s",
                market, exc,
            )
            record = ExecutionRecord(
                ts_ms=ts_ms,
                market=market,
                direction=direction,
                notional_usd_target=notional_usd,
                gmx_result=None,
                binance_result=None,
                guard_block=None,
                reconcile_block=None,
                success_both_legs=False,
                error=f"handle_signal_exception: {exc.__class__.__name__}: {exc}",
            )
            await self._xadd_execution(record)
            return record

    async def _handle_signal_inner(
        self,
        *,
        ts_ms: int,
        market: str,
        direction: str,
        notional_usd: float,
    ) -> ExecutionRecord:
        """Per-signal body — separated so the outer wrapper can catch
        every exception path uniformly."""
        # ── Step 1: PilotGuard check ────────────────────────────────────
        guard_result = await self._guard.check(market, notional_usd)
        if not guard_result.allowed:
            log.info(
                "funding_arb.consumer.guard_denied market=%s gate=%s "
                "notional_usd=%.4f reason=%s",
                market, guard_result.gate, notional_usd, guard_result.reason,
            )
            record = ExecutionRecord(
                ts_ms=ts_ms,
                market=market,
                direction=direction,
                notional_usd_target=notional_usd,
                gmx_result=None,
                binance_result=None,
                guard_block={
                    "allowed": guard_result.allowed,
                    "gate": guard_result.gate,
                    "reason": guard_result.reason,
                },
                reconcile_block=None,
                success_both_legs=False,
                error=f"guard_denied: gate={guard_result.gate}",
            )
            await self._xadd_execution(record)
            return record

        # ── Step 2: derive direction → legs ─────────────────────────────
        try:
            gmx_is_long = _direction_to_gmx_is_long(direction)
            binance_side = _direction_to_binance_side(direction)
        except ValueError as exc:
            log.warning(
                "funding_arb.consumer.bad_direction market=%s direction=%s err=%s",
                market, direction, exc,
            )
            record = ExecutionRecord(
                ts_ms=ts_ms,
                market=market,
                direction=direction,
                notional_usd_target=notional_usd,
                gmx_result=None,
                binance_result=None,
                guard_block=None,
                reconcile_block=None,
                success_both_legs=False,
                error=f"bad_direction: {exc}",
            )
            await self._xadd_execution(record)
            return record

        # ── Step 3: build OrderIntent (GMX leg) ─────────────────────────
        intent = self._build_intent(
            market=market,
            is_long=gmx_is_long,
            notional_usd=notional_usd,
        )
        if intent is None:
            record = ExecutionRecord(
                ts_ms=ts_ms,
                market=market,
                direction=direction,
                notional_usd_target=notional_usd,
                gmx_result=None,
                binance_result=None,
                guard_block=None,
                reconcile_block=None,
                success_both_legs=False,
                error="intent_construction_failed (unknown market or missing key)",
            )
            await self._xadd_execution(record)
            return record

        # ── Step 4: fetch positions + reconcile ─────────────────────────
        try:
            positions = await self._gmx_position_reader_fn(intent.account)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "funding_arb.consumer.position_fetch_failed market=%s err=%s",
                market, exc,
            )
            positions = []

        reconcile = gmx_position_reader.reconcile_intent(intent, positions)
        if reconcile.action == "ABORT":
            log.warning(
                "funding_arb.consumer.reconcile_abort market=%s reason=%s",
                market, reconcile.reason,
            )
            record = ExecutionRecord(
                ts_ms=ts_ms,
                market=market,
                direction=direction,
                notional_usd_target=notional_usd,
                gmx_result=None,
                binance_result=None,
                guard_block=None,
                reconcile_block={
                    "action": reconcile.action,
                    "reason": reconcile.reason,
                },
                success_both_legs=False,
                error=f"reconcile_abort: {reconcile.reason}",
            )
            await self._xadd_execution(record)
            return record

        # ── Step 5: parallel broadcast ──────────────────────────────────
        gmx_result, binance_result = await self._dispatch_legs(
            intent=intent,
            market=market,
            binance_side=binance_side,
            notional_usd=notional_usd,
        )

        # ── Step 6: success determination + loss recording ──────────────
        success_both = self._is_success(gmx_result, binance_result)
        error = self._compose_error(gmx_result, binance_result)

        gmx_tx_hash = getattr(gmx_result, "tx_hash", None) if gmx_result else None
        binance_order_id = (
            getattr(binance_result, "order_id", None) if binance_result else None
        )

        record = ExecutionRecord(
            ts_ms=ts_ms,
            market=market,
            direction=direction,
            notional_usd_target=notional_usd,
            gmx_result=_serialize_result(gmx_result),
            binance_result=_serialize_result(binance_result),
            guard_block=None,
            reconcile_block=(
                None if reconcile.action == "PROCEED"
                else {"action": reconcile.action, "reason": reconcile.reason}
            ),
            success_both_legs=success_both,
            error=error,
            realized_pnl_usd=0.0,  # G7.2 will populate from settlement loop
            gmx_tx_hash=gmx_tx_hash,
            binance_order_id=binance_order_id,
        )

        await self._xadd_execution(record)

        # Loss recording — G6 cooldown depends on this.
        if _is_loss_shaped_gmx_result(gmx_result) or _is_loss_shaped_binance_result(
            binance_result,
        ):
            try:
                await pilot_guard.record_loss(_now_ms())
            except Exception as exc:  # noqa: BLE001 — cooldown record must not crash loop
                log.warning(
                    "funding_arb.consumer.record_loss_failed err=%s", exc,
                )

        return record

    # ── Helpers — private ────────────────────────────────────────────────

    def _build_intent(
        self,
        *,
        market: str,
        is_long: bool,
        notional_usd: float,
    ) -> OrderIntent | None:
        """Construct an OrderIntent from a market alias + side + size.

        Returns None when the market alias is unknown or the executor
        key is not configured — both cases mean we can't legitimately
        build a signable intent.
        """
        market_meta = ARBITRUM_MARKETS.get(market)
        if market_meta is None:
            log.warning(
                "funding_arb.consumer.unknown_market market=%s", market,
            )
            return None

        executor_address = gmx_signer.get_executor_address()
        if executor_address is None:
            log.warning(
                "funding_arb.consumer.no_executor_key market=%s "
                "(set GMX_EXECUTOR_KEY or provision /srv/secrets/gmx_executor_key)",
                market,
            )
            return None

        # Always use USDC as collateral — both legs use stable collateral
        # so funding-arb PnL doesn't drift on index-token vol.
        collateral_token = market_meta.short_collateral_token
        collateral_amount = int(notional_usd * _USDC_UNITS_PER_USD)
        size_delta_usd = int(notional_usd * _GMX_USD_SCALE)

        # current_price_1e30 is required by the encoder for the band
        # computation. For G7.1 we use a deliberately-coarse placeholder
        # (1e30 = $1.00) because the consumer's per-signal mark-price
        # source isn't plumbed yet — the acceptable-price band is
        # multiplicative so the BAND COMPUTATION still works (the
        # encoder lifts the band off the current_price * bps/10_000).
        # The DRY-RUN simulation in submit_signed validates encoding
        # shape, not price economics. When the live path lands (G7.2+),
        # `mark_price_fn` should be plumbed through and used here.
        current_price_1e30 = 1 * _GMX_USD_SCALE  # $1 placeholder

        return OrderIntent(
            market=market,
            is_long=is_long,
            is_increase=True,  # OPEN; G7.1 doesn't ship closes
            collateral_token=collateral_token,
            initial_collateral_delta_amount=collateral_amount,
            size_delta_usd=size_delta_usd,
            current_price_1e30=current_price_1e30,
            acceptable_price_band_bps=_acceptable_band_bps_for(market),
            execution_fee_wei=_DEFAULT_EXECUTION_FEE_WEI,
            account=executor_address,
        )

    async def _dispatch_legs(
        self,
        *,
        intent: OrderIntent,
        market: str,
        binance_side: str,
        notional_usd: float,
    ) -> tuple[Any, Any]:
        """Run GMX + Binance legs concurrently. Returns (gmx_result, binance_result).

        Each leg is wrapped in its own task so an exception in one
        doesn't kill the other. Exceptions are converted to None and
        the `error` field on the ExecutionRecord captures the failure.
        """
        dry_run = settings.funding_arb_executor_dry_run

        gmx_task = asyncio.create_task(
            self._run_gmx_leg(intent=intent, dry_run=dry_run),
        )
        binance_task = asyncio.create_task(
            self._run_binance_leg(
                market=market,
                side=binance_side,
                notional_usd=notional_usd,
                dry_run=dry_run,
            ),
        )

        outcomes = await asyncio.gather(
            gmx_task, binance_task, return_exceptions=True,
        )
        gmx_outcome: Any = outcomes[0]
        binance_outcome: Any = outcomes[1]
        gmx_result: Any = (
            None if isinstance(gmx_outcome, BaseException) else gmx_outcome
        )
        binance_result: Any = (
            None if isinstance(binance_outcome, BaseException) else binance_outcome
        )
        if isinstance(gmx_outcome, BaseException):
            log.warning(
                "funding_arb.consumer.gmx_leg_exception market=%s err=%s",
                market, gmx_outcome,
            )
        if isinstance(binance_outcome, BaseException):
            log.warning(
                "funding_arb.consumer.binance_leg_exception market=%s err=%s",
                market, binance_outcome,
            )
        return gmx_result, binance_result

    async def _run_gmx_leg(
        self,
        *,
        intent: OrderIntent,
        dry_run: bool,
    ) -> Any:
        """Sign + submit the GMX leg. Returns the SendResult."""
        signed = await self._gmx_sign_fn(intent)
        return await self._gmx_submit_fn(signed, intent, dry_run=dry_run)

    async def _run_binance_leg(
        self,
        *,
        market: str,
        side: str,
        notional_usd: float,
        dry_run: bool,
    ) -> Any:
        """Compute mark price + place the Binance hedge order."""
        symbol = BINANCE_SYMBOL_BY_ALIAS.get(market)
        if symbol is None:
            log.warning(
                "funding_arb.consumer.unknown_binance_symbol market=%s", market,
            )
            return None
        mark_price = await self._mark_price_fn(symbol)
        if mark_price is None or mark_price <= 0:
            log.warning(
                "funding_arb.consumer.no_mark_price symbol=%s — "
                "using fallback $1.00 (dry_run only safe)",
                symbol,
            )
            mark_price = 1.0
        return await self._binance_order_fn(
            symbol, side, notional_usd,
            mark_price=mark_price,
            dry_run=dry_run,
        )

    def _is_success(self, gmx_result: Any, binance_result: Any) -> bool:
        """Both legs must report submitted=True for full success.

        Under dry-run, BOTH legs return submitted=False (sim only) and
        we treat THAT as success-shaped too (the operator opted into
        paper, so no real broadcast is the expected outcome). The
        distinguishing field is `error` — if either leg has an error,
        not a success.
        """
        if gmx_result is None or binance_result is None:
            return False
        # GMX side
        gmx_error = getattr(gmx_result, "error", None)
        gmx_sim = getattr(gmx_result, "dry_run_simulation", None)
        # Binance side
        binance_error_code = getattr(binance_result, "error_code", None)
        binance_gate = getattr(binance_result, "gate_blocked", None)
        binance_dry_run_request = getattr(binance_result, "dry_run_request", None)

        # In live-broadcast mode: both submitted=True and no errors.
        gmx_submitted = getattr(gmx_result, "submitted", False)
        binance_submitted = getattr(binance_result, "submitted", False)
        if gmx_submitted and binance_submitted:
            return binance_error_code is None and gmx_error is None

        # In dry-run mode (both legs simulated): success = no errors +
        # both legs produced their respective sim artifact.
        if not settings.funding_arb_executor_dry_run:
            return False
        gmx_sim_ok = gmx_sim is not None and (
            getattr(gmx_sim, "ok", False)
            or getattr(gmx_sim, "revert_known_acceptable", False)
        )
        binance_dry_ok = (
            binance_dry_run_request is not None
            and binance_error_code is None
            and binance_gate is None
        )
        return gmx_sim_ok and binance_dry_ok

    def _compose_error(self, gmx_result: Any, binance_result: Any) -> str | None:
        """Combine non-fatal errors from both legs into one message.

        Returns None when both legs are clean. We don't crash on
        partial failure — just log it; the operator's audit trail
        does the rest.
        """
        parts: list[str] = []
        if gmx_result is None:
            parts.append("gmx_leg=None")
        else:
            err = getattr(gmx_result, "error", None)
            if err:
                parts.append(f"gmx={err}")
        if binance_result is None:
            parts.append("binance_leg=None")
        else:
            err_code = getattr(binance_result, "error_code", None)
            err_msg = getattr(binance_result, "error_msg", None)
            gate = getattr(binance_result, "gate_blocked", None)
            if err_code is not None:
                parts.append(f"binance_code={err_code} msg={err_msg}")
            if gate is not None:
                parts.append(f"binance_gate={gate}")
        return "; ".join(parts) if parts else None

    async def _xadd_execution(self, record: ExecutionRecord) -> None:
        """XADD the record to the executions stream. Best-effort.

        Failures DO NOT crash the loop — but DO log a warning, since
        a missing stream entry shifts the daily_pnl gate's view of
        the day.
        """
        try:
            await self._redis().xadd(
                settings.funding_arb_executions_stream_key,
                _record_to_stream_fields(record),
                maxlen=settings.funding_arb_executions_maxlen,
                approximate=True,
            )
        except Exception as exc:  # noqa: BLE001 — never crash the loop on XADD
            log.warning(
                "funding_arb.consumer.xadd_failed market=%s err=%s",
                record.market, exc,
            )

    # ── Subscribe loop ───────────────────────────────────────────────────

    async def run(self, stop_event: asyncio.Event) -> None:
        """Main loop — subscribe to funding_arb:signals; dispatch each.

        Errors per-signal don't kill the loop — caught in
        `handle_signal`. The whole loop survives Redis disconnects
        too: a pub/sub failure leads to a short backoff and a re-subscribe.

        Respects `stop_event` — checks it after every message and on
        each backoff cycle. Returns when set.
        """
        backoff_s = 1.0
        max_backoff_s = 30.0
        log.info(
            "funding_arb.consumer.run channel=%s",
            settings.funding_arb_signals_channel,
        )

        while not stop_event.is_set():
            try:
                await self._subscribe_and_dispatch(stop_event)
                backoff_s = 1.0  # reset on clean exit
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "funding_arb.consumer.subscribe_loop_error err=%s "
                    "backoff_s=%.1f",
                    exc, backoff_s,
                )
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=backoff_s)
                    return  # stop set during backoff
                except TimeoutError:
                    backoff_s = min(backoff_s * 2, max_backoff_s)

    async def _subscribe_and_dispatch(self, stop_event: asyncio.Event) -> None:
        """One subscribe-and-listen cycle. Returns when stop_event set
        OR when the pubsub iteration cleanly exits."""
        redis = self._redis()
        pubsub = redis.pubsub()
        try:
            await pubsub.subscribe(settings.funding_arb_signals_channel)
            log.info(
                "funding_arb.consumer.subscribed channel=%s",
                settings.funding_arb_signals_channel,
            )
            while not stop_event.is_set():
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0,
                )
                if message is None:
                    continue
                await self._handle_message(message)
        finally:
            try:
                await pubsub.unsubscribe(settings.funding_arb_signals_channel)
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "funding_arb.consumer.unsubscribe_failed err=%s", exc,
                )
            try:
                await pubsub.close()
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "funding_arb.consumer.pubsub_close_failed err=%s", exc,
                )

    async def _handle_message(self, message: dict[str, Any]) -> None:
        """Parse one pub/sub message and dispatch to handle_signal."""
        data = message.get("data")
        if data is None:
            return
        if isinstance(data, (bytes, bytearray)):
            try:
                data = bytes(data).decode("utf-8")
            except UnicodeDecodeError as exc:
                log.warning("funding_arb.consumer.bad_message_encoding err=%s", exc)
                return
        if not isinstance(data, str):
            log.warning(
                "funding_arb.consumer.bad_message_shape type=%s", type(data),
            )
            return
        try:
            signal = json.loads(data)
        except (TypeError, ValueError) as exc:
            log.warning(
                "funding_arb.consumer.bad_message_json err=%s body=%s",
                exc, data[:200],
            )
            return
        if not isinstance(signal, dict):
            log.warning(
                "funding_arb.consumer.bad_message_not_dict body=%s", data[:200],
            )
            return
        await self.handle_signal(signal)


# ──────────────────────────────────────────────────────────────────────────
# Default mark-price fetcher — None for now (lives in cli for the smoke);
# the consumer falls back to $1.00 in dry-run if not provided. A real
# live build (G7.2+) plugs `/fapi/v1/premiumIndex` in here.
# ──────────────────────────────────────────────────────────────────────────


async def _default_mark_price_fn(symbol: str) -> float | None:
    """Default mark-price stub. Returns None → consumer falls back to
    a synthetic $1.00 placeholder. Replace in a real live deploy.
    """
    _ = symbol
    return None


__all__ = [
    "ExecutionRecord",
    "FundingArbExecutor",
]
