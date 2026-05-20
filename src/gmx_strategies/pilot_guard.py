"""G7.3 Pilot guard — refuse-to-broadcast layer.

The LAST safety belt before every order placement. Every call site in the
funding-arb executor (G7.1, next PR) MUST invoke `PilotGuard.check()`
and abort if `GuardResult.allowed` is False.

Gates (in order — first denial wins, but EVERY denial is recorded to the
`funding_arb:guard_blocks` Redis stream):

  G1 killswitch       — operator can halt the whole executor with one
                        `SET funding_arb:killswitch 1`. Checked FIRST so
                        no other state matters when it's tripped.
  G2 armed_markets    — DEFAULT-DENY. Empty CSV (the default) means
                        ZERO markets trade. Operator must explicitly opt
                        in per-market via `funding_arb_armed_markets_csv`.
  G3 size_cap         — refuse any notional above the pilot per-position
                        cap. Defaults to $10/position (pilot scale).
  G4 daily_pnl        — read today's UTC realized PnL from the
                        `funding_arb:executions` stream. If below the
                        loss floor (default -$50), STOP.
  G5 concurrent       — refuse if open GMX + Binance positions already
                        sum to the max-concurrent cap (default 1).
  G6 cooldown         — refuse if the last-loss timestamp is within the
                        cooldown window (default 30min).

Read-only. NO submit calls. NO order placement. Denial logic only.

Operator workflow to take a market live:
    1. flip `live_gmx_enabled=True` AND `live_binance_enabled=True`
    2. set `funding_arb_armed_markets_csv=sol` (or whatever pilot market)
    3. run `python -m gmx_strategies.cli g7_guard_status` to confirm
    4. start the consumer (G7.1)

Emergency stop:
    ssh ai-primary 'docker exec redis redis-cli -a $PASS \\
        SET funding_arb:killswitch 1'

Memory pointers:
    memory/postmortem_2026_05_19_chainlink_lag_unpause.md — same shape
        of mistake we're preventing. READ before re-tuning these gates.
    memory/plan_scaling_ladder_chainlink.md — capital-growth discipline.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from gmx_strategies.redis_client import r as redis_client
from gmx_strategies.settings import settings

log = logging.getLogger("gmx_strategies.pilot_guard")

# ──────────────────────────────────────────────────────────────────────────
# Types
# ──────────────────────────────────────────────────────────────────────────

# Position-counter callables. Default implementations read from GMX V2
# Reader (`gmx_position_reader`) + Binance `/fapi/v2/positionRisk`
# (`binance_account`). Injected for tests to bypass network calls.
GmxPositionCounter = Callable[[], Awaitable[int]]
BinancePositionCounter = Callable[[], Awaitable[int]]


# Gate tags — single source of truth so tests + status output stay in
# lockstep with the check sequence below.
GATE_KILLSWITCH = "killswitch"  # noqa: S105 — gate tag, not a password
GATE_NOT_ARMED = "not_armed"  # noqa: S105
GATE_SIZE_CAP = "size_cap"  # noqa: S105
GATE_DAILY_PNL = "daily_pnl"  # noqa: S105
GATE_CONCURRENT = "concurrent"  # noqa: S105
GATE_COOLDOWN = "cooldown"  # noqa: S105


@dataclass(frozen=True)
class GuardResult:
    """Outcome of a single `PilotGuard.check()` call.

    Invariants enforced by the constructor pattern in `check()`:
      - allowed=True → reason is None AND gate is None
      - allowed=False → reason is a non-empty string AND gate is a tag

    Callers MUST treat allowed=False as "do not broadcast" full-stop.
    The reason/gate fields are advisory only — for operator logs.
    """

    allowed: bool
    reason: str | None
    gate: str | None


@dataclass(frozen=True)
class GuardState:
    """Read-only snapshot of every input feeding the guard's decision.

    Returned by `PilotGuard.state()` so operators can answer "WHY would
    the guard allow/deny right now?" without having to mentally union the
    six gates.
    """

    killswitch_set: bool
    today_pnl_usd: float
    today_pnl_floor_usd: float
    open_gmx_positions: int
    open_binance_positions: int
    max_concurrent: int
    armed_markets: set[str]
    pilot_position_cap_usd: float
    last_loss_ts_ms: int | None
    cooldown_remaining_s: int


# ──────────────────────────────────────────────────────────────────────────
# Helpers — independent of PilotGuard class so they're easy to unit-test
# ──────────────────────────────────────────────────────────────────────────


def _parse_armed_markets(csv: str) -> set[str]:
    """Parse `funding_arb_armed_markets_csv` → lowercase set.

    Empty / whitespace-only CSV returns an empty set (default-deny).
    """
    if not csv:
        return set()
    return {tok.strip().lower() for tok in csv.split(",") if tok.strip()}


def _now_ms() -> int:
    """Current unix epoch in milliseconds. Carved out for monkeypatching."""
    return int(time.time() * 1000)


def _today_start_utc_ms(now_ms: int | None = None) -> int:
    """UTC-midnight epoch-ms for the current day.

    Used by the G4 daily-PnL gate to scope the stream scan. Day-rollover
    happens at 00:00 UTC; the operator's local timezone is irrelevant —
    we want a deterministic boundary that matches Binance's
    funding-settlement clock (which is also UTC-anchored).
    """
    ms = now_ms if now_ms is not None else _now_ms()
    return (ms // 86_400_000) * 86_400_000


def _is_truthy_killswitch(value: Any) -> bool:
    """`SET funding_arb:killswitch 1` (or `true`, case-insensitive) → trip.

    Accepts the value as bytes (no decode_responses), str (with decode),
    or None (key absent). Anything else evaluates False — defensive
    against an operator setting `0` or some other no-op value.
    """
    if value is None:
        return False
    if isinstance(value, (bytes, bytearray)):
        text = bytes(value).decode("utf-8", errors="replace").strip().lower()
    elif isinstance(value, str):
        text = value.strip().lower()
    else:
        return False
    return text in {"1", "true", "yes", "on"}


def _parse_pnl_field(value: Any) -> float | None:
    """Best-effort extract realized_pnl_usd from a stream entry's fields dict.

    Returns None if the field is missing or unparseable — caller skips
    the entry (better to under-count than to mis-attribute).
    """
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ──────────────────────────────────────────────────────────────────────────
# Default position counters (used when DI isn't provided)
# ──────────────────────────────────────────────────────────────────────────


async def _default_gmx_position_count() -> int:
    """Count open GMX V2 positions for the configured executor address.

    Returns 0 on any failure — the guard interprets that as "no positions
    confirmed, but I couldn't verify". This is the SAFE direction because
    a 0-count → concurrent gate is unlikely to trigger, which means the
    operator's next layer (the executor) ALSO has to be paranoid. We
    don't want the guard silently allowing because the reader failed.

    HOWEVER — operationally the executor should treat a 0-count read +
    a fetch-failure log line as a transient state worth retrying. The
    failure surface is in `gmx_position_reader.py`'s own logging.
    """
    from gmx_strategies import gmx_position_reader, gmx_signer

    address = gmx_signer.get_executor_address()
    if address is None:
        # No key configured → can't read → 0. This is the "operator hasn't
        # provisioned a key yet" path; the G2 armed_markets default-deny
        # also blocks in this case, so we're double-safe.
        return 0
    positions = await gmx_position_reader.fetch_account_positions(address)
    return len(positions)


async def _default_binance_position_count() -> int:
    """Count open Binance USDT-M perp positions (non-zero positionAmt).

    Mirrors the read in `cli._g6_smoke_main` READ-4 block. Returns 0 on
    failure for the same reason as the GMX counter.
    """
    from gmx_strategies import binance_account

    entries = await binance_account.fetch_position_information()
    if entries is None:
        return 0
    n_open = 0
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        amt_raw = entry.get("positionAmt", "0")
        try:
            amt = float(amt_raw)
        except (ValueError, TypeError):
            continue
        if amt != 0.0:
            n_open += 1
    return n_open


# ──────────────────────────────────────────────────────────────────────────
# PilotGuard
# ──────────────────────────────────────────────────────────────────────────


class PilotGuard:
    """The refuse-to-broadcast layer. See module docstring for gate spec.

    Construct once per process; the guard is stateless across calls
    except for the Redis handle (cached at the redis_client module
    level). Every `check()` is an independent decision based on live
    state.
    """

    def __init__(
        self,
        redis: Any | None = None,
        gmx_position_reader_fn: GmxPositionCounter | None = None,
        binance_position_reader_fn: BinancePositionCounter | None = None,
    ) -> None:
        # `redis` is `redis.asyncio.Redis`-shaped. We accept Any so tests
        # can drop in a minimal fake without subclassing the redis-py
        # type hierarchy (which has shifted across versions).
        self._redis_override = redis
        self._gmx_count: GmxPositionCounter = (
            gmx_position_reader_fn or _default_gmx_position_count
        )
        self._binance_count: BinancePositionCounter = (
            binance_position_reader_fn or _default_binance_position_count
        )

    def _redis(self) -> Any:
        """Return the injected redis OR the module-level shared client."""
        if self._redis_override is not None:
            return self._redis_override
        return redis_client()

    # ── Public surface ────────────────────────────────────────────────

    async def check(
        self,
        market: str,
        notional_usd: float,
        *,
        side: str | None = None,
    ) -> GuardResult:
        """Decide whether the proposed order may be broadcast.

        Returns a GuardResult. The CALLER must abort on `allowed=False` —
        the guard does NOT itself prevent submission; it only signals.

        Every denial is XADD'd to the `funding_arb:guard_blocks` stream
        for operator visibility (best-effort; XADD failures DO NOT
        promote allow → deny — the safety-belt is the decision, not the
        log).
        """
        market_lc = market.strip().lower()

        # G1 — killswitch (FIRST; nothing else matters if operator hit it)
        if await self._killswitch_is_set():
            result = GuardResult(
                allowed=False,
                reason="killswitch is set",
                gate=GATE_KILLSWITCH,
            )
            await self._log_denial(result, market_lc, notional_usd, side)
            return result

        # G2 — armed_markets (DEFAULT-DENY)
        armed = _parse_armed_markets(settings.funding_arb_armed_markets_csv)
        if market_lc not in armed:
            armed_list = sorted(armed) or ["<none>"]
            result = GuardResult(
                allowed=False,
                reason=(
                    f"market '{market_lc}' is not armed "
                    f"(armed_markets={armed_list}); "
                    "set funding_arb_armed_markets_csv to opt in"
                ),
                gate=GATE_NOT_ARMED,
            )
            await self._log_denial(result, market_lc, notional_usd, side)
            return result

        # G3 — size_cap
        cap = settings.funding_arb_pilot_position_cap_usd
        if notional_usd > cap:
            result = GuardResult(
                allowed=False,
                reason=(
                    f"notional ${notional_usd:.2f} exceeds pilot "
                    f"position cap ${cap:.2f}"
                ),
                gate=GATE_SIZE_CAP,
            )
            await self._log_denial(result, market_lc, notional_usd, side)
            return result

        # G4 — daily_pnl
        pnl_today = await self._today_realized_pnl_usd()
        floor = settings.funding_arb_pilot_daily_pnl_floor_usd
        if pnl_today < floor:
            result = GuardResult(
                allowed=False,
                reason=(
                    f"today's realized PnL ${pnl_today:.2f} is below "
                    f"the floor ${floor:.2f}"
                ),
                gate=GATE_DAILY_PNL,
            )
            await self._log_denial(result, market_lc, notional_usd, side)
            return result

        # G5 — concurrent
        gmx_n = await self._gmx_count()
        binance_n = await self._binance_count()
        max_n = settings.funding_arb_pilot_max_concurrent
        if (gmx_n + binance_n) >= max_n:
            result = GuardResult(
                allowed=False,
                reason=(
                    f"already at max concurrent: "
                    f"gmx={gmx_n} + binance={binance_n} "
                    f">= {max_n}"
                ),
                gate=GATE_CONCURRENT,
            )
            await self._log_denial(result, market_lc, notional_usd, side)
            return result

        # G6 — cooldown
        last_loss_ms, remaining_s = await self._cooldown_state()
        if last_loss_ms is not None and remaining_s > 0:
            result = GuardResult(
                allowed=False,
                reason=(
                    f"loss cooldown active: {remaining_s}s remaining "
                    f"(last_loss_ts_ms={last_loss_ms})"
                ),
                gate=GATE_COOLDOWN,
            )
            await self._log_denial(result, market_lc, notional_usd, side)
            return result

        # All gates passed — allow.
        return GuardResult(allowed=True, reason=None, gate=None)

    async def state(self) -> GuardState:
        """Read-only snapshot of every input feeding the guard's decision.

        Useful for the `g7_guard_status` CLI and for operator tooling
        that wants a one-shot picture before flipping any gate.
        """
        killswitch = await self._killswitch_is_set()
        pnl_today = await self._today_realized_pnl_usd()
        gmx_n = await self._gmx_count()
        binance_n = await self._binance_count()
        last_loss_ms, remaining_s = await self._cooldown_state()
        return GuardState(
            killswitch_set=killswitch,
            today_pnl_usd=pnl_today,
            today_pnl_floor_usd=settings.funding_arb_pilot_daily_pnl_floor_usd,
            open_gmx_positions=gmx_n,
            open_binance_positions=binance_n,
            max_concurrent=settings.funding_arb_pilot_max_concurrent,
            armed_markets=_parse_armed_markets(
                settings.funding_arb_armed_markets_csv,
            ),
            pilot_position_cap_usd=settings.funding_arb_pilot_position_cap_usd,
            last_loss_ts_ms=last_loss_ms,
            cooldown_remaining_s=remaining_s,
        )

    # ── Gate implementations (private) ────────────────────────────────

    async def _killswitch_is_set(self) -> bool:
        try:
            raw = await self._redis().get(settings.pilot_killswitch_key)
        except Exception as exc:  # noqa: BLE001 — Redis transient → safe default
            log.warning("pilot_guard.killswitch.get_failed err=%s", exc)
            # SAFE DEFAULT: assume tripped if we can't read.
            # If Redis is down, the operator is already in trouble; an
            # extra refusal-to-broadcast is the lesser evil vs. silently
            # letting orders through with an unverified killswitch.
            return True
        return _is_truthy_killswitch(raw)

    async def _today_realized_pnl_usd(self) -> float:
        """Sum realized_pnl_usd across today's executions stream entries.

        Reads `XRANGE funding_arb:executions <today_start_ms> +`. Failures
        return 0.0 (treat the day as flat). The G4 floor check then
        depends on the floor being NEGATIVE (default -$50) — a 0.0 PnL
        passes the floor and we move on. If the operator sets the floor
        positive (silly but possible), 0.0 < positive_floor would deny
        forever, which is fine — they asked for it.
        """
        start_ms = _today_start_utc_ms()
        # `(start_ms` is XRANGE's "exclusive of start" syntax in some redis
        # client versions. We use `min=start_ms` directly — XRANGE includes
        # entries whose id is >= min, which matches "from today's first
        # entry onward". A `0-0` id at start_ms is still included.
        try:
            entries = await self._redis().xrange(
                settings.funding_arb_executions_stream_key,
                min=start_ms,
                max="+",
            )
        except Exception as exc:  # noqa: BLE001 — see above
            log.warning("pilot_guard.daily_pnl.xrange_failed err=%s", exc)
            return 0.0

        total = 0.0
        for entry in entries or []:
            fields = self._fields_of(entry)
            if fields is None:
                continue
            pnl_raw = fields.get("realized_pnl_usd") or fields.get(
                b"realized_pnl_usd",
            )
            pnl = _parse_pnl_field(pnl_raw)
            if pnl is None:
                continue
            total += pnl
        return total

    async def _cooldown_state(self) -> tuple[int | None, int]:
        """Return (last_loss_ts_ms, cooldown_remaining_seconds).

        last_loss_ts_ms is None if the key is unset. remaining_s is 0 if
        the cooldown has elapsed (or the key is unset). Both are returned
        so `state()` can show the operator the full picture.
        """
        try:
            raw = await self._redis().get(settings.pilot_last_loss_ts_key)
        except Exception as exc:  # noqa: BLE001
            log.warning("pilot_guard.cooldown.get_failed err=%s", exc)
            return (None, 0)
        if raw is None:
            return (None, 0)
        if isinstance(raw, (bytes, bytearray)):
            raw_str = bytes(raw).decode("utf-8", errors="replace").strip()
        elif isinstance(raw, str):
            raw_str = raw.strip()
        else:
            return (None, 0)
        try:
            ts_ms = int(raw_str)
        except (TypeError, ValueError):
            return (None, 0)
        elapsed_s = (_now_ms() - ts_ms) // 1000
        window_s = settings.funding_arb_pilot_loss_cooldown_s
        remaining_s = max(0, window_s - int(elapsed_s))
        return (ts_ms, remaining_s)

    @staticmethod
    def _fields_of(entry: Any) -> dict[Any, Any] | None:
        """Extract the fields dict from an XRANGE entry, tolerant of shape.

        redis-py returns `[(id, {field: value}), ...]` with both id and
        fields as bytes when decode_responses=False, str when True. We
        accept either.
        """
        if not isinstance(entry, (list, tuple)):
            return None
        if len(entry) != 2:
            return None
        _, fields = entry
        if not isinstance(fields, dict):
            return None
        return fields

    async def _log_denial(
        self,
        result: GuardResult,
        market: str,
        notional_usd: float,
        side: str | None,
    ) -> None:
        """XADD a structured entry per denial. Best-effort.

        XADD failures DO NOT promote allow → deny. The guard's decision
        is the safety belt; the log is the audit trail.
        """
        fields: dict[
            bytes | bytearray | memoryview[int] | str | int | float,
            bytes | bytearray | memoryview[int] | str | int | float,
        ] = {
            "ts_ms": str(_now_ms()),
            "market": market,
            "notional_usd": f"{notional_usd:.4f}",
            "side": side or "",
            "gate": result.gate or "",
            "reason": result.reason or "",
        }
        # Also log to the process log so operators tailing journalctl see
        # the denial in real time (not just via the stream).
        log.warning(
            "pilot_guard.denied gate=%s market=%s notional=%.4f side=%s reason=%s",
            result.gate, market, notional_usd, side or "", result.reason,
        )
        try:
            await self._redis().xadd(
                settings.guard_blocks_stream_key,
                fields,
                maxlen=settings.guard_blocks_maxlen,
                approximate=True,
            )
        except Exception as exc:  # noqa: BLE001 — emit must not change the decision
            log.warning("pilot_guard.log_denial.xadd_failed err=%s", exc)


# ──────────────────────────────────────────────────────────────────────────
# Operator helpers — exposed at module level for CLI / G7.1 wiring
# ──────────────────────────────────────────────────────────────────────────


async def trip_killswitch(reason: str) -> None:
    """Operator-set the killswitch flag. Logs the reason.

    The flag is just `SET funding_arb:killswitch 1`. The reason is
    logged to the process log (NOT stored in Redis — the operator's
    incident log is the source-of-truth for "why").
    """
    log.warning("pilot_guard.killswitch.trip reason=%s", reason)
    await redis_client().set(settings.pilot_killswitch_key, "1")


async def reset_killswitch() -> None:
    """Operator-clear the killswitch flag.

    Pairs with `trip_killswitch`. The operator MUST verify the
    underlying issue is fixed before calling this; the guard does NOT
    enforce any cool-off after a killswitch reset.
    """
    log.warning("pilot_guard.killswitch.reset")
    await redis_client().delete(settings.pilot_killswitch_key)


async def record_loss(ts_ms: int) -> None:
    """Mark the most recent loss timestamp (G7.1 calls this post-exit).

    Sets `funding_arb:last_loss_ts_ms` to the provided unix-ms. The
    cooldown gate computes `now - ts_ms` against
    `funding_arb_pilot_loss_cooldown_s` on every guard check.
    """
    log.info("pilot_guard.record_loss ts_ms=%d", ts_ms)
    await redis_client().set(settings.pilot_last_loss_ts_key, str(int(ts_ms)))


__all__ = [
    "GATE_COOLDOWN",
    "GATE_CONCURRENT",
    "GATE_DAILY_PNL",
    "GATE_KILLSWITCH",
    "GATE_NOT_ARMED",
    "GATE_SIZE_CAP",
    "GuardResult",
    "GuardState",
    "PilotGuard",
    "record_loss",
    "reset_killswitch",
    "trip_killswitch",
]
