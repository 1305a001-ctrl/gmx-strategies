"""Binance USDT-M Futures exchange-info reader (G6.1 — CEX hedge leg).

First-coding-task of G6, per `memory/arch_binance_executor_audit.md` §7 / H1.
Reads `/fapi/v1/exchangeInfo` (public, no auth), parses per-symbol filters
(LOT_SIZE, MARKET_LOT_SIZE, PRICE_FILTER, MIN_NOTIONAL), caches them with a
TTL, and exposes pure rounding helpers callers MUST go through before
placing any order.

WHY THIS EXISTS — short version:
  The G6 audit flagged that BTCUSDT's `MIN_NOTIONAL` is ~$100 — 10× above
  our $10/trade cap. Every BTC hedge would reject with Binance error `-4164`
  (MIN_NOTIONAL) without this filter awareness. ETH/SOL/DOGE/XRP min
  notionals are typically lower ($5–$20) but ALSO symbol-specific and
  dynamic; hard-coding them is a trap. The downstream executor must read
  filters live at startup, cache, and snap-round every quantity through
  `round_qty_down` before submitting an order. Otherwise sizing errors
  silently turn into `-1111 PRECISION` or `-4164 MIN_NOTIONAL` rejections.

WHAT THIS IS NOT (deferred to G6.2+):
  - NO HMAC signing. NO `X-MBX-APIKEY` header. exchangeInfo is public.
  - NO order placement. NO account state reads.
  - NO position-mode (`/fapi/v1/positionSide/dual`) check — that's auth and
    belongs in G6.2's startup gate.
  - NO `marginType` / `leverage` POSTs — G6.3.

Per the audit's SDK guidance (§13): hand-rolled httpx + JSON. No new deps.
Match the style of `binance_funding.py`: best-effort error handling, return
empty dict on any failure, never raise — the caller's loop must keep
running.

Filter precedence:
  Binance returns multiple LOT_SIZE-family filters. For MARKET orders the
  relevant one is `MARKET_LOT_SIZE`; for LIMIT it's `LOT_SIZE`. Their values
  ARE NOT always identical (MARKET_LOT_SIZE typically has a wider stepSize).
  G6 v0.1 places only MARKET orders → we prefer MARKET_LOT_SIZE when present
  and fall back to LOT_SIZE otherwise. Doc'd here so future hands don't
  silently regress to LOT_SIZE.

Rounding correctness:
  We use `Decimal` not float for the floor-to-step math. With stepSize=0.001
  and qty=0.0234, `0.0234 / 0.001` floats to 23.399999... which floors to
  23 — correct, but only because the stepSize happened to give us a
  representable float. With stepSize=0.1 and qty=0.7, `0.7 / 0.1` floats to
  6.999999... which floors to 6 (WRONG — should be 7). Decimal arithmetic
  avoids this entire class of bug. The audit memory references PRECISION
  rejects (`-1111`) as a HIGH-severity break; this is the silent-bug
  surface that produces them.

TTL cache:
  Module-level cache. `get_cached_exchange_info(force_refresh=False)`
  returns the in-memory dict if fresh, refreshes on miss/stale. TTL is
  `settings.binance_exchange_info_ttl_s` (default 3600s = 1h). Filter values
  rarely change but DO change (Binance may bump minQty or MIN_NOTIONAL during
  volatile episodes). A 1h refresh is paranoid-cheap (1 request, weight 1).
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from typing import Any

import httpx

from gmx_strategies.settings import settings

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────
# Data model
# ──────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class SymbolInfo:
    """Per-symbol filters snapshot from /fapi/v1/exchangeInfo.

    All fields are plain Python floats / ints / strs — easy to log, pickle,
    serialize. Conversion from Binance's stringified-decimal payload happens
    in `_parse_symbol`. Use the module's helper functions (`round_qty_down`
    etc.) for any math against these — they handle decimal correctness.

    Fields:
      symbol           — exchange symbol (e.g. "BTCUSDT")
      base_asset       — base ticker (e.g. "BTC")
      quote_asset      — quote ticker (e.g. "USDT")
      price_precision  — digits after the decimal for prices (per-symbol meta)
      quantity_precision — digits after the decimal for quantity
      lot_min          — minQty from MARKET_LOT_SIZE (fallback: LOT_SIZE)
      lot_max          — maxQty from MARKET_LOT_SIZE (fallback: LOT_SIZE)
      lot_step         — stepSize for quantity rounding (MARKET_LOT_SIZE preferred)
      price_tick       — tickSize from PRICE_FILTER (minimum price increment)
      min_notional     — `notional` from MIN_NOTIONAL filter (USDT for USDT-M)
    """

    symbol: str
    base_asset: str
    quote_asset: str
    price_precision: int
    quantity_precision: int
    lot_min: float
    lot_max: float
    lot_step: float
    price_tick: float
    min_notional: float


# ──────────────────────────────────────────────────────────────────────────
# Pure math helpers (no I/O)
# ──────────────────────────────────────────────────────────────────────────


def _floor_to_step(value: float, step: float) -> float:
    """Floor `value` to the nearest multiple of `step` using Decimal math.

    Returns 0.0 if step <= 0 (defensive against malformed filters).

    Critical: float arithmetic introduces drift on stepSize values like 0.1
    or 0.01 because those are not exactly representable in IEEE-754. Using
    Decimal sidesteps this entire class of bug. See module docstring for
    the canonical failure example.
    """
    if step <= 0:
        return 0.0
    if value <= 0:
        return 0.0
    # Use str() to bypass float-imprecision when constructing the Decimal —
    # Decimal(0.1) is 0.10000000000000000555..., Decimal("0.1") is exact.
    d_value = Decimal(str(value))
    d_step = Decimal(str(step))
    # quantize-then-divide style: floor(value / step) * step.
    floored_units = (d_value / d_step).to_integral_value(rounding=ROUND_DOWN)
    result = floored_units * d_step
    return float(result)


def round_qty_down(info: SymbolInfo, qty: float) -> float:
    """Round a quantity DOWN to the nearest `lot_step` multiple.

    Always rounds down — never up — to guarantee min-notional checks AFTER
    rounding still hold. Caller must subsequently verify the rounded qty
    still passes `passes_min_notional`; rounding down a borderline qty CAN
    push it below the min.

    Returns 0.0 if the resulting qty would be below `lot_min` (the order
    would be rejected anyway, so signal that up the stack).
    """
    rounded = _floor_to_step(qty, info.lot_step)
    if rounded < info.lot_min:
        return 0.0
    return rounded


def round_price(info: SymbolInfo, price: float) -> float:
    """Round a price to the nearest `price_tick`.

    Uses standard round-half-to-even (banker's rounding via Decimal default).
    For MARKET orders we don't send a price field, but this is here for the
    LIMIT path G6 v0.2 will need.
    """
    if info.price_tick <= 0:
        return float(price)
    d_price = Decimal(str(price))
    d_tick = Decimal(str(info.price_tick))
    # quantize the price to a multiple of the tick: round(price / tick) * tick
    units = (d_price / d_tick).to_integral_value()  # default ROUND_HALF_EVEN
    return float(units * d_tick)


def passes_min_notional(info: SymbolInfo, qty: float, price: float) -> bool:
    """True iff `qty * price >= info.min_notional`.

    Used as the LAST gate before submitting an order. Caller should fail
    loud / route to a different symbol when this returns False, NOT silently
    bump the qty up.
    """
    if qty <= 0 or price <= 0:
        return False
    notional = qty * price
    return notional >= info.min_notional


def quantity_from_notional(
    info: SymbolInfo, notional_usd: float, price: float,
) -> float:
    """Compute a Binance hedge-leg quantity from a target USD notional.

    Given the GMX leg's USD notional (the funding-arb position size) and
    the current mark price, returns the Binance hedge quantity rounded DOWN
    to the symbol's `lot_step`.

    Returns 0.0 if:
      - price <= 0 (bad input)
      - the resulting quantity rounds below `lot_min`
      - notional_usd < info.min_notional (no point computing — would reject)

    Caller MUST check the result is non-zero AND call `passes_min_notional`
    against the rounded qty * price — rounding DOWN can push a borderline
    notional under the min, in which case the symbol is not usable at that
    cap and the strategy must drop it.
    """
    if price <= 0:
        return 0.0
    if notional_usd <= 0:
        return 0.0
    # Cheap fast-path: if the requested notional itself is below the symbol's
    # min, the rounded qty cannot pass anyway. Short-circuit.
    if notional_usd < info.min_notional:
        return 0.0
    raw_qty = notional_usd / price
    return round_qty_down(info, raw_qty)


# ──────────────────────────────────────────────────────────────────────────
# Parsing
# ──────────────────────────────────────────────────────────────────────────


def _coerce_float(raw: Any) -> float | None:
    """Parse a Binance stringified-decimal into a float. None on bad input."""
    if raw is None:
        return None
    if not isinstance(raw, (str, int, float)):
        return None
    try:
        value = float(raw)
    except (ValueError, TypeError):
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def _coerce_int(raw: Any) -> int | None:
    """Parse a Binance int-like field. None on bad input."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        # bool is a subclass of int in Python — explicitly reject.
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        if math.isnan(raw) or math.isinf(raw):
            return None
        return int(raw)
    if isinstance(raw, str):
        try:
            return int(raw)
        except (ValueError, TypeError):
            return None
    return None


def _parse_symbol(entry: dict[str, Any]) -> SymbolInfo | None:
    """Parse one element of `symbols[]` into a SymbolInfo.

    Returns None on any missing/malformed field. The caller drops Nones
    silently so partial outages on a few symbols don't poison the full map.
    """
    symbol = entry.get("symbol")
    base = entry.get("baseAsset")
    quote = entry.get("quoteAsset")
    if not (isinstance(symbol, str) and isinstance(base, str) and isinstance(quote, str)):
        return None

    price_precision = _coerce_int(entry.get("pricePrecision"))
    qty_precision = _coerce_int(entry.get("quantityPrecision"))
    if price_precision is None or qty_precision is None:
        return None

    filters = entry.get("filters")
    if not isinstance(filters, list):
        return None

    # Index filters by their `filterType` for O(1) lookup. Some filter types
    # appear only on certain symbols (e.g. POSITION_RISK_CONTROL) — we only
    # need PRICE_FILTER, LOT_SIZE/MARKET_LOT_SIZE, MIN_NOTIONAL.
    filters_by_type: dict[str, dict[str, Any]] = {}
    for f in filters:
        if not isinstance(f, dict):
            continue
        ft = f.get("filterType")
        if isinstance(ft, str):
            filters_by_type[ft] = f

    # MARKET orders are bounded by MARKET_LOT_SIZE when present, LOT_SIZE
    # otherwise. We prefer MARKET_LOT_SIZE because G6 v0.1 only places
    # MARKET orders.
    lot_filter = filters_by_type.get("MARKET_LOT_SIZE") or filters_by_type.get("LOT_SIZE")
    if not isinstance(lot_filter, dict):
        return None

    lot_min = _coerce_float(lot_filter.get("minQty"))
    lot_max = _coerce_float(lot_filter.get("maxQty"))
    lot_step = _coerce_float(lot_filter.get("stepSize"))
    if lot_min is None or lot_max is None or lot_step is None or lot_step <= 0:
        return None

    price_filter = filters_by_type.get("PRICE_FILTER")
    if not isinstance(price_filter, dict):
        return None
    price_tick = _coerce_float(price_filter.get("tickSize"))
    if price_tick is None:
        return None

    # Binance's MIN_NOTIONAL filter uses the field name `notional` for
    # USDT-M Futures (confirmed against the docs). Some legacy paths used
    # `minNotional`; we accept both for forward-compatibility.
    min_notional_filter = filters_by_type.get("MIN_NOTIONAL")
    if not isinstance(min_notional_filter, dict):
        return None
    raw_min_notional = min_notional_filter.get("notional")
    if raw_min_notional is None:
        raw_min_notional = min_notional_filter.get("minNotional")
    min_notional = _coerce_float(raw_min_notional)
    if min_notional is None:
        return None

    return SymbolInfo(
        symbol=symbol,
        base_asset=base,
        quote_asset=quote,
        price_precision=price_precision,
        quantity_precision=qty_precision,
        lot_min=lot_min,
        lot_max=lot_max,
        lot_step=lot_step,
        price_tick=price_tick,
        min_notional=min_notional,
    )


# ──────────────────────────────────────────────────────────────────────────
# Fetch
# ──────────────────────────────────────────────────────────────────────────


async def fetch_exchange_info(
    client: httpx.AsyncClient | None = None,
) -> dict[str, SymbolInfo]:
    """Fetch /fapi/v1/exchangeInfo and parse all symbols.

    Returns a dict keyed by symbol → SymbolInfo. On any HTTP / parse failure
    returns an EMPTY dict. NEVER raises — caller must keep its loop alive.

    Optional `client` lets the caller share an existing httpx.AsyncClient
    (avoids reconnect overhead when called as part of a startup sequence
    that also pulls /fapi/v1/positionSide/dual etc.). When None, we own the
    client lifetime and close it on exit.
    """
    url = f"{settings.binance_fapi_base_url}/fapi/v1/exchangeInfo"
    timeout = httpx.Timeout(settings.binance_funding_timeout_s)

    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=timeout)

    try:
        try:
            resp = await client.get(url)
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            log.warning("binance_exchange_info.http_error err=%s", exc)
            return {}
        if resp.status_code != 200:
            log.warning(
                "binance_exchange_info.bad_status status=%d", resp.status_code,
            )
            return {}
        try:
            body = resp.json()
        except (ValueError, TypeError):
            log.warning("binance_exchange_info.bad_json")
            return {}
        if not isinstance(body, dict):
            log.warning(
                "binance_exchange_info.bad_body_shape type=%s",
                type(body).__name__,
            )
            return {}
        symbols = body.get("symbols")
        if not isinstance(symbols, list):
            log.warning("binance_exchange_info.missing_symbols_field")
            return {}

        out: dict[str, SymbolInfo] = {}
        n_skipped = 0
        for entry in symbols:
            if not isinstance(entry, dict):
                n_skipped += 1
                continue
            parsed = _parse_symbol(entry)
            if parsed is None:
                n_skipped += 1
                continue
            out[parsed.symbol] = parsed

        log.info(
            "binance_exchange_info.read_ok n_symbols=%d n_skipped=%d",
            len(out), n_skipped,
        )
        return out
    finally:
        if owns_client and client is not None:
            await client.aclose()


# ──────────────────────────────────────────────────────────────────────────
# TTL cache
# ──────────────────────────────────────────────────────────────────────────

# Module-level cache. Reset to None on a forced refresh or first miss.
_cache: dict[str, SymbolInfo] | None = None
_cache_ts: float | None = None


def _is_cache_fresh() -> bool:
    """True iff the cache exists and has not exceeded its TTL."""
    if _cache is None or _cache_ts is None:
        return False
    age = time.time() - _cache_ts
    return age < settings.binance_exchange_info_ttl_s


async def get_cached_exchange_info(
    force_refresh: bool = False,
    client: httpx.AsyncClient | None = None,
) -> dict[str, SymbolInfo]:
    """Return the cached exchange-info map, refreshing on miss / stale / force.

    On cache miss, calls `fetch_exchange_info` and stores the result. If the
    refresh fetch returns an empty dict (e.g. transient HTTP error), the
    OLD cache is preserved — we'd rather serve slightly stale filters than
    nothing at all, because the next caller will retry on its next sweep.
    Only on a totally cold cache (never populated) do we return {} on error.
    """
    global _cache, _cache_ts
    if not force_refresh and _is_cache_fresh():
        # mypy: _cache is non-None when _is_cache_fresh() is true.
        return _cache or {}
    fresh = await fetch_exchange_info(client=client)
    if not fresh:
        # Refresh failed — keep serving stale rather than wipe.
        log.warning(
            "binance_exchange_info.refresh_failed serving_stale=%s",
            _cache is not None,
        )
        return _cache or {}
    _cache = fresh
    _cache_ts = time.time()
    return _cache


def _reset_cache_for_testing() -> None:
    """Test hook: clear the module-level cache. Not part of the public API."""
    global _cache, _cache_ts
    _cache = None
    _cache_ts = None


__all__ = [
    "SymbolInfo",
    "fetch_exchange_info",
    "get_cached_exchange_info",
    "passes_min_notional",
    "quantity_from_notional",
    "round_price",
    "round_qty_down",
]
