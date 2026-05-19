"""Live GMX V2 Reader integration (G2) — funding rate + OI per market.

Replaces the mocked `fetch_gmx_funding` in `funding_arb_runtime.py` with a
real on-chain read against `Reader.getMarketInfo(DataStore, MarketPrices, marketKey)`
and `DataStore.getUint(openInterestKey(...))` per side, on Arbitrum.

Prices come from the operator's chainlink-streams Redis topology — keys of
the shape `chainlink:{alias}:latest` carry a JSON payload with the field
`benchmark_price_float64`. We do NOT call any external price API in this
module; the Streams feed is the source of truth for the live oracle prices
fed into GMX's MarketPrices struct.

Sign convention for returned FundingState:
    funding_rate_per_8h > 0  ⇒  longs pay shorts (matches funding_arb.py).

Failure modes (ALL return None — caller must keep the loop alive):
  - Market disabled (`MarketInfo.isDisabled == true`)
  - Missing Streams price for index / long / short token
  - RPC error / revert
  - Decode failure
  - Malformed price JSON

Implementation note: this module uses raw httpx JSON-RPC (matching OCDE's
`hyperevm_reader.py` pattern) rather than `Web3.HTTPProvider`. Rationale:
the call set is small (3 calls per market — getMarketInfo + 2× DataStore.getUint),
we need precise control over timeouts/error handling, and the rest of this
package is already async/httpx. eth_abi (ships with web3.py) is used to
encode struct args and decode the MarketInfo return struct.

Reader/DataStore addresses + scale come from settings — overridable via env.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx
from eth_abi import decode, encode  # type: ignore[attr-defined]  # eth_abi lacks __all__
from web3 import Web3

from gmx_strategies.funding_arb import FundingState
from gmx_strategies.markets import ARBITRUM_MARKETS
from gmx_strategies.redis_client import r
from gmx_strategies.settings import settings

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────
# Per-token decimals (verified on-chain or from GMX official tokens.ts
# 2026-05-20). The ERC20 tokens (WETH/WBTC/USDC/WSOL) carry on-chain
# `decimals()`; the synthetic index tokens (BTC/DOGE/XRP synthetic) do NOT
# expose decimals() — they're virtual addresses in GMX's market config
# space — so the convention from `gmx-io/gmx-interface/sdk/src/configs/tokens.ts`
# is authoritative. We hardcode rather than runtime-fetch so a future
# GMX redeploy doesn't silently drift this map under us.
# ──────────────────────────────────────────────────────────────────────────
_TOKEN_DECIMALS: dict[str, int] = {
    # ERC20 longs/shorts (verified on-chain 2026-05-20):
    "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": 18,  # WETH
    "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f": 8,  # WBTC
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831": 6,  # USDC
    "0x2bcc6d6cdbbdc0a4071e48bb3b969b06b3330c07": 9,  # WSOL (Wormhole)
    # GMX synthetic index tokens (from gmx-io/gmx-interface tokens.ts):
    "0x47904963fc8b2340414262125af798b9655e58cd": 8,  # BTC synthetic
    "0xc4da4c24fd591125c3f47b340b6f4f76111883d8": 8,  # DOGE synthetic
    "0xc14e065b0067de91534e032868f5ac6ecf2c6868": 6,  # XRP synthetic
}

# Mapping of GMX market alias -> chainlink-streams alias for the INDEX token
# price. The same alias is used as the Redis key suffix
# (`chainlink:{alias}:latest`). chainlink-streams currently emits 7 feeds:
# btc, eth, sol, doge, xrp, wsteth, cbeth, usdc — we only need the 5 here.
_MARKET_TO_STREAMS_ALIAS: dict[str, str] = {
    "btc": "btc",
    "eth": "eth",
    "sol": "sol",
    "doge": "doge",
    "xrp": "xrp",
}

# Token-address -> streams alias for ERC20 long/short tokens. The Streams
# feeds for WETH/USDC are aliased "eth"/"usdc" on the operator's setup;
# WBTC reuses the "btc" feed (price is identical USD-side) and WSOL reuses
# "sol". When wsBTC / kSOL get added back, extend this map.
_TOKEN_ADDR_TO_STREAMS_ALIAS: dict[str, str] = {
    "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": "eth",  # WETH
    "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f": "btc",  # WBTC
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831": "usdc",  # USDC
    "0x2bcc6d6cdbbdc0a4071e48bb3b969b06b3330c07": "sol",  # WSOL
}


# Function selectors (precomputed from keccak("name(types)")[:4]).
_SEL_GET_MARKET_INFO = "0x" + Web3.keccak(
    text=(
        "getMarketInfo(address,"
        "((uint256,uint256),(uint256,uint256),(uint256,uint256)),"
        "address)"
    )
)[:4].hex()
_SEL_GET_UINT = "0x" + Web3.keccak(text="getUint(bytes32)")[:4].hex()


# Type spec for decoding the ReaderUtils.MarketInfo return struct. This is
# the exact ABI shape from `deployments/arbitrum/Reader.json` (verified
# 2026-05-20) — careful: every nested tuple gets its own pair of parens.
#
# Structure (top-level):
#   ReaderUtils.MarketInfo {
#     Market.Props market { address, address, address, address }
#     uint256 borrowingFactorPerSecondForLongs
#     uint256 borrowingFactorPerSecondForShorts
#     ReaderUtils.BaseFundingValues baseFunding { ... nested ... }   // ignored
#     MarketUtils.GetNextFundingAmountPerSizeResult nextFunding {
#       bool longsPayShorts
#       uint256 fundingFactorPerSecond
#       int256 nextSavedFundingFactorPerSecond
#       MarketUtils.PositionType fundingFeeAmountPerSizeDelta { ... }
#       MarketUtils.PositionType claimableFundingAmountPerSizeDelta { ... }
#     }
#     ReaderUtils.VirtualInventory virtualInventory { ... }            // ignored
#     bool isDisabled
#   }
#
# We must include EVERY field in the type spec for eth_abi to walk the
# encoded bytes correctly, even fields we don't read. Tuples use the
# `()` syntax; nested structs nest.
# MarketUtils.CollateralType = (uint256 longToken, uint256 shortToken)
_COLLATERAL_TYPE = "(uint256,uint256)"
# MarketUtils.PositionType = (CollateralType long, CollateralType short)
_POSITION_TYPE = f"({_COLLATERAL_TYPE},{_COLLATERAL_TYPE})"
# ReaderUtils.BaseFundingValues = (PositionType fundingFeeAmountPerSize,
#                                  PositionType claimableFundingAmountPerSize)
_BASE_FUNDING_TYPE = f"({_POSITION_TYPE},{_POSITION_TYPE})"
# MarketUtils.GetNextFundingAmountPerSizeResult — has 5 fields:
#  bool longsPayShorts, uint256 fundingFactorPerSecond, int256 nextSaved,
#  PositionType fundingFeeAmountPerSizeDelta,
#  PositionType claimableFundingAmountPerSizeDelta
_NEXT_FUNDING_TYPE = (
    "("
    "bool,"
    "uint256,"
    "int256,"
    f"{_POSITION_TYPE},"
    f"{_POSITION_TYPE}"
    ")"
)
_VIRTUAL_INVENTORY_TYPE = "(uint256,uint256,int256)"
_MARKET_PROPS_TYPE = "(address,address,address,address)"

# Full MarketInfo type spec used to decode the Reader.getMarketInfo response.
_MARKET_INFO_TYPE = (
    "("
    f"{_MARKET_PROPS_TYPE},"        # market
    "uint256,"                      # borrowingFactorPerSecondForLongs
    "uint256,"                      # borrowingFactorPerSecondForShorts
    f"{_BASE_FUNDING_TYPE},"        # baseFunding
    f"{_NEXT_FUNDING_TYPE},"        # nextFunding
    f"{_VIRTUAL_INVENTORY_TYPE},"   # virtualInventory
    "bool"                          # isDisabled
    ")"
)


# DataStore key derivations. GMX V2 keys are bytes32 derived via keccak256
# of a (uint, tag, params...) tuple — see `MarketUtils.openInterestKey`:
#   keccak256(abi.encode(OPEN_INTEREST, market, collateralToken, isLong))
# where `OPEN_INTEREST = keccak256(abi.encode("OPEN_INTEREST"))`.
# CRITICAL: the tag is `keccak256(abi.encode("OPEN_INTEREST"))`, NOT
# `keccak256("OPEN_INTEREST")` — Solidity's abi.encode of a string pads
# with offset + length, so the two hashes differ. Source: Keys.sol in
# gmx-io/gmx-synthetics. Result is 30-decimal-scaled USD.
_OPEN_INTEREST_TAG = Web3.keccak(encode(["string"], ["OPEN_INTEREST"]))


def _open_interest_storage_key(
    market: str, collateral_token: str, is_long: bool,
) -> bytes:
    """Compute the bytes32 DataStore key for `openInterestKey`.

    Mirrors `MarketUtils.openInterestKey` in gmx-synthetics. Result is
    30-decimal USD-scaled when read via `DataStore.getUint`.
    """
    return Web3.keccak(
        encode(
            ["bytes32", "address", "address", "bool"],
            [_OPEN_INTEREST_TAG, market, collateral_token, is_long],
        )
    )


def _scale_price_to_gmx(price_usd: float, token_decimals: int) -> int:
    """Convert a USD float price to GMX's 30-decimal-minus-token-decimals fixed point.

    GMX V2 prices in storage are scaled to `10**(30 - token_decimals)` so
    that `price_scaled * size_in_token_native_decimals` lands at 30-decimal
    USD. e.g. for ETH (18-dec): scaled_price has 12 decimals of precision;
    for USDC (6-dec): scaled_price has 24 decimals.

    Naïve `price_usd * 10**24` blows past float64 precision (~15 sig digits)
    for stablecoins where the result is in the 1e24 range. To stay exact we
    work with a Decimal that has enough precision for any realistic price
    × any of GMX's scaling factors.
    """
    from decimal import Decimal, getcontext

    # 40 digits is more than enough for `BTC@$1e6 * 10**22` (≈ 1e28).
    getcontext().prec = 60
    scaled = Decimal(repr(price_usd)) * (Decimal(10) ** (30 - token_decimals))
    return int(scaled)


# Stablecoin fallback prices — used when chainlink:<alias>:latest is missing
# (e.g., operator's Streams entitlement covers 7 risk assets but not the
# stablecoin collateral side of GMX V2 markets). USDC has been within ±20bps
# of $1.00 for >99% of its lifetime; using a $1.00 constant for MarketPrices
# construction introduces at most ~0.2% USD-side error in OI scaling — fine
# for funding-arb signal generation. If higher precision is needed later,
# wire pyth:<alias>:latest as a secondary source (OCDE republishes Pyth USDC).
_STABLECOIN_FALLBACK_USD: dict[str, float] = {
    "usdc": 1.0,
    "usdt": 1.0,
    "dai": 1.0,
    "fdusd": 1.0,
}


async def _get_streams_price(alias: str, redis: Any) -> float | None:
    """Read `benchmark_price_float64` from `chainlink:{alias}:latest`.

    Returns None when the key is missing, the JSON malformed, or the
    field absent — EXCEPT for known stablecoins (USDC/USDT/DAI/FDUSD),
    which fall back to $1.00 USD via `_STABLECOIN_FALLBACK_USD`. Rationale:
    the operator's 7-feed Streams entitlement covers risk assets only,
    not stablecoins, and constructing MarketPrices for any GMX V2 market
    requires a price for the short-collateral stablecoin.

    The Redis client used is the package's shared async client
    (decode_responses=True so we get strings, not bytes).
    """
    key = f"chainlink:{alias}:latest"
    try:
        raw = await redis.get(key)
    except Exception as exc:  # noqa: BLE001
        log.warning("gmx_reader.streams_get_failed key=%s err=%s", key, exc)
        return _STABLECOIN_FALLBACK_USD.get(alias.lower())
    if raw is None:
        fb = _STABLECOIN_FALLBACK_USD.get(alias.lower())
        if fb is not None:
            log.debug("gmx_reader.streams_missing_stable_fallback key=%s fallback=%s", key, fb)
            return fb
        log.warning("gmx_reader.streams_missing key=%s", key)
        return None
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        log.warning("gmx_reader.streams_bad_json key=%s", key)
        return _STABLECOIN_FALLBACK_USD.get(alias.lower())
    price = payload.get("benchmark_price_float64")
    if not isinstance(price, (int, float)) or price <= 0:
        log.warning("gmx_reader.streams_bad_price key=%s value=%r", key, price)
        return _STABLECOIN_FALLBACK_USD.get(alias.lower())
    return float(price)


async def _build_market_prices(
    market_alias: str,
    index_token: str,
    long_token: str,
    short_token: str,
    redis: Any,
) -> tuple[tuple[int, int], tuple[int, int], tuple[int, int]] | None:
    """Construct the MarketPrices tuple from Streams + token decimals.

    Returns (indexTokenPrice, longTokenPrice, shortTokenPrice) where each
    inner tuple is `(min, max)` of the GMX-scaled price. We set min == max
    from the Streams benchmark price (no separate min/max source); GMX
    accepts equal min/max for view calls. Returns None on any missing input.
    """
    # Index price comes from the per-market alias mapping (synthetic markets
    # use the alias derived from the asset name, not the indexToken address).
    streams_index_alias = _MARKET_TO_STREAMS_ALIAS.get(market_alias)
    if streams_index_alias is None:
        log.warning(
            "gmx_reader.no_streams_alias market=%s", market_alias,
        )
        return None
    index_price_usd = await _get_streams_price(streams_index_alias, redis)
    if index_price_usd is None:
        return None

    # Long/short token prices come from their ERC20 -> streams-alias map.
    long_alias = _TOKEN_ADDR_TO_STREAMS_ALIAS.get(long_token.lower())
    short_alias = _TOKEN_ADDR_TO_STREAMS_ALIAS.get(short_token.lower())
    if long_alias is None:
        log.warning(
            "gmx_reader.no_long_streams_alias market=%s token=%s",
            market_alias, long_token,
        )
        return None
    if short_alias is None:
        log.warning(
            "gmx_reader.no_short_streams_alias market=%s token=%s",
            market_alias, short_token,
        )
        return None
    long_price_usd = await _get_streams_price(long_alias, redis)
    short_price_usd = await _get_streams_price(short_alias, redis)
    if long_price_usd is None or short_price_usd is None:
        return None

    # Decimals — we may be receiving lowercased addresses; the map is
    # already lowercased for lookup consistency.
    idx_dec = _TOKEN_DECIMALS.get(index_token.lower())
    long_dec = _TOKEN_DECIMALS.get(long_token.lower())
    short_dec = _TOKEN_DECIMALS.get(short_token.lower())
    if idx_dec is None or long_dec is None or short_dec is None:
        log.warning(
            "gmx_reader.unknown_decimals market=%s idx=%s long=%s short=%s",
            market_alias, index_token, long_token, short_token,
        )
        return None

    idx_scaled = _scale_price_to_gmx(index_price_usd, idx_dec)
    long_scaled = _scale_price_to_gmx(long_price_usd, long_dec)
    short_scaled = _scale_price_to_gmx(short_price_usd, short_dec)
    # Streams gives a single benchmark price; min == max is acceptable for
    # view calls (`getMarketInfo` accepts any min<=max pair).
    return (
        (idx_scaled, idx_scaled),
        (long_scaled, long_scaled),
        (short_scaled, short_scaled),
    )


def _decode_market_info(
    result_hex: str,
) -> dict[str, Any] | None:
    """Decode the Reader.getMarketInfo return blob → dict of relevant fields.

    Returns None on decode failure. Only extracts the fields we need:
      - `is_disabled` (bool)
      - `longs_pay_shorts` (bool)
      - `funding_factor_per_second` (int, 30-decimal fixed-point)
      - `borrowing_factor_per_second_for_longs` (int, 30-decimal)
      - `borrowing_factor_per_second_for_shorts` (int, 30-decimal)
    """
    if not isinstance(result_hex, str) or not result_hex.startswith("0x"):
        return None
    try:
        body = bytes.fromhex(result_hex[2:])
    except ValueError:
        return None
    if len(body) == 0:
        return None
    try:
        decoded = decode([_MARKET_INFO_TYPE], body)
    except Exception as exc:  # noqa: BLE001
        log.warning("gmx_reader.decode_failed err=%s", exc)
        return None
    # decoded is a tuple of one element (the MarketInfo struct as a tuple)
    (market_info,) = decoded
    (
        _market,
        borrowing_for_longs,
        borrowing_for_shorts,
        _base_funding,
        next_funding,
        _virtual_inventory,
        is_disabled,
    ) = market_info
    (
        longs_pay_shorts,
        funding_factor_per_second,
        _next_saved,
        _fee_delta,
        _claim_delta,
    ) = next_funding
    return {
        "is_disabled": bool(is_disabled),
        "longs_pay_shorts": bool(longs_pay_shorts),
        "funding_factor_per_second": int(funding_factor_per_second),
        "borrowing_factor_per_second_for_longs": int(borrowing_for_longs),
        "borrowing_factor_per_second_for_shorts": int(borrowing_for_shorts),
    }


async def _eth_call(
    client: httpx.AsyncClient,
    rpc_url: str,
    to_address: str,
    data: str,
) -> str | None:
    """Best-effort eth_call. Returns the hex result or None on any failure."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [{"to": to_address, "data": data}, "latest"],
    }
    try:
        resp = await client.post(rpc_url, json=payload)
    except (httpx.HTTPError, httpx.TimeoutException) as exc:
        log.warning("gmx_reader.rpc_http_error err=%s", exc)
        return None
    if resp.status_code != 200:
        log.warning("gmx_reader.rpc_bad_status status=%d", resp.status_code)
        return None
    try:
        body = resp.json()
    except (ValueError, TypeError):
        log.warning("gmx_reader.rpc_bad_json")
        return None
    if not isinstance(body, dict):
        log.warning("gmx_reader.rpc_bad_body_shape")
        return None
    if "error" in body:
        log.warning("gmx_reader.rpc_error err=%s", body["error"])
        return None
    result = body.get("result")
    if not isinstance(result, str):
        log.warning("gmx_reader.rpc_missing_result")
        return None
    return result


async def _read_open_interest_one(
    client: httpx.AsyncClient,
    rpc_url: str,
    datastore: str,
    market_address: str,
    collateral_token: str,
    is_long: bool,
) -> float | None:
    """Read a single openInterestKey(market, collateral, isLong) → USD float.

    Returns the 30-decimal USD value divided by 1e30, or None on failure.
    """
    storage_key = _open_interest_storage_key(market_address, collateral_token, is_long)
    data = _SEL_GET_UINT + storage_key.hex()
    result_hex = await _eth_call(client, rpc_url, datastore, data)
    if result_hex is None:
        return None
    try:
        raw = int(result_hex, 16)
    except ValueError:
        log.warning("gmx_reader.oi_decode_failed result=%s", result_hex)
        return None
    return raw / 1e30


async def _read_open_interest_total_usd(
    client: httpx.AsyncClient,
    rpc_url: str,
    datastore: str,
    market_address: str,
    long_token: str,
    short_token: str,
    is_long: bool,
) -> float | None:
    """Read total OI USD for a side, summing both collateral options.

    GMX V2 stores OI keyed by (market, collateralToken, isLong). A long
    position can be collateralized with EITHER the long token (e.g. WETH
    for ETH-long) OR the short token (USDC). Same for shorts. The pool
    divisor is 2 when long==short token, else 1 — see `MarketUtils.getPoolDivisor`.

    Returns the summed USD value or None on any underlying call failure.
    """
    long_collat_oi = await _read_open_interest_one(
        client, rpc_url, datastore, market_address, long_token, is_long,
    )
    if long_collat_oi is None:
        return None
    short_collat_oi = await _read_open_interest_one(
        client, rpc_url, datastore, market_address, short_token, is_long,
    )
    if short_collat_oi is None:
        return None
    divisor = 2.0 if long_token.lower() == short_token.lower() else 1.0
    return (long_collat_oi + short_collat_oi) / divisor


async def fetch_gmx_funding_live(  # noqa: PLR0911
    market: str, chain: str = "arbitrum",
) -> FundingState | None:
    """Live read of GMX V2 funding rate + OI for one market.

    Reads prices from chainlink-streams Redis, constructs the
    MarketUtils.MarketPrices struct, calls Reader.getMarketInfo, decodes
    the funding factor + sign, and reads long/short OI from DataStore.

    Returns a FundingState matching the pure-helper sign convention
    (positive = longs pay shorts). Returns None on ANY failure path —
    never raises. Caller (funding_arb_runtime._process_market) keeps the
    loop alive on None.

    Note: only arbitrum is wired in G2; other chains return None.
    """
    if chain != "arbitrum":
        log.warning("gmx_reader.unsupported_chain chain=%s", chain)
        return None
    gmx_market = ARBITRUM_MARKETS.get(market)
    if gmx_market is None:
        log.warning("gmx_reader.unknown_market market=%s", market)
        return None

    # Resolve the on-chain index/long/short token addresses for THIS market
    # by calling Reader.getMarket. We could hardcode these from markets.py
    # but they're cheap to re-read and any future GMX market reshuffle is
    # caught here rather than silently producing wrong prices.
    redis = r()
    market_address = gmx_market.market_address

    # We'll need two sequential RPC calls: getMarket -> getMarketInfo.
    timeout = httpx.Timeout(settings.gmx_reader_timeout_s)
    async with httpx.AsyncClient(timeout=timeout) as client:
        # Step 1: getMarket → Market.Props { marketToken, indexToken, long, short }
        get_market_sel = "0x" + Web3.keccak(
            text="getMarket(address,address)",
        )[:4].hex()
        get_market_data = get_market_sel + encode(
            ["address", "address"],
            [settings.gmx_datastore_address_arbitrum, market_address],
        ).hex()
        market_result_hex = await _eth_call(
            client,
            settings.arbitrum_rpc_url,
            settings.gmx_reader_address_arbitrum,
            get_market_data,
        )
        if market_result_hex is None:
            return None
        try:
            market_body = bytes.fromhex(market_result_hex[2:])
            (market_props,) = decode([_MARKET_PROPS_TYPE], market_body)
        except Exception as exc:  # noqa: BLE001
            log.warning("gmx_reader.market_decode_failed market=%s err=%s", market, exc)
            return None
        _market_token, index_token, long_token, short_token = market_props
        # Defensive zero-address check (the ghost-market wsteth bug).
        if index_token == "0x0000000000000000000000000000000000000000":
            log.warning("gmx_reader.zero_market_struct market=%s", market)
            return None

        # Step 2: build MarketPrices tuple from Streams + decimals
        prices = await _build_market_prices(
            market, index_token, long_token, short_token, redis,
        )
        if prices is None:
            return None
        (idx_pp, long_pp, short_pp) = prices

        # Step 3: getMarketInfo(dataStore, prices, marketKey)
        encoded_args = encode(
            [
                "address",
                "((uint256,uint256),(uint256,uint256),(uint256,uint256))",
                "address",
            ],
            [
                settings.gmx_datastore_address_arbitrum,
                (idx_pp, long_pp, short_pp),
                market_address,
            ],
        )
        info_data = _SEL_GET_MARKET_INFO + encoded_args.hex()
        info_result_hex = await _eth_call(
            client,
            settings.arbitrum_rpc_url,
            settings.gmx_reader_address_arbitrum,
            info_data,
        )
        if info_result_hex is None:
            return None
        info = _decode_market_info(info_result_hex)
        if info is None:
            return None
        if info["is_disabled"]:
            # Trap-surface WARN: market is governance-disabled. Loop already
            # handles this gracefully (None propagates to the per-market
            # try/except and the sweep continues) but the operator needs to
            # see this in stderr — silent delisting was the wsteth bug.
            log.warning(
                "gmx_reader.market_disabled market=%s — market is disabled, returning None",
                market,
            )
            return None

        # Step 4: read long-side and short-side OI in USD. GMX V2 stores OI
        # keyed by (market, collateralToken, isLong) so a long position can
        # be held with EITHER the long or the short collateral; we sum both
        # to get the side's total. See `MarketUtils.getOpenInterest` in
        # gmx-synthetics. No PnL adjustment here — for funding-arb we want
        # the gross OI for imbalance direction; the PnL-adjusted path
        # (Reader.getOpenInterestWithPnl) is also available but costs 2x
        # extra RPC round-trips for the same first-order signal.
        longs_oi_usd = await _read_open_interest_total_usd(
            client,
            settings.arbitrum_rpc_url,
            settings.gmx_datastore_address_arbitrum,
            market_address,
            long_token,
            short_token,
            True,
        )
        shorts_oi_usd = await _read_open_interest_total_usd(
            client,
            settings.arbitrum_rpc_url,
            settings.gmx_datastore_address_arbitrum,
            market_address,
            long_token,
            short_token,
            False,
        )
        if longs_oi_usd is None or shorts_oi_usd is None:
            return None

    # Step 5: convert per-second factor → per-8h signed rate.
    # rate_per_8h = factor_per_second * 8 * 3600 / 10**scale * sign
    # `funding_factor_per_second` is uint256 — sign comes from `longs_pay_shorts`.
    seconds_per_8h = 8 * 3600
    scale = 10 ** settings.gmx_funding_factor_scale
    magnitude = info["funding_factor_per_second"] * seconds_per_8h / scale
    sign = 1.0 if info["longs_pay_shorts"] else -1.0
    rate_per_8h = magnitude * sign

    log.info(
        "gmx_reader.read_ok market=%s rate_per_8h=%.6f "
        "longs_oi_usd=%.0f shorts_oi_usd=%.0f longs_pay=%s",
        market, rate_per_8h, longs_oi_usd, shorts_oi_usd, info["longs_pay_shorts"],
    )
    return FundingState(
        market=market,
        longs_oi_usd=longs_oi_usd,
        shorts_oi_usd=shorts_oi_usd,
        funding_rate_per_8h=rate_per_8h,
    )


__all__ = ["fetch_gmx_funding_live"]
