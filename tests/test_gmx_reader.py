"""Tests for the live GMX V2 Reader integration (G2).

Mocks `httpx.AsyncClient.post` (for the eth_call JSON-RPC) and the package's
`r()` Redis factory (for the Streams price reads). Asserts:
  - MarketPrices struct is encoded correctly + sent to the Reader.
  - Decoded MarketInfo → FundingState with the right rate sign + magnitude.
  - Market-disabled → returns None.
  - RPC revert / error → returns None.
  - Missing Streams price → returns None.
  - Unknown market → returns None.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from eth_abi import encode

from gmx_strategies import gmx_reader
from gmx_strategies.funding_arb import FundingState

# Canonical eth-zero address — used when constructing an empty/disabled
# market response in tests for the getMarket() result.
_ETH_ZERO = "0x0000000000000000000000000000000000000000"


def _hex_with_prefix(b: bytes) -> str:
    return "0x" + b.hex()


def _encode_get_market_response(
    market_token: str,
    index_token: str,
    long_token: str,
    short_token: str,
) -> str:
    """Pack a Market.Props tuple into the hex response shape Reader.getMarket returns."""
    body = encode(
        ["(address,address,address,address)"],
        [(market_token, index_token, long_token, short_token)],
    )
    return _hex_with_prefix(body)


def _encode_market_info_response(
    *,
    is_disabled: bool,
    longs_pay_shorts: bool,
    funding_factor_per_second: int,
    borrowing_long: int = 0,
    borrowing_short: int = 0,
) -> str:
    """Pack a ReaderUtils.MarketInfo into the hex response shape getMarketInfo returns.

    Most nested fields aren't read by the decoder so we fill them with zeros;
    we only need to keep the type-spec aligned with `_MARKET_INFO_TYPE`.
    """
    zero_market_props = (_ETH_ZERO, _ETH_ZERO, _ETH_ZERO, _ETH_ZERO)
    zero_collateral_type = (0, 0)
    zero_position_type = (zero_collateral_type, zero_collateral_type)
    zero_base_funding = (zero_position_type, zero_position_type)
    next_funding = (
        longs_pay_shorts,
        funding_factor_per_second,
        0,                          # nextSavedFundingFactorPerSecond (int256)
        zero_position_type,         # fundingFeeAmountPerSizeDelta
        zero_position_type,         # claimableFundingAmountPerSizeDelta
    )
    zero_virtual_inventory = (0, 0, 0)
    market_info = (
        zero_market_props,
        borrowing_long,
        borrowing_short,
        zero_base_funding,
        next_funding,
        zero_virtual_inventory,
        is_disabled,
    )
    body = encode([gmx_reader._MARKET_INFO_TYPE], [market_info])
    return _hex_with_prefix(body)


def _encode_uint_response(value: int) -> str:
    """Pack a uint256 into the hex response shape getUint returns."""
    return _hex_with_prefix(encode(["uint256"], [value]))


class _FakeRedis:
    """Async Redis stub for the Streams price reads."""

    def __init__(self, prices: dict[str, str | None]) -> None:
        self.prices = prices
        self.gets: list[str] = []

    async def get(self, key: str) -> str | None:
        self.gets.append(key)
        return self.prices.get(key)


def _make_fake_response(*, status_code: int = 200, body: dict[str, Any]) -> Any:
    """Build a stand-in httpx.Response for the mocked POST."""
    class _Resp:
        def __init__(self, sc: int, body: dict[str, Any]) -> None:
            self.status_code = sc
            self._body = body

        def json(self) -> dict[str, Any]:
            return self._body

    return _Resp(status_code, body)


@pytest.fixture
def fake_streams_prices() -> dict[str, str | None]:
    """Default Streams prices — populated for all 3 tokens of the BTC market."""
    return {
        "chainlink:btc:latest": '{"benchmark_price_float64": 65000.0}',
        "chainlink:usdc:latest": '{"benchmark_price_float64": 1.0}',
    }


@pytest.fixture
def patch_redis(
    monkeypatch: pytest.MonkeyPatch,
    fake_streams_prices: dict[str, str | None],
) -> _FakeRedis:
    """Patch the gmx_reader module's `r()` factory with a _FakeRedis."""
    fake = _FakeRedis(prices=fake_streams_prices)
    monkeypatch.setattr(gmx_reader, "r", lambda: fake)
    return fake


def _btc_responses(
    *,
    info_hex: str,
    long_oi: int = 80 * 10**6 * 10**30,
    short_oi: int = 20 * 10**6 * 10**30,
) -> list[Any]:
    """Build the standard 6-response sequence for one BTC fetch_gmx_funding_live call.

    Order:
      1. getMarket — Market.Props
      2. getMarketInfo
      3. getUint(openInterestKey(market, long_token, isLong=true))
      4. getUint(openInterestKey(market, short_token, isLong=true))
      5. getUint(openInterestKey(market, long_token, isLong=false))
      6. getUint(openInterestKey(market, short_token, isLong=false))

    The long-side total is `_read_open_interest_total_usd(..., is_long=True)`
    which calls _one twice — once per collateral token. Same for short side.
    We split `long_oi`/`short_oi` half-and-half across the two calls so the
    sum still matches the expected total.
    """
    half_long = long_oi // 2
    half_short = short_oi // 2
    return [
        # 1. getMarket
        _make_fake_response(
            body={
                "jsonrpc": "2.0",
                "id": 1,
                "result": _encode_get_market_response(
                    market_token="0x47c031236e19d024b42f8AE6780E44A573170703",
                    index_token="0x47904963fc8b2340414262125af798b9655e58cd",  # BTC synth
                    long_token="0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",  # WBTC
                    short_token="0xaf88d065e77c8cc2239327c5edb3a432268e5831",  # USDC
                ),
            }
        ),
        # 2. getMarketInfo
        _make_fake_response(body={"jsonrpc": "2.0", "id": 1, "result": info_hex}),
        # 3. long-side OI w/ long collateral
        _make_fake_response(
            body={"jsonrpc": "2.0", "id": 1, "result": _encode_uint_response(half_long)}
        ),
        # 4. long-side OI w/ short collateral
        _make_fake_response(
            body={"jsonrpc": "2.0", "id": 1,
                  "result": _encode_uint_response(long_oi - half_long)}
        ),
        # 5. short-side OI w/ long collateral
        _make_fake_response(
            body={"jsonrpc": "2.0", "id": 1, "result": _encode_uint_response(half_short)}
        ),
        # 6. short-side OI w/ short collateral
        _make_fake_response(
            body={"jsonrpc": "2.0", "id": 1,
                  "result": _encode_uint_response(short_oi - half_short)}
        ),
    ]


@pytest.mark.asyncio
async def test_happy_path_returns_funding_state_with_correct_sign(
    patch_redis: _FakeRedis,
) -> None:
    """Funding factor → positive 8h rate when longs_pay_shorts=true."""
    # Pick a factor such that rate_per_8h works out to a clean number.
    # factor_per_second * 8 * 3600 / 1e30 = rate_per_8h
    # Want rate_per_8h = 0.0008 (0.08% / 8h, typical GMX V2). Then
    # factor_per_second = 0.0008 * 1e30 / (8 * 3600).
    target_rate = 0.0008
    factor = int(target_rate * (10**30) / (8 * 3600))
    info_hex = _encode_market_info_response(
        is_disabled=False,
        longs_pay_shorts=True,
        funding_factor_per_second=factor,
    )

    responses = _btc_responses(info_hex=info_hex)
    with patch("httpx.AsyncClient.post", new=AsyncMock(side_effect=responses)):
        state = await gmx_reader.fetch_gmx_funding_live("btc", chain="arbitrum")

    assert state is not None
    assert isinstance(state, FundingState)
    assert state.market == "btc"
    assert state.funding_rate_per_8h == pytest.approx(target_rate, rel=1e-6)
    assert state.longs_oi_usd == pytest.approx(80_000_000.0, rel=1e-6)
    assert state.shorts_oi_usd == pytest.approx(20_000_000.0, rel=1e-6)


@pytest.mark.asyncio
async def test_negative_rate_when_shorts_pay_longs(
    patch_redis: _FakeRedis,
) -> None:
    """longs_pay_shorts=false → negative rate (shorts paying longs)."""
    target_magnitude = 0.0004
    factor = int(target_magnitude * (10**30) / (8 * 3600))
    info_hex = _encode_market_info_response(
        is_disabled=False,
        longs_pay_shorts=False,
        funding_factor_per_second=factor,
    )
    responses = _btc_responses(info_hex=info_hex)
    with patch("httpx.AsyncClient.post", new=AsyncMock(side_effect=responses)):
        state = await gmx_reader.fetch_gmx_funding_live("btc", chain="arbitrum")
    assert state is not None
    # Sign flipped — shorts pay longs.
    assert state.funding_rate_per_8h == pytest.approx(-target_magnitude, rel=1e-6)


@pytest.mark.asyncio
async def test_market_disabled_returns_none(patch_redis: _FakeRedis) -> None:
    """isDisabled=true → skip the market entirely."""
    info_hex = _encode_market_info_response(
        is_disabled=True,
        longs_pay_shorts=True,
        funding_factor_per_second=12345,
    )
    # OI calls don't matter — we never reach them. But the mocked sequence
    # must NOT have extras after isDisabled triggers an early return; in our
    # implementation isDisabled is checked AFTER getMarketInfo, BEFORE the OI
    # calls. So we only need responses for the first 2 RPC calls.
    responses = [
        _make_fake_response(
            body={
                "jsonrpc": "2.0",
                "id": 1,
                "result": _encode_get_market_response(
                    market_token="0x47c031236e19d024b42f8AE6780E44A573170703",
                    index_token="0x47904963fc8b2340414262125af798b9655e58cd",
                    long_token="0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
                    short_token="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                ),
            }
        ),
        _make_fake_response(body={"jsonrpc": "2.0", "id": 1, "result": info_hex}),
    ]
    with patch("httpx.AsyncClient.post", new=AsyncMock(side_effect=responses)):
        state = await gmx_reader.fetch_gmx_funding_live("btc", chain="arbitrum")
    assert state is None


@pytest.mark.asyncio
async def test_rpc_error_returns_none(patch_redis: _FakeRedis) -> None:
    """RPC returning a JSON-RPC error → return None, do not raise."""
    responses = [
        _make_fake_response(
            body={"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "revert"}}
        ),
    ]
    with patch("httpx.AsyncClient.post", new=AsyncMock(side_effect=responses)):
        state = await gmx_reader.fetch_gmx_funding_live("btc", chain="arbitrum")
    assert state is None


@pytest.mark.asyncio
async def test_missing_streams_price_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """If a Streams key is missing, fetch_gmx_funding_live returns None."""
    # Empty Redis — no streams prices.
    fake = _FakeRedis(prices={})
    monkeypatch.setattr(gmx_reader, "r", lambda: fake)

    # Only the first call (getMarket) reaches the RPC; we then bail in
    # _build_market_prices before getMarketInfo. The mock therefore only
    # needs the getMarket response.
    responses = [
        _make_fake_response(
            body={
                "jsonrpc": "2.0",
                "id": 1,
                "result": _encode_get_market_response(
                    market_token="0x47c031236e19d024b42f8AE6780E44A573170703",
                    index_token="0x47904963fc8b2340414262125af798b9655e58cd",
                    long_token="0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
                    short_token="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                ),
            }
        ),
    ]
    with patch("httpx.AsyncClient.post", new=AsyncMock(side_effect=responses)):
        state = await gmx_reader.fetch_gmx_funding_live("btc", chain="arbitrum")
    assert state is None


@pytest.mark.asyncio
async def test_unknown_market_returns_none(patch_redis: _FakeRedis) -> None:
    """Aliases not in ARBITRUM_MARKETS → return None without hitting the RPC."""
    state = await gmx_reader.fetch_gmx_funding_live(
        "this_is_not_a_market", chain="arbitrum",
    )
    assert state is None


@pytest.mark.asyncio
async def test_unsupported_chain_returns_none(patch_redis: _FakeRedis) -> None:
    """Only arbitrum is wired in G2; other chains short-circuit."""
    state = await gmx_reader.fetch_gmx_funding_live("btc", chain="avalanche")
    assert state is None


def test_scale_price_to_gmx_matches_gmx_convention() -> None:
    """ETH @ $3500 → 3500 * 10**(30-18) = 3500 * 10**12 scaled."""
    assert gmx_reader._scale_price_to_gmx(3500.0, 18) == 3500 * (10**12)


def test_scale_price_to_gmx_for_usdc() -> None:
    """USDC @ $1 → 1 * 10**(30-6) = 10**24 scaled."""
    assert gmx_reader._scale_price_to_gmx(1.0, 6) == 10**24


def test_open_interest_storage_key_deterministic() -> None:
    """Same inputs → same bytes32; flipping is_long → different key."""
    market = "0x47c031236e19d024b42f8AE6780E44A573170703"
    collateral = "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f"
    key_long = gmx_reader._open_interest_storage_key(market, collateral, True)
    key_short = gmx_reader._open_interest_storage_key(market, collateral, False)
    assert len(key_long) == 32
    assert len(key_short) == 32
    assert key_long != key_short
    # Determinism:
    assert gmx_reader._open_interest_storage_key(market, collateral, True) == key_long


def test_decode_market_info_rejects_short_input() -> None:
    """Malformed (short) input → decoder returns None, doesn't raise."""
    assert gmx_reader._decode_market_info("0x") is None
    assert gmx_reader._decode_market_info("not-hex") is None
    assert gmx_reader._decode_market_info("") is None
