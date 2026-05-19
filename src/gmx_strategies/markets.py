"""GMX V2 market metadata — alias → (market address, collateral token).

For the on-chain Reader.getPosition() recheck (G7) we need to know:
  - market_address: the GMX V2 market contract token
  - long_collateral_token:  what long positions use
  - short_collateral_token: what short positions use

GMX V2 distinguishes long-side collateral (the index asset, e.g. WETH
for ETH-long) from short-side collateral (typically USDC). This map
hard-codes the live deployments. Refresh when new markets ship.

Source of truth: https://github.com/gmx-io/gmx-synthetics/tree/main/deployments
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GMXMarket:
    """One market's contract addresses + collateral tokens."""

    alias: str
    chain: str
    market_address: str
    long_collateral_token: str  # e.g. WETH for ETH-long
    short_collateral_token: str  # typically USDC


# ─── Arbitrum (chainId 42161) ──────────────────────────────────────────


# Token addresses (canonical, lowercased for consistency with TOKEN_TO_ALIAS).
_ARB_WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
_ARB_WBTC = "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f"
_ARB_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
_ARB_LINK = "0xf97f4df75117a78c1a5a0dbb814af92458539fb4"
_ARB_ARB = "0x912ce59144191c1204e64559fe8253a0e49e6548"
_ARB_WSOL = "0x2bcc6d6cdbbdc0a4071e48bb3b969b06b3330c07"  # Wormhole SOL on Arbitrum


ARBITRUM_MARKETS: dict[str, GMXMarket] = {
    "btc": GMXMarket(
        alias="btc",
        chain="arbitrum",
        market_address="0x47c031236e19d024b42f8AE6780E44A573170703",
        long_collateral_token=_ARB_WBTC,
        short_collateral_token=_ARB_USDC,
    ),
    "eth": GMXMarket(
        alias="eth",
        chain="arbitrum",
        market_address="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
        long_collateral_token=_ARB_WETH,
        short_collateral_token=_ARB_USDC,
    ),
    "sol": GMXMarket(
        alias="sol",
        chain="arbitrum",
        market_address="0x09400D9DB990D5ed3f35D7be61DfAEB900Af03C9",
        long_collateral_token=_ARB_WSOL,
        short_collateral_token=_ARB_USDC,
    ),
    "link": GMXMarket(
        alias="link",
        chain="arbitrum",
        market_address="0x7f1fa204bb700853D36994DA19F830b6Ad18455C",
        long_collateral_token=_ARB_LINK,
        short_collateral_token=_ARB_USDC,
    ),
    "arb": GMXMarket(
        alias="arb",
        chain="arbitrum",
        market_address="0xC25cEf6061Cf5dE5eb761b50E4743c1F5D7E5407",
        long_collateral_token=_ARB_ARB,
        short_collateral_token=_ARB_USDC,
    ),
    # NOTE: wsteth was a GMX V2 Arbitrum market in earlier versions but
    # has been delisted — Reader.getMarket() returns a zero-struct for
    # 0x0Cf1fb4d1FF67A3D8Ca92c9d6643F8F9be8e03E4 against the current
    # Reader 0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789 (verified
    # 2026-05-20). Removed to prevent G2 from polling a ghost market.
    # If GMX re-adds wsteth as a perp, repopulate with the new address.

    # DOGE + XRP are synthetic-index markets on GMX V2 Arbitrum — no native
    # ERC20 for the index. Long/short collateral is WETH/USDC per
    # synthetics config (gmx-io/gmx-interface sdk/src/configs/markets.ts).
    # Verified deployed + alive via Reader.getMarket() on Arbitrum
    # mainnet 2026-05-20 against the current Reader 0x470f…0789.
    "doge": GMXMarket(
        alias="doge",
        chain="arbitrum",
        market_address="0x6853EA96FF216fAb11D2d930CE3C508556A4bdc4",
        long_collateral_token=_ARB_WETH,
        short_collateral_token=_ARB_USDC,
    ),
    "xrp": GMXMarket(
        alias="xrp",
        chain="arbitrum",
        market_address="0x0CCB4fAa6f1F1B30911619f1184082aB4E25813c",
        long_collateral_token=_ARB_WETH,
        short_collateral_token=_ARB_USDC,
    ),
}


# ─── Avalanche (chainId 43114) ─────────────────────────────────────────


_AVAX_WAVAX = "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"
_AVAX_WBTCB = "0x152b9d0fdc40c096757f570a51e494bd4b943e50"
_AVAX_WETHE = "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab"
_AVAX_USDC = "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"


AVALANCHE_MARKETS: dict[str, GMXMarket] = {
    "btc": GMXMarket(
        alias="btc",
        chain="avalanche",
        market_address="0xFb02132333A79C8B5Bd0b64E3AbccA5f7fAf2937",
        long_collateral_token=_AVAX_WBTCB,
        short_collateral_token=_AVAX_USDC,
    ),
    "eth": GMXMarket(
        alias="eth",
        chain="avalanche",
        market_address="0xBb84D79159D6bBE1DE148Dc82640CaA677e06126",
        long_collateral_token=_AVAX_WETHE,
        short_collateral_token=_AVAX_USDC,
    ),
    "avax": GMXMarket(
        alias="avax",
        chain="avalanche",
        market_address="0x913C1F46b48b3eD35E7dc3Cf754d4ae8499F31CF",
        long_collateral_token=_AVAX_WAVAX,
        short_collateral_token=_AVAX_USDC,
    ),
}


MARKETS_BY_CHAIN: dict[str, dict[str, GMXMarket]] = {
    "arbitrum": ARBITRUM_MARKETS,
    "avalanche": AVALANCHE_MARKETS,
}


def market_for(chain: str, alias: str) -> GMXMarket | None:
    """Pure: lookup. Returns None when (chain, alias) unknown."""
    return MARKETS_BY_CHAIN.get(chain, {}).get(alias)


def collateral_token_for(
    chain: str,
    alias: str,
    *,
    is_long: bool,
) -> str:
    """Pure: collateral token address for a (chain, alias, side) position.

    Returns empty string when market unknown. Caller refuses live-fire
    on empty-string collateral.
    """
    m = market_for(chain, alias)
    if m is None:
        return ""
    return m.long_collateral_token if is_long else m.short_collateral_token


__all__ = [
    "GMXMarket",
    "ARBITRUM_MARKETS",
    "AVALANCHE_MARKETS",
    "MARKETS_BY_CHAIN",
    "market_for",
    "collateral_token_for",
]
