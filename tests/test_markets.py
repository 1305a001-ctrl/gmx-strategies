"""Market metadata + collateral lookup tests."""
from __future__ import annotations

from gmx_strategies import markets


def test_arbitrum_markets_have_btc_eth_sol():
    for alias in ("btc", "eth", "sol"):
        m = markets.market_for("arbitrum", alias)
        assert m is not None
        assert m.market_address.startswith("0x")
        assert m.long_collateral_token.startswith("0x")
        assert m.short_collateral_token.startswith("0x")


def test_market_for_unknown_chain():
    assert markets.market_for("solana", "btc") is None


def test_market_for_unknown_alias():
    assert markets.market_for("arbitrum", "made_up") is None


def test_collateral_token_for_long_uses_long_collateral():
    eth_market = markets.market_for("arbitrum", "eth")
    assert eth_market is not None
    tok = markets.collateral_token_for("arbitrum", "eth", is_long=True)
    assert tok == eth_market.long_collateral_token


def test_collateral_token_for_short_uses_short_collateral():
    eth_market = markets.market_for("arbitrum", "eth")
    assert eth_market is not None
    tok = markets.collateral_token_for("arbitrum", "eth", is_long=False)
    assert tok == eth_market.short_collateral_token


def test_collateral_token_for_unknown_returns_empty():
    assert markets.collateral_token_for("arbitrum", "made_up", is_long=True) == ""
    assert markets.collateral_token_for("solana", "btc", is_long=False) == ""


def test_short_collateral_is_usdc_for_arbitrum_eth():
    """Short positions on Arbitrum ETH should be backed by USDC."""
    tok = markets.collateral_token_for("arbitrum", "eth", is_long=False)
    assert tok.lower() == "0xaf88d065e77c8cc2239327c5edb3a432268e5831"


def test_avalanche_has_avax_market():
    """Avalanche-native asset has a market."""
    m = markets.market_for("avalanche", "avax")
    assert m is not None
    assert m.alias == "avax"
