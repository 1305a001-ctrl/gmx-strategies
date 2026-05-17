"""GMX V2 contract addresses + minimal ABIs for liquidation execution.

The liquidation flow on GMX V2 differs from Aave/Compound. The user
doesn't call a single `liquidate()` function. Instead, liquidations
happen through the OrderHandler + ReferralStorage system:

  1. Keeper calls `Liquidations.executeLiquidation(account, market,
     collateralToken, isLong, oracleParams)` on the LiquidationHandler.
  2. Inside the call, the contract pulls oracle prices from the
     oracleParams blob (signed Chainlink Data Streams reports).
  3. The position is marked under water; collateral is seized; the
     keeper receives the liquidation fee.

Per-chain deployments are stable across versions; double-check the
addresses against https://github.com/gmx-io/gmx-synthetics/tree/main/deployments
before any live flip.
"""
from __future__ import annotations

from typing import Any


# Arbitrum mainnet (chain_id=42161) — primary deployment.
ARBITRUM_CONTRACTS: dict[str, str] = {
    "exchange_router":      "0x900173A66dbD345006C51fA35fA3aB760FcD843b",
    "liquidation_handler":  "0xdAb9bA9e3a301CCb353f18B4C8542BA2149E4010",
    "order_handler":        "0xB0Fc2a48b873da40e7bc25658e5E6137616AC2Ee",
    "reader":               "0xf60becbba223EEA9495Da3f606753867eC10d139",
    "data_store":           "0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8",
    "oracle":               "0x918b60ba71badfada72ef3a6c6f71d0fdd07976d",
    "event_emitter":        "0xC8ee91A54287Db53897056e12D9819156D3822Fb",
}

# Avalanche mainnet (chain_id=43114) — secondary, lower volume.
AVALANCHE_CONTRACTS: dict[str, str] = {
    "exchange_router":      "0x79be2F4eC8A4143BaF963206cF133f3710856D0a",
    "liquidation_handler":  "0xC22A2F5e1FaCC4D24bf3Be51c4B7BAcD06A8b58A",
    "order_handler":        "0x352f684ab9e97a6321a13CF03A61316B681D9fD2",
    "reader":               "0xd92BFFB6D2cFe61c0eb44a1F18816725EBaFf8aD",
    "data_store":           "0x2F0b22339414ADeD7D5F06f9D604c7fF5b2fe3f6",
    "oracle":               "0x4ad5f3aedf7Cf2099D45fdcc4F02C24deDfc3A38",
    "event_emitter":        "0xDb17B211c34240B014ab6d61d4A31FA0C0e20c26",
}


CONTRACTS_BY_CHAIN: dict[str, dict[str, str]] = {
    "arbitrum":  ARBITRUM_CONTRACTS,
    "avalanche": AVALANCHE_CONTRACTS,
}


def contract_for(chain: str, name: str) -> str:
    """Pure: lookup contract address. Empty string if unknown."""
    return CONTRACTS_BY_CHAIN.get(chain, {}).get(name, "")


# Minimal LiquidationHandler ABI — only the function we need.
# Full ABI is large; we vendor just the slice we call.
#
# executeLiquidation params:
#   account          — position owner address
#   market           — GMX market contract (synthetic market token)
#   collateralToken  — token used as collateral (WETH/USDC/etc)
#   isLong           — true for long, false for short
#   oracleParams     — packed Chainlink Data Streams reports
LIQUIDATION_HANDLER_ABI: list[dict[str, Any]] = [{
    "inputs": [
        {"internalType": "address", "name": "account",         "type": "address"},
        {"internalType": "address", "name": "market",          "type": "address"},
        {"internalType": "address", "name": "collateralToken", "type": "address"},
        {"internalType": "bool",    "name": "isLong",          "type": "bool"},
        {
            "components": [
                {"internalType": "address[]", "name": "tokens",                 "type": "address[]"},
                {"internalType": "address[]", "name": "providers",              "type": "address[]"},
                {"internalType": "bytes[]",   "name": "data",                   "type": "bytes[]"},
            ],
            "internalType": "struct OracleUtils.SetPricesParams",
            "name": "oracleParams",
            "type": "tuple",
        },
    ],
    "name": "executeLiquidation",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function",
}]


__all__ = [
    "ARBITRUM_CONTRACTS",
    "AVALANCHE_CONTRACTS",
    "CONTRACTS_BY_CHAIN",
    "LIQUIDATION_HANDLER_ABI",
    "contract_for",
]
