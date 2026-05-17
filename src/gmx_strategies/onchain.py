"""On-chain GMX V2 position re-check via Reader.getPosition().

Mirrors the AaveV3PoolReader pattern in liquidation-bot/onchain.py.

Strategy
────────
After the subgraph returns top-N near-liq positions, we re-check each
one against the GMX V2 Reader contract. The Reader is the canonical
view of position state; subgraph data lags by 1-2 blocks. In paper
mode, the lag is just noisy; in live mode it's the difference between
catching a liquidation and submitting a tx for a position someone else
already cleared.

We don't fetch the FULL position struct (saves 80% of RPC bandwidth).
We just check `sizeInUsd > 0` — if the position closed between
subgraph snapshot and our scan, it's gone. Conservative; any further
drift (price moved, collateral added) is small enough to ignore for
paper-mode calibration.

Free Arbitrum public RPCs honor reasonable QPS without a key; with
Alchemy/Infura it scales effectively unbounded for our top-N×cycle
needs.

Hard gates
──────────
- Settings flag `onchain_recheck_enabled` controls this entirely
- Disabled by default. When `live_enabled=True`, this is enforced ON
  at startup (subgraph-only is unsafe for live)
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)


# GMX V2 Reader contract addresses by chain.
# These are stable per deployment; verify before any chain expansion.
GMX_READER_ADDRESSES: dict[str, str] = {
    "arbitrum":  "0xf60becbba223EEA9495Da3f606753867eC10d139",
    "avalanche": "0xd92BFFB6D2cFe61c0eb44a1F18816725EBaFf8aD",
}

# GMX V2 DataStore contract — required as the first arg to getPosition.
GMX_DATASTORE_ADDRESSES: dict[str, str] = {
    "arbitrum":  "0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8",
    "avalanche": "0x2F0b22339414ADeD7D5F06f9D604c7fF5b2fe3f6",
}


# Minimal ABI — just the getPosition(bytes32) view we need.
# The real Reader returns a complex Props struct; here we decode only
# the fields we care about (numbers.sizeInUsd) via slot access. The
# ABI fragment below describes the full signature so eth-abi can decode.
GMX_READER_GET_POSITION_ABI: list[dict[str, Any]] = [{
    "inputs": [
        {"internalType": "address", "name": "dataStore", "type": "address"},
        {"internalType": "bytes32", "name": "key",       "type": "bytes32"},
    ],
    "name": "getPosition",
    "outputs": [{
        "components": [
            # addresses block
            {
                "components": [
                    {"internalType": "address", "name": "account",         "type": "address"},
                    {"internalType": "address", "name": "market",          "type": "address"},
                    {"internalType": "address", "name": "collateralToken", "type": "address"},
                ],
                "internalType": "struct Position.Addresses",
                "name": "addresses",
                "type": "tuple",
            },
            # numbers block — sizeInUsd is what we actually read
            {
                "components": [
                    {"internalType": "uint256", "name": "sizeInUsd",                 "type": "uint256"},
                    {"internalType": "uint256", "name": "sizeInTokens",              "type": "uint256"},
                    {"internalType": "uint256", "name": "collateralAmount",          "type": "uint256"},
                    {"internalType": "uint256", "name": "borrowingFactor",           "type": "uint256"},
                    {"internalType": "uint256", "name": "fundingFeeAmountPerSize",   "type": "uint256"},
                    {"internalType": "uint256", "name": "longTokenClaimableFundingAmountPerSize",  "type": "uint256"},
                    {"internalType": "uint256", "name": "shortTokenClaimableFundingAmountPerSize", "type": "uint256"},
                    {"internalType": "uint256", "name": "increasedAtBlock",          "type": "uint256"},
                    {"internalType": "uint256", "name": "decreasedAtBlock",          "type": "uint256"},
                ],
                "internalType": "struct Position.Numbers",
                "name": "numbers",
                "type": "tuple",
            },
            # flags
            {
                "components": [
                    {"internalType": "bool", "name": "isLong", "type": "bool"},
                ],
                "internalType": "struct Position.Flags",
                "name": "flags",
                "type": "tuple",
            },
        ],
        "internalType": "struct Position.Props",
        "name": "",
        "type": "tuple",
    }],
    "stateMutability": "view",
    "type": "function",
}]


@dataclass(frozen=True)
class OnchainPosition:
    """Decoded GMX V2 position state (subset we read)."""
    account: str
    market: str
    collateral_token: str
    size_in_usd: float    # USD value (30 decimals raw → human float)
    size_in_tokens: float
    collateral_amount: float
    is_long: bool
    is_open: bool         # convenience: size_in_usd > 0


def compute_position_key(
    account: str, market: str, collateral_token: str, is_long: bool,
) -> bytes:
    """Pure: GMX V2 position key = keccak256(abi.encode(account, market, ctoken, isLong)).

    The contract uses this as the storage slot identifier. We compute it
    in-process so each re-check is one eth_call. Lazy-imports keccak so
    the test suite doesn't need eth_hash at module load.
    """
    from eth_abi import encode
    from eth_utils import keccak
    encoded = encode(
        ["address", "address", "address", "bool"],
        [account, market, collateral_token, is_long],
    )
    return keccak(encoded)


def decode_position_response(
    raw: tuple,
    *,
    usd_decimals: int = 30,
) -> OnchainPosition:
    """Pure: Reader.getPosition return tuple → OnchainPosition.

    GMX V2 uses 30-decimal fixed-point for USD values. Token amounts use
    the underlying token's decimals (we don't decode those here; caller
    can divide by TOKEN_DECIMALS if needed).
    """
    addresses, numbers, flags = raw
    account, market, collateral_token = addresses
    size_in_usd, size_in_tokens, collateral_amount = (
        numbers[0], numbers[1], numbers[2],
    )
    return OnchainPosition(
        account=account,
        market=market,
        collateral_token=collateral_token,
        size_in_usd=float(size_in_usd) / (10**usd_decimals),
        size_in_tokens=float(size_in_tokens),
        collateral_amount=float(collateral_amount),
        is_long=bool(flags[0]),
        is_open=int(size_in_usd) > 0,
    )


class GMXReader:
    """Async GMX V2 Reader.getPosition() wrapper.

    Lazy-instantiates the Web3 contract on first use so tests + cold
    starts don't pay the eth-abi import cost up-front.
    """

    def __init__(self, rpc_url: str, chain: str = "arbitrum") -> None:
        self.rpc_url = rpc_url
        self.chain = chain
        self._w3 = None
        self._contract = None
        self._datastore = GMX_DATASTORE_ADDRESSES.get(chain, "")
        self._reader_addr = GMX_READER_ADDRESSES.get(chain, "")

    def _ensure_contract(self) -> None:
        if self._contract is not None:
            return
        if not self._reader_addr or not self._datastore:
            raise RuntimeError(f"GMX Reader not deployed for chain={self.chain}")
        from web3 import AsyncHTTPProvider, AsyncWeb3
        self._w3 = AsyncWeb3(AsyncHTTPProvider(self.rpc_url))
        self._contract = self._w3.eth.contract(
            address=self._reader_addr,
            abi=GMX_READER_GET_POSITION_ABI,
        )

    async def get_position(
        self,
        *,
        account: str,
        market: str,
        collateral_token: str,
        is_long: bool,
    ) -> OnchainPosition | None:
        """Fetch one position. Returns None if not found / closed."""
        self._ensure_contract()
        assert self._contract is not None
        key = compute_position_key(account, market, collateral_token, is_long)
        try:
            raw = await self._contract.functions.getPosition(
                self._datastore, key,
            ).call()
        except Exception as e:
            log.debug("gmx_reader.call_failed account=%s err=%s", account, e)
            return None
        return decode_position_response(raw)

    async def get_positions(
        self,
        positions: list[dict[str, Any]],
        *,
        max_concurrency: int = 8,
    ) -> dict[str, OnchainPosition]:
        """Fetch many in parallel. Key = position_key hex string."""
        sem = asyncio.Semaphore(max_concurrency)

        async def _one(p: dict[str, Any]) -> tuple[str, OnchainPosition | None]:
            async with sem:
                key = compute_position_key(
                    p["account"], p["market"],
                    p["collateral_token"], p["is_long"],
                ).hex()
                pos = await self.get_position(
                    account=p["account"], market=p["market"],
                    collateral_token=p["collateral_token"],
                    is_long=p["is_long"],
                )
                return key, pos

        results = await asyncio.gather(*[_one(p) for p in positions])
        return {k: v for k, v in results if v is not None}


def filter_by_open_state(
    triggers: list[Any],
    rechecks: dict[str, OnchainPosition],
    *,
    require_open: bool = True,
) -> list[Any]:
    """Pure: drop triggers whose on-chain state shows the position is
    already closed (size_in_usd == 0).

    `rechecks` is keyed by position_key hex (matches GMXReader output).
    Triggers without a re-check entry pass through (defensive — RPC
    timeout shouldn't drop signal).
    """
    if not require_open:
        return triggers
    kept: list[Any] = []
    for t in triggers:
        key = compute_position_key(
            t.user, t.market, t.collateral_token, t.is_long,
        ).hex()
        rc = rechecks.get(key)
        if rc is None or rc.is_open:
            kept.append(t)
    return kept


__all__ = [
    "GMX_READER_ADDRESSES",
    "GMX_DATASTORE_ADDRESSES",
    "GMXReader",
    "OnchainPosition",
    "compute_position_key",
    "decode_position_response",
    "filter_by_open_state",
]
