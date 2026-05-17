"""GMX V2 liquidation-tx builder.

Builds the unsigned transaction dict for `LiquidationHandler.executeLiquidation`.
Pure function — does NOT broadcast, does NOT sign. Consumed by tx_signer.py
which handles those steps.

Oracle params
─────────────
GMX V2 requires fresh Chainlink Data Streams reports passed inline with the
tx. The strategy-runners chainlink-streams service maintains live reports
in Redis at `chainlink:<alias>:reports`. The builder reads the most-recent
report per asset, decodes the signed blob, and packs it into the
SetPricesParams tuple expected by the contract.

Gas + fees
──────────
- Arbitrum liquidations average ~250-400k gas. Bumping by 1.5x for
  safety margin.
- We use EIP-1559 (maxFeePerGas + maxPriorityFeePerGas) for predictable
  inclusion. Priority fee defaults to Arbitrum's recommended 0.01 gwei.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from gmx_strategies import contracts

log = logging.getLogger(__name__)


# Conservative gas defaults for Arbitrum liquidations. Operator tunes per
# observed receipts (set high initially so we don't fail under-priced).
DEFAULT_GAS_LIMIT = 600_000          # 1.5x typical liquidate observed
DEFAULT_MAX_FEE_GWEI = 0.1           # arbitrum is cheap
DEFAULT_PRIORITY_FEE_GWEI = 0.01     # arbitrum baseline


@dataclass(frozen=True)
class OracleReport:
    """One Chainlink Data Streams report blob.

    `token` is the asset's contract address (not alias) because the
    OracleUtils contract uses addresses, not symbols.
    `provider` is the Chainlink verifier proxy address.
    `data` is the raw signed report bytes from chainlink-streams.
    """
    token: str
    provider: str
    data: bytes


@dataclass(frozen=True)
class LiquidationTxRequest:
    """All inputs needed to build a liquidate tx — pure data."""
    chain: str
    account: str
    market: str
    collateral_token: str
    is_long: bool
    oracle_reports: tuple[OracleReport, ...]
    # Network / wallet context
    nonce: int
    chain_id: int
    sender_address: str


def _hex_to_bytes(s: str) -> bytes:
    """Pure: convert 0x... hex string to bytes. Empty/invalid → b''."""
    if not s:
        return b""
    s = s[2:] if s.startswith("0x") else s
    try:
        return bytes.fromhex(s)
    except ValueError:
        return b""


def _decode_chainlink_report(payload: str) -> bytes | None:
    """Pure: decode a JSON-wrapped chainlink:<alias>:reports stream entry.

    Strategy-runners chainlink-streams publishes:
      {"price": "...", "report_blob": "0x...", "verifier_proxy": "0x...", ...}
    We extract report_blob as bytes for the oracleParams call.
    """
    if not payload:
        return None
    try:
        d = json.loads(payload)
    except (TypeError, ValueError):
        return None
    if not isinstance(d, dict):
        return None
    blob_hex = d.get("report_blob") or d.get("blob")
    if not isinstance(blob_hex, str):
        return None
    blob = _hex_to_bytes(blob_hex)
    return blob if blob else None


def pack_oracle_params(reports: tuple[OracleReport, ...]) -> tuple[list, list, list]:
    """Pure: pack OracleReport list → (tokens[], providers[], data[]) tuple.

    Matches the SetPricesParams struct in the LIQUIDATION_HANDLER_ABI.
    Returns three parallel arrays so eth_abi can encode the tuple.
    """
    return (
        [r.token for r in reports],
        [r.provider for r in reports],
        [r.data for r in reports],
    )


def build_liquidation_tx(req: LiquidationTxRequest) -> dict[str, Any]:
    """Pure: build the unsigned tx dict.

    The result can be:
      - signed by web3.py's `Account.sign_transaction(tx, key)`
      - submitted via `eth_sendRawTransaction`

    No I/O, no chain queries here — caller supplies nonce + chain_id.
    """
    handler = contracts.contract_for(req.chain, "liquidation_handler")
    if not handler:
        raise ValueError(f"no liquidation_handler address for chain={req.chain}")

    # Encode the function call data using eth_abi (lazy import to keep
    # module load cheap when only the dataclasses are needed).
    from eth_abi import encode
    from eth_utils import function_signature_to_4byte_selector, to_checksum_address

    selector = function_signature_to_4byte_selector(
        "executeLiquidation(address,address,address,bool,(address[],address[],bytes[]))",
    )
    tokens, providers, blobs = pack_oracle_params(req.oracle_reports)
    encoded_args = encode(
        ["address", "address", "address", "bool", "(address[],address[],bytes[])"],
        [
            to_checksum_address(req.account),
            to_checksum_address(req.market),
            to_checksum_address(req.collateral_token),
            req.is_long,
            (
                [to_checksum_address(t) for t in tokens],
                [to_checksum_address(p) for p in providers],
                blobs,
            ),
        ],
    )
    calldata = selector + encoded_args

    tx: dict[str, Any] = {
        "type":                 2,  # EIP-1559
        "chainId":              req.chain_id,
        "from":                 to_checksum_address(req.sender_address),
        "to":                   to_checksum_address(handler),
        "nonce":                int(req.nonce),
        "gas":                  DEFAULT_GAS_LIMIT,
        "maxFeePerGas":         int(DEFAULT_MAX_FEE_GWEI * 10**9),
        "maxPriorityFeePerGas": int(DEFAULT_PRIORITY_FEE_GWEI * 10**9),
        "value":                0,
        "data":                 "0x" + calldata.hex(),
    }
    return tx


# Chain ID lookup for the EIP-1559 chainId field.
CHAIN_IDS: dict[str, int] = {
    "arbitrum":  42161,
    "avalanche": 43114,
}


def chain_id_for(chain: str) -> int:
    """Pure: chain alias → numeric chain id, 0 if unknown."""
    return CHAIN_IDS.get(chain, 0)


__all__ = [
    "OracleReport",
    "LiquidationTxRequest",
    "DEFAULT_GAS_LIMIT",
    "DEFAULT_MAX_FEE_GWEI",
    "DEFAULT_PRIORITY_FEE_GWEI",
    "pack_oracle_params",
    "build_liquidation_tx",
    "chain_id_for",
    "CHAIN_IDS",
]
