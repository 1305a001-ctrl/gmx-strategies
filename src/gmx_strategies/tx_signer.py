"""Sign + submit the liquidation tx, then wait for receipt.

Three responsibilities, each a discrete function:
  - estimate_gas(): RPC eth_estimateGas + bump by safety factor
  - sign_tx():       sign the dict with the operator's private key
  - submit_and_wait(): eth_sendRawTransaction → wait → return receipt

Each function fail-fast on its specific failure mode so the caller can
discriminate (gas_estimation_failed vs sign_failed vs broadcast_failed
vs revert).
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)


# Multiplier applied to gas estimate. Liquidation paths sometimes consume
# more than the simulation suggests due to oracle params and event emits.
GAS_ESTIMATE_SAFETY_BUMP = 1.5


@dataclass(frozen=True)
class SignedTx:
    """Result of sign_tx — raw_transaction is the bytes to broadcast."""
    raw_transaction: bytes
    tx_hash: bytes


@dataclass(frozen=True)
class Receipt:
    """Subset of the eth_getTransactionReceipt fields we use."""
    tx_hash: str
    block_number: int
    status: int           # 1 = success, 0 = revert
    gas_used: int
    effective_gas_price: int    # wei
    revert_reason: str = ""


async def estimate_gas(*, rpc_url: str, tx: dict[str, Any]) -> int:
    """Async: RPC eth_estimateGas, return bumped value.

    Raises RuntimeError on estimation failure (revert, network, etc.).
    """
    from web3 import AsyncHTTPProvider, AsyncWeb3
    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
    try:
        raw = await w3.eth.estimate_gas({
            "from":  tx["from"],
            "to":    tx["to"],
            "data":  tx["data"],
            "value": tx.get("value", 0),
        })
    except Exception as e:
        raise RuntimeError(f"gas_estimation_failed: {e}") from e
    bumped = int(raw * GAS_ESTIMATE_SAFETY_BUMP)
    log.info("gmx.gas_estimate raw=%d bumped=%d", raw, bumped)
    return bumped


def sign_tx(*, tx: dict[str, Any], private_key: str) -> SignedTx:
    """Sign with the operator's key. Synchronous (web3.py signing is sync).

    The private_key must be supplied by the caller; we don't load it here
    so no module-level secret read is possible.
    """
    if not private_key:
        raise RuntimeError("private_key empty — refusing to sign")
    from eth_account import Account
    try:
        signed = Account.sign_transaction(tx, private_key)
    except Exception as e:
        raise RuntimeError(f"sign_failed: {e}") from e
    return SignedTx(
        raw_transaction=signed.raw_transaction,
        tx_hash=signed.hash,
    )


async def submit_and_wait(
    *,
    rpc_url: str,
    signed: SignedTx,
    timeout_sec: float = 60.0,
    poll_interval_sec: float = 1.0,
) -> Receipt:
    """Async: broadcast + poll for receipt.

    Returns Receipt on inclusion (status=1 OR status=0; caller checks).
    Raises RuntimeError on broadcast failure or timeout.
    """
    from web3 import AsyncHTTPProvider, AsyncWeb3
    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
    try:
        tx_hash = await w3.eth.send_raw_transaction(signed.raw_transaction)
    except Exception as e:
        raise RuntimeError(f"broadcast_failed: {e}") from e

    tx_hash_str = tx_hash.hex()
    log.info("gmx.tx_broadcasted hash=%s", tx_hash_str)

    deadline = asyncio.get_event_loop().time() + timeout_sec
    while asyncio.get_event_loop().time() < deadline:
        try:
            rcpt = await w3.eth.get_transaction_receipt(tx_hash)
        except Exception:
            rcpt = None
        if rcpt is not None:
            revert_reason = ""
            if int(rcpt.get("status", 0)) == 0:
                revert_reason = await _try_extract_revert_reason(
                    w3, tx_hash_str, rcpt,
                )
            return Receipt(
                tx_hash=tx_hash_str,
                block_number=int(rcpt.get("blockNumber", 0)),
                status=int(rcpt.get("status", 0)),
                gas_used=int(rcpt.get("gasUsed", 0)),
                effective_gas_price=int(rcpt.get("effectiveGasPrice", 0)),
                revert_reason=revert_reason,
            )
        await asyncio.sleep(poll_interval_sec)

    raise RuntimeError(
        f"receipt_timeout hash={tx_hash_str} after {timeout_sec}s",
    )


async def _try_extract_revert_reason(w3: Any, tx_hash: str, rcpt: dict) -> str:
    """Try to extract revert reason by replaying the tx as a call.

    Best-effort; on any failure returns empty string. The receipt status=0
    is the authoritative signal; the reason is just for log color.
    """
    try:
        tx = await w3.eth.get_transaction(tx_hash)
        await w3.eth.call({
            "from":  tx["from"],
            "to":    tx["to"],
            "data":  tx["input"],
            "value": tx.get("value", 0),
        }, block_identifier=rcpt.get("blockNumber", "latest"))
    except Exception as e:
        return str(e)[:200]
    return ""


__all__ = [
    "SignedTx",
    "Receipt",
    "GAS_ESTIMATE_SAFETY_BUMP",
    "estimate_gas",
    "sign_tx",
    "submit_and_wait",
]
