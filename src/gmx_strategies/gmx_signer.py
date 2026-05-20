"""GMX V2 transaction signing + (gated) submission — G5.2.

This module layers ON TOP of `gmx_order_encoder` (G5.1). The encoder
produces the calldata + the `eth_call` simulation harness; this module
adds the next two layers:

  1. SIGNING — wrap the multicall calldata in an EIP-1559 transaction,
     populate nonce / fees / gas / value, and sign with the operator's
     private key via `eth_account.Account.sign_transaction()`.

  2. SUBMISSION (GATED) — broadcast via `eth_sendRawTransaction`, but
     ONLY if BOTH `settings.live_gmx_enabled is True` AND the caller
     passes `dry_run=False` AND the signed-tx `from` matches the loaded
     key's derived address AND the intent's `account` matches it too.
     Any gate failing → falls back to `eth_call` simulation; nothing
     touches mempool. Paper-safe by default.

DESIGN NOTES (READ BEFORE TOUCHING):

  - **The private key NEVER appears in logs, exceptions, or returned
    dicts.** The module loads it ONCE at first call (lazily) into a
    module-level `_KEY` variable; `get_executor_address()` derives the
    EOA but never exposes the key. If you change this module, run
    `pytest -q tests/test_gmx_signer.py 2>&1 | grep -iE "0x[0-9a-f]{60,}"`
    and confirm zero hits.

  - **The reusable-infrastructure mandate**: G5.2 ships the signing
    capability but does NOT wire it into any runtime. The funding-arb
    backtest at memory/research_funding_arb_backtest_30d.md killed the
    thesis on all 5 markets — see PR description. A future strategy
    (G7.x) will be the first consumer of `submit_signed`.

  - **`live_gmx_enabled` is the only live-broadcast gate.** It replaces
    the vestigial `live_enabled` boolean that was in `settings.py` but
    never referenced anywhere. Per-venue gates (GMX vs Binance) are
    explicit; flipping one does not enable the other.

GATE MATRIX (`submit_signed`):

    | live_gmx_enabled | dry_run | key present | account match | broadcast? |
    |------------------|---------|-------------|---------------|------------|
    | False            | any     | any         | any           | NO (sim)   |
    | True             | True    | any         | any           | NO (sim)   |
    | True             | False   | False       | n/a           | RuntimeErr |
    | True             | False   | True        | False         | NO (sim)   |
    | True             | False   | True        | True          | YES        |

KEY LOADING precedence (highest first):

  1. File at `settings.gmx_executor_key_path` (default
     `/srv/secrets/gmx_executor_key`). Operator provisioning convention:
     `chown root:root && chmod 0400`. Whitespace stripped on read; empty
     file is rejected.

  2. Environment variable `GMX_EXECUTOR_KEY` (uppercase). Dev convenience
     ONLY — production deploys MUST use the file path. The env path
     exists so a local dev shell can run the smoke CLI without writing
     to /srv/secrets.

  3. Neither present → module-level `_KEY = None`. Functions that need
     it raise `RuntimeError` with a clear pointer at both provisioning
     mechanisms.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

import httpx
from eth_account import Account
from eth_account.signers.local import LocalAccount

from gmx_strategies import gmx_order_encoder
from gmx_strategies.gmx_order_encoder import OrderIntent, SimulationResult
from gmx_strategies.settings import settings

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────
# Constants — fee multiplier ceiling from audit Q3 (verified on-chain
# 2026-05-20 against Arbitrum DataStore: MAX_EXECUTION_FEE_MULTIPLIER_FACTOR
# = 100 * 10**30 in the 30-decimal fixed-point convention, i.e. 100x).
# We never let the buffered execution fee exceed this multiple of the
# audit-baseline fee — anything above that screams keeper-budget bug.
# ──────────────────────────────────────────────────────────────────────────
MAX_EXECUTION_FEE_MULTIPLIER_FACTOR = 100  # documented for future use; not enforced here

# Module-level key cache (lazy-loaded). Never logged, never returned, never
# embedded in exception messages. See `_load_key_once` for the load path.
_KEY: str | None = None
_KEY_LOADED: bool = False  # distinguishes "never tried" from "tried and not found"


# ──────────────────────────────────────────────────────────────────────────
# Frozen dataclass — module's public result type
# ──────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SendResult:
    """Outcome of `submit_signed` — captures sim or broadcast result.

    - `submitted=True` → broadcast actually went out (all gates cleared).
      `tx_hash` and (best-effort) receipt fields populated.

    - `submitted=False` → either explicit dry-run, gate-fail, or transport
      error. `dry_run_simulation` carries the `eth_call` result. `error`
      is set when a gate refused (e.g. account mismatch).
    """

    submitted: bool
    dry_run_simulation: SimulationResult | None
    tx_hash: str | None
    block_number: int | None
    gas_used: int | None
    status: int | None  # 1 = success, 0 = revert
    error: str | None


# ──────────────────────────────────────────────────────────────────────────
# Key loading — module-private
# ──────────────────────────────────────────────────────────────────────────


def _load_key_once() -> str | None:
    """Lazy-load the executor key. Idempotent; safe to call repeatedly.

    Reads from (in priority order):
      1. File at `settings.gmx_executor_key_path`
      2. Env var `GMX_EXECUTOR_KEY`
      3. Returns None if neither is present.

    NEVER logs the key (only the source + the derived address by way of
    a later `get_executor_address()` call, which is what callers should
    rely on for confirmation). Truncated logging here would still leak
    bits via comparison — so don't.
    """
    global _KEY, _KEY_LOADED  # noqa: PLW0603 — module-private cache by design
    if _KEY_LOADED:
        return _KEY

    # Path 1: file
    key_path = Path(settings.gmx_executor_key_path)
    if key_path.is_file():
        try:
            raw = key_path.read_text().strip()
        except OSError as exc:
            # File exists but unreadable (permissions, etc). Surface the
            # PATH so the operator can fix it — but NOT the contents.
            log.warning(
                "gmx_signer.key_file_unreadable path=%s err=%s",
                key_path, exc.__class__.__name__,
            )
            raw = ""
        if raw:
            log.info("gmx_signer.key_loaded source=file path=%s", key_path)
            _KEY = raw if raw.startswith("0x") else "0x" + raw
            _KEY_LOADED = True
            return _KEY
        # File present but empty — log + fall through to env
        log.warning("gmx_signer.key_file_empty path=%s", key_path)

    # Path 2: env var (dev convenience)
    env_key = os.environ.get("GMX_EXECUTOR_KEY", "").strip()
    if env_key:
        log.info("gmx_signer.key_loaded source=env_var")
        _KEY = env_key if env_key.startswith("0x") else "0x" + env_key
        _KEY_LOADED = True
        return _KEY

    # Path 3: nothing — record that we tried so we don't re-stat the file every call
    _KEY_LOADED = True
    _KEY = None
    return None


def _reset_key_cache_for_tests() -> None:
    """Test-only: clear the module-level key cache between tests.

    Production code MUST NOT call this. Tests that monkeypatch the key
    source (env var, settings.gmx_executor_key_path) call this first to
    force a re-read on the next `_load_key_once()`.
    """
    global _KEY, _KEY_LOADED  # noqa: PLW0603
    _KEY = None
    _KEY_LOADED = False


def _local_account() -> LocalAccount | None:
    """Pure-ish: return a LocalAccount from the loaded key, or None.

    `LocalAccount` carries the private key in-memory but its public surface
    (`.address`) is the only thing we ever expose externally.
    """
    key = _load_key_once()
    if key is None:
        return None
    return Account.from_key(key)


# ──────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────


def get_executor_address() -> str | None:
    """Return the EOA address derived from the loaded key, or None.

    NEVER returns or logs the key itself. If no key is configured, returns
    None — callers should treat that as "signing path unavailable, fall
    back to encoder simulation only".
    """
    acct = _local_account()
    if acct is None:
        return None
    return acct.address


def _require_key_or_raise() -> LocalAccount:
    """Internal: load + return LocalAccount, or raise RuntimeError.

    The error message names BOTH provisioning mechanisms so the operator
    has a clear fix path. The message NEVER includes a partial key.
    """
    acct = _local_account()
    if acct is None:
        raise RuntimeError(
            "GMX_EXECUTOR_KEY not configured — see "
            "/srv/secrets/gmx_executor_key or GMX_EXECUTOR_KEY env",
        )
    return acct


async def _eth_call(
    *, rpc_url: str, method: str, params: list, client: httpx.AsyncClient,
) -> dict:
    """Internal: minimal JSON-RPC POST. Returns the parsed body or {}.

    Used for the dynamic reads (`eth_getTransactionCount`, `eth_gasPrice`,
    `eth_sendRawTransaction`, `eth_getTransactionReceipt`). The encoder's
    own `simulate_order` continues to handle `eth_call` for sims; we use
    a simpler local helper here because we need the raw body, not a
    SimulationResult.
    """
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        resp = await client.post(rpc_url, json=payload)
    except (httpx.HTTPError, httpx.TimeoutException) as exc:
        log.warning("gmx_signer.rpc_http_error method=%s err=%s", method, exc)
        return {}
    if resp.status_code != 200:
        log.warning("gmx_signer.rpc_bad_status method=%s status=%d", method, resp.status_code)
        return {}
    try:
        body = resp.json()
    except (ValueError, TypeError):
        log.warning("gmx_signer.rpc_bad_json method=%s", method)
        return {}
    if not isinstance(body, dict):
        return {}
    return body


def _build_eip1559_tx(
    *,
    intent: OrderIntent,
    chain_id: int,
    nonce: int,
    max_priority_fee_per_gas: int,
    max_fee_per_gas: int,
    gas_limit: int,
) -> dict:
    """Pure: assemble the EIP-1559 transaction dict ready for signing.

    Uses `gmx_order_encoder._encode_multicall(intent)` to produce calldata
    — keeps the encoder as the single source of truth for the multicall
    shape. `to` is ExchangeRouter; `value` is the execution fee (it gets
    forwarded as msg.value to sendWnt, which wraps it to WETH).
    """
    multicall_data = gmx_order_encoder._encode_multicall(intent)
    return {
        "type": 2,
        "chainId": chain_id,
        "nonce": nonce,
        "maxPriorityFeePerGas": max_priority_fee_per_gas,
        "maxFeePerGas": max_fee_per_gas,
        "gas": gas_limit,
        "to": settings.gmx_exchange_router_address_arbitrum,
        "value": intent.execution_fee_wei,
        "data": "0x" + multicall_data.hex(),
    }


async def sign_order(
    intent: OrderIntent,
    *,
    chain_id: int | None = None,
    rpc_url: str | None = None,
    client: httpx.AsyncClient | None = None,
    nonce: int | None = None,
) -> dict:
    """Build + sign an EIP-1559 tx for the createOrder multicall.

    Returns a dict (NOT a frozen dataclass — callers may want to inspect
    raw fields):
      {
        "raw":      "0x..." hex of the serialized signed tx,
        "hash":     "0x..." computed tx hash,
        "nonce":    int,
        "from":     "0x..." EOA address derived from the key,
        "tx_dict":  the EIP-1559 dict that was signed (no key inside),
      }

    NEVER returns the private key. NEVER logs it.

    Nonce / gas-price source:
      - If `nonce` is passed in (tests, or operator override), uses it.
      - Else queries `eth_getTransactionCount` against `rpc_url`.
      - `max_priority_fee_per_gas` = `settings.gmx_max_priority_fee_gwei * 1e9`.
      - `max_fee_per_gas` = current `eth_gasPrice` + the priority fee.
      - `gas_limit` = `INCREASE_ORDER_GAS_LIMIT` * (1 + safety_margin/100).

    Raises RuntimeError if no key is configured.
    """
    acct = _require_key_or_raise()
    eff_chain_id = chain_id if chain_id is not None else settings.gmx_chain_id_arbitrum
    eff_rpc = rpc_url if rpc_url is not None else settings.arbitrum_rpc_url

    own_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=httpx.Timeout(settings.gmx_reader_timeout_s))
        own_client = True
    try:
        # Resolve nonce
        if nonce is None:
            body = await _eth_call(
                rpc_url=eff_rpc,
                method="eth_getTransactionCount",
                params=[acct.address, "pending"],
                client=client,
            )
            nonce_hex = body.get("result")
            if not isinstance(nonce_hex, str):
                raise RuntimeError(
                    f"gmx_signer.sign_order: could not fetch nonce for {acct.address}",
                )
            nonce = int(nonce_hex, 16)

        # Resolve fees
        priority_fee_wei = int(settings.gmx_max_priority_fee_gwei * 1e9)
        body = await _eth_call(
            rpc_url=eff_rpc, method="eth_gasPrice", params=[], client=client,
        )
        gas_price_hex = body.get("result")
        if not isinstance(gas_price_hex, str):
            raise RuntimeError("gmx_signer.sign_order: could not fetch gas price")
        base_gas_price = int(gas_price_hex, 16)
        # `maxFeePerGas` must cover both base fee + priority. We use
        # current `gasPrice` as the cap; on Arbitrum this is close to
        # baseFee since priority fees are tiny (0.01 gwei).
        max_fee_per_gas = base_gas_price + priority_fee_wei

        # Gas limit — audit-verified gas limit + safety margin pct
        base_gas_limit = settings.gmx_increase_order_gas_limit
        margin_pct = settings.gmx_gas_limit_safety_margin_pct
        gas_limit = base_gas_limit * (100 + margin_pct) // 100

        # Build + sign
        tx_dict = _build_eip1559_tx(
            intent=intent,
            chain_id=eff_chain_id,
            nonce=nonce,
            max_priority_fee_per_gas=priority_fee_wei,
            max_fee_per_gas=max_fee_per_gas,
            gas_limit=gas_limit,
        )
        signed = acct.sign_transaction(tx_dict)

        # `signed.raw_transaction` and `.hash` are bytes; convert to hex
        # (NEVER include the key or partial-key derivatives in the dict).
        return {
            "raw": "0x" + signed.raw_transaction.hex(),
            "hash": "0x" + signed.hash.hex(),
            "nonce": nonce,
            "from": acct.address,
            "tx_dict": tx_dict,
        }
    finally:
        if own_client:
            await client.aclose()


async def simulate_signed(
    signed_tx: dict,
    *,
    rpc_url: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> SimulationResult:
    """Dry-run path: re-issue the signed tx's payload as an `eth_call`.

    Takes the `tx_dict` from `sign_order`'s return, builds an eth_call
    payload (from + to + value + data), and delegates to the encoder's
    response-body classifier. Never broadcasts.

    Returns a `SimulationResult` (same shape as `simulate_order`).
    """
    tx = signed_tx.get("tx_dict")
    if not isinstance(tx, dict):
        return SimulationResult(
            ok=False, revert_selector=None, revert_known_acceptable=False,
            revert_reason_name=None, raw_response=None,
        )

    eff_rpc = rpc_url if rpc_url is not None else settings.arbitrum_rpc_url

    # The `tx_dict` is a signing payload — convert to eth_call params.
    # `value` is an int in the dict; serialize to 0x-hex for the RPC.
    value = tx.get("value", 0)
    data = tx.get("data", "0x")
    payload = {
        "from": signed_tx.get("from"),
        "to": tx.get("to"),
        "value": hex(value) if isinstance(value, int) else str(value),
        "data": data,
    }
    rpc_payload = {
        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [payload, "latest"],
    }

    own_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=httpx.Timeout(settings.gmx_reader_timeout_s))
        own_client = True
    try:
        try:
            resp = await client.post(eff_rpc, json=rpc_payload)
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            log.warning("gmx_signer.sim_http_error err=%s", exc)
            return SimulationResult(
                ok=False, revert_selector=None, revert_known_acceptable=False,
                revert_reason_name=None, raw_response=None,
            )
        if resp.status_code != 200:
            return SimulationResult(
                ok=False, revert_selector=None, revert_known_acceptable=False,
                revert_reason_name=None, raw_response=None,
            )
        try:
            body = resp.json()
        except (ValueError, TypeError):
            return SimulationResult(
                ok=False, revert_selector=None, revert_known_acceptable=False,
                revert_reason_name=None, raw_response=None,
            )
        if not isinstance(body, dict):
            return SimulationResult(
                ok=False, revert_selector=None, revert_known_acceptable=False,
                revert_reason_name=None, raw_response=None,
            )
        return gmx_order_encoder._classify_response_body(body)
    finally:
        if own_client:
            await client.aclose()


def _validate_gates_for_broadcast(
    signed_tx: dict,
    intent: OrderIntent,
) -> str | None:
    """Pure: return None if all gates allow broadcast, else error string.

    Validates:
      - signed_tx has the 'raw' and 'from' keys
      - signed_tx['from'] matches `get_executor_address()`
      - `intent.account` matches `get_executor_address()`

    The `live_gmx_enabled` and `dry_run` gates are checked in
    `submit_signed` before this is called.
    """
    raw = signed_tx.get("raw")
    if not isinstance(raw, str) or not raw.startswith("0x"):
        return "signed_tx missing valid 'raw' field"

    signer_address = get_executor_address()
    if signer_address is None:
        # Shouldn't reach here — submit_signed loads the key first — but
        # guard anyway. Note: don't include any key-derived state in error.
        return "no executor key loaded"

    from_addr = signed_tx.get("from")
    if not isinstance(from_addr, str) or from_addr.lower() != signer_address.lower():
        return (
            f"signed_tx['from']={from_addr!r} does not match "
            f"executor address={signer_address!r}"
        )

    if intent.account.lower() != signer_address.lower():
        return (
            f"intent.account={intent.account!r} does not match "
            f"executor address={signer_address!r}"
        )
    return None


async def submit_signed(
    signed_tx: dict,
    intent: OrderIntent,
    *,
    rpc_url: str | None = None,
    client: httpx.AsyncClient | None = None,
    dry_run: bool = True,
) -> SendResult:
    """Gated broadcast path — see module docstring for the gate matrix.

    By default (`dry_run=True`), simulates via `simulate_signed` and
    returns `submitted=False`. Even with `dry_run=False`, refuses to
    broadcast unless `settings.live_gmx_enabled` is True AND the from-
    address / intent-account match the loaded key's EOA.

    On every refusal-fall-through, runs the simulation so the caller
    still gets the structural-correctness signal.

    Raises RuntimeError ONLY if no key is configured (since you cannot
    have a `signed_tx` without one — defensive).
    """
    # Defensive: even though sign_order would have failed earlier without
    # a key, callers might hand-craft a signed_tx dict in tests. Reject.
    _ = _require_key_or_raise()  # raises if no key

    eff_rpc = rpc_url if rpc_url is not None else settings.arbitrum_rpc_url

    # Gate 1: explicit dry_run from caller
    if dry_run:
        log.info("gmx_signer.submit gate=dry_run path=simulate")
        sim = await simulate_signed(signed_tx, rpc_url=eff_rpc, client=client)
        return SendResult(
            submitted=False, dry_run_simulation=sim,
            tx_hash=None, block_number=None, gas_used=None, status=None,
            error=None,
        )

    # Gate 2: global live-GMX kill switch (settings default = False)
    if not settings.live_gmx_enabled:
        log.warning(
            "gmx_signer.submit gate=live_gmx_disabled path=simulate "
            "(settings.live_gmx_enabled=False)",
        )
        sim = await simulate_signed(signed_tx, rpc_url=eff_rpc, client=client)
        return SendResult(
            submitted=False, dry_run_simulation=sim,
            tx_hash=None, block_number=None, gas_used=None, status=None,
            error="live_gmx_enabled is False",
        )

    # Gate 3: signed-tx integrity + account-match
    gate_err = _validate_gates_for_broadcast(signed_tx, intent)
    if gate_err is not None:
        log.warning("gmx_signer.submit gate=integrity err=%s path=simulate", gate_err)
        sim = await simulate_signed(signed_tx, rpc_url=eff_rpc, client=client)
        return SendResult(
            submitted=False, dry_run_simulation=sim,
            tx_hash=None, block_number=None, gas_used=None, status=None,
            error=gate_err,
        )

    # All gates clear — broadcast
    raw_hex = signed_tx["raw"]
    log.info("gmx_signer.submit gate=clear path=broadcast")

    own_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=httpx.Timeout(settings.gmx_reader_timeout_s))
        own_client = True
    try:
        body = await _eth_call(
            rpc_url=eff_rpc, method="eth_sendRawTransaction",
            params=[raw_hex], client=client,
        )
        tx_hash = body.get("result")
        if not isinstance(tx_hash, str):
            err = body.get("error")
            err_msg = (
                err.get("message", "unknown error") if isinstance(err, dict)
                else "unknown error"
            )
            log.error("gmx_signer.broadcast_failed err=%s", err_msg)
            return SendResult(
                submitted=False, dry_run_simulation=None,
                tx_hash=None, block_number=None, gas_used=None, status=None,
                error=f"eth_sendRawTransaction failed: {err_msg}",
            )

        log.info("gmx_signer.broadcast_ok tx_hash=%s", tx_hash)

        # Best-effort receipt poll (single-shot — caller can re-poll)
        receipt_body = await _eth_call(
            rpc_url=eff_rpc, method="eth_getTransactionReceipt",
            params=[tx_hash], client=client,
        )
        receipt = receipt_body.get("result") if isinstance(receipt_body, dict) else None

        block_number: int | None = None
        gas_used: int | None = None
        status_int: int | None = None
        if isinstance(receipt, dict):
            try:
                bn = receipt.get("blockNumber")
                if isinstance(bn, str):
                    block_number = int(bn, 16)
                gu = receipt.get("gasUsed")
                if isinstance(gu, str):
                    gas_used = int(gu, 16)
                st = receipt.get("status")
                if isinstance(st, str):
                    status_int = int(st, 16)
            except (ValueError, TypeError):
                # Malformed receipt fields — return tx_hash but no enriched data
                pass

        return SendResult(
            submitted=True, dry_run_simulation=None,
            tx_hash=tx_hash, block_number=block_number,
            gas_used=gas_used, status=status_int,
            error=None,
        )
    finally:
        if own_client:
            await client.aclose()


__all__ = [
    "MAX_EXECUTION_FEE_MULTIPLIER_FACTOR",
    "SendResult",
    "get_executor_address",
    "sign_order",
    "simulate_signed",
    "submit_signed",
]
