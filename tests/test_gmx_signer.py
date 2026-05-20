"""Tests for the GMX V2 transaction signer + (gated) submission (G5.2).

Asserts the G5.2 contract:
  - Key loading: file path > env var > None (no key configured)
  - `get_executor_address()` returns the EOA address derived from the loaded
    key, or None when no key is configured.
  - `sign_order()` produces a valid EIP-1559 transaction (correct chainId,
    correct from-address, deterministic raw bytes for fixed inputs).
  - `simulate_signed()` issues `eth_call` (NOT `eth_sendRawTransaction`).
  - `submit_signed(dry_run=True)` STAYS on the simulate path.
  - `submit_signed(dry_run=False)` with `live_gmx_enabled=False` STAYS on
    the simulate path (the kill switch is the hard wall).
  - `submit_signed(dry_run=False)` with `live_gmx_enabled=True` DOES call
    `eth_sendRawTransaction`.
  - `RuntimeError` raised when functions that need a key are called
    without one configured.
  - Account-mismatch refuses to broadcast, falls back to simulation.

CRITICAL: No test in this file may log, print, or assert on the value of
the private key. Tests use a published test vector
(`0x{32x'11'}` → address `0x19E7E376E7C213B7E7e7e46cc70A5dD086DAff2A`).
The vector is well-known and has no funds; using it cannot leak anything
real. But the test suite still avoids comparing to the key bytes
themselves — only to the DERIVED address.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from gmx_strategies import gmx_order_encoder, gmx_signer
from gmx_strategies.gmx_order_encoder import OrderIntent, SimulationResult
from gmx_strategies.settings import settings

# ──────────────────────────────────────────────────────────────────────────
# Test vector — published, no funds, used only to derive the EOA address.
# See https://github.com/ethereum/eth-account/blob/main/tests for the
# all-ones private key + its expected address.
# ──────────────────────────────────────────────────────────────────────────
_TEST_KEY = "0x" + "1" * 64
_TEST_ADDRESS = "0x19E7E376E7C213B7E7e7e46cc70A5dD086DAff2A"
_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


@pytest.fixture(autouse=True)
def _reset_signer_key_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Before each test, clear the signer's key cache + env + key-path.

    Without this, the first test to load the key would cache it module-
    wide and downstream tests could not unset it.
    """
    # Remove env key
    monkeypatch.delenv("GMX_EXECUTOR_KEY", raising=False)
    # Point key path at a non-existent file
    monkeypatch.setattr(settings, "gmx_executor_key_path", "/nonexistent/path")
    # Clear cache
    gmx_signer._reset_key_cache_for_tests()


def _make_intent(**overrides: Any) -> OrderIntent:
    """Baseline OrderIntent — SOL long, $10 USDC, dummy account default."""
    base = {
        "market": "sol",
        "is_long": True,
        "is_increase": True,
        "collateral_token": _USDC,
        "initial_collateral_delta_amount": 10_000_000,
        "size_delta_usd": 10 * 10**30,
        "current_price_1e30": 150 * 10**22,  # $150 SOL, GMX-scaled
        "acceptable_price_band_bps": 350,
        "execution_fee_wei": 5 * 10**14,
        "account": "0x0000000000000000000000000000000000000001",
    }
    base.update(overrides)
    return OrderIntent(**base)  # type: ignore[arg-type]


def _fake_response(*, status_code: int = 200, body: dict[str, Any]) -> Any:
    """Stand-in for httpx.Response with .status_code + .json()."""

    class _Resp:
        def __init__(self, sc: int, body: dict[str, Any]) -> None:
            self.status_code = sc
            self._body = body

        def json(self) -> dict[str, Any]:
            return self._body

    return _Resp(status_code, body)


# ──────────────────────────────────────────────────────────────────────────
# Key loading + address derivation
# ──────────────────────────────────────────────────────────────────────────


def test_get_executor_address_returns_none_when_no_key() -> None:
    """No file at gmx_executor_key_path + no env var → None."""
    assert gmx_signer.get_executor_address() is None


def test_get_executor_address_from_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """GMX_EXECUTOR_KEY env var → derived address matches the test vector."""
    monkeypatch.setenv("GMX_EXECUTOR_KEY", _TEST_KEY)
    gmx_signer._reset_key_cache_for_tests()
    address = gmx_signer.get_executor_address()
    assert address is not None
    assert address.lower() == _TEST_ADDRESS.lower()


def test_get_executor_address_from_file(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """File at gmx_executor_key_path → derived address matches test vector."""
    key_file = tmp_path / "gmx_executor_key"
    key_file.write_text(_TEST_KEY + "\n")  # whitespace stripped
    monkeypatch.setattr(settings, "gmx_executor_key_path", str(key_file))
    gmx_signer._reset_key_cache_for_tests()
    address = gmx_signer.get_executor_address()
    assert address is not None
    assert address.lower() == _TEST_ADDRESS.lower()


def test_file_takes_precedence_over_env_var(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When BOTH the file AND the env var are set, the FILE wins (production
    convention: env is dev-only)."""
    # Env-var key is the all-ones vector (test address 0x19E7…)
    monkeypatch.setenv("GMX_EXECUTOR_KEY", _TEST_KEY)
    # File holds a DIFFERENT vector (all-twos → different address)
    other_key = "0x" + "2" * 64
    key_file = tmp_path / "gmx_executor_key"
    key_file.write_text(other_key)
    monkeypatch.setattr(settings, "gmx_executor_key_path", str(key_file))
    gmx_signer._reset_key_cache_for_tests()
    address = gmx_signer.get_executor_address()
    # Should be the FILE-derived address, NOT _TEST_ADDRESS
    assert address is not None
    assert address.lower() != _TEST_ADDRESS.lower()


def test_empty_key_file_falls_back_to_env(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty file is treated as 'no key here', falls through to env."""
    key_file = tmp_path / "gmx_executor_key"
    key_file.write_text("")  # explicitly empty
    monkeypatch.setattr(settings, "gmx_executor_key_path", str(key_file))
    monkeypatch.setenv("GMX_EXECUTOR_KEY", _TEST_KEY)
    gmx_signer._reset_key_cache_for_tests()
    address = gmx_signer.get_executor_address()
    assert address is not None
    assert address.lower() == _TEST_ADDRESS.lower()


# ──────────────────────────────────────────────────────────────────────────
# sign_order — EIP-1559 transaction shape + determinism
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_sign_order_raises_without_key() -> None:
    """No key configured → `sign_order` raises RuntimeError with both
    provisioning sources named."""
    intent = _make_intent(account=_TEST_ADDRESS)
    with pytest.raises(RuntimeError, match="GMX_EXECUTOR_KEY"):
        await gmx_signer.sign_order(intent)


@pytest.mark.asyncio
async def test_sign_order_produces_signed_tx(monkeypatch: pytest.MonkeyPatch) -> None:
    """With key configured + RPC mocked, sign_order returns the expected
    dict shape with `raw`/`hash`/`nonce`/`from`/`tx_dict`."""
    monkeypatch.setenv("GMX_EXECUTOR_KEY", _TEST_KEY)
    gmx_signer._reset_key_cache_for_tests()

    # Mock RPC to return nonce=7 + gasPrice=0.1 gwei
    async def fake_post(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        method = json.get("method")
        if method == "eth_getTransactionCount":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x7"})
        if method == "eth_gasPrice":
            return _fake_response(body={
                "jsonrpc": "2.0", "id": 1, "result": hex(100_000_000),  # 0.1 gwei
            })
        return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x"})

    intent = _make_intent(account=_TEST_ADDRESS)
    with patch("httpx.AsyncClient.post", new=fake_post):
        signed = await gmx_signer.sign_order(intent)

    assert set(signed.keys()) == {"raw", "hash", "nonce", "from", "tx_dict"}
    assert signed["nonce"] == 7
    assert signed["from"].lower() == _TEST_ADDRESS.lower()
    assert signed["raw"].startswith("0x02")  # EIP-1559 type byte
    assert signed["hash"].startswith("0x") and len(signed["hash"]) == 66

    # The tx_dict should have all the expected EIP-1559 fields
    tx = signed["tx_dict"]
    assert tx["type"] == 2
    assert tx["chainId"] == settings.gmx_chain_id_arbitrum
    assert tx["nonce"] == 7
    assert tx["to"].lower() == settings.gmx_exchange_router_address_arbitrum.lower()
    assert tx["value"] == intent.execution_fee_wei
    assert tx["data"].startswith("0xac9650d8")  # multicall selector

    # The tx_dict NEVER carries the private key
    assert "key" not in tx
    assert "privateKey" not in tx
    assert "private_key" not in tx


@pytest.mark.asyncio
async def test_sign_order_is_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    """Same key + same nonce + same intent → identical raw bytes.

    This is critical for the operator to be able to re-derive a tx hash
    independently and verify it before broadcast.
    """
    monkeypatch.setenv("GMX_EXECUTOR_KEY", _TEST_KEY)
    gmx_signer._reset_key_cache_for_tests()

    intent = _make_intent(account=_TEST_ADDRESS)

    async def fake_post(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        method = json.get("method")
        if method == "eth_getTransactionCount":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x3"})
        if method == "eth_gasPrice":
            # 0.1 gwei = 100_000_000 wei = 0x5f5e100
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x5f5e100"})
        return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x"})

    with patch("httpx.AsyncClient.post", new=fake_post):
        signed_a = await gmx_signer.sign_order(intent, nonce=3)
        signed_b = await gmx_signer.sign_order(intent, nonce=3)

    # With identical nonce + intent, raw + hash MUST match
    assert signed_a["raw"] == signed_b["raw"]
    assert signed_a["hash"] == signed_b["hash"]


@pytest.mark.asyncio
async def test_sign_order_uses_safety_margin_gas_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """gas limit = base * (100 + margin_pct) / 100 — defaults to 3.6M."""
    monkeypatch.setenv("GMX_EXECUTOR_KEY", _TEST_KEY)
    gmx_signer._reset_key_cache_for_tests()
    intent = _make_intent(account=_TEST_ADDRESS)

    async def fake_post(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        method = json.get("method")
        if method == "eth_getTransactionCount":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x0"})
        if method == "eth_gasPrice":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x1"})
        return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x"})

    with patch("httpx.AsyncClient.post", new=fake_post):
        signed = await gmx_signer.sign_order(intent, nonce=0)

    expected = (
        settings.gmx_increase_order_gas_limit
        * (100 + settings.gmx_gas_limit_safety_margin_pct)
        // 100
    )
    assert signed["tx_dict"]["gas"] == expected


# ──────────────────────────────────────────────────────────────────────────
# simulate_signed — eth_call only, never broadcasts
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_simulate_signed_calls_eth_call_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """simulate_signed must use `eth_call` (NOT `eth_sendRawTransaction`)."""
    monkeypatch.setenv("GMX_EXECUTOR_KEY", _TEST_KEY)
    gmx_signer._reset_key_cache_for_tests()
    intent = _make_intent(account=_TEST_ADDRESS)

    # Build a signed tx via sign_order (with mocked RPC for nonce/gasprice)
    async def fake_sign_rpc(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        method = json.get("method")
        if method == "eth_getTransactionCount":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x0"})
        if method == "eth_gasPrice":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x1"})
        return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x"})

    with patch("httpx.AsyncClient.post", new=fake_sign_rpc):
        signed = await gmx_signer.sign_order(intent)

    # Now mock the simulate_signed RPC, capturing the called method.
    calls: list[str] = []

    async def fake_sim_rpc(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        calls.append(json.get("method"))
        # Return a known-acceptable revert (InsufficientWntAmountForExecutionFee)
        return _fake_response(body={
            "jsonrpc": "2.0", "id": 1,
            "error": {
                "code": 3, "message": "execution reverted",
                "data": "0x3a78cd7e" + "0" * 128,
            },
        })

    with patch("httpx.AsyncClient.post", new=fake_sim_rpc):
        result = await gmx_signer.simulate_signed(signed)

    assert isinstance(result, SimulationResult)
    assert result.revert_known_acceptable is True
    assert result.revert_reason_name == "InsufficientWntAmountForExecutionFee"
    assert calls == ["eth_call"]
    assert "eth_sendRawTransaction" not in calls


# ──────────────────────────────────────────────────────────────────────────
# submit_signed — the GATE MATRIX
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_submit_signed_dry_run_does_not_broadcast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`submit_signed(dry_run=True)` MUST NOT call eth_sendRawTransaction
    — even if `live_gmx_enabled=True`. The caller's explicit dry_run
    trumps the global gate."""
    monkeypatch.setenv("GMX_EXECUTOR_KEY", _TEST_KEY)
    monkeypatch.setattr(settings, "live_gmx_enabled", True)
    gmx_signer._reset_key_cache_for_tests()
    intent = _make_intent(account=_TEST_ADDRESS)

    async def fake_sign_rpc(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        method = json.get("method")
        if method == "eth_getTransactionCount":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x0"})
        if method == "eth_gasPrice":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x1"})
        return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x"})

    with patch("httpx.AsyncClient.post", new=fake_sign_rpc):
        signed = await gmx_signer.sign_order(intent)

    calls: list[str] = []

    async def fake_submit_rpc(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        calls.append(json.get("method"))
        return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x"})

    with patch("httpx.AsyncClient.post", new=fake_submit_rpc):
        result = await gmx_signer.submit_signed(signed, intent, dry_run=True)

    assert result.submitted is False
    assert result.dry_run_simulation is not None
    assert "eth_sendRawTransaction" not in calls
    # eth_call was the only RPC method invoked
    assert all(c == "eth_call" for c in calls)


@pytest.mark.asyncio
async def test_submit_signed_disabled_gate_does_not_broadcast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`dry_run=False` + `live_gmx_enabled=False` → STILL simulate.
    The kill switch is the hard wall."""
    monkeypatch.setenv("GMX_EXECUTOR_KEY", _TEST_KEY)
    monkeypatch.setattr(settings, "live_gmx_enabled", False)
    gmx_signer._reset_key_cache_for_tests()
    intent = _make_intent(account=_TEST_ADDRESS)

    async def fake_sign_rpc(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        method = json.get("method")
        if method == "eth_getTransactionCount":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x0"})
        if method == "eth_gasPrice":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x1"})
        return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x"})

    with patch("httpx.AsyncClient.post", new=fake_sign_rpc):
        signed = await gmx_signer.sign_order(intent)

    calls: list[str] = []

    async def fake_submit_rpc(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        calls.append(json.get("method"))
        return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x"})

    with patch("httpx.AsyncClient.post", new=fake_submit_rpc):
        result = await gmx_signer.submit_signed(signed, intent, dry_run=False)

    assert result.submitted is False
    assert result.dry_run_simulation is not None
    assert result.error == "live_gmx_enabled is False"
    assert "eth_sendRawTransaction" not in calls


@pytest.mark.asyncio
async def test_submit_signed_all_gates_clear_does_broadcast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`dry_run=False` + `live_gmx_enabled=True` + matching accounts →
    eth_sendRawTransaction is called."""
    monkeypatch.setenv("GMX_EXECUTOR_KEY", _TEST_KEY)
    monkeypatch.setattr(settings, "live_gmx_enabled", True)
    gmx_signer._reset_key_cache_for_tests()
    # Critical: intent.account MUST match the executor's derived address
    intent = _make_intent(account=_TEST_ADDRESS)

    async def fake_sign_rpc(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        method = json.get("method")
        if method == "eth_getTransactionCount":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x0"})
        if method == "eth_gasPrice":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x1"})
        return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x"})

    with patch("httpx.AsyncClient.post", new=fake_sign_rpc):
        signed = await gmx_signer.sign_order(intent)

    calls: list[str] = []
    tx_hash = "0x" + "ab" * 32

    async def fake_submit_rpc(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        method = json.get("method")
        calls.append(method)
        if method == "eth_sendRawTransaction":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": tx_hash})
        if method == "eth_getTransactionReceipt":
            return _fake_response(body={
                "jsonrpc": "2.0", "id": 1,
                "result": {
                    "blockNumber": hex(123_456_789),
                    "gasUsed": hex(2_750_000),
                    "status": "0x1",
                },
            })
        return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x"})

    with patch("httpx.AsyncClient.post", new=fake_submit_rpc):
        result = await gmx_signer.submit_signed(signed, intent, dry_run=False)

    assert result.submitted is True
    assert result.tx_hash == tx_hash
    assert result.block_number == 123_456_789
    assert result.gas_used == 2_750_000
    assert result.status == 1
    assert "eth_sendRawTransaction" in calls


@pytest.mark.asyncio
async def test_submit_signed_account_mismatch_does_not_broadcast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If `intent.account` doesn't match the executor's derived address,
    refuse to broadcast even with `live_gmx_enabled=True`."""
    monkeypatch.setenv("GMX_EXECUTOR_KEY", _TEST_KEY)
    monkeypatch.setattr(settings, "live_gmx_enabled", True)
    gmx_signer._reset_key_cache_for_tests()

    # The signed_tx will be built with the EXECUTOR's address (the key
    # forces it), but we tamper with `intent.account` AFTER signing so
    # the gate fails.
    intent_signed = _make_intent(account=_TEST_ADDRESS)

    async def fake_sign_rpc(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        method = json.get("method")
        if method == "eth_getTransactionCount":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x0"})
        if method == "eth_gasPrice":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x1"})
        return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x"})

    with patch("httpx.AsyncClient.post", new=fake_sign_rpc):
        signed = await gmx_signer.sign_order(intent_signed)

    # Now submit with a DIFFERENT account intent
    intent_mismatch = _make_intent(
        account="0x0000000000000000000000000000000000000099",
    )

    calls: list[str] = []

    async def fake_submit_rpc(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        calls.append(json.get("method"))
        return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x"})

    with patch("httpx.AsyncClient.post", new=fake_submit_rpc):
        result = await gmx_signer.submit_signed(
            signed, intent_mismatch, dry_run=False,
        )

    assert result.submitted is False
    assert result.error is not None
    assert "does not match" in result.error
    assert "eth_sendRawTransaction" not in calls


@pytest.mark.asyncio
async def test_submit_signed_without_key_raises() -> None:
    """If no key is configured at all, `submit_signed` raises RuntimeError
    — you should never have a signed_tx without a key."""
    intent = _make_intent(account=_TEST_ADDRESS)
    fake_signed = {
        "raw": "0x02" + "f" * 100,
        "hash": "0x" + "ab" * 32,
        "nonce": 0,
        "from": _TEST_ADDRESS,
        "tx_dict": {"type": 2, "to": "0x0000000000000000000000000000000000000000",
                    "value": 0, "data": "0x"},
    }
    with pytest.raises(RuntimeError, match="GMX_EXECUTOR_KEY"):
        await gmx_signer.submit_signed(fake_signed, intent, dry_run=False)


@pytest.mark.asyncio
async def test_submit_signed_broadcast_failure_returns_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`eth_sendRawTransaction` returning a JSON-RPC error (e.g. nonce too
    low, insufficient funds) → SendResult(submitted=False, error=<msg>)."""
    monkeypatch.setenv("GMX_EXECUTOR_KEY", _TEST_KEY)
    monkeypatch.setattr(settings, "live_gmx_enabled", True)
    gmx_signer._reset_key_cache_for_tests()
    intent = _make_intent(account=_TEST_ADDRESS)

    async def fake_sign_rpc(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        method = json.get("method")
        if method == "eth_getTransactionCount":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x0"})
        if method == "eth_gasPrice":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x1"})
        return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x"})

    with patch("httpx.AsyncClient.post", new=fake_sign_rpc):
        signed = await gmx_signer.sign_order(intent)

    async def fake_submit_rpc(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        method = json.get("method")
        if method == "eth_sendRawTransaction":
            return _fake_response(body={
                "jsonrpc": "2.0", "id": 1,
                "error": {"code": -32000, "message": "nonce too low"},
            })
        return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x"})

    with patch("httpx.AsyncClient.post", new=fake_submit_rpc):
        result = await gmx_signer.submit_signed(signed, intent, dry_run=False)

    assert result.submitted is False
    assert result.tx_hash is None
    assert result.error is not None
    assert "nonce too low" in result.error


# ──────────────────────────────────────────────────────────────────────────
# Security: no key in logs or returned dicts
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_signed_tx_dict_does_not_contain_private_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sanity: the returned signed dict has no key-shaped string anywhere
    in its serialized form. The all-ones vector has a distinctive shape
    (60+ ones) we can search for."""
    monkeypatch.setenv("GMX_EXECUTOR_KEY", _TEST_KEY)
    gmx_signer._reset_key_cache_for_tests()
    intent = _make_intent(account=_TEST_ADDRESS)

    async def fake_post(self: Any, url: str, json: Any) -> Any:  # noqa: A002, ARG001
        method = json.get("method")
        if method == "eth_getTransactionCount":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x0"})
        if method == "eth_gasPrice":
            return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x1"})
        return _fake_response(body={"jsonrpc": "2.0", "id": 1, "result": "0x"})

    with patch("httpx.AsyncClient.post", new=fake_post):
        signed = await gmx_signer.sign_order(intent)

    serialized = str(signed)
    # No long run of "1" anywhere — the test key has 64 of them in a row
    assert "1" * 50 not in serialized
    # Key bytes never appear (with or without 0x prefix)
    assert _TEST_KEY not in serialized
    assert _TEST_KEY.removeprefix("0x") not in serialized


def test_unused_imports_for_completeness() -> None:
    """Smoke: gmx_order_encoder is the dependency we rely on. Confirm
    the module is importable + the multicall encoder is still present
    (regression guard against an accidental rename in a future PR)."""
    assert hasattr(gmx_order_encoder, "_encode_multicall")
    assert callable(gmx_order_encoder._encode_multicall)
