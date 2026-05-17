"""GMX V2 tx builder — pure helpers."""
from __future__ import annotations

import pytest

from gmx_strategies import contracts, tx_builder
from gmx_strategies.tx_builder import (
    LiquidationTxRequest,
    OracleReport,
    build_liquidation_tx,
    chain_id_for,
    pack_oracle_params,
)


def test_chain_id_lookup():
    assert chain_id_for("arbitrum") == 42161
    assert chain_id_for("avalanche") == 43114
    assert chain_id_for("ethereum") == 0  # unknown
    assert chain_id_for("") == 0


def test_contract_for_arbitrum_known():
    assert contracts.contract_for("arbitrum", "liquidation_handler").startswith("0x")
    assert contracts.contract_for("arbitrum", "reader").startswith("0x")
    assert contracts.contract_for("arbitrum", "data_store").startswith("0x")


def test_contract_for_unknown_chain():
    assert contracts.contract_for("solana", "reader") == ""


def test_contract_for_unknown_name():
    assert contracts.contract_for("arbitrum", "made_up_contract") == ""


def test_pack_oracle_params_empty():
    tokens, providers, blobs = pack_oracle_params(())
    assert tokens == []
    assert providers == []
    assert blobs == []


def test_pack_oracle_params_single():
    rep = OracleReport(
        token="0x0000000000000000000000000000000000000001",
        provider="0x0000000000000000000000000000000000000002",
        data=b"\x01\x02\x03",
    )
    tokens, providers, blobs = pack_oracle_params((rep,))
    assert tokens == [rep.token]
    assert providers == [rep.provider]
    assert blobs == [b"\x01\x02\x03"]


def test_pack_oracle_params_multi():
    reps = (
        OracleReport(
            token="0x1111111111111111111111111111111111111111",
            provider="0x2222222222222222222222222222222222222222",
            data=b"\x01",
        ),
        OracleReport(
            token="0x3333333333333333333333333333333333333333",
            provider="0x4444444444444444444444444444444444444444",
            data=b"\x02",
        ),
    )
    tokens, providers, blobs = pack_oracle_params(reps)
    assert len(tokens) == 2
    assert tokens[0] != tokens[1]
    assert blobs == [b"\x01", b"\x02"]


def test_hex_to_bytes_pure_helper():
    assert tx_builder._hex_to_bytes("0xabcd") == b"\xab\xcd"
    assert tx_builder._hex_to_bytes("abcd") == b"\xab\xcd"
    assert tx_builder._hex_to_bytes("") == b""
    assert tx_builder._hex_to_bytes("not hex") == b""


def test_decode_chainlink_report_happy_path():
    payload = '{"price": "100.0", "report_blob": "0xdeadbeef", "verifier_proxy": "0x1"}'
    blob = tx_builder._decode_chainlink_report(payload)
    assert blob == b"\xde\xad\xbe\xef"


def test_decode_chainlink_report_missing_blob():
    assert tx_builder._decode_chainlink_report('{"price": "100"}') is None


def test_decode_chainlink_report_malformed_json():
    assert tx_builder._decode_chainlink_report("not json") is None
    assert tx_builder._decode_chainlink_report("") is None


def test_build_liquidation_tx_happy_path():
    rep = OracleReport(
        token="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",  # WETH on Arbitrum
        provider="0x478Aa2aC9F6D65F84e09D9185d126c3a17c2a93C",
        data=b"\x01\x02\x03\x04",
    )
    req = LiquidationTxRequest(
        chain="arbitrum",
        account="0x1111111111111111111111111111111111111111",
        market="0x2222222222222222222222222222222222222222",
        collateral_token="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        is_long=True,
        oracle_reports=(rep,),
        nonce=42,
        chain_id=42161,
        sender_address="0x3333333333333333333333333333333333333333",
    )
    tx = build_liquidation_tx(req)
    assert tx["type"] == 2
    assert tx["chainId"] == 42161
    assert tx["nonce"] == 42
    assert tx["gas"] == tx_builder.DEFAULT_GAS_LIMIT
    assert tx["maxFeePerGas"] > 0
    assert tx["maxPriorityFeePerGas"] > 0
    assert tx["value"] == 0
    # Calldata starts with the 4-byte selector then encoded args
    assert tx["data"].startswith("0x")
    # The encoded data should be substantial (call + 5 args + variable arrays)
    assert len(tx["data"]) > 200


def test_build_liquidation_tx_unknown_chain_raises():
    rep = OracleReport(
        token="0x1111111111111111111111111111111111111111",
        provider="0x2222222222222222222222222222222222222222",
        data=b"\x01",
    )
    req = LiquidationTxRequest(
        chain="solana",
        account="0x3333333333333333333333333333333333333333",
        market="0x4444444444444444444444444444444444444444",
        collateral_token="0x5555555555555555555555555555555555555555",
        is_long=True,
        oracle_reports=(rep,),
        nonce=0,
        chain_id=0,
        sender_address="0x6666666666666666666666666666666666666666",
    )
    with pytest.raises(ValueError, match="liquidation_handler"):
        build_liquidation_tx(req)
