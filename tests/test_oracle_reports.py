"""Oracle reports consumer — pure helpers."""
from __future__ import annotations

from gmx_strategies import oracle_reports


def test_required_aliases_long_position():
    """Long ETH position: collateral IS the index asset (no USDC needed)."""
    aliases = oracle_reports.required_aliases_for("arbitrum", "eth", is_long=True)
    assert "eth" in aliases
    # Long ETH uses WETH collateral; same token → no USDC report needed
    assert "usdc" not in aliases


def test_required_aliases_short_position():
    """Short ETH position: collateral is USDC; need both reports."""
    aliases = oracle_reports.required_aliases_for("arbitrum", "eth", is_long=False)
    assert "eth" in aliases
    assert "usdc" in aliases


def test_required_aliases_unknown_market():
    assert oracle_reports.required_aliases_for("arbitrum", "made_up", True) == ()


def test_verifier_proxy_per_chain():
    assert oracle_reports.VERIFIER_PROXY_BY_CHAIN["arbitrum"].startswith("0x")
    assert oracle_reports.VERIFIER_PROXY_BY_CHAIN["avalanche"].startswith("0x")


def test_decode_blob_happy_path():
    payload = '{"report_blob": "0xdeadbeef", "ts_unix": 1700000000.0}'
    result = oracle_reports._decode_blob_from_payload(payload)
    assert result is not None
    blob, ts = result
    assert blob == b"\xde\xad\xbe\xef"
    assert ts == 1700000000.0


def test_decode_blob_with_blob_field_alias():
    payload = '{"blob": "0xabcd", "ts_unix": 1700000000.0}'
    result = oracle_reports._decode_blob_from_payload(payload)
    assert result is not None
    blob, _ = result
    assert blob == b"\xab\xcd"


def test_decode_blob_no_payload():
    assert oracle_reports._decode_blob_from_payload(None) is None
    assert oracle_reports._decode_blob_from_payload("") is None


def test_decode_blob_malformed_json():
    assert oracle_reports._decode_blob_from_payload("not json {") is None


def test_decode_blob_missing_blob_field():
    assert oracle_reports._decode_blob_from_payload('{"price": "100"}') is None


def test_decode_blob_empty_blob():
    assert oracle_reports._decode_blob_from_payload('{"report_blob": ""}') is None


def test_alias_token_address_known_alias():
    addr = oracle_reports.alias_token_address("arbitrum", "eth")
    assert addr.startswith("0x")
    assert len(addr) == 42


def test_alias_token_address_unknown():
    assert oracle_reports.alias_token_address("arbitrum", "made_up") == ""
