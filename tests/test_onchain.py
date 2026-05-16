"""On-chain GMX V2 re-check helpers — pure tests.

The actual eth_call lives in GMXReader.get_position(); we mock it out
or skip live network. Pure helpers (compute_position_key,
decode_position_response, filter_by_open_state) are testable without
network.
"""
from __future__ import annotations

from dataclasses import dataclass

from gmx_strategies import onchain


def test_reader_address_for_arbitrum_set():
    assert onchain.GMX_READER_ADDRESSES["arbitrum"].startswith("0x")
    assert onchain.GMX_DATASTORE_ADDRESSES["arbitrum"].startswith("0x")


def test_compute_position_key_deterministic():
    """Same inputs → same key. Different is_long → different key."""
    args = {
        "account": "0x0000000000000000000000000000000000000001",
        "market": "0x0000000000000000000000000000000000000002",
        "collateral_token": "0x0000000000000000000000000000000000000003",
    }
    k_long = onchain.compute_position_key(**args, is_long=True)
    k_short = onchain.compute_position_key(**args, is_long=False)
    k_long_again = onchain.compute_position_key(**args, is_long=True)
    assert k_long == k_long_again
    assert k_long != k_short
    assert len(k_long) == 32  # keccak256


def test_decode_position_response_size_zero_is_closed():
    """sizeInUsd == 0 → is_open False (position closed)."""
    raw = (
        # addresses
        ("0xacc", "0xmkt", "0xctk"),
        # numbers — size_in_usd, size_in_tokens, collateral_amount, ...
        (0, 0, 0, 0, 0, 0, 0, 0, 0),
        # flags
        (True,),
    )
    pos = onchain.decode_position_response(raw)
    assert pos.is_open is False
    assert pos.size_in_usd == 0.0


def test_decode_position_response_size_positive_is_open():
    raw = (
        ("0xacc", "0xmkt", "0xctk"),
        # 1 USD position = 1e30 raw (30-decimal fixed-point)
        (10**30, 10**18, 10**18, 0, 0, 0, 0, 0, 0),
        (True,),
    )
    pos = onchain.decode_position_response(raw)
    assert pos.is_open is True
    assert pos.size_in_usd == 1.0
    assert pos.is_long is True


def test_decode_position_response_short_position():
    raw = (
        ("0xacc", "0xmkt", "0xctk"),
        (10**30, 0, 0, 0, 0, 0, 0, 0, 0),
        (False,),
    )
    pos = onchain.decode_position_response(raw)
    assert pos.is_long is False


@dataclass
class _StubTrigger:
    user: str
    market: str
    collateral_token: str
    is_long: bool


def test_filter_by_open_state_drops_closed_positions():
    t_open = _StubTrigger(
        user="0xaaa1111111111111111111111111111111111111",
        market="0xbbb2222222222222222222222222222222222222",
        collateral_token="0xccc3333333333333333333333333333333333333",
        is_long=True,
    )
    t_closed = _StubTrigger(
        user="0xaaa4444444444444444444444444444444444444",
        market="0xbbb5555555555555555555555555555555555555",
        collateral_token="0xccc6666666666666666666666666666666666666",
        is_long=False,
    )

    k_open = onchain.compute_position_key(
        t_open.user, t_open.market, t_open.collateral_token, t_open.is_long,
    ).hex()
    k_closed = onchain.compute_position_key(
        t_closed.user, t_closed.market, t_closed.collateral_token, t_closed.is_long,
    ).hex()

    rechecks = {
        k_open: onchain.OnchainPosition(
            account=t_open.user, market=t_open.market,
            collateral_token=t_open.collateral_token,
            size_in_usd=100.0, size_in_tokens=0.0, collateral_amount=0.0,
            is_long=True, is_open=True,
        ),
        k_closed: onchain.OnchainPosition(
            account=t_closed.user, market=t_closed.market,
            collateral_token=t_closed.collateral_token,
            size_in_usd=0.0, size_in_tokens=0.0, collateral_amount=0.0,
            is_long=False, is_open=False,
        ),
    }
    kept = onchain.filter_by_open_state([t_open, t_closed], rechecks)
    assert kept == [t_open]


def test_filter_by_open_state_passes_through_missing_rechecks():
    """RPC timeout → no entry → trigger passes through (fail-open, defensive)."""
    t = _StubTrigger(
        user="0x1111111111111111111111111111111111111111",
        market="0x2222222222222222222222222222222222222222",
        collateral_token="0x3333333333333333333333333333333333333333",
        is_long=True,
    )
    kept = onchain.filter_by_open_state([t], rechecks={})
    assert kept == [t]


def test_filter_by_open_state_disabled_returns_all():
    t = _StubTrigger(
        user="0x4444444444444444444444444444444444444444",
        market="0x5555555555555555555555555555555555555555",
        collateral_token="0x6666666666666666666666666666666666666666",
        is_long=True,
    )
    kept = onchain.filter_by_open_state([t, t], rechecks={}, require_open=False)
    assert len(kept) == 2
