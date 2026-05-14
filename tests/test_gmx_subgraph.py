"""Tests for the GMX V2 subgraph adapter (pure parsing + async fetcher)."""
from __future__ import annotations

import pytest

from gmx_strategies.gmx_subgraph import (
    MARKET_ADDRESS_TO_ALIAS,
    POSITIONS_QUERY,
    RawSubgraphPosition,
    fetch_open_positions,
    parse_raw_subgraph_position,
    raw_to_gmx_position,
)

# ─── parse_raw_subgraph_position (pure) ────────────────────────────


def _row(**overrides) -> dict:
    base = {
        "id": "0xpos1",
        "account": "0xUSER1",
        "market": "0x47c031236e19d024B42f8AE6780E44A573170703",
        "isLong": True,
        # GMX V2 1e30 USD precision — these represent $5000 size and $500 col
        "sizeInUsd": "5000000000000000000000000000000000",
        "collateralUsd": "500000000000000000000000000000000",
    }
    base.update(overrides)
    return base


def test_parse_valid_long_row() -> None:
    raw = parse_raw_subgraph_position(_row())
    assert raw is not None
    assert raw.id == "0xpos1"
    assert raw.account == "0xuser1"
    assert raw.market_address == "0x47c031236e19d024b42f8ae6780e44a573170703"
    assert raw.is_long is True
    assert raw.size_usd == pytest.approx(5000.0)
    assert raw.collateral_usd == pytest.approx(500.0)


def test_parse_valid_short_row() -> None:
    raw = parse_raw_subgraph_position(_row(isLong=False))
    assert raw is not None
    assert raw.is_long is False


def test_parse_is_long_string_form() -> None:
    """Some subgraphs return booleans as strings."""
    raw = parse_raw_subgraph_position(_row(isLong="true"))
    assert raw is not None
    assert raw.is_long is True
    raw2 = parse_raw_subgraph_position(_row(isLong="false"))
    assert raw2 is not None
    assert raw2.is_long is False


def test_parse_account_lowercased() -> None:
    raw = parse_raw_subgraph_position(_row(account="0xABCDEF"))
    assert raw is not None
    assert raw.account == "0xabcdef"


def test_parse_market_address_lowercased() -> None:
    raw = parse_raw_subgraph_position(_row(market="0xABCDEF"))
    assert raw is not None
    assert raw.market_address == "0xabcdef"


def test_parse_falls_back_to_collateralAmountUsd() -> None:
    row = _row()
    del row["collateralUsd"]
    row["collateralAmountUsd"] = "500000000000000000000000000000000"
    raw = parse_raw_subgraph_position(row)
    assert raw is not None
    assert raw.collateral_usd == pytest.approx(500.0)


def test_parse_missing_id_returns_none() -> None:
    row = _row()
    del row["id"]
    assert parse_raw_subgraph_position(row) is None


def test_parse_missing_account_returns_none() -> None:
    row = _row()
    del row["account"]
    assert parse_raw_subgraph_position(row) is None


def test_parse_missing_market_returns_none() -> None:
    row = _row()
    del row["market"]
    assert parse_raw_subgraph_position(row) is None


def test_parse_zero_size_returns_none() -> None:
    """sizeInUsd = 0 means the position is closed; skip."""
    raw = parse_raw_subgraph_position(_row(sizeInUsd="0"))
    assert raw is None


def test_parse_zero_collateral_returns_none() -> None:
    raw = parse_raw_subgraph_position(_row(collateralUsd="0"))
    assert raw is None


def test_parse_garbage_size_returns_none() -> None:
    raw = parse_raw_subgraph_position(_row(sizeInUsd="not-a-number"))
    assert raw is None


def test_parse_non_dict_returns_none() -> None:
    assert parse_raw_subgraph_position(None) is None
    assert parse_raw_subgraph_position("string") is None
    assert parse_raw_subgraph_position([1, 2]) is None


def test_parse_invalid_isLong_type_returns_none() -> None:
    raw = parse_raw_subgraph_position(_row(isLong=42))
    assert raw is None


# ─── raw_to_gmx_position (pure) ────────────────────────────────────


def _raw(entry_price: float | None = 80_000.0) -> RawSubgraphPosition:
    return RawSubgraphPosition(
        id="0x1",
        account="0xuser",
        market_address="0x47c031236e19d024b42f8ae6780e44a573170703",   # BTC
        is_long=True,
        size_usd=5000.0,
        collateral_usd=500.0,
        entry_price=entry_price,
    )


def test_raw_to_gmx_position_btc_uses_subgraph_entry_price() -> None:
    pos = raw_to_gmx_position(_raw())
    assert pos is not None
    assert pos.user == "0xuser"
    assert pos.market == "btc"
    assert pos.is_long is True
    assert pos.size_usd == 5000.0
    assert pos.collateral_usd == 500.0
    assert pos.entry_price == 80_000.0    # from subgraph
    assert pos.leverage == 10.0


def test_raw_to_gmx_position_explicit_override_wins() -> None:
    """An explicit entry_price kwarg overrides whatever the subgraph said."""
    pos = raw_to_gmx_position(_raw(entry_price=70_000.0), entry_price=85_000.0)
    assert pos is not None
    assert pos.entry_price == 85_000.0


def test_raw_to_gmx_position_no_entry_price_anywhere_returns_none() -> None:
    """Subgraph entryPrice missing AND no override → None."""
    assert raw_to_gmx_position(_raw(entry_price=None)) is None


def test_raw_to_gmx_position_unknown_market_returns_none() -> None:
    raw = RawSubgraphPosition(
        id="0x1", account="0xuser",
        market_address="0xdeadbeef" * 5,    # 40 chars but not in map
        is_long=True, size_usd=5000.0, collateral_usd=500.0,
        entry_price=80_000.0,
    )
    assert raw_to_gmx_position(raw) is None


def test_raw_to_gmx_position_zero_price_returns_none() -> None:
    assert raw_to_gmx_position(_raw(), entry_price=0.0) is None
    assert raw_to_gmx_position(_raw(), entry_price=-1.0) is None


def test_raw_to_gmx_position_custom_alias_map() -> None:
    """Caller can supply a custom map to test isolation from the
    module-level MARKET_ADDRESS_TO_ALIAS."""
    raw = RawSubgraphPosition(
        id="x", account="0xa", market_address="0xnewmarket",
        is_long=True, size_usd=1000.0, collateral_usd=100.0,
        entry_price=1.0,
    )
    pos = raw_to_gmx_position(
        raw, alias_map={"0xnewmarket": "test-asset"},
    )
    assert pos is not None
    assert pos.market == "test-asset"


def test_raw_to_gmx_position_custom_threshold() -> None:
    pos = raw_to_gmx_position(
        _raw(), liquidation_threshold_pct=0.01,
    )
    assert pos is not None
    assert pos.liquidation_threshold_pct == 0.01


def test_parse_includes_entry_price() -> None:
    """Sanity that parser captures entryPrice when present."""
    row = _row()
    row["entryPrice"] = "80000000000000000000000000000000000"   # $80k * 1e30
    raw = parse_raw_subgraph_position(row)
    assert raw is not None
    assert raw.entry_price == pytest.approx(80_000.0)


def test_parse_no_entry_price_is_none_not_zero() -> None:
    """If entryPrice is absent, the field is None (not 0.0) — callers
    distinguish 'missing' from 'really zero'."""
    row = _row()
    row.pop("entryPrice", None)
    raw = parse_raw_subgraph_position(row)
    assert raw is not None
    assert raw.entry_price is None


def test_parse_zero_entry_price_treated_as_missing() -> None:
    """Zero is treated as missing — zero is not a valid entry."""
    row = _row()
    row["entryPrice"] = "0"
    raw = parse_raw_subgraph_position(row)
    assert raw is not None
    assert raw.entry_price is None


def test_market_address_map_has_all_expected_assets() -> None:
    """Sanity: the map contains BTC/ETH/SOL/LINK/AVAX/BNB/XRP/DOGE/HYPE/AAVE."""
    aliases = set(MARKET_ADDRESS_TO_ALIAS.values())
    assert {"btc", "eth", "sol", "link", "avax", "bnb", "xrp", "doge",
            "hype", "aave"}.issubset(aliases)


def test_market_address_keys_are_lowercased() -> None:
    """All keys must be lowercase — we lowercase the subgraph market
    addresses, so the map keys MUST be lowercase too."""
    for addr in MARKET_ADDRESS_TO_ALIAS.keys():
        assert addr == addr.lower(), f"{addr} should be lowercase"
        assert addr.startswith("0x"), f"{addr} should start with 0x"


# ─── fetch_open_positions (async, mocked client) ──────────────────


class _FakeResp:
    def __init__(self, status: int = 200, body: dict | None = None) -> None:
        self.status_code = status
        self._body = body or {}

    def json(self):
        return self._body


class _FakeHttpx:
    def __init__(self, pages: list[dict], status: int = 200,
                 raise_on_call: Exception | None = None):
        self.pages = pages
        self.status = status
        self.raise_on_call = raise_on_call
        self.calls: list[dict] = []

    async def post(self, url: str, *, json: dict):
        self.calls.append({"url": url, "body": json})
        if self.raise_on_call:
            raise self.raise_on_call
        skip = int(json.get("variables", {}).get("skip", 0))
        first = int(json.get("variables", {}).get("first", 200))
        page_idx = skip // first
        if page_idx >= len(self.pages):
            return _FakeResp(self.status, {"data": {"positions": []}})
        return _FakeResp(self.status, {"data": {"positions": self.pages[page_idx]}})


@pytest.mark.asyncio
async def test_fetch_open_positions_happy_path() -> None:
    pages = [
        [_row(id="a"), _row(id="b")],
        [_row(id="c")],
    ]
    client = _FakeHttpx(pages)
    out = await fetch_open_positions(
        client, "https://example.com/subgraph",
        page_size=2, max_pages=5,
    )
    ids = [p.id for p in out]
    assert ids == ["a", "b", "c"]
    assert len(client.calls) == 2


@pytest.mark.asyncio
async def test_fetch_open_positions_empty_url_returns_empty() -> None:
    """Empty URL = no subgraph configured → no work done, no HTTP attempt."""
    client = _FakeHttpx([])
    out = await fetch_open_positions(client, "")
    assert out == []
    assert client.calls == []


@pytest.mark.asyncio
async def test_fetch_open_positions_caps_at_max_pages() -> None:
    pages = [[_row(id=f"p{i}")] for i in range(20)]
    client = _FakeHttpx(pages)
    out = await fetch_open_positions(
        client, "https://example.com",
        page_size=1, max_pages=3,
    )
    assert len(out) == 3
    assert len(client.calls) == 3


@pytest.mark.asyncio
async def test_fetch_open_positions_handles_non_200() -> None:
    client = _FakeHttpx([], status=500)
    out = await fetch_open_positions(client, "https://example.com")
    assert out == []


@pytest.mark.asyncio
async def test_fetch_open_positions_handles_graphql_errors() -> None:
    """GraphQL errors come back as HTTP 200 with body.errors set."""

    class _Client:
        calls: list = []
        async def post(self, url, *, json):
            self.calls.append({"url": url, "body": json})
            return _FakeResp(200, {"errors": [{"message": "schema error"}]})

    out = await fetch_open_positions(_Client(), "https://example.com")
    assert out == []


@pytest.mark.asyncio
async def test_fetch_open_positions_handles_http_exception() -> None:
    client = _FakeHttpx([], raise_on_call=ConnectionError("network down"))
    out = await fetch_open_positions(client, "https://example.com")
    assert out == []


@pytest.mark.asyncio
async def test_fetch_open_positions_passes_variables() -> None:
    pages = [[_row(id="a")]]
    client = _FakeHttpx(pages)
    await fetch_open_positions(
        client, "https://example.com",
        page_size=50, max_pages=1,
    )
    assert client.calls[0]["body"]["variables"] == {"first": 50, "skip": 0}
    assert client.calls[0]["body"]["query"] == POSITIONS_QUERY


@pytest.mark.asyncio
async def test_fetch_rejects_http_url_for_ssrf_defense() -> None:
    """SSRF defense: only https:// URLs accepted. http://, file://, etc rejected."""
    pages = [[_row(id="a")]]
    client = _FakeHttpx(pages)
    for bad in ["http://example.com", "file:///etc/passwd",
                "gopher://internal:8080", "ftp://internal/positions",
                "javascript:alert(1)"]:
        out = await fetch_open_positions(client, bad)
        assert out == [], f"unsafe URL {bad} should be rejected"
    # No actual HTTP calls were made for the rejected URLs
    assert client.calls == []


@pytest.mark.asyncio
async def test_fetch_rejects_over_long_url() -> None:
    """URL >1024 chars rejected — defends against pathological config."""
    pages = [[_row(id="a")]]
    client = _FakeHttpx(pages)
    bad = "https://example.com/" + "a" * 1100
    out = await fetch_open_positions(client, bad)
    assert out == []
    assert client.calls == []


def test_is_safe_subgraph_url() -> None:
    """Direct pure test of the URL safety predicate."""
    from gmx_strategies.gmx_subgraph import _is_safe_subgraph_url
    assert _is_safe_subgraph_url("https://api.goldsky.com/api/subgraph/x") is True
    assert _is_safe_subgraph_url("http://api.goldsky.com/x") is False
    assert _is_safe_subgraph_url("file:///etc/passwd") is False
    assert _is_safe_subgraph_url("") is False
    assert _is_safe_subgraph_url(None) is False
    assert _is_safe_subgraph_url(123) is False
    assert _is_safe_subgraph_url("https://example.com/" + "x" * 2000) is False


@pytest.mark.asyncio
async def test_fetch_open_positions_drops_malformed_rows() -> None:
    """Rows that don't parse cleanly are silently skipped — caller doesn't
    crash on one bad row in the middle."""
    pages = [[_row(id="good1"), {"junk": "row"}, _row(id="good2")]]
    client = _FakeHttpx(pages)
    out = await fetch_open_positions(client, "https://example.com")
    ids = [p.id for p in out]
    assert ids == ["good1", "good2"]
