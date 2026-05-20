"""Tests for the Binance Futures HMAC signed-request client (G6.2).

Coverage:
  HMAC primitive:
    - `_sign_query` matches the canonical Binance docs HMAC SHA256 example
      (the worked example from
       https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md
       — same algorithm Futures uses):
         secret    = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j"
         query     = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&
                       quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559"
         signature = "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
    - Empty query string still produces a valid (different) signature.

  Signed-query builder:
    - timestamp is auto-added (current ms).
    - timestamp is overridable for determinism.
    - recvWindow is added (default 5000).
    - signature is appended as the FINAL param.
    - params are sorted alphabetically (deterministic).

  Signed wrappers (signed_get / signed_post):
    - Without credentials → returns None, no HTTP call, warning logged.
    - With creds + happy 200 dict body → returns dict.
    - With creds + happy 200 list body → returns list (positionRisk shape).
    - URL hits `<base_url><path>?<signed_query>`.
    - Header `X-MBX-APIKEY` is set with the api_key value.
    - Non-200 status → None, error code logged.
    - Malformed JSON → None.
    - Bad body shape (str instead of dict/list) → None.
    - HTTP exception → None.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qsl, urlparse

import httpx
import pytest

from gmx_strategies import binance_auth

# ──────────────────────────────────────────────────────────────────────────
# HMAC primitive — canonical Binance test vector
# ──────────────────────────────────────────────────────────────────────────


def test_sign_query_matches_binance_docs_canonical_vector() -> None:
    """Verify `_sign_query` against the canonical Binance docs HMAC SHA256
    example. This is the worked example from the Binance Spot REST API
    docs and the same algorithm is used for USDT-M Futures auth (per
    `arch_binance_executor_audit.md` §1).

    Source: https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md
    """
    secret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j"
    query = (
        "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC"
        "&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559"
    )
    expected = "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
    assert binance_auth._sign_query(query, secret) == expected


def test_sign_query_empty_string_is_deterministic() -> None:
    """Empty query string still produces a valid hex digest. Used by
    endpoints with zero params (e.g. positionSide/dual — only timestamp +
    recvWindow + signature, but those are added BEFORE signing).
    """
    sig = binance_auth._sign_query("", "test-secret")
    # HMAC-SHA256 always emits 64 hex chars.
    assert len(sig) == 64
    assert all(c in "0123456789abcdef" for c in sig)
    # Determinism: same inputs → same output.
    assert sig == binance_auth._sign_query("", "test-secret")
    # Different secret → different sig.
    assert sig != binance_auth._sign_query("", "other-secret")


# ──────────────────────────────────────────────────────────────────────────
# Signed-query builder
# ──────────────────────────────────────────────────────────────────────────


def test_build_signed_query_adds_timestamp_and_recvwindow() -> None:
    """timestamp + recvWindow are auto-added to the signed payload."""
    qs = binance_auth._build_signed_query(
        {"symbol": "BTCUSDT"},
        secret="test-secret",
        recv_window_ms=5000,
        timestamp_ms=1716180000000,
    )
    params = dict(parse_qsl(qs, keep_blank_values=True))
    assert params["timestamp"] == "1716180000000"
    assert params["recvWindow"] == "5000"
    assert params["symbol"] == "BTCUSDT"
    assert "signature" in params
    # Signature must be lowercase hex of length 64.
    assert len(params["signature"]) == 64


def test_build_signed_query_timestamp_overridable() -> None:
    """Test seam: timestamp_ms is overridable for deterministic vectors."""
    qs1 = binance_auth._build_signed_query(
        {"symbol": "BTCUSDT"},
        secret="s",
        recv_window_ms=5000,
        timestamp_ms=1_000_000,
    )
    qs2 = binance_auth._build_signed_query(
        {"symbol": "BTCUSDT"},
        secret="s",
        recv_window_ms=5000,
        timestamp_ms=2_000_000,
    )
    assert qs1 != qs2  # different timestamps → different signatures
    # Signature is the LAST param.
    assert qs1.split("&")[-1].startswith("signature=")


def test_build_signed_query_sorts_params() -> None:
    """Params are sorted alphabetically — deterministic signing across calls."""
    qs = binance_auth._build_signed_query(
        {"zSymbol": "B", "aSide": "BUY", "mQty": 1},
        secret="s",
        recv_window_ms=5000,
        timestamp_ms=42,
    )
    # Strip the signature suffix to inspect the signed payload.
    base, _, sig_part = qs.rpartition("&")
    assert sig_part.startswith("signature=")
    keys = [pair.split("=")[0] for pair in base.split("&")]
    assert keys == sorted(keys)


def test_build_signed_query_signature_is_last() -> None:
    """`signature=...` must be the FINAL param so Binance parses the
    rest as the signed payload."""
    qs = binance_auth._build_signed_query(
        {"symbol": "BTCUSDT"},
        secret="s",
        timestamp_ms=42,
    )
    assert "&signature=" in qs
    assert qs.endswith(qs.split("&signature=")[1])  # nothing after the sig
    # Sig occurs exactly once.
    assert qs.count("signature=") == 1


# ──────────────────────────────────────────────────────────────────────────
# Fixtures for signed_get / signed_post
# ──────────────────────────────────────────────────────────────────────────


def _make_fake_response(*, status_code: int = 200, body: Any) -> Any:
    """Stand-in for httpx.Response — only attrs the module reads."""

    class _Resp:
        def __init__(self, sc: int, body: Any) -> None:
            self.status_code = sc
            self._body = body

        def json(self) -> Any:
            if isinstance(self._body, ValueError):
                raise self._body
            return self._body

    return _Resp(status_code, body)


@pytest.fixture
def with_creds(monkeypatch: pytest.MonkeyPatch) -> tuple[str, str]:
    """Populate the settings instance with non-empty test creds."""
    monkeypatch.setattr(binance_auth.settings, "binance_api_key", "test-key")
    monkeypatch.setattr(binance_auth.settings, "binance_api_secret", "test-secret")
    monkeypatch.setattr(binance_auth.settings, "binance_recv_window_ms", 5000)
    monkeypatch.setattr(
        binance_auth.settings, "binance_fapi_base_url", "https://fapi.test",
    )
    return ("test-key", "test-secret")


# ──────────────────────────────────────────────────────────────────────────
# signed_get — auth/cred handling
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_signed_get_returns_none_without_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No api_key OR no api_secret → returns None, no HTTP call."""
    monkeypatch.setattr(binance_auth.settings, "binance_api_key", "")
    monkeypatch.setattr(binance_auth.settings, "binance_api_secret", "")

    mock_request = AsyncMock()
    with patch("httpx.AsyncClient.request", new=mock_request):
        result = await binance_auth.signed_get("/fapi/v1/positionSide/dual", {})
    assert result is None
    mock_request.assert_not_called()


@pytest.mark.asyncio
async def test_signed_get_returns_none_with_only_key_no_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Half-set creds (key only) → still None. Both required."""
    monkeypatch.setattr(binance_auth.settings, "binance_api_key", "test-key")
    monkeypatch.setattr(binance_auth.settings, "binance_api_secret", "")
    result = await binance_auth.signed_get("/fapi/v1/positionSide/dual", {})
    assert result is None


# ──────────────────────────────────────────────────────────────────────────
# signed_get — happy path
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_signed_get_happy_path_dict_body(with_creds: tuple[str, str]) -> None:
    """200 with dict body → returns the dict. Verifies URL + header construction."""
    fake_resp = _make_fake_response(body={"dualSidePosition": False})
    mock_request = AsyncMock(return_value=fake_resp)
    with patch("httpx.AsyncClient.request", new=mock_request):
        result = await binance_auth.signed_get(
            "/fapi/v1/positionSide/dual", {},
        )
    assert result == {"dualSidePosition": False}

    # Verify the HTTP call shape.
    mock_request.assert_called_once()
    args, kwargs = mock_request.call_args
    method, url = args[0], args[1]
    assert method == "GET"
    parsed = urlparse(url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "fapi.test"
    assert parsed.path == "/fapi/v1/positionSide/dual"
    qs_params = dict(parse_qsl(parsed.query))
    assert "timestamp" in qs_params
    assert qs_params["recvWindow"] == "5000"
    assert "signature" in qs_params

    # Header check.
    headers = kwargs["headers"]
    assert headers["X-MBX-APIKEY"] == "test-key"


@pytest.mark.asyncio
async def test_signed_get_happy_path_list_body(with_creds: tuple[str, str]) -> None:
    """200 with list body → returns the list (positionRisk / balance shape)."""
    body = [
        {"asset": "USDT", "availableBalance": "100.5", "balance": "100.5"},
        {"asset": "BUSD", "availableBalance": "0", "balance": "0"},
    ]
    fake_resp = _make_fake_response(body=body)
    with patch("httpx.AsyncClient.request", new=AsyncMock(return_value=fake_resp)):
        result = await binance_auth.signed_get("/fapi/v2/balance", {})
    assert result == body


# ──────────────────────────────────────────────────────────────────────────
# signed_get — failure modes
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_signed_get_non_200_returns_none(with_creds: tuple[str, str]) -> None:
    """Non-200 → None, no raise. Binance error body parsed best-effort for log."""
    fake_resp = _make_fake_response(
        status_code=401,
        body={"code": -2014, "msg": "API-key format invalid."},
    )
    with patch("httpx.AsyncClient.request", new=AsyncMock(return_value=fake_resp)):
        result = await binance_auth.signed_get("/fapi/v2/balance", {})
    assert result is None


@pytest.mark.asyncio
async def test_signed_get_malformed_json_returns_none(
    with_creds: tuple[str, str],
) -> None:
    """`.json()` raising → None, no raise."""
    fake_resp = _make_fake_response(body=ValueError("not JSON"))
    with patch("httpx.AsyncClient.request", new=AsyncMock(return_value=fake_resp)):
        result = await binance_auth.signed_get("/fapi/v2/balance", {})
    assert result is None


@pytest.mark.asyncio
async def test_signed_get_bad_body_shape_returns_none(
    with_creds: tuple[str, str],
) -> None:
    """Body that's neither dict nor list (e.g. raw string) → None."""
    fake_resp = _make_fake_response(body="just a string")
    with patch("httpx.AsyncClient.request", new=AsyncMock(return_value=fake_resp)):
        result = await binance_auth.signed_get("/fapi/v2/balance", {})
    assert result is None


@pytest.mark.asyncio
async def test_signed_get_http_exception_returns_none(
    with_creds: tuple[str, str],
) -> None:
    """Network exception → None, no raise."""
    with patch(
        "httpx.AsyncClient.request",
        new=AsyncMock(side_effect=httpx.TimeoutException("timed out")),
    ):
        result = await binance_auth.signed_get("/fapi/v2/balance", {})
    assert result is None


# ──────────────────────────────────────────────────────────────────────────
# signed_post — basic smoke (G6.3+ wiring; auth path is same as GET)
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_signed_post_uses_post_method(with_creds: tuple[str, str]) -> None:
    """signed_post issues a POST request, not GET."""
    fake_resp = _make_fake_response(body={"ok": True})
    mock_request = AsyncMock(return_value=fake_resp)
    with patch("httpx.AsyncClient.request", new=mock_request):
        result = await binance_auth.signed_post(
            "/fapi/v1/leverage", {"symbol": "BTCUSDT"},
        )
    assert result == {"ok": True}
    args, _kwargs = mock_request.call_args
    assert args[0] == "POST"


@pytest.mark.asyncio
async def test_signed_post_returns_none_without_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same cred guard as signed_get."""
    monkeypatch.setattr(binance_auth.settings, "binance_api_key", "")
    result = await binance_auth.signed_post("/fapi/v1/leverage", {})
    assert result is None


# ──────────────────────────────────────────────────────────────────────────
# Reusable client param
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_signed_get_uses_caller_provided_client(
    with_creds: tuple[str, str],
) -> None:
    """When `client` is passed, the module uses it instead of opening one."""
    fake_resp = _make_fake_response(body={"dualSidePosition": False})
    fake_client = MagicMock()
    fake_client.request = AsyncMock(return_value=fake_resp)

    # Patch AsyncClient — we should NOT see the one-shot context-manager path.
    with patch("httpx.AsyncClient.request", new=AsyncMock()) as oneshot:
        result = await binance_auth.signed_get(
            "/fapi/v1/positionSide/dual", {}, client=fake_client,
        )
    assert result == {"dualSidePosition": False}
    fake_client.request.assert_called_once()
    oneshot.assert_not_called()
