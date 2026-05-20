"""Binance USDT-M Futures HMAC-SHA256 signed-request client (G6.2).

The auth foundation for every Binance Futures private-endpoint call.
Implements the HMAC signing pattern from `arch_binance_executor_audit.md`
§1 (Authentication) + §12 (error handling) and §13 (SDK choice — hand-rolled
httpx + HMAC, no SDK).

WHY THIS EXISTS — short version:
  G6.1 (binance_exchange_info.py) reads the public `/fapi/v1/exchangeInfo`
  endpoint without auth. Every OTHER endpoint G6 needs — position-mode
  read, account balance, position info, margin-type, leverage, order
  placement — requires HMAC-SHA256 signing per Binance's USDT-M Futures
  API spec.

  This module is the single source of truth for signing. Every signed call
  in G6 goes through `signed_get` / `signed_post`. This means we audit
  ONE signing path, not N.

  Per the audit §13: hand-rolled `httpx` + stdlib `hashlib` + stdlib `hmac`
  is the right choice. The signing logic is ~10 lines and matches the
  reference implementation at `github.com/binance/binance-signature-examples`.
  No new deps — `httpx==0.28.1` is already pinned.

WHAT THIS IS NOT (deferred to G6.3+):
  - NO order placement. NO POST /fapi/v1/order. NO `newClientOrderId`
    generation. NO reconciliation pattern.
  - NO `marginType` POST. NO `leverage` POST. Those require the auth from
    this module BUT the policy decisions (when to flip ISOLATED, target
    leverage value) belong in G6.3.
  - NO websocket userDataStream listenKey. NO market-data streams. G6 v0.1
    is REST-only; userDataStream is G6.4+.
  - NO withdrawal-permission-requiring calls. Period. The operator's key
    MUST NOT have `enableWithdrawals` enabled — this is doc'd in the
    README "G6 — Binance auth setup" section.

SIGNING SPEC (verified against Binance docs 2026-05-20):
  Signed endpoint URL:
    https://fapi.binance.com<path>?<paramsWithoutSignature>&signature=<hex>
  Required header:
    X-MBX-APIKEY: <api_key>
  Signature:
    HMAC_SHA256(secret_key, query_string + body_string)
  Where:
    - `timestamp` (ms since epoch) is MANDATORY on every signed call.
    - `recvWindow` (ms) is OPTIONAL — bounds validation freshness window.
      Defaults to 5000ms per Binance docs. We pass it explicitly so the
      signed payload is deterministic across our codebase.
    - For most G6 endpoints body is empty → `totalParams = query_string`.
    - Signature case-insensitive at verification (we emit lowercase hex).

PARAM ORDERING:
  Binance's docs say signature is computed over `totalParams` "in the order
  they appear in the request". In practice the field order in the query
  string is what matters; we sort params alphabetically (deterministic +
  matches Binance's signature-examples Python reference + matches what
  the binance-signature-examples repo does).

URLENCODING:
  We use `urllib.parse.urlencode(params, doseq=False)` which produces
  `key=value&key=value` with proper percent-encoding. Binance accepts
  this. The signature is computed over the EXACT bytes of the encoded
  query string — any mismatch (extra spaces, wrong percent-encoding)
  → `-1022 INVALID_SIGNATURE`.

SECRETS HYGIENE (CRITICAL):
  - NEVER log the api_secret. NEVER log the signed query string (it
    contains the signature, which is non-secret but pairs with the
    request — easy to confuse later as "ok to log").
  - NEVER log the X-MBX-APIKEY header content.
  - DO log: path, status code, error code, error message (Binance's
    error bodies don't include secrets).
  - The api_secret is loaded from `settings.binance_api_secret` which is
    populated via env (BINANCE_API_SECRET) or /srv/secrets/binance_api_secret
    (operator's convention). Never committed.

FAILURE HANDLING:
  Every signed call returns `None` on ANY failure (HTTP error, non-200
  status, JSON parse error, missing creds). NEVER raises. Caller decides
  what to do. This matches the binance_funding.py + binance_exchange_info.py
  style — one keep-the-loop-alive contract across the package.

  The ONE module-level function that IS allowed to raise is
  `binance_startup_check.assert_one_way_position_mode` (G6.2 separate file).
  That's its job: refuse-to-run gate.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from typing import Any
from urllib.parse import urlencode

import httpx

from gmx_strategies.settings import settings

log = logging.getLogger(__name__)


# Module-level header name for the API key. Constant for testability.
_API_KEY_HEADER = "X-MBX-APIKEY"


# ──────────────────────────────────────────────────────────────────────────
# HMAC core
# ──────────────────────────────────────────────────────────────────────────


def _sign_query(query_string: str, secret: str) -> str:
    """Compute HMAC-SHA256 hex of `query_string` keyed by `secret`.

    Pure function — no I/O, no time, no settings read. The single primitive
    every signed call composes against.

    Args:
        query_string: The string to sign. For most G6 endpoints this is
            the URL-encoded query (no leading `?`). For endpoints with a
            body, the spec says `totalParams = query_string + body` —
            callers must concatenate before calling here.
        secret: The API secret. Treated as a UTF-8 byte string per
            Binance's reference implementation.

    Returns:
        Lowercase hex digest (Binance accepts either case but we emit
        lowercase for determinism).
    """
    return hmac.new(
        secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _build_signed_query(
    params: dict[str, str | int | float],
    secret: str,
    *,
    recv_window_ms: int = 5000,
    timestamp_ms: int | None = None,
) -> str:
    """Build the signed query string for a Binance signed-endpoint call.

    Adds `timestamp` (ms now) + `recvWindow`, sorts params for determinism,
    urlencodes, computes the HMAC over the encoded string, appends
    `signature=<hex>`. Returns the full query string (no leading `?`).

    `timestamp_ms` is overridable for testing (deterministic vectors). In
    production it defaults to `time.time() * 1000` floored to int ms.

    Returns a string of the form:
      `key1=val1&key2=val2&recvWindow=5000&timestamp=1716180000000&signature=abc...`

    NOTE: the returned string contains a valid signature that pairs with
    the secret used to compute it. While the signature itself is not a
    secret (it's derived), do NOT log this string — it's clearly tied to
    a successful auth attempt and could leak structural details.
    """
    if timestamp_ms is None:
        timestamp_ms = int(time.time() * 1000)

    # Merge required Binance params. `timestamp` + `recvWindow` are part
    # of the signed payload.
    merged: dict[str, str | int | float] = dict(params)
    merged["recvWindow"] = recv_window_ms
    merged["timestamp"] = timestamp_ms

    # Sort keys for determinism. Binance signs over the query bytes;
    # ordering within the request is whatever WE send, but sorting matches
    # the reference Python implementation and makes signed payloads
    # reproducible across machines / test runs.
    sorted_items = sorted(merged.items(), key=lambda kv: kv[0])
    query_string = urlencode(sorted_items, doseq=False)

    signature = _sign_query(query_string, secret)
    return f"{query_string}&signature={signature}"


# ──────────────────────────────────────────────────────────────────────────
# Cred + client helpers
# ──────────────────────────────────────────────────────────────────────────


def _have_credentials() -> tuple[str, str] | None:
    """Return (api_key, api_secret) if both are set, else None.

    Returns None and logs a single WARNING if EITHER credential is missing.
    Callers treat None as "unconfigured, skip the call". The audit memo
    treats missing creds as an operator-side gap, not a fatal error.
    """
    api_key = settings.binance_api_key
    api_secret = settings.binance_api_secret
    if not api_key or not api_secret:
        log.warning(
            "binance_auth.no_credentials key_set=%s secret_set=%s",
            bool(api_key), bool(api_secret),
        )
        return None
    return api_key, api_secret


def _build_headers(api_key: str) -> dict[str, str]:
    """Construct the X-MBX-APIKEY header dict. Isolated for testability."""
    return {_API_KEY_HEADER: api_key}


# ──────────────────────────────────────────────────────────────────────────
# Signed request wrappers
# ──────────────────────────────────────────────────────────────────────────


async def signed_get(
    path: str,
    params: dict[str, str | int | float] | None = None,
    *,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any] | list[Any] | None:
    """Issue a signed GET to `<binance_fapi_base_url><path>` with `params`.

    Used for read-only signed endpoints: positionSide/dual, account balance,
    position information, order query.

    Returns the parsed JSON body (dict or list — Binance returns either
    depending on endpoint) or None on ANY failure. NEVER raises.

    `client` is overridable for testing or for callers that want to share
    a session across many calls (rate-limit headers, connection pooling).
    When None we open a one-shot httpx.AsyncClient per call.

    Args:
        path: API path starting with `/`. Example: `/fapi/v1/positionSide/dual`.
        params: Query parameters (without timestamp/recvWindow/signature —
            this function adds them). Pass `{}` or `None` for endpoints
            with no params.
        client: Optional pre-built httpx.AsyncClient to reuse.

    Returns:
        Parsed JSON body on success, None on any failure path.
    """
    return await _signed_request("GET", path, params or {}, client=client)


async def signed_post(
    path: str,
    params: dict[str, str | int | float] | None = None,
    *,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any] | list[Any] | None:
    """Issue a signed POST to `<binance_fapi_base_url><path>` with `params`.

    Used for state-changing signed endpoints (margin-type, leverage,
    order placement). G6.2 does NOT call this directly — it's defined
    here for G6.3+ reuse so all signing lives in this module.

    Binance accepts both query-string and body for POST params; the
    reference implementation puts everything on the query string for
    POST too (simpler signing — `totalParams = query_string`, body is
    empty). We follow that convention.

    Returns the parsed JSON body or None on ANY failure. NEVER raises.
    """
    return await _signed_request("POST", path, params or {}, client=client)


async def signed_delete(
    path: str,
    params: dict[str, str | int | float] | None = None,
    *,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any] | list[Any] | None:
    """Issue a signed DELETE to `<binance_fapi_base_url><path>` with `params`.

    Used for `DELETE /fapi/v1/order` (cancel) — added in G6.4 alongside
    order placement. Same HMAC pattern as `signed_get` / `signed_post`;
    method is the only diff. Binance accepts cancel params on the query
    string with an empty body, identical to the POST convention.

    Returns the parsed JSON body or None on ANY failure. NEVER raises.
    """
    return await _signed_request("DELETE", path, params or {}, client=client)


async def _signed_request(
    method: str,
    path: str,
    params: dict[str, str | int | float],
    *,
    client: httpx.AsyncClient | None,
) -> dict[str, Any] | list[Any] | None:
    """Internal: shared GET/POST plumbing.

    Composes:
      1. Cred check — bail with None if either api_key/secret is empty.
      2. Sign the query string (params + recvWindow + timestamp + signature).
      3. Issue the HTTP call with X-MBX-APIKEY header.
      4. Best-effort error handling — log + None on any non-happy path.

    See module docstring for the full failure-mode list.
    """
    creds = _have_credentials()
    if creds is None:
        return None
    api_key, api_secret = creds

    signed_query = _build_signed_query(
        params,
        api_secret,
        recv_window_ms=settings.binance_recv_window_ms,
    )
    url = f"{settings.binance_fapi_base_url}{path}?{signed_query}"
    headers = _build_headers(api_key)
    timeout = httpx.Timeout(settings.binance_funding_timeout_s)

    # NOTE: do NOT log `url` (contains signature) or `headers` (contains
    # api key). Log `path` + `method` only.
    try:
        if client is None:
            async with httpx.AsyncClient(timeout=timeout) as one_shot:
                resp = await one_shot.request(method, url, headers=headers)
        else:
            resp = await client.request(method, url, headers=headers)
    except (httpx.HTTPError, httpx.TimeoutException) as exc:
        log.warning(
            "binance_auth.http_error method=%s path=%s err=%s",
            method, path, exc,
        )
        return None

    if resp.status_code != 200:
        # Binance error bodies look like {"code": -1022, "msg": "..."}.
        # Parse best-effort for the log line; the audit §12 catalogs the
        # codes G6 callers care about (-4061 hedge-mode, -2019 margin,
        # -4164 min-notional, etc).
        err_code: int | None = None
        err_msg: str | None = None
        try:
            err_body = resp.json()
            if isinstance(err_body, dict):
                code = err_body.get("code")
                msg = err_body.get("msg")
                if isinstance(code, int):
                    err_code = code
                if isinstance(msg, str):
                    err_msg = msg
        except (ValueError, TypeError):
            pass
        log.warning(
            "binance_auth.bad_status method=%s path=%s status=%d code=%s msg=%s",
            method, path, resp.status_code, err_code, err_msg,
        )
        return None

    try:
        body = resp.json()
    except (ValueError, TypeError):
        log.warning(
            "binance_auth.bad_json method=%s path=%s status=%d",
            method, path, resp.status_code,
        )
        return None

    if not isinstance(body, (dict, list)):
        log.warning(
            "binance_auth.bad_body_shape method=%s path=%s type=%s",
            method, path, type(body).__name__,
        )
        return None

    log.info(
        "binance_auth.signed_ok method=%s path=%s",
        method, path,
    )
    return body


__all__ = [
    "signed_delete",
    "signed_get",
    "signed_post",
]
