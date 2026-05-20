"""Tests for the G6.3 Binance Futures testnet/mainnet smoke CLI subcommand.

Every Binance / signed-read function is mocked. The smoke is glue logic
that orchestrates G6.1 + G6.2 reads + G3 reads and folds them into
pass/fail per check with a final exit code — these tests pin that
orchestration behaviour and the exit-code contract.

Exit codes:
  0 — all checks passed
  2 — at least one functional check failed (e.g. hedge mode, missing
      G6 market, funding-rate path returned nothing)
  3 — credentials not configured (api_key OR api_secret unset)
  4 — every signed read returned None (API down / unreachable / wrong
      base URL / bad IP allowlist)

We assert on stdout (via capsys) for the pass/fail markers + summary
lines so any future formatting tweak is a deliberate test edit, not a
silent regression.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from gmx_strategies import cli
from gmx_strategies.binance_exchange_info import SymbolInfo

# ──────────────────────────────────────────────────────────────────────────
# Fixtures / helpers
# ──────────────────────────────────────────────────────────────────────────


def _make_symbol_info(
    symbol: str,
    *,
    lot_step: float = 0.001,
    lot_min: float = 0.001,
    min_notional: float = 50.0,
) -> SymbolInfo:
    """Build a SymbolInfo with sensible defaults. Tests override the bits
    they care about."""
    return SymbolInfo(
        symbol=symbol,
        base_asset=symbol.replace("USDT", ""),
        quote_asset="USDT",
        price_precision=2,
        quantity_precision=3,
        lot_min=lot_min,
        lot_max=1_000_000.0,
        lot_step=lot_step,
        price_tick=0.1,
        min_notional=min_notional,
    )


def _all_markets_present() -> dict[str, SymbolInfo]:
    """Build a happy-path exchange-info map with all 5 G6 markets."""
    return {
        "BTCUSDT": _make_symbol_info("BTCUSDT", lot_step=0.001, min_notional=50.0),
        "ETHUSDT": _make_symbol_info("ETHUSDT", lot_step=0.001, min_notional=20.0),
        "SOLUSDT": _make_symbol_info("SOLUSDT", lot_step=0.01, min_notional=5.0),
        "DOGEUSDT": _make_symbol_info("DOGEUSDT", lot_step=1.0, min_notional=5.0),
        "XRPUSDT": _make_symbol_info("XRPUSDT", lot_step=0.1, min_notional=5.0),
    }


def _all_fundings_present() -> dict[str, float]:
    """Build a happy-path funding-rates map with all 5 G6 aliases."""
    return {
        "btc": 0.00006219,
        "eth": 0.00005819,
        "sol": -0.00010000,
        "doge": 0.00007500,
        "xrp": 0.00004200,
    }


def _happy_balance() -> list[dict[str, Any]]:
    """Build a happy-path balance list with a USDT entry."""
    return [
        {
            "asset": "USDT",
            "balance": "10000.00",
            "availableBalance": "10000.00",
            "crossWalletBalance": "10000.00",
        },
    ]


# Sentinel for "explicitly leave at default" vs "explicit None". Lets a
# test pass `balance=None` to model a signed-read failure while preserving
# the convenience of omitting an override entirely.
_UNSET: Any = object()


class _SmokeMocks:
    """Aggregate every patched function for `_g6_smoke_main` in one place.

    Tests construct an instance with overrides and call `.apply()` inside
    a context manager. Default returns model the happy-path testnet
    state: one-way mode, $10k USDT, no open positions, all markets present,
    all fundings present, startup gate passes.

    Pass `None` explicitly to mock a signed-read failure (e.g.
    `balance=None` → `fetch_account_balance` returns None). Omit the kwarg
    entirely to use the happy-path default.
    """

    def __init__(
        self,
        *,
        api_key: str = "test-key",
        api_secret: str = "test-secret",  # noqa: S107 — test fixture
        base_url: str = "https://demo-fapi.binance.com",
        position_mode: Any = _UNSET,  # False = one-way (happy path)
        balance: Any = _UNSET,
        usdt_free: Any = _UNSET,
        positions: Any = _UNSET,
        exchange_info: Any = _UNSET,
        fundings: Any = _UNSET,
        startup_check_raises: Exception | None = None,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.position_mode = False if position_mode is _UNSET else position_mode
        self.balance = _happy_balance() if balance is _UNSET else balance
        self.usdt_free = 10000.0 if usdt_free is _UNSET else usdt_free
        self.positions = [] if positions is _UNSET else positions
        self.exchange_info = (
            _all_markets_present() if exchange_info is _UNSET else exchange_info
        )
        self.fundings = (
            _all_fundings_present() if fundings is _UNSET else fundings
        )
        self.startup_check_raises = startup_check_raises

    def apply(self) -> list[Any]:
        """Return a list of context managers — caller chains them via
        `contextlib.ExitStack` or nested `with` blocks."""
        # Build the assert_one_way side_effect once so both raise and
        # silent-return cases are handled.
        if self.startup_check_raises is None:
            startup_mock = AsyncMock(return_value=None)
        else:
            startup_mock = AsyncMock(side_effect=self.startup_check_raises)

        return [
            patch("gmx_strategies.cli.settings.binance_api_key", self.api_key),
            patch("gmx_strategies.cli.settings.binance_api_secret", self.api_secret),
            patch(
                "gmx_strategies.cli.settings.binance_fapi_base_url", self.base_url,
            ),
            patch(
                "gmx_strategies.binance_account.fetch_position_mode",
                new=AsyncMock(return_value=self.position_mode),
            ),
            patch(
                "gmx_strategies.binance_account.fetch_account_balance",
                new=AsyncMock(return_value=self.balance),
            ),
            patch(
                "gmx_strategies.binance_account.fetch_usdt_free_margin",
                new=AsyncMock(return_value=self.usdt_free),
            ),
            patch(
                "gmx_strategies.binance_account.fetch_position_information",
                new=AsyncMock(return_value=self.positions),
            ),
            patch(
                "gmx_strategies.binance_exchange_info.fetch_exchange_info",
                new=AsyncMock(return_value=self.exchange_info),
            ),
            patch(
                "gmx_strategies.binance_funding.fetch_all_cex_fundings",
                new=AsyncMock(return_value=self.fundings),
            ),
            patch(
                "gmx_strategies.binance_startup_check.assert_one_way_position_mode",
                new=startup_mock,
            ),
        ]


def _run_smoke(
    mocks: _SmokeMocks,
    *,
    force_refresh: bool = False,
) -> int:
    """Apply every mock and run the smoke. Returns the exit code."""
    import contextlib

    argv = ["g6_smoke"]
    if force_refresh:
        argv.append("--force-refresh-exchange-info")

    with contextlib.ExitStack() as stack:
        for cm in mocks.apply():
            stack.enter_context(cm)
        return cli.main(argv)
    raise AssertionError("unreachable")  # pragma: no cover — appeases mypy


# ──────────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────────


def test_happy_path_all_pass_exit_0(capsys: pytest.CaptureFixture[str]) -> None:
    """Every check passes → exit 0 + summary line says PASS=9 FAIL=0."""
    exit_code = _run_smoke(_SmokeMocks())
    out = capsys.readouterr().out
    assert exit_code == 0
    # All 9 checks pass: AUTH-1, AUTH-2 testnet, READ-1..4, PUBLIC-1..2,
    # CONSISTENCY-1.
    assert "PASS=9" in out
    assert "FAIL=0" in out
    assert "exit_code: 0" in out
    # Each check should appear in the output by name.
    for name in (
        "AUTH-1 credentials configured",
        "AUTH-2 base URL configured",
        "READ-1 position mode",
        "READ-2 account balance",
        "READ-3 USDT free margin",
        "READ-4 position information",
        "PUBLIC-1 exchange info",
        "PUBLIC-2 funding rates",
        "CONSISTENCY-1 position-mode gate",
    ):
        assert name in out


# ──────────────────────────────────────────────────────────────────────────
# Exit code 3 — credentials not configured
# ──────────────────────────────────────────────────────────────────────────


def test_missing_api_key_exit_3(capsys: pytest.CaptureFixture[str]) -> None:
    """api_key empty → exit 3 with credentials not configured message."""
    exit_code = _run_smoke(_SmokeMocks(api_key=""))
    out = capsys.readouterr().out
    assert exit_code == 3
    assert "[FAIL] AUTH-1 credentials configured" in out
    assert "credentials not configured" in out
    # We must NOT proceed past AUTH-1 if creds are missing — the smoke
    # early-outs to avoid 7 redundant FAILs.
    assert "READ-1" not in out


def test_missing_api_secret_exit_3(capsys: pytest.CaptureFixture[str]) -> None:
    """api_secret empty → exit 3 (symmetric to missing key)."""
    exit_code = _run_smoke(_SmokeMocks(api_secret=""))
    out = capsys.readouterr().out
    assert exit_code == 3
    assert "[FAIL] AUTH-1 credentials configured" in out


def test_creds_must_not_appear_in_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """API key + secret are NOT printed even on success. Pin the boundary."""
    exit_code = _run_smoke(_SmokeMocks(
        api_key="REAL-LOOKING-KEY-ABC123-DEF456",
        api_secret="REAL-LOOKING-SECRET-XYZ789",  # noqa: S106
    ))
    out = capsys.readouterr().out
    assert exit_code == 0
    assert "REAL-LOOKING-KEY-ABC123-DEF456" not in out
    assert "REAL-LOOKING-SECRET-XYZ789" not in out


# ──────────────────────────────────────────────────────────────────────────
# Exit code 2 — functional failure
# ──────────────────────────────────────────────────────────────────────────


def test_hedge_mode_exit_2_with_specific_message(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """fetch_position_mode → True (hedge) → READ-1 fails, exit 2.

    Also tests CONSISTENCY-1 surfaces the same finding (assert_one_way
    will be made to raise to mirror the real flow)."""
    exit_code = _run_smoke(_SmokeMocks(
        position_mode=True,
        startup_check_raises=RuntimeError(
            "BINANCE: account is in HEDGE mode. Switch to ONE-WAY in the UI "
            "before running G6 executor. Every order without positionSide will "
            "fail -4061.",
        ),
    ))
    out = capsys.readouterr().out
    assert exit_code == 2
    assert "[FAIL] READ-1 position mode" in out
    assert "HEDGE mode detected" in out
    # The CONSISTENCY-1 gate also fails (mirrors what G6.4 boot would see)
    assert "[FAIL] CONSISTENCY-1 position-mode gate" in out


def test_missing_btcusdt_market_exit_2(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """exchange_info missing BTCUSDT → PUBLIC-1 fails with specific message."""
    info = _all_markets_present()
    del info["BTCUSDT"]
    exit_code = _run_smoke(_SmokeMocks(exchange_info=info))
    out = capsys.readouterr().out
    assert exit_code == 2
    assert "[FAIL] PUBLIC-1 exchange info" in out
    assert "missing markets: BTCUSDT" in out


def test_partial_failure_emits_both_pass_and_fail_lines(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """One check fails, others pass → exit 2 with BOTH pass + fail lines."""
    # Drop SOLUSDT from exchange info but keep everything else healthy.
    info = _all_markets_present()
    del info["SOLUSDT"]
    exit_code = _run_smoke(_SmokeMocks(exchange_info=info))
    out = capsys.readouterr().out
    assert exit_code == 2
    # At least one pass and at least one fail should be present.
    assert "[PASS]" in out
    assert "[FAIL]" in out
    # The fail should call out the missing market explicitly.
    assert "missing markets: SOLUSDT" in out
    # READ-1 / READ-2 / READ-3 still pass (signed reads happy)
    assert "[PASS] READ-1" in out
    assert "[PASS] READ-2" in out
    assert "[PASS] READ-3" in out


def test_empty_funding_dict_exit_2(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """fetch_all_cex_fundings returns {} → PUBLIC-2 fails."""
    exit_code = _run_smoke(_SmokeMocks(fundings={}))
    out = capsys.readouterr().out
    assert exit_code == 2
    assert "[FAIL] PUBLIC-2 funding rates" in out


# ──────────────────────────────────────────────────────────────────────────
# Exit code 4 — API unreachable (every signed read returned None)
# ──────────────────────────────────────────────────────────────────────────


def test_auth_failure_all_signed_none_exit_4(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Every signed read returns None → exit 4 (API down / unreachable).

    This is the auth/network failure surface — distinct from a functional
    failure because the operator's recovery is different (debug creds /
    IP / clock, not fix account config)."""
    exit_code = _run_smoke(_SmokeMocks(
        position_mode=None,
        balance=None,
        usdt_free=None,
        positions=None,
        # Public reads still work — they don't require auth.
        # If they ALSO failed it would mean the network is down.
        startup_check_raises=RuntimeError(
            "BINANCE: cannot verify position mode — auth issue or API down",
        ),
    ))
    out = capsys.readouterr().out
    assert exit_code == 4
    assert "exit_code: 4" in out
    assert "API unreachable" in out


# ──────────────────────────────────────────────────────────────────────────
# AUTH-2 mainnet warning
# ──────────────────────────────────────────────────────────────────────────


def test_mainnet_base_url_warns_not_fails(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Mainnet base URL → WARN (not FAIL) — operator can still proceed."""
    exit_code = _run_smoke(_SmokeMocks(base_url="https://fapi.binance.com"))
    out = capsys.readouterr().out
    # Should still exit 0 since the only WARN doesn't push n_fail up.
    assert exit_code == 0
    assert "[WARN] AUTH-2" in out
    assert "mainnet detected" in out
    assert "WARN=1" in out


# ──────────────────────────────────────────────────────────────────────────
# --force-refresh-exchange-info
# ──────────────────────────────────────────────────────────────────────────


def test_force_refresh_flag_does_not_break(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The --force-refresh-exchange-info flag should run identically to
    the default path on the happy case. It's a cache-reset side-effect;
    behaviour should be the same."""
    exit_code = _run_smoke(_SmokeMocks(), force_refresh=True)
    out = capsys.readouterr().out
    assert exit_code == 0
    assert "PASS=9" in out


# ──────────────────────────────────────────────────────────────────────────
# Edge cases — balance shape robustness
# ──────────────────────────────────────────────────────────────────────────


def test_balance_empty_list_warns(capsys: pytest.CaptureFixture[str]) -> None:
    """An empty balance list is unusual but not a hard fail (fresh
    account that's never deposited). Should WARN."""
    exit_code = _run_smoke(_SmokeMocks(balance=[]))
    out = capsys.readouterr().out
    # Empty list is a WARN, not a FAIL. No FAIL means exit 0.
    assert exit_code == 0
    assert "[WARN] READ-2" in out
    assert "fresh account" in out


def test_position_info_with_open_position_counts_it(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A non-zero positionAmt should be counted in n_open."""
    positions = [
        {"symbol": "BTCUSDT", "positionAmt": "0.005", "positionSide": "BOTH"},
        {"symbol": "ETHUSDT", "positionAmt": "0", "positionSide": "BOTH"},
        {"symbol": "SOLUSDT", "positionAmt": "-1.5", "positionSide": "BOTH"},
    ]
    exit_code = _run_smoke(_SmokeMocks(positions=positions))
    out = capsys.readouterr().out
    assert exit_code == 0
    # Two non-zero positions (BTC long + SOL short); one flat.
    assert "n_entries=3 n_open=2" in out
