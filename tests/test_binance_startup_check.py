"""Tests for the Binance Futures startup-check refuse-to-run gate (G6.2).

This module IS the H3 audit gate — the only G6.2 module allowed to raise.
Tests assert:

  - HEDGE mode (fetch_position_mode → True) raises with the exact message
    operators see in alerts.
  - UNKNOWN (fetch_position_mode → None) raises with the auth-issue message.
  - ONE-WAY (fetch_position_mode → False) returns silently.
  - Exception messages are exactly what the audit prescribes — operators
    rely on the wording in on-call runbooks.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from gmx_strategies import binance_startup_check


@pytest.mark.asyncio
async def test_hedge_mode_raises_with_hard_message() -> None:
    """`dualSidePosition=true` (hedge) → RuntimeError with the exact
    message operators see. Wording is load-bearing for the runbook."""
    with patch(
        "gmx_strategies.binance_startup_check.fetch_position_mode",
        new=AsyncMock(return_value=True),
    ):
        with pytest.raises(RuntimeError) as exc_info:
            await binance_startup_check.assert_one_way_position_mode()
    msg = str(exc_info.value)
    # Match the actual exception text (must contain the audit-prescribed cues).
    assert "HEDGE" in msg
    assert "ONE-WAY" in msg
    assert "-4061" in msg


@pytest.mark.asyncio
async def test_unknown_mode_raises_with_api_down_message() -> None:
    """`None` return (auth gap or API down) → RuntimeError with the
    'cannot verify' wording — distinct from the hedge-mode message so
    operators triage differently."""
    with patch(
        "gmx_strategies.binance_startup_check.fetch_position_mode",
        new=AsyncMock(return_value=None),
    ):
        with pytest.raises(RuntimeError) as exc_info:
            await binance_startup_check.assert_one_way_position_mode()
    msg = str(exc_info.value)
    assert "cannot verify position mode" in msg
    assert "auth issue or API down" in msg


@pytest.mark.asyncio
async def test_one_way_mode_passes_silently() -> None:
    """`dualSidePosition=false` (one-way) → no raise, no return value."""
    with patch(
        "gmx_strategies.binance_startup_check.fetch_position_mode",
        new=AsyncMock(return_value=False),
    ):
        # Should not raise.
        result = await binance_startup_check.assert_one_way_position_mode()
    assert result is None


@pytest.mark.asyncio
async def test_hedge_and_unknown_messages_are_distinct() -> None:
    """Sanity: the two raise paths produce DIFFERENT exception messages
    so on-call paging can route correctly (UI flip vs auth/network triage)."""
    with patch(
        "gmx_strategies.binance_startup_check.fetch_position_mode",
        new=AsyncMock(return_value=True),
    ):
        with pytest.raises(RuntimeError) as hedge_exc:
            await binance_startup_check.assert_one_way_position_mode()

    with patch(
        "gmx_strategies.binance_startup_check.fetch_position_mode",
        new=AsyncMock(return_value=None),
    ):
        with pytest.raises(RuntimeError) as unknown_exc:
            await binance_startup_check.assert_one_way_position_mode()

    assert str(hedge_exc.value) != str(unknown_exc.value)
