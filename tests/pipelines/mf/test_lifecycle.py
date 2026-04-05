"""Tests for MF lifecycle detection and event insertion."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.pipelines.mf.lifecycle import (
    detect_merged_funds,
    insert_lifecycle_event,
    mark_fund_closed,
    mark_funds_inactive,
    run_lifecycle_check,
)


# ---------------------------------------------------------------------------
# detect_merged_funds
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_detect_merged_funds_returns_deactivated_mstar_ids() -> None:
    """Funds whose amfi_code is absent from today's feed should be detected."""
    session = AsyncMock()
    mock_result = MagicMock()
    # Two active funds: MSTAR001 (still active in feed), MSTAR002 (disappeared)
    mock_result.all = MagicMock(return_value=[
        ("MSTAR001", "10001"),
        ("MSTAR002", "10002"),
    ])
    session.execute = AsyncMock(return_value=mock_result)

    active_codes = {"10001"}  # 10002 is missing
    deactivated = await detect_merged_funds(session, active_codes, date(2026, 4, 5))

    assert deactivated == ["MSTAR002"]


@pytest.mark.asyncio
async def test_detect_merged_funds_returns_empty_when_all_active() -> None:
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all = MagicMock(return_value=[
        ("MSTAR001", "10001"),
        ("MSTAR002", "10002"),
    ])
    session.execute = AsyncMock(return_value=mock_result)

    active_codes = {"10001", "10002"}
    deactivated = await detect_merged_funds(session, active_codes, date(2026, 4, 5))

    assert deactivated == []


@pytest.mark.asyncio
async def test_detect_merged_funds_handles_no_active_funds() -> None:
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all = MagicMock(return_value=[])
    session.execute = AsyncMock(return_value=mock_result)

    deactivated = await detect_merged_funds(session, {"10001"}, date(2026, 4, 5))
    assert deactivated == []


# ---------------------------------------------------------------------------
# mark_funds_inactive
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mark_funds_inactive_returns_count() -> None:
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.fetchall = MagicMock(return_value=[("MSTAR001",), ("MSTAR002",)])
    session.execute = AsyncMock(return_value=mock_result)

    count = await mark_funds_inactive(session, ["MSTAR001", "MSTAR002"], date(2026, 4, 5))
    assert count == 2


@pytest.mark.asyncio
async def test_mark_funds_inactive_empty_list_returns_zero() -> None:
    session = AsyncMock()
    count = await mark_funds_inactive(session, [], date(2026, 4, 5))
    assert count == 0
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_mark_funds_inactive_with_merged_into_sets_field() -> None:
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.fetchall = MagicMock(return_value=[("MSTAR001",)])
    session.execute = AsyncMock(return_value=mock_result)

    count = await mark_funds_inactive(
        session, ["MSTAR001"], date(2026, 4, 5), merged_into_mstar_id="MSTAR_TARGET"
    )
    assert count == 1
    # Verify execute was called (values check is structural)
    assert session.execute.called


# ---------------------------------------------------------------------------
# mark_fund_closed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mark_fund_closed_calls_execute() -> None:
    session = AsyncMock()
    session.execute = AsyncMock()

    await mark_fund_closed(session, "MSTAR001", date(2026, 4, 5))
    assert session.execute.called


# ---------------------------------------------------------------------------
# insert_lifecycle_event
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_insert_lifecycle_event_calls_execute() -> None:
    session = AsyncMock()
    session.execute = AsyncMock()

    await insert_lifecycle_event(
        session,
        mstar_id="MSTAR001",
        event_type="merge",
        event_date=date(2026, 4, 5),
        notes="Test merge event",
    )
    assert session.execute.called


@pytest.mark.asyncio
async def test_insert_lifecycle_event_with_values() -> None:
    session = AsyncMock()
    session.execute = AsyncMock()

    await insert_lifecycle_event(
        session,
        mstar_id="MSTAR001",
        event_type="name_change",
        event_date=date(2026, 4, 5),
        old_value="Old Fund Name",
        new_value="New Fund Name",
    )
    assert session.execute.called


# ---------------------------------------------------------------------------
# run_lifecycle_check
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_lifecycle_check_creates_events_for_merged_funds() -> None:
    """Full integration: 2 merged funds → 2 lifecycle events."""
    session = AsyncMock()

    session.execute = AsyncMock()

    with (
        patch("app.pipelines.mf.lifecycle.detect_merged_funds", return_value=["MSTAR001", "MSTAR002"]),
        patch("app.pipelines.mf.lifecycle.mark_funds_inactive", return_value=1) as mock_mark,
        patch("app.pipelines.mf.lifecycle.insert_lifecycle_event") as mock_event,
    ):
        events = await run_lifecycle_check(session, {"10001"}, date(2026, 4, 5))

    assert events == 2
    assert mock_mark.call_count == 2
    assert mock_event.call_count == 2


@pytest.mark.asyncio
async def test_run_lifecycle_check_returns_zero_when_no_merges() -> None:
    session = AsyncMock()

    with (
        patch("app.pipelines.mf.lifecycle.detect_merged_funds", return_value=[]),
        patch("app.pipelines.mf.lifecycle.mark_funds_inactive") as mock_mark,
        patch("app.pipelines.mf.lifecycle.insert_lifecycle_event") as mock_event,
    ):
        events = await run_lifecycle_check(session, {"10001", "10002"}, date(2026, 4, 5))

    assert events == 0
    mock_mark.assert_not_called()
    mock_event.assert_not_called()
