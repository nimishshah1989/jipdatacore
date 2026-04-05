"""Tests for SLA enforcement."""

from __future__ import annotations

from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest

from app.orchestrator.sla import SLABreachEvent, SLAChecker, SLAConfig

IST = ZoneInfo("Asia/Kolkata")


def _make_session(pipeline_statuses: dict[str, str]) -> AsyncMock:
    """Create a mock session that returns given pipeline statuses."""
    session = AsyncMock()

    # First execute call: get latest run numbers
    latest_run_result = MagicMock()
    latest_run_rows = []
    for pipeline_name, run_number in [(p, 1) for p in pipeline_statuses]:
        row = MagicMock()
        row.pipeline_name = pipeline_name
        row.max_run = 1
        latest_run_rows.append(row)
    latest_run_result.fetchall.return_value = latest_run_rows

    # Second execute call: get statuses for latest runs
    status_result = MagicMock()
    status_rows = []
    for pipeline_name, status in pipeline_statuses.items():
        row = MagicMock()
        row.pipeline_name = pipeline_name
        row.status = status
        status_rows.append(row)
    status_result.fetchall.return_value = status_rows

    session.execute.side_effect = [latest_run_result, status_result]
    return session


class TestSLAConfig:
    def test_default_slas_non_empty(self) -> None:
        checker = SLAChecker()
        assert len(checker._slas) > 0

    def test_get_sla_by_name(self) -> None:
        checker = SLAChecker()
        sla = checker.get_sla("nse_bhav")
        assert sla is not None
        assert sla.pipeline_name == "nse_bhav"
        assert sla.deadline_hour == 8
        assert sla.deadline_minute == 30
        assert sla.severity == "critical"

    def test_get_sla_unknown_returns_none(self) -> None:
        checker = SLAChecker()
        assert checker.get_sla("nonexistent_pipeline") is None

    def test_deadline_for_date_returns_ist(self) -> None:
        sla = SLAConfig("nse_bhav", 8, 30, "critical")
        checker = SLAChecker(slas=[sla])
        bd = date(2026, 4, 5)
        deadline = checker._deadline_for_date(sla, bd)
        assert deadline.tzinfo == IST
        assert deadline.hour == 8
        assert deadline.minute == 30


class TestSLACheck:
    @pytest.mark.asyncio
    async def test_no_breach_before_deadline(self) -> None:
        sla = SLAConfig("nse_bhav", 8, 30, "critical")
        checker = SLAChecker(slas=[sla])
        bd = date(2026, 4, 5)

        # Now is before deadline
        now = datetime(2026, 4, 5, 8, 0, 0, tzinfo=IST)
        session = _make_session({"nse_bhav": "running"})

        breaches = await checker.check(session, bd, now=now)
        assert len(breaches) == 0

    @pytest.mark.asyncio
    async def test_breach_detected_when_past_deadline_and_not_complete(self) -> None:
        sla = SLAConfig("nse_bhav", 8, 30, "critical")
        checker = SLAChecker(slas=[sla])
        bd = date(2026, 4, 5)

        # Now is after deadline, pipeline still running
        now = datetime(2026, 4, 5, 9, 0, 0, tzinfo=IST)
        session = _make_session({"nse_bhav": "running"})

        breaches = await checker.check(session, bd, now=now)
        assert len(breaches) == 1
        breach = breaches[0]
        assert breach.pipeline_name == "nse_bhav"
        assert breach.current_status == "running"
        assert breach.severity == "critical"

    @pytest.mark.asyncio
    async def test_no_breach_when_complete_past_deadline(self) -> None:
        sla = SLAConfig("nse_bhav", 8, 30, "critical")
        checker = SLAChecker(slas=[sla])
        bd = date(2026, 4, 5)

        # Past deadline, but pipeline succeeded
        now = datetime(2026, 4, 5, 9, 0, 0, tzinfo=IST)
        session = _make_session({"nse_bhav": "success"})

        breaches = await checker.check(session, bd, now=now)
        assert len(breaches) == 0

    @pytest.mark.asyncio
    async def test_no_breach_when_partial_past_deadline(self) -> None:
        sla = SLAConfig("nse_bhav", 8, 30, "critical")
        checker = SLAChecker(slas=[sla])
        bd = date(2026, 4, 5)

        now = datetime(2026, 4, 5, 9, 0, 0, tzinfo=IST)
        session = _make_session({"nse_bhav": "partial"})

        breaches = await checker.check(session, bd, now=now)
        assert len(breaches) == 0

    @pytest.mark.asyncio
    async def test_no_breach_when_skipped(self) -> None:
        sla = SLAConfig("nse_bhav", 8, 30, "critical")
        checker = SLAChecker(slas=[sla])
        bd = date(2026, 4, 5)

        now = datetime(2026, 4, 5, 9, 0, 0, tzinfo=IST)
        session = _make_session({"nse_bhav": "skipped"})

        breaches = await checker.check(session, bd, now=now)
        assert len(breaches) == 0

    @pytest.mark.asyncio
    async def test_breach_when_not_started(self) -> None:
        sla = SLAConfig("amfi_nav", 10, 0, "warning")
        checker = SLAChecker(slas=[sla])
        bd = date(2026, 4, 5)

        now = datetime(2026, 4, 5, 11, 0, 0, tzinfo=IST)
        # Pipeline not in de_pipeline_log at all
        session = _make_session({})
        # Override to return empty results
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        session.execute.side_effect = [mock_result, mock_result]

        breaches = await checker.check(session, bd, now=now)
        assert len(breaches) == 1
        assert breaches[0].current_status == "not_started"
        assert breaches[0].severity == "warning"

    @pytest.mark.asyncio
    async def test_multiple_slas_multiple_breaches(self) -> None:
        slas = [
            SLAConfig("pipeline_a", 8, 0, "critical"),
            SLAConfig("pipeline_b", 9, 0, "warning"),
        ]
        checker = SLAChecker(slas=slas)
        bd = date(2026, 4, 5)

        now = datetime(2026, 4, 5, 10, 0, 0, tzinfo=IST)
        session = _make_session({"pipeline_a": "failed", "pipeline_b": "pending"})

        breaches = await checker.check(session, bd, now=now)
        assert len(breaches) == 2
        names = {b.pipeline_name for b in breaches}
        assert names == {"pipeline_a", "pipeline_b"}


class TestSLABreachEvent:
    def test_breach_event_attributes(self) -> None:
        now = datetime(2026, 4, 5, 9, 0, 0, tzinfo=IST)
        deadline = datetime(2026, 4, 5, 8, 30, 0, tzinfo=IST)
        breach = SLABreachEvent(
            pipeline_name="nse_bhav",
            business_date=date(2026, 4, 5),
            deadline_ist=deadline,
            current_status="running",
            severity="critical",
            detected_at=now,
        )
        assert breach.pipeline_name == "nse_bhav"
        assert breach.severity == "critical"
        assert breach.detected_at > breach.deadline_ist
