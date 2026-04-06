"""Unit tests for pre-computation QA checks.

Each check function is tested independently using a mock AsyncSession.
The run_pre_computation_qa entry point is tested for aggregation logic.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.computation.pre_qa import (
    BENCHMARKS,
    FLOW_STALE_DAYS,
    HOLDINGS_STALE_DAYS,
    MF_NAV_CRITICAL,
    MF_NAV_WARNING,
    OHLCV_CRITICAL,
    OHLCV_WARNING,
    check_benchmark_availability,
    check_flow_data_availability,
    check_high_low_consistency,
    check_holdings_freshness,
    check_mf_nav_coverage,
    check_mf_nav_non_negative,
    check_no_negative_prices,
    check_ohlcv_coverage,
    check_price_spikes,
    check_zero_volume_pct,
    run_pre_computation_qa,
)
from app.computation.qa_types import QAReport, StepResult

BUSINESS_DATE = date(2026, 4, 4)


# ---------------------------------------------------------------------------
# Helpers: build mock session that returns fixed scalar / rows
# ---------------------------------------------------------------------------


def _mock_session_scalar(value: int | float | None) -> AsyncMock:
    """Build an AsyncSession mock whose execute() returns a scalar result."""
    result_mock = MagicMock()
    result_mock.scalar_one.return_value = value
    session = AsyncMock()
    session.execute.return_value = result_mock
    return session


def _mock_session_fetchone(row: tuple) -> AsyncMock:
    """Build an AsyncSession mock whose execute() returns a single fetchone row."""
    result_mock = MagicMock()
    result_mock.fetchone.return_value = row
    session = AsyncMock()
    session.execute.return_value = result_mock
    return session


def _mock_session_fetchall(rows: list[tuple]) -> AsyncMock:
    """Build an AsyncSession mock whose execute() returns fetchall rows."""
    result_mock = MagicMock()
    result_mock.fetchall.return_value = rows
    session = AsyncMock()
    session.execute.return_value = result_mock
    return session


# ---------------------------------------------------------------------------
# QAReport / StepResult unit tests
# ---------------------------------------------------------------------------


def test_step_result_mark_complete_sets_status() -> None:
    step = StepResult(step_name="test_step", status="running")
    step.mark_complete("passed")
    assert step.status == "passed"
    assert step.completed_at is not None
    assert step.duration_ms is not None
    assert step.duration_ms >= 0


def test_step_result_errors_accumulate() -> None:
    step = StepResult(step_name="test_step", status="running")
    step.errors.append("error 1")
    step.errors.append("error 2")
    assert len(step.errors) == 2


def test_qa_report_add_step_failed_overrides_passed() -> None:
    report = QAReport(phase="pre_qa", business_date=BUSINESS_DATE)
    ok_step = StepResult(step_name="ok", status="running")
    ok_step.mark_complete("passed")
    fail_step = StepResult(step_name="fail", status="running")
    fail_step.mark_complete("failed")
    report.add_step(ok_step)
    assert report.overall_status == "passed"
    report.add_step(fail_step)
    assert report.overall_status == "failed"


def test_qa_report_add_step_warning_does_not_override_failed() -> None:
    report = QAReport(phase="pre_qa", business_date=BUSINESS_DATE)
    fail_step = StepResult(step_name="fail", status="running")
    fail_step.mark_complete("failed")
    warn_step = StepResult(step_name="warn", status="running")
    warn_step.mark_complete("warning")
    report.add_step(fail_step)
    report.add_step(warn_step)
    assert report.overall_status == "failed"


def test_qa_report_warning_upgrades_passed() -> None:
    report = QAReport(phase="pre_qa", business_date=BUSINESS_DATE)
    warn_step = StepResult(step_name="warn", status="running")
    warn_step.mark_complete("warning")
    report.add_step(warn_step)
    assert report.overall_status == "warning"


def test_qa_report_counts() -> None:
    report = QAReport(phase="pre_qa", business_date=BUSINESS_DATE)
    for status in ("passed", "passed", "warning", "failed"):
        s = StepResult(step_name=f"s_{status}", status="running")
        s.mark_complete(status)
        report.add_step(s)
    assert report.passed_count == 2
    assert report.warning_count == 1
    assert report.failed_count == 1


def test_qa_report_to_dict_structure() -> None:
    report = QAReport(phase="pre_qa", business_date=BUSINESS_DATE)
    s = StepResult(step_name="x", status="running")
    s.mark_complete("passed")
    report.add_step(s)
    report.mark_complete()
    d = report.to_dict()
    assert d["phase"] == "pre_qa"
    assert d["overall_status"] == "passed"
    assert len(d["steps"]) == 1
    assert d["steps"][0]["step_name"] == "x"


# ---------------------------------------------------------------------------
# check_ohlcv_coverage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_ohlcv_coverage_passed() -> None:
    session = _mock_session_scalar(2000)
    step = await check_ohlcv_coverage(session, BUSINESS_DATE)
    assert step.status == "passed"
    assert step.rows_affected == 2000
    assert step.details["validated_rows"] == 2000
    assert step.errors == []


@pytest.mark.asyncio
async def test_check_ohlcv_coverage_warning() -> None:
    session = _mock_session_scalar(400)
    step = await check_ohlcv_coverage(session, BUSINESS_DATE)
    assert step.status == "warning"
    assert step.rows_affected == 400
    assert len(step.errors) == 1


@pytest.mark.asyncio
async def test_check_ohlcv_coverage_failed_critical() -> None:
    session = _mock_session_scalar(100)
    step = await check_ohlcv_coverage(session, BUSINESS_DATE)
    assert step.status == "failed"
    assert step.rows_affected == 100
    assert any(str(OHLCV_CRITICAL) in e for e in step.errors)


@pytest.mark.asyncio
async def test_check_ohlcv_coverage_boundary_exactly_critical() -> None:
    """Exactly at the critical threshold = warning (not failed)."""
    session = _mock_session_scalar(OHLCV_CRITICAL)
    step = await check_ohlcv_coverage(session, BUSINESS_DATE)
    assert step.status == "warning"


@pytest.mark.asyncio
async def test_check_ohlcv_coverage_boundary_exactly_warning() -> None:
    """Exactly at the warning threshold = passed."""
    session = _mock_session_scalar(OHLCV_WARNING)
    step = await check_ohlcv_coverage(session, BUSINESS_DATE)
    assert step.status == "passed"


# ---------------------------------------------------------------------------
# check_no_negative_prices
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_no_negative_prices_passed() -> None:
    session = _mock_session_scalar(0)
    step = await check_no_negative_prices(session, BUSINESS_DATE)
    assert step.status == "passed"
    assert step.errors == []


@pytest.mark.asyncio
async def test_check_no_negative_prices_failed() -> None:
    session = _mock_session_scalar(3)
    step = await check_no_negative_prices(session, BUSINESS_DATE)
    assert step.status == "failed"
    assert step.rows_affected == 3
    assert len(step.errors) == 1


# ---------------------------------------------------------------------------
# check_high_low_consistency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_high_low_consistency_passed() -> None:
    session = _mock_session_fetchall([])
    step = await check_high_low_consistency(session, BUSINESS_DATE)
    assert step.status == "passed"
    assert step.details["violation_count"] == 0
    assert step.details["sample_symbols"] == []


@pytest.mark.asyncio
async def test_check_high_low_consistency_failed_includes_symbols() -> None:
    session = _mock_session_fetchall([("RELIANCE",), ("INFY",)])
    step = await check_high_low_consistency(session, BUSINESS_DATE)
    assert step.status == "failed"
    assert step.details["violation_count"] == 2
    assert "RELIANCE" in step.details["sample_symbols"]
    assert len(step.errors) == 1


# ---------------------------------------------------------------------------
# check_zero_volume_pct
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_zero_volume_pct_passed() -> None:
    # 5 out of 2000 = 0.25% — under threshold
    session = _mock_session_fetchone((5, 2000))
    step = await check_zero_volume_pct(session, BUSINESS_DATE)
    assert step.status == "passed"
    assert step.details["zero_volume_count"] == 5
    assert step.details["total_rows"] == 2000


@pytest.mark.asyncio
async def test_check_zero_volume_pct_warning_exceeds_threshold() -> None:
    # 300 out of 2000 = 15% — over 10% threshold
    session = _mock_session_fetchone((300, 2000))
    step = await check_zero_volume_pct(session, BUSINESS_DATE)
    assert step.status == "warning"
    assert len(step.errors) == 1


@pytest.mark.asyncio
async def test_check_zero_volume_pct_zero_total() -> None:
    """No rows at all — should pass (can't exceed percentage)."""
    session = _mock_session_fetchone((0, 0))
    step = await check_zero_volume_pct(session, BUSINESS_DATE)
    assert step.status == "passed"


@pytest.mark.asyncio
async def test_check_zero_volume_pct_boundary() -> None:
    """Exactly 10% — should pass (not strictly greater than)."""
    session = _mock_session_fetchone((200, 2000))
    step = await check_zero_volume_pct(session, BUSINESS_DATE)
    assert step.status == "passed"


# ---------------------------------------------------------------------------
# check_price_spikes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_price_spikes_passed() -> None:
    # 10 spikes out of 2000 = 0.5% — under 5% threshold
    session = _mock_session_fetchone((10, 2000))
    step = await check_price_spikes(session, BUSINESS_DATE)
    assert step.status == "passed"
    assert step.details["spike_count"] == 10


@pytest.mark.asyncio
async def test_check_price_spikes_failed_exceeds_threshold() -> None:
    # 200 spikes out of 2000 = 10% — over 5% threshold
    session = _mock_session_fetchone((200, 2000))
    step = await check_price_spikes(session, BUSINESS_DATE)
    assert step.status == "failed"
    assert step.rows_affected == 200
    assert len(step.errors) == 1


@pytest.mark.asyncio
async def test_check_price_spikes_no_universe() -> None:
    """Empty universe — should pass."""
    session = _mock_session_fetchone((0, 0))
    step = await check_price_spikes(session, BUSINESS_DATE)
    assert step.status == "passed"


@pytest.mark.asyncio
async def test_check_price_spikes_details_populated() -> None:
    session = _mock_session_fetchone((50, 1000))
    step = await check_price_spikes(session, BUSINESS_DATE)
    assert "spike_pct" in step.details
    assert step.details["spike_pct"] == 5.0


# ---------------------------------------------------------------------------
# check_benchmark_availability
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_benchmark_availability_passed_all_found() -> None:
    rows = [(b,) for b in BENCHMARKS]
    session = _mock_session_fetchall(rows)
    step = await check_benchmark_availability(session, BUSINESS_DATE)
    assert step.status == "passed"
    assert step.details["missing"] == []


@pytest.mark.asyncio
async def test_check_benchmark_availability_failed_some_missing() -> None:
    # Only NIFTY 50 found
    session = _mock_session_fetchall([("NIFTY 50",)])
    step = await check_benchmark_availability(session, BUSINESS_DATE)
    assert step.status == "failed"
    assert "NIFTY 500" in step.details["missing"]
    assert "NIFTY MIDCAP 100" in step.details["missing"]
    assert len(step.errors) == 1


@pytest.mark.asyncio
async def test_check_benchmark_availability_failed_none_found() -> None:
    session = _mock_session_fetchall([])
    step = await check_benchmark_availability(session, BUSINESS_DATE)
    assert step.status == "failed"
    assert len(step.details["missing"]) == len(BENCHMARKS)


# ---------------------------------------------------------------------------
# check_mf_nav_coverage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_mf_nav_coverage_passed() -> None:
    session = _mock_session_scalar(500)
    step = await check_mf_nav_coverage(session, BUSINESS_DATE)
    assert step.status == "passed"
    assert step.details["distinct_funds"] == 500


@pytest.mark.asyncio
async def test_check_mf_nav_coverage_warning() -> None:
    session = _mock_session_scalar(250)
    step = await check_mf_nav_coverage(session, BUSINESS_DATE)
    assert step.status == "warning"


@pytest.mark.asyncio
async def test_check_mf_nav_coverage_failed_critical() -> None:
    session = _mock_session_scalar(100)
    step = await check_mf_nav_coverage(session, BUSINESS_DATE)
    assert step.status == "failed"
    assert any(str(MF_NAV_CRITICAL) in e for e in step.errors)


@pytest.mark.asyncio
async def test_check_mf_nav_coverage_boundary_at_critical() -> None:
    """Exactly at MF_NAV_CRITICAL = warning (not failed)."""
    session = _mock_session_scalar(MF_NAV_CRITICAL)
    step = await check_mf_nav_coverage(session, BUSINESS_DATE)
    assert step.status == "warning"


@pytest.mark.asyncio
async def test_check_mf_nav_coverage_boundary_at_warning() -> None:
    """Exactly at MF_NAV_WARNING = passed."""
    session = _mock_session_scalar(MF_NAV_WARNING)
    step = await check_mf_nav_coverage(session, BUSINESS_DATE)
    assert step.status == "passed"


# ---------------------------------------------------------------------------
# check_mf_nav_non_negative
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_mf_nav_non_negative_passed() -> None:
    session = _mock_session_scalar(0)
    step = await check_mf_nav_non_negative(session, BUSINESS_DATE)
    assert step.status == "passed"
    assert step.errors == []


@pytest.mark.asyncio
async def test_check_mf_nav_non_negative_failed() -> None:
    session = _mock_session_scalar(2)
    step = await check_mf_nav_non_negative(session, BUSINESS_DATE)
    assert step.status == "failed"
    assert step.rows_affected == 2
    assert len(step.errors) == 1


# ---------------------------------------------------------------------------
# check_holdings_freshness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_holdings_freshness_passed_recent() -> None:
    latest = date(2026, 3, 31)  # 4 days before business_date
    session = _mock_session_scalar(latest)
    step = await check_holdings_freshness(session, BUSINESS_DATE)
    assert step.status == "passed"
    assert step.details["days_since_last_holdings"] == 4


@pytest.mark.asyncio
async def test_check_holdings_freshness_warning_stale() -> None:
    # 100 days before business_date
    latest = date(2025, 12, 26)
    session = _mock_session_scalar(latest)
    step = await check_holdings_freshness(session, BUSINESS_DATE)
    assert step.status == "warning"
    assert step.details["days_since_last_holdings"] > HOLDINGS_STALE_DAYS
    assert len(step.errors) == 1


@pytest.mark.asyncio
async def test_check_holdings_freshness_warning_no_data() -> None:
    session = _mock_session_scalar(None)
    step = await check_holdings_freshness(session, BUSINESS_DATE)
    assert step.status == "warning"
    assert step.details["latest_holdings_date"] is None


@pytest.mark.asyncio
async def test_check_holdings_freshness_boundary_exactly_90_days() -> None:
    """Exactly 90 days old = passed (not warning, uses strict >)."""
    import datetime
    latest = BUSINESS_DATE - datetime.timedelta(days=HOLDINGS_STALE_DAYS)
    session = _mock_session_scalar(latest)
    step = await check_holdings_freshness(session, BUSINESS_DATE)
    assert step.status == "passed"


# ---------------------------------------------------------------------------
# check_flow_data_availability
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_flow_data_availability_passed_recent() -> None:
    latest = date(2026, 4, 3)  # 1 day before business_date
    session = _mock_session_scalar(latest)
    step = await check_flow_data_availability(session, BUSINESS_DATE)
    assert step.status == "passed"


@pytest.mark.asyncio
async def test_check_flow_data_availability_warning_stale() -> None:
    # 10 days before business_date — over 5-day threshold
    latest = date(2026, 3, 25)
    session = _mock_session_scalar(latest)
    step = await check_flow_data_availability(session, BUSINESS_DATE)
    assert step.status == "warning"
    assert step.details["days_since_last_flow"] > FLOW_STALE_DAYS
    assert len(step.errors) == 1


@pytest.mark.asyncio
async def test_check_flow_data_availability_warning_no_data() -> None:
    session = _mock_session_scalar(None)
    step = await check_flow_data_availability(session, BUSINESS_DATE)
    assert step.status == "warning"
    assert step.details["latest_flow_date"] is None


@pytest.mark.asyncio
async def test_check_flow_data_availability_boundary_exactly_5_days() -> None:
    """Exactly 5 days old = passed (not warning, uses strict >)."""
    import datetime
    latest = BUSINESS_DATE - datetime.timedelta(days=FLOW_STALE_DAYS)
    session = _mock_session_scalar(latest)
    step = await check_flow_data_availability(session, BUSINESS_DATE)
    assert step.status == "passed"


# ---------------------------------------------------------------------------
# run_pre_computation_qa — integration of all checks
# ---------------------------------------------------------------------------


def _build_all_passing_session() -> AsyncMock:
    """Build a session that returns valid data for all 10 checks."""
    session = AsyncMock()

    results = [
        # 1. check_ohlcv_coverage — scalar 2000
        _scalar_result(2000),
        # 2. check_no_negative_prices — scalar 0
        _scalar_result(0),
        # 3. check_high_low_consistency — fetchall []
        _fetchall_result([]),
        # 4. check_zero_volume_pct — fetchone (10, 2000)
        _fetchone_result((10, 2000)),
        # 5. check_price_spikes — fetchone (5, 2000)
        _fetchone_result((5, 2000)),
        # 6. check_benchmark_availability — fetchall all 3 benchmarks
        _fetchall_result([(b,) for b in BENCHMARKS]),
        # 7. check_mf_nav_coverage — scalar 500
        _scalar_result(500),
        # 8. check_mf_nav_non_negative — scalar 0
        _scalar_result(0),
        # 9. check_holdings_freshness — scalar date (3 days ago)
        _scalar_result(date(2026, 4, 1)),
        # 10. check_flow_data_availability — scalar date (1 day ago)
        _scalar_result(date(2026, 4, 3)),
    ]

    session.execute = AsyncMock(side_effect=results)
    return session


def _scalar_result(value):
    m = MagicMock()
    m.scalar_one.return_value = value
    return m


def _fetchone_result(row):
    m = MagicMock()
    m.fetchone.return_value = row
    return m


def _fetchall_result(rows):
    m = MagicMock()
    m.fetchall.return_value = rows
    return m


@pytest.mark.asyncio
async def test_run_pre_computation_qa_all_passed() -> None:
    session = _build_all_passing_session()
    report = await run_pre_computation_qa(session, BUSINESS_DATE)
    assert isinstance(report, QAReport)
    assert report.phase == "pre_qa"
    assert report.business_date == BUSINESS_DATE
    assert len(report.steps) == 10
    assert report.overall_status == "passed"
    assert report.failed_count == 0


@pytest.mark.asyncio
async def test_run_pre_computation_qa_returns_failed_on_critical() -> None:
    """If ohlcv_coverage fails (critical), overall_status must be 'failed'."""
    session = AsyncMock()
    results = [
        _scalar_result(0),        # ohlcv_coverage fails (0 rows)
        _scalar_result(0),        # no_negative_prices passes
        _fetchall_result([]),     # high_low_consistency passes
        _fetchone_result((0, 2000)),  # zero_volume_pct passes
        _fetchone_result((0, 2000)),  # price_spikes passes
        _fetchall_result([(b,) for b in BENCHMARKS]),  # benchmarks pass
        _scalar_result(500),      # mf_nav_coverage passes
        _scalar_result(0),        # mf_nav_non_negative passes
        _scalar_result(date(2026, 4, 1)),  # holdings freshness passes
        _scalar_result(date(2026, 4, 3)),  # flow availability passes
    ]
    session.execute = AsyncMock(side_effect=results)
    report = await run_pre_computation_qa(session, BUSINESS_DATE)
    assert report.overall_status == "failed"
    assert report.failed_count >= 1


@pytest.mark.asyncio
async def test_run_pre_computation_qa_step_names_correct() -> None:
    session = _build_all_passing_session()
    report = await run_pre_computation_qa(session, BUSINESS_DATE)
    names = [s.step_name for s in report.steps]
    assert "check_ohlcv_coverage" in names
    assert "check_no_negative_prices" in names
    assert "check_high_low_consistency" in names
    assert "check_zero_volume_pct" in names
    assert "check_price_spikes" in names
    assert "check_benchmark_availability" in names
    assert "check_mf_nav_coverage" in names
    assert "check_mf_nav_non_negative" in names
    assert "check_holdings_freshness" in names
    assert "check_flow_data_availability" in names


@pytest.mark.asyncio
async def test_run_pre_computation_qa_exception_handled() -> None:
    """If a check raises an unexpected exception, it's captured as failed step."""
    session = AsyncMock()
    session.execute = AsyncMock(side_effect=RuntimeError("DB gone"))
    report = await run_pre_computation_qa(session, BUSINESS_DATE)
    # All steps should have errored but report still returns
    assert isinstance(report, QAReport)
    for step in report.steps:
        assert step.status == "failed"
        assert any("Unexpected error" in e for e in step.errors)
    assert report.overall_status == "failed"


@pytest.mark.asyncio
async def test_run_pre_computation_qa_timing_populated() -> None:
    session = _build_all_passing_session()
    report = await run_pre_computation_qa(session, BUSINESS_DATE)
    assert report.completed_at is not None
    assert report.duration_ms is not None
    assert report.duration_ms >= 0


@pytest.mark.asyncio
async def test_run_pre_computation_qa_to_dict_has_all_keys() -> None:
    session = _build_all_passing_session()
    report = await run_pre_computation_qa(session, BUSINESS_DATE)
    d = report.to_dict()
    assert d["phase"] == "pre_qa"
    assert d["overall_status"] == "passed"
    assert d["passed"] == 10
    assert d["failed"] == 0
    assert len(d["steps"]) == 10
