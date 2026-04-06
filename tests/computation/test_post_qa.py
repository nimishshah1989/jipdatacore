"""Unit tests for post-computation QA checks.

Tests cover pure logic (StepResult/QAReport types) and mocked DB checks.
All DB calls are mocked via AsyncMock so no real database connection needed.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.computation.qa_types import QAReport, StepResult
from app.computation.post_qa import (
    CONFIDENCE_WEIGHTS,
    VALID_REGIMES,
    check_breadth_consistency,
    check_cross_table_consistency,
    check_fund_derived_coverage,
    check_regime_validity,
    check_rs_distribution,
    check_rs_scores_populated,
    check_technicals_populated,
    check_technicals_range,
    run_post_computation_qa,
)


BUSINESS_DATE = date(2026, 4, 1)


# ---------------------------------------------------------------------------
# QAReport / StepResult unit tests (no DB)
# ---------------------------------------------------------------------------


def test_step_result_fields() -> None:
    step = StepResult(name="test_check", status="passed", message="All good")
    assert step.name == "test_check"
    assert step.status == "passed"
    assert step.message == "All good"
    assert step.details == {}


def test_step_result_with_details() -> None:
    step = StepResult(
        name="check_x",
        status="warning",
        message="Some warning",
        details={"count": 5, "threshold": 10},
    )
    assert step.details["count"] == 5


def test_qa_report_empty() -> None:
    report = QAReport(phase="post_qa", business_date=BUSINESS_DATE)
    assert report.passed == 0
    assert report.warnings == 0
    assert report.failed == 0
    assert report.overall_status == "passed"


def test_qa_report_overall_status_failed_wins() -> None:
    report = QAReport(phase="post_qa", business_date=BUSINESS_DATE)
    report.steps.append(StepResult(name="a", status="passed", message="ok"))
    report.steps.append(StepResult(name="b", status="warning", message="warn"))
    report.steps.append(StepResult(name="c", status="failed", message="fail"))
    assert report.overall_status == "failed"


def test_qa_report_overall_status_warning() -> None:
    report = QAReport(phase="post_qa", business_date=BUSINESS_DATE)
    report.steps.append(StepResult(name="a", status="passed", message="ok"))
    report.steps.append(StepResult(name="b", status="warning", message="warn"))
    assert report.overall_status == "warning"


def test_qa_report_overall_status_all_passed() -> None:
    report = QAReport(phase="post_qa", business_date=BUSINESS_DATE)
    report.steps.append(StepResult(name="a", status="passed", message="ok"))
    report.steps.append(StepResult(name="b", status="passed", message="ok2"))
    assert report.overall_status == "passed"


def test_qa_report_counts() -> None:
    report = QAReport(phase="post_qa", business_date=BUSINESS_DATE)
    report.steps.extend([
        StepResult(name="a", status="passed", message=""),
        StepResult(name="b", status="passed", message=""),
        StepResult(name="c", status="warning", message=""),
        StepResult(name="d", status="failed", message=""),
    ])
    assert report.passed == 2
    assert report.warnings == 1
    assert report.failed == 1


def test_qa_report_summary_structure() -> None:
    report = QAReport(phase="post_qa", business_date=BUSINESS_DATE)
    report.steps.append(StepResult(name="x", status="passed", message="fine", details={"k": 1}))
    summary = report.summary()
    assert summary["phase"] == "post_qa"
    assert summary["business_date"] == str(BUSINESS_DATE)
    assert "overall_status" in summary
    assert len(summary["steps"]) == 1
    assert summary["steps"][0]["details"] == {"k": 1}


def test_confidence_weights_sum_to_one() -> None:
    """CONFIDENCE_WEIGHTS must sum to 1.0."""
    total = sum(CONFIDENCE_WEIGHTS.values())
    assert abs(total - 1.0) < 1e-10


def test_valid_regimes_contains_expected() -> None:
    assert VALID_REGIMES == {"BULL", "BEAR", "SIDEWAYS", "RECOVERY"}


# ---------------------------------------------------------------------------
# Helper: build a mock AsyncSession
# ---------------------------------------------------------------------------


def _make_session(*scalar_or_row_sequence: Any) -> AsyncMock:
    """Build an AsyncMock session that returns results in order."""
    session = AsyncMock()
    results = []
    for item in scalar_or_row_sequence:
        mock_result = MagicMock()
        if isinstance(item, (int, float)) or item is None:
            mock_result.scalar.return_value = item
            mock_result.fetchone.return_value = None
        else:
            # It's a row-like object
            mock_result.scalar.return_value = None
            mock_result.fetchone.return_value = item
        results.append(mock_result)

    session.execute = AsyncMock(side_effect=results)
    return session


# ---------------------------------------------------------------------------
# check_technicals_populated tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_technicals_populated_passed() -> None:
    session = _make_session(900, 1000)  # tech_count=900, price_count=1000 → 90%
    result = await check_technicals_populated(session, BUSINESS_DATE)
    assert result.status == "passed"
    assert result.details["technical_rows"] == 900
    assert result.details["price_rows"] == 1000
    assert result.details["coverage_pct"] == 90.0


@pytest.mark.asyncio
async def test_check_technicals_populated_zero_returns_failed() -> None:
    session = _make_session(0, 1000)
    result = await check_technicals_populated(session, BUSINESS_DATE)
    assert result.status == "failed"
    assert "No rows" in result.message


@pytest.mark.asyncio
async def test_check_technicals_populated_below_80pct_warning() -> None:
    session = _make_session(700, 1000)  # 70%
    result = await check_technicals_populated(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "below 80%" in result.message


@pytest.mark.asyncio
async def test_check_technicals_populated_zero_price_count() -> None:
    """If price_count is 0, coverage is treated as 100%."""
    session = _make_session(500, 0)
    result = await check_technicals_populated(session, BUSINESS_DATE)
    assert result.status == "passed"
    assert result.details["coverage_pct"] == 100.0


@pytest.mark.asyncio
async def test_check_technicals_populated_exception_returns_failed() -> None:
    session = AsyncMock()
    session.execute = AsyncMock(side_effect=RuntimeError("DB error"))
    result = await check_technicals_populated(session, BUSINESS_DATE)
    assert result.status == "failed"
    assert "DB error" in result.message


# ---------------------------------------------------------------------------
# check_technicals_range tests
# ---------------------------------------------------------------------------


def _make_row(**kwargs: Any) -> MagicMock:
    row = MagicMock()
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


def _make_session_with_rows(*rows_or_scalars: Any) -> AsyncMock:
    """Each element is either a scalar (int/float/None) or a MagicMock row."""
    session = AsyncMock()
    results = []
    for item in rows_or_scalars:
        mock_result = MagicMock()
        if isinstance(item, (int, float)) or item is None:
            mock_result.scalar.return_value = item
            mock_result.fetchone.return_value = None
        else:
            mock_result.fetchone.return_value = item
            mock_result.scalar.return_value = None
        results.append(mock_result)
    session.execute = AsyncMock(side_effect=results)
    return session


@pytest.mark.asyncio
async def test_check_technicals_range_passed() -> None:
    row = _make_row(total=1000, outliers=10)  # 1% outliers
    session = _make_session_with_rows(row)
    result = await check_technicals_range(session, BUSINESS_DATE)
    assert result.status == "passed"
    assert result.details["outlier_count"] == 10


@pytest.mark.asyncio
async def test_check_technicals_range_warning_high_outliers() -> None:
    row = _make_row(total=1000, outliers=60)  # 6% > 5%
    session = _make_session_with_rows(row)
    result = await check_technicals_range(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "6.0%" in result.message


@pytest.mark.asyncio
async def test_check_technicals_range_no_rows_passes() -> None:
    row = _make_row(total=0, outliers=0)
    session = _make_session_with_rows(row)
    result = await check_technicals_range(session, BUSINESS_DATE)
    assert result.status == "passed"
    assert "No rows" in result.message


@pytest.mark.asyncio
async def test_check_technicals_range_exactly_5pct_passes() -> None:
    row = _make_row(total=100, outliers=5)  # exactly 5%, not > 5%
    session = _make_session_with_rows(row)
    result = await check_technicals_range(session, BUSINESS_DATE)
    assert result.status == "passed"


@pytest.mark.asyncio
async def test_check_technicals_range_exception() -> None:
    session = AsyncMock()
    session.execute = AsyncMock(side_effect=RuntimeError("timeout"))
    result = await check_technicals_range(session, BUSINESS_DATE)
    assert result.status == "failed"


# ---------------------------------------------------------------------------
# check_rs_scores_populated tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_rs_scores_populated_zero_rows_failed() -> None:
    session = _make_session_with_rows(0)
    result = await check_rs_scores_populated(session, BUSINESS_DATE)
    assert result.status == "failed"
    assert "No RS score rows" in result.message


@pytest.mark.asyncio
async def test_check_rs_scores_populated_passed() -> None:
    session = _make_session_with_rows(3000, 3, 0)  # total, benchmarks, out_of_range
    result = await check_rs_scores_populated(session, BUSINESS_DATE)
    assert result.status == "passed"
    assert result.details["benchmark_count"] == 3
    assert result.details["rs_composite_out_of_range"] == 0


@pytest.mark.asyncio
async def test_check_rs_scores_populated_low_benchmarks_warning() -> None:
    session = _make_session_with_rows(1000, 1, 0)  # only 1 benchmark
    result = await check_rs_scores_populated(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "1/3" in result.message


@pytest.mark.asyncio
async def test_check_rs_scores_populated_out_of_range_warning() -> None:
    session = _make_session_with_rows(1000, 3, 50)  # 50 out of range
    result = await check_rs_scores_populated(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "50" in result.message


# ---------------------------------------------------------------------------
# check_rs_distribution tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_rs_distribution_passed() -> None:
    row = _make_row(mean_rs=0.1, stddev_rs=1.5, row_count=1000)
    session = _make_session_with_rows(row)
    result = await check_rs_distribution(session, BUSINESS_DATE)
    assert result.status == "passed"
    assert abs(result.details["mean_rs_composite"] - 0.1) < 0.001


@pytest.mark.asyncio
async def test_check_rs_distribution_no_rows_warning() -> None:
    row = _make_row(mean_rs=None, stddev_rs=None, row_count=0)
    session = _make_session_with_rows(row)
    result = await check_rs_distribution(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "No RS composite data" in result.message


@pytest.mark.asyncio
async def test_check_rs_distribution_mean_too_high_warning() -> None:
    row = _make_row(mean_rs=3.5, stddev_rs=1.5, row_count=500)  # abs > 2.0
    session = _make_session_with_rows(row)
    result = await check_rs_distribution(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "Mean" in result.message


@pytest.mark.asyncio
async def test_check_rs_distribution_stddev_too_high_warning() -> None:
    row = _make_row(mean_rs=0.0, stddev_rs=6.0, row_count=500)  # > 5.0
    session = _make_session_with_rows(row)
    result = await check_rs_distribution(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "Stddev" in result.message


@pytest.mark.asyncio
async def test_check_rs_distribution_stddev_too_low_warning() -> None:
    row = _make_row(mean_rs=0.0, stddev_rs=0.05, row_count=500)  # < 0.1
    session = _make_session_with_rows(row)
    result = await check_rs_distribution(session, BUSINESS_DATE)
    assert result.status == "warning"


# ---------------------------------------------------------------------------
# check_breadth_consistency tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_breadth_consistency_passed() -> None:
    row = _make_row(
        advance=400, decline=300, unchanged=300,
        total_stocks=1000, ad_ratio=1.33,
        pct_above_200dma=55.0, pct_above_50dma=60.0,
    )
    session = _make_session_with_rows(row)
    result = await check_breadth_consistency(session, BUSINESS_DATE)
    assert result.status == "passed"


@pytest.mark.asyncio
async def test_check_breadth_consistency_no_row_warning() -> None:
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    session = AsyncMock()
    session.execute = AsyncMock(return_value=mock_result)
    result = await check_breadth_consistency(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "No breadth data" in result.message


@pytest.mark.asyncio
async def test_check_breadth_consistency_total_mismatch_warning() -> None:
    # 400+300+200 = 900, but total_stocks=1000 → 10% discrepancy > 5%
    row = _make_row(
        advance=400, decline=300, unchanged=200,
        total_stocks=1000, ad_ratio=1.33,
        pct_above_200dma=55.0, pct_above_50dma=60.0,
    )
    session = _make_session_with_rows(row)
    result = await check_breadth_consistency(session, BUSINESS_DATE)
    assert result.status == "warning"


@pytest.mark.asyncio
async def test_check_breadth_consistency_pct_out_of_range_warning() -> None:
    row = _make_row(
        advance=400, decline=300, unchanged=300,
        total_stocks=1000, ad_ratio=1.33,
        pct_above_200dma=105.0,  # > 100
        pct_above_50dma=60.0,
    )
    session = _make_session_with_rows(row)
    result = await check_breadth_consistency(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "pct_above_200dma" in result.message


@pytest.mark.asyncio
async def test_check_breadth_consistency_ad_ratio_zero_warning() -> None:
    row = _make_row(
        advance=400, decline=300, unchanged=300,
        total_stocks=1000, ad_ratio=0.0,  # <= 0
        pct_above_200dma=55.0, pct_above_50dma=60.0,
    )
    session = _make_session_with_rows(row)
    result = await check_breadth_consistency(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "ad_ratio" in result.message


# ---------------------------------------------------------------------------
# check_regime_validity tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_regime_validity_no_row_warning() -> None:
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    session = AsyncMock()
    session.execute = AsyncMock(return_value=mock_result)
    result = await check_regime_validity(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "computation may have skipped" in result.message


@pytest.mark.asyncio
async def test_check_regime_validity_passed() -> None:
    # Confidence = 70*0.30 + 65*0.25 + 60*0.15 + 55*0.15 + 50*0.15
    # = 21 + 16.25 + 9 + 8.25 + 7.5 = 62.0
    row = _make_row(
        regime="BULL",
        confidence=62.0,
        breadth_score=70.0,
        momentum_score=65.0,
        volume_score=60.0,
        global_score=55.0,
        fii_score=50.0,
    )
    session = _make_session_with_rows(row)
    result = await check_regime_validity(session, BUSINESS_DATE)
    assert result.status == "passed"
    assert result.details["regime"] == "BULL"


@pytest.mark.asyncio
async def test_check_regime_validity_invalid_regime_warning() -> None:
    row = _make_row(
        regime="CRASH",  # invalid
        confidence=50.0,
        breadth_score=50.0,
        momentum_score=50.0,
        volume_score=50.0,
        global_score=50.0,
        fii_score=50.0,
    )
    session = _make_session_with_rows(row)
    result = await check_regime_validity(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "Invalid regime" in result.message


@pytest.mark.asyncio
async def test_check_regime_validity_confidence_out_of_range_warning() -> None:
    row = _make_row(
        regime="BULL",
        confidence=110.0,  # > 100
        breadth_score=70.0,
        momentum_score=70.0,
        volume_score=70.0,
        global_score=70.0,
        fii_score=70.0,
    )
    session = _make_session_with_rows(row)
    result = await check_regime_validity(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "outside [0, 100]" in result.message


@pytest.mark.asyncio
async def test_check_regime_validity_confidence_formula_mismatch_warning() -> None:
    # Expected ~50, but stored 60 — difference > 1.0
    row = _make_row(
        regime="SIDEWAYS",
        confidence=60.0,
        breadth_score=50.0,
        momentum_score=50.0,
        volume_score=50.0,
        global_score=50.0,
        fii_score=50.0,
    )
    session = _make_session_with_rows(row)
    result = await check_regime_validity(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "weighted sum" in result.message


# ---------------------------------------------------------------------------
# check_fund_derived_coverage tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_fund_derived_coverage_no_rows_warning() -> None:
    session = _make_session_with_rows(0)
    result = await check_fund_derived_coverage(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "No fund derived rows" in result.message


@pytest.mark.asyncio
async def test_check_fund_derived_coverage_passed() -> None:
    vrow = _make_row(
        sharpe_violations=0,
        beta_violations=0,
        drawdown_violations=0,
        volatility_violations=0,
        coverage_violations=0,
    )
    session = _make_session_with_rows(500, vrow)
    result = await check_fund_derived_coverage(session, BUSINESS_DATE)
    assert result.status == "passed"
    assert result.details["total_violations"] == 0


@pytest.mark.asyncio
async def test_check_fund_derived_coverage_violations_warning() -> None:
    vrow = _make_row(
        sharpe_violations=5,
        beta_violations=2,
        drawdown_violations=1,
        volatility_violations=0,
        coverage_violations=0,
    )
    session = _make_session_with_rows(500, vrow)
    result = await check_fund_derived_coverage(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert result.details["total_violations"] == 8


# ---------------------------------------------------------------------------
# check_cross_table_consistency tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_cross_table_consistency_passed() -> None:
    session = _make_session_with_rows(0, 500)  # orphan=0, total=500
    result = await check_cross_table_consistency(session, BUSINESS_DATE)
    assert result.status == "passed"
    assert result.details["orphan_entity_count"] == 0
    assert result.details["total_equity_entities"] == 500


@pytest.mark.asyncio
async def test_check_cross_table_consistency_orphans_warning() -> None:
    session = _make_session_with_rows(15, 500)  # 15 orphans
    result = await check_cross_table_consistency(session, BUSINESS_DATE)
    assert result.status == "warning"
    assert "15" in result.message
    assert result.details["orphan_entity_count"] == 15


@pytest.mark.asyncio
async def test_check_cross_table_consistency_exception() -> None:
    session = AsyncMock()
    session.execute = AsyncMock(side_effect=RuntimeError("connection lost"))
    result = await check_cross_table_consistency(session, BUSINESS_DATE)
    assert result.status == "failed"


# ---------------------------------------------------------------------------
# run_post_computation_qa integration test (all mocked)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_post_computation_qa_returns_report_with_8_steps() -> None:
    """Verify run_post_computation_qa calls all 8 checks and returns QAReport."""
    passed_result = StepResult(name="x", status="passed", message="ok")

    with (
        patch("app.computation.post_qa.check_technicals_populated", return_value=passed_result) as m1,
        patch("app.computation.post_qa.check_technicals_range", return_value=passed_result) as m2,
        patch("app.computation.post_qa.check_rs_scores_populated", return_value=passed_result) as m3,
        patch("app.computation.post_qa.check_rs_distribution", return_value=passed_result) as m4,
        patch("app.computation.post_qa.check_breadth_consistency", return_value=passed_result) as m5,
        patch("app.computation.post_qa.check_regime_validity", return_value=passed_result) as m6,
        patch("app.computation.post_qa.check_fund_derived_coverage", return_value=passed_result) as m7,
        patch("app.computation.post_qa.check_cross_table_consistency", return_value=passed_result) as m8,
    ):
        session = AsyncMock()
        report = await run_post_computation_qa(session, BUSINESS_DATE)

    assert isinstance(report, QAReport)
    assert report.phase == "post_qa"
    assert report.business_date == BUSINESS_DATE
    assert len(report.steps) == 8
    assert report.overall_status == "passed"
    assert report.passed == 8

    for mock_fn in [m1, m2, m3, m4, m5, m6, m7, m8]:
        mock_fn.assert_called_once_with(session, BUSINESS_DATE)


@pytest.mark.asyncio
async def test_run_post_computation_qa_phase_is_post_qa() -> None:
    passed_result = StepResult(name="x", status="passed", message="ok")
    with (
        patch("app.computation.post_qa.check_technicals_populated", return_value=passed_result),
        patch("app.computation.post_qa.check_technicals_range", return_value=passed_result),
        patch("app.computation.post_qa.check_rs_scores_populated", return_value=passed_result),
        patch("app.computation.post_qa.check_rs_distribution", return_value=passed_result),
        patch("app.computation.post_qa.check_breadth_consistency", return_value=passed_result),
        patch("app.computation.post_qa.check_regime_validity", return_value=passed_result),
        patch("app.computation.post_qa.check_fund_derived_coverage", return_value=passed_result),
        patch("app.computation.post_qa.check_cross_table_consistency", return_value=passed_result),
    ):
        session = AsyncMock()
        report = await run_post_computation_qa(session, BUSINESS_DATE)

    assert report.phase == "post_qa"


@pytest.mark.asyncio
async def test_run_post_computation_qa_no_gating_on_failure() -> None:
    """Even if checks fail, all 8 must run (no short-circuit gating)."""
    failed_result = StepResult(name="x", status="failed", message="broken")
    with (
        patch("app.computation.post_qa.check_technicals_populated", return_value=failed_result),
        patch("app.computation.post_qa.check_technicals_range", return_value=failed_result),
        patch("app.computation.post_qa.check_rs_scores_populated", return_value=failed_result),
        patch("app.computation.post_qa.check_rs_distribution", return_value=failed_result),
        patch("app.computation.post_qa.check_breadth_consistency", return_value=failed_result),
        patch("app.computation.post_qa.check_regime_validity", return_value=failed_result),
        patch("app.computation.post_qa.check_fund_derived_coverage", return_value=failed_result),
        patch("app.computation.post_qa.check_cross_table_consistency", return_value=failed_result),
    ):
        session = AsyncMock()
        report = await run_post_computation_qa(session, BUSINESS_DATE)

    assert len(report.steps) == 8
    assert report.failed == 8
    assert report.overall_status == "failed"
