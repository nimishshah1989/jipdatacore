"""Unit tests for app.computation.spot_check and app.computation.qa_types.

Tests cover:
  - QAReport and StepResult dataclasses
  - Helper functions (_deviation_pct, _classify_deviation, _safe_float, _rederive_regime)
  - All 5 spot-check functions (mocked DB via AsyncMock)
  - run_spot_checks integration
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.computation.qa_types import QAReport, StepResult
from app.computation.spot_check import (
    SPOT_CHECK_SYMBOLS,
    _classify_deviation,
    _deviation_pct,
    _rederive_regime,
    _safe_float,
    run_spot_checks,
    spot_check_breadth_arithmetic,
    spot_check_regime_self_consistency,
    spot_check_rs_self_consistency,
    spot_check_technicals,
    spot_check_vs_marketpulse,
)

BUSINESS_DATE = date(2025, 12, 31)


# ===========================================================================
# QAReport / StepResult tests
# ===========================================================================


def test_step_result_defaults() -> None:
    step = StepResult(name="my_check", status="passed")
    assert step.name == "my_check"
    assert step.status == "passed"
    assert step.message == ""
    assert step.details == {}


def test_step_result_to_dict() -> None:
    step = StepResult(name="foo", status="failed", message="err", details={"k": 1})
    d = step.to_dict()
    assert d["name"] == "foo"
    assert d["status"] == "failed"
    assert d["message"] == "err"
    assert d["details"] == {"k": 1}


def test_qa_report_counts() -> None:
    report = QAReport(phase="test", business_date=BUSINESS_DATE)
    report.add_step(StepResult(name="a", status="passed"))
    report.add_step(StepResult(name="b", status="failed"))
    report.add_step(StepResult(name="c", status="warning"))
    report.add_step(StepResult(name="d", status="skipped"))

    assert report.passed == 1
    assert report.failed == 1
    assert report.warnings == 1
    assert report.skipped == 1


def test_qa_report_overall_status_failed_dominates() -> None:
    report = QAReport(phase="test", business_date=BUSINESS_DATE)
    report.add_step(StepResult(name="a", status="passed"))
    report.add_step(StepResult(name="b", status="failed"))
    assert report.overall_status == "failed"


def test_qa_report_overall_status_warning_when_no_failures() -> None:
    report = QAReport(phase="test", business_date=BUSINESS_DATE)
    report.add_step(StepResult(name="a", status="passed"))
    report.add_step(StepResult(name="b", status="warning"))
    assert report.overall_status == "warning"


def test_qa_report_overall_status_passed() -> None:
    report = QAReport(phase="test", business_date=BUSINESS_DATE)
    report.add_step(StepResult(name="a", status="passed"))
    report.add_step(StepResult(name="b", status="passed"))
    assert report.overall_status == "passed"


def test_qa_report_overall_status_all_skipped() -> None:
    report = QAReport(phase="test", business_date=BUSINESS_DATE)
    report.add_step(StepResult(name="a", status="skipped"))
    report.add_step(StepResult(name="b", status="skipped"))
    assert report.overall_status == "skipped"


def test_qa_report_to_dict_structure() -> None:
    report = QAReport(phase="spot_check", business_date=BUSINESS_DATE)
    report.add_step(StepResult(name="x", status="passed"))
    d = report.to_dict()
    assert d["phase"] == "spot_check"
    assert d["business_date"] == "2025-12-31"
    assert "generated_at" in d
    assert "overall_status" in d
    assert "summary" in d
    assert "steps" in d
    assert d["summary"]["total"] == 1


def test_qa_report_from_steps() -> None:
    steps = [StepResult(name="s", status="passed")]
    report = QAReport.from_steps("p", BUSINESS_DATE, steps)
    assert report.phase == "p"
    assert report.passed == 1


# ===========================================================================
# Helper function tests
# ===========================================================================


def test_deviation_pct_zero_reference_returns_zero() -> None:
    assert _deviation_pct(100.0, 0.0) == 0.0


def test_deviation_pct_no_deviation() -> None:
    assert _deviation_pct(100.0, 100.0) == pytest.approx(0.0, abs=1e-9)


def test_deviation_pct_positive() -> None:
    # 110 vs 100 → 10%
    assert _deviation_pct(110.0, 100.0) == pytest.approx(10.0, rel=1e-6)


def test_deviation_pct_negative_direction() -> None:
    # absolute: 90 vs 100 → 10%
    assert _deviation_pct(90.0, 100.0) == pytest.approx(10.0, rel=1e-6)


def test_classify_deviation_match() -> None:
    assert _classify_deviation(0.0) == "match"
    assert _classify_deviation(1.9) == "match"


def test_classify_deviation_close() -> None:
    assert _classify_deviation(2.0) == "close"
    assert _classify_deviation(5.0) == "close"


def test_classify_deviation_mismatch() -> None:
    assert _classify_deviation(5.1) == "mismatch"
    assert _classify_deviation(100.0) == "mismatch"


def test_safe_float_none_returns_none() -> None:
    assert _safe_float(None) is None


def test_safe_float_decimal() -> None:
    val = _safe_float(Decimal("123.4567"))
    assert val == pytest.approx(123.4567, rel=1e-6)


def test_safe_float_nan_returns_none() -> None:
    assert _safe_float(float("nan")) is None


def test_safe_float_inf_returns_none() -> None:
    assert _safe_float(float("inf")) is None


def test_safe_float_string() -> None:
    assert _safe_float("99.9") == pytest.approx(99.9, rel=1e-6)


def test_safe_float_bad_string_returns_none() -> None:
    assert _safe_float("abc") is None


# ===========================================================================
# _rederive_regime tests
# ===========================================================================


def test_rederive_regime_bull() -> None:
    assert _rederive_regime(confidence=65.0, breadth_score=65.0, momentum_score=50.0) == "BULL"


def test_rederive_regime_bear_low_confidence() -> None:
    assert _rederive_regime(confidence=38.0, breadth_score=50.0, momentum_score=50.0) == "BEAR"


def test_rederive_regime_bear_low_breadth() -> None:
    assert _rederive_regime(confidence=55.0, breadth_score=30.0, momentum_score=50.0) == "BEAR"


def test_rederive_regime_recovery() -> None:
    # confidence mid-range AND momentum > breadth
    assert _rederive_regime(confidence=50.0, breadth_score=45.0, momentum_score=60.0) == "RECOVERY"


def test_rederive_regime_sideways() -> None:
    # confidence mid-range, momentum <= breadth
    assert _rederive_regime(confidence=50.0, breadth_score=55.0, momentum_score=50.0) == "SIDEWAYS"


# ===========================================================================
# spot_check_technicals — mocked DB and yfinance
# ===========================================================================


def _make_mock_session() -> AsyncMock:
    """Return a minimal AsyncSession mock."""
    session = AsyncMock()
    return session


def _make_db_row(**kwargs: Any) -> MagicMock:
    """Return a MagicMock that acts as a DB row with given attributes."""
    row = MagicMock()
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


@pytest.mark.asyncio
async def test_spot_check_technicals_no_db_row_skipped() -> None:
    """When no DB row exists for a symbol, that symbol is skipped."""
    session = _make_mock_session()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    session.execute.return_value = mock_result

    # yfinance is imported inline inside the function — patch via sys.modules
    import pandas as pd
    import numpy as np

    # Build a fake DataFrame that yfinance would return
    idx = pd.date_range(end="2025-12-31", periods=250, freq="B")
    prices = np.linspace(2000, 2500, 250)
    fake_df = pd.DataFrame({"Close": prices}, index=idx)

    mock_yf = MagicMock()
    mock_yf.download.return_value = fake_df

    with patch.dict("sys.modules", {"yfinance": mock_yf}):
        step = await spot_check_technicals(session, BUSINESS_DATE)

    assert step.name == "spot_check_technicals"
    checks = step.details.get("checks", [])
    skipped = [c for c in checks if c["status"] == "skipped"]
    assert len(skipped) == len(SPOT_CHECK_SYMBOLS)


@pytest.mark.asyncio
async def test_spot_check_technicals_yfinance_import_error() -> None:
    """If yfinance is not importable, return warning status.

    Simulated by temporarily removing yfinance from sys.modules and ensuring
    the import inside spot_check_technicals raises ImportError.
    """
    import sys

    session = _make_mock_session()

    # Remove yfinance from sys.modules so the inline import will fail
    original = sys.modules.pop("yfinance", None)
    # Also make the import fail by inserting None which triggers ImportError on attribute access
    # Actually set it to a sentinel that causes import to raise
    sys.modules["yfinance"] = None  # type: ignore[assignment]

    try:
        # The inline `import yfinance as yf` will raise ImportError because the
        # module is set to None in sys.modules (Python raises ImportError for None entries)
        step = await spot_check_technicals(session, BUSINESS_DATE)
    finally:
        # Restore sys.modules
        if original is not None:
            sys.modules["yfinance"] = original
        else:
            sys.modules.pop("yfinance", None)

    assert step.status == "warning"
    assert "yfinance" in step.message.lower()


# ---------------------------------------------------------------------------
# spot_check_rs_self_consistency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spot_check_rs_self_consistency_no_row() -> None:
    """Missing RS row → skipped for that symbol."""
    session = _make_mock_session()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    session.execute.return_value = mock_result

    step = await spot_check_rs_self_consistency(session, BUSINESS_DATE)
    assert step.name == "spot_check_rs_self_consistency"
    skipped = [c for c in step.details["checks"] if c["status"] == "skipped"]
    assert len(skipped) == len(SPOT_CHECK_SYMBOLS)


@pytest.mark.asyncio
async def test_spot_check_rs_self_consistency_match() -> None:
    """Correctly recomputed composite matches stored → passed."""
    session = _make_mock_session()

    # Build a row where composite matches exact weighted sum
    rs_1w, rs_1m, rs_3m, rs_6m, rs_12m = 1.0, 2.0, 3.0, 4.0, 5.0
    composite = (
        rs_1w * 0.10 + rs_1m * 0.20 + rs_3m * 0.30 + rs_6m * 0.25 + rs_12m * 0.15
    )

    db_row = _make_db_row(
        rs_composite=Decimal(str(round(composite, 6))),
        rs_1w=Decimal(str(rs_1w)),
        rs_1m=Decimal(str(rs_1m)),
        rs_3m=Decimal(str(rs_3m)),
        rs_6m=Decimal(str(rs_6m)),
        rs_12m=Decimal(str(rs_12m)),
    )
    mock_result = MagicMock()
    mock_result.fetchone.return_value = db_row
    session.execute.return_value = mock_result

    step = await spot_check_rs_self_consistency(session, BUSINESS_DATE)
    assert step.status == "passed"
    matches = [c for c in step.details["checks"] if c["status"] == "match"]
    assert len(matches) == len(SPOT_CHECK_SYMBOLS)


@pytest.mark.asyncio
async def test_spot_check_rs_self_consistency_mismatch() -> None:
    """Stored composite deviates from recomputed → failed status."""
    session = _make_mock_session()

    rs_1w, rs_1m, rs_3m, rs_6m, rs_12m = 1.0, 2.0, 3.0, 4.0, 5.0
    composite = (
        rs_1w * 0.10 + rs_1m * 0.20 + rs_3m * 0.30 + rs_6m * 0.25 + rs_12m * 0.15
    )
    # Inject wrong composite
    wrong_composite = composite + 5.0

    db_row = _make_db_row(
        rs_composite=Decimal(str(round(wrong_composite, 6))),
        rs_1w=Decimal(str(rs_1w)),
        rs_1m=Decimal(str(rs_1m)),
        rs_3m=Decimal(str(rs_3m)),
        rs_6m=Decimal(str(rs_6m)),
        rs_12m=Decimal(str(rs_12m)),
    )
    mock_result = MagicMock()
    mock_result.fetchone.return_value = db_row
    session.execute.return_value = mock_result

    step = await spot_check_rs_self_consistency(session, BUSINESS_DATE)
    assert step.status == "failed"
    mismatches = [c for c in step.details["checks"] if c["status"] == "mismatch"]
    assert len(mismatches) > 0


# ---------------------------------------------------------------------------
# spot_check_breadth_arithmetic
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spot_check_breadth_no_row_skipped() -> None:
    session = _make_mock_session()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    session.execute.return_value = mock_result

    step = await spot_check_breadth_arithmetic(session, BUSINESS_DATE)
    assert step.status == "skipped"


@pytest.mark.asyncio
async def test_spot_check_breadth_arithmetic_correct() -> None:
    """advance + decline + unchanged == total_stocks → passed."""
    session = _make_mock_session()
    row = _make_db_row(
        advance=500,
        decline=300,
        unchanged=200,
        total_stocks=1000,
        pct_above_200dma=Decimal("55.00"),
        pct_above_50dma=Decimal("48.00"),
    )
    mock_result = MagicMock()
    mock_result.fetchone.return_value = row
    session.execute.return_value = mock_result

    step = await spot_check_breadth_arithmetic(session, BUSINESS_DATE)
    assert step.status == "passed"
    assert step.details["total_match"] is True
    assert step.details["pct_200_valid"] is True
    assert step.details["pct_50_valid"] is True


@pytest.mark.asyncio
async def test_spot_check_breadth_arithmetic_mismatch() -> None:
    """advance + decline + unchanged != total_stocks → failed."""
    session = _make_mock_session()
    row = _make_db_row(
        advance=500,
        decline=300,
        unchanged=200,
        total_stocks=999,  # wrong
        pct_above_200dma=Decimal("55.00"),
        pct_above_50dma=Decimal("48.00"),
    )
    mock_result = MagicMock()
    mock_result.fetchone.return_value = row
    session.execute.return_value = mock_result

    step = await spot_check_breadth_arithmetic(session, BUSINESS_DATE)
    assert step.status == "failed"
    assert "total_stocks" in step.message


@pytest.mark.asyncio
async def test_spot_check_breadth_pct_out_of_range() -> None:
    """pct_above_200dma > 100 → failed."""
    session = _make_mock_session()
    row = _make_db_row(
        advance=600,
        decline=300,
        unchanged=100,
        total_stocks=1000,
        pct_above_200dma=Decimal("105.00"),  # invalid
        pct_above_50dma=Decimal("48.00"),
    )
    mock_result = MagicMock()
    mock_result.fetchone.return_value = row
    session.execute.return_value = mock_result

    step = await spot_check_breadth_arithmetic(session, BUSINESS_DATE)
    assert step.status == "failed"
    assert "pct_above_200dma" in step.message


# ---------------------------------------------------------------------------
# spot_check_regime_self_consistency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spot_check_regime_no_row_skipped() -> None:
    session = _make_mock_session()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    session.execute.return_value = mock_result

    step = await spot_check_regime_self_consistency(session, BUSINESS_DATE)
    assert step.status == "skipped"


@pytest.mark.asyncio
async def test_spot_check_regime_self_consistency_passed() -> None:
    """Stored regime matches recomputed → passed."""
    session = _make_mock_session()

    breadth, momentum, volume, global_s, fii = 70.0, 65.0, 60.0, 55.0, 50.0
    confidence = (
        breadth * 0.30
        + momentum * 0.25
        + volume * 0.15
        + global_s * 0.15
        + fii * 0.15
    )
    # Both high → BULL
    row = _make_db_row(
        regime="BULL",
        confidence=Decimal(str(round(confidence, 2))),
        breadth_score=Decimal(str(breadth)),
        momentum_score=Decimal(str(momentum)),
        volume_score=Decimal(str(volume)),
        global_score=Decimal(str(global_s)),
        fii_score=Decimal(str(fii)),
    )
    mock_result = MagicMock()
    mock_result.fetchone.return_value = row
    session.execute.return_value = mock_result

    step = await spot_check_regime_self_consistency(session, BUSINESS_DATE)
    assert step.status == "passed"
    assert step.details["confidence_match"] is True
    assert step.details["regime_match"] is True


@pytest.mark.asyncio
async def test_spot_check_regime_confidence_mismatch() -> None:
    """Stored confidence deviates by > 1.0 → failed."""
    session = _make_mock_session()

    breadth, momentum, volume, global_s, fii = 70.0, 65.0, 60.0, 55.0, 50.0
    real_confidence = (
        breadth * 0.30 + momentum * 0.25 + volume * 0.15 + global_s * 0.15 + fii * 0.15
    )
    wrong_confidence = real_confidence + 10.0  # delta > 1.0

    row = _make_db_row(
        regime="BULL",
        confidence=Decimal(str(round(wrong_confidence, 2))),
        breadth_score=Decimal(str(breadth)),
        momentum_score=Decimal(str(momentum)),
        volume_score=Decimal(str(volume)),
        global_score=Decimal(str(global_s)),
        fii_score=Decimal(str(fii)),
    )
    mock_result = MagicMock()
    mock_result.fetchone.return_value = row
    session.execute.return_value = mock_result

    step = await spot_check_regime_self_consistency(session, BUSINESS_DATE)
    assert step.status == "failed"
    assert step.details["confidence_match"] is False


@pytest.mark.asyncio
async def test_spot_check_regime_label_mismatch() -> None:
    """Stored regime label differs from rederived → failed."""
    session = _make_mock_session()

    # Scores that rederive to SIDEWAYS but stored as BULL
    breadth, momentum, volume, global_s, fii = 50.0, 50.0, 50.0, 50.0, 50.0
    confidence = (
        breadth * 0.30 + momentum * 0.25 + volume * 0.15 + global_s * 0.15 + fii * 0.15
    )

    row = _make_db_row(
        regime="BULL",  # wrong
        confidence=Decimal(str(round(confidence, 2))),
        breadth_score=Decimal(str(breadth)),
        momentum_score=Decimal(str(momentum)),
        volume_score=Decimal(str(volume)),
        global_score=Decimal(str(global_s)),
        fii_score=Decimal(str(fii)),
    )
    mock_result = MagicMock()
    mock_result.fetchone.return_value = row
    session.execute.return_value = mock_result

    step = await spot_check_regime_self_consistency(session, BUSINESS_DATE)
    assert step.status == "failed"
    assert step.details["regime_match"] is False


# ---------------------------------------------------------------------------
# spot_check_vs_marketpulse
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spot_check_vs_marketpulse_not_configured() -> None:
    """When fie_v3_database_url is empty, return skipped."""
    session = _make_mock_session()

    with patch("app.computation.spot_check.get_settings") as mock_settings:
        settings_obj = MagicMock()
        settings_obj.fie_v3_database_url = ""
        mock_settings.return_value = settings_obj

        step = await spot_check_vs_marketpulse(session, BUSINESS_DATE)

    assert step.status == "skipped"
    assert "not configured" in step.message.lower()


@pytest.mark.asyncio
async def test_spot_check_vs_marketpulse_configured_stub() -> None:
    """When DB is configured, return warning (stub not yet implemented)."""
    session = _make_mock_session()

    with patch("app.computation.spot_check.get_settings") as mock_settings:
        settings_obj = MagicMock()
        settings_obj.fie_v3_database_url = "postgresql://user:pass@host/db"
        mock_settings.return_value = settings_obj

        step = await spot_check_vs_marketpulse(session, BUSINESS_DATE)

    assert step.status == "warning"


# ---------------------------------------------------------------------------
# run_spot_checks integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_spot_checks_returns_qa_report() -> None:
    """run_spot_checks returns a QAReport with 5 steps."""
    session = _make_mock_session()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    session.execute.return_value = mock_result

    with patch("app.computation.spot_check.get_settings") as mock_settings:
        settings_obj = MagicMock()
        settings_obj.fie_v3_database_url = ""
        mock_settings.return_value = settings_obj

        # Patch yfinance import to avoid real network calls
        with patch.dict("sys.modules", {"yfinance": None}):
            report = await run_spot_checks(session, BUSINESS_DATE)

    assert isinstance(report, QAReport)
    assert report.phase == "spot_check"
    assert report.business_date == BUSINESS_DATE
    assert len(report.steps) == 5


@pytest.mark.asyncio
async def test_run_spot_checks_catches_step_exception() -> None:
    """If a spot-check function raises, run_spot_checks captures it as failed."""
    session = _make_mock_session()
    session.execute.side_effect = RuntimeError("DB connection error")

    with patch("app.computation.spot_check.get_settings") as mock_settings:
        settings_obj = MagicMock()
        settings_obj.fie_v3_database_url = ""
        mock_settings.return_value = settings_obj

        with patch.dict("sys.modules", {"yfinance": None}):
            report = await run_spot_checks(session, BUSINESS_DATE)

    assert isinstance(report, QAReport)
    # Some steps will have failed due to the DB exception
    # (technicals handles import separately, others hit DB)
    failed_steps = [s for s in report.steps if s.status == "failed"]
    # At least breadth, rs, and regime checks hit the DB
    assert len(failed_steps) >= 1


@pytest.mark.asyncio
async def test_run_spot_checks_overall_status_is_string() -> None:
    """overall_status must be one of the valid strings."""
    session = _make_mock_session()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    session.execute.return_value = mock_result

    with patch("app.computation.spot_check.get_settings") as mock_settings:
        settings_obj = MagicMock()
        settings_obj.fie_v3_database_url = ""
        mock_settings.return_value = settings_obj

        with patch.dict("sys.modules", {"yfinance": None}):
            report = await run_spot_checks(session, BUSINESS_DATE)

    assert report.overall_status in {"passed", "warning", "failed", "skipped"}


@pytest.mark.asyncio
async def test_run_spot_checks_to_dict_serialisable() -> None:
    """to_dict() should return a plain dict with no unserialised types."""
    session = _make_mock_session()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    session.execute.return_value = mock_result

    with patch("app.computation.spot_check.get_settings") as mock_settings:
        settings_obj = MagicMock()
        settings_obj.fie_v3_database_url = ""
        mock_settings.return_value = settings_obj

        with patch.dict("sys.modules", {"yfinance": None}):
            report = await run_spot_checks(session, BUSINESS_DATE)

    import json
    d = report.to_dict()
    # Should not raise
    json.dumps(d)
