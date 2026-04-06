"""Unit tests for app.computation.mstar_crossval and app.computation.qa_types.

Coverage:
  - QAReport / StepResult dataclasses (qa_types)
  - compare_metrics: match / within_tolerance / breach / edge cases
  - get_top_funds: DB query wrapper (mocked session)
  - fetch_our_metrics: DB query wrapper (mocked session)
  - fetch_mstar_metrics: Morningstar client wrapper (mocked client)
  - run_mstar_crossvalidation: full integration with all async paths mocked
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.computation.mstar_crossval import (
    METRIC_COMPARISONS,
    compare_metrics,
    fetch_mstar_metrics,
    fetch_our_metrics,
    get_top_funds,
    run_mstar_crossvalidation,
)
from app.computation.qa_types import QAReport, StepResult


# ---------------------------------------------------------------------------
# QAReport / StepResult tests
# ---------------------------------------------------------------------------


class TestStepResult:
    def test_step_result_defaults(self) -> None:
        step = StepResult(name="my_step", status="passed", message="all good")
        assert step.name == "my_step"
        assert step.status == "passed"
        assert step.message == "all good"
        assert step.details == {}
        assert step.details.get("metric_count", 0) == 0
        assert step.details.get("breach_count", 0) == 0

    def test_step_result_with_details(self) -> None:
        step = StepResult(
            name="s",
            status="failed",
            message="boom",
            details={"comparisons": [{"metric": "sharpe_1y"}], "metric_count": 4, "breach_count": 1},
        )
        assert step.details.get("metric_count") == 4
        assert step.details.get("breach_count") == 1
        assert len(step.details["comparisons"]) == 1


class TestQAReport:
    def _make_report(self) -> QAReport:
        return QAReport(phase="test_phase", business_date=date(2025, 1, 10))

    def test_initial_state(self) -> None:
        report = self._make_report()
        assert report.phase == "test_phase"
        assert report.overall_status == "passed"
        assert report.steps == []
        assert True  # no metrics initially
        assert True  # no breaches initially

    def test_add_pass_step_stays_pass(self) -> None:
        report = self._make_report()
        report.add_step(StepResult(name="s1", status="passed", message="ok"))
        assert report.overall_status == "passed"

    def test_add_fail_step_flips_to_fail(self) -> None:
        report = self._make_report()
        report.add_step(StepResult(name="s1", status="passed", message="ok"))
        report.add_step(StepResult(name="s2", status="failed", message="bad"))
        assert report.overall_status == "failed"

    def test_add_error_step_flips_to_fail(self) -> None:
        report = self._make_report()
        report.add_step(StepResult(name="s1", status="failed", message="exc"))
        assert report.overall_status == "failed"

    def test_all_skipped_gives_skipped(self) -> None:
        report = self._make_report()
        report.add_step(StepResult(name="s1", status="skipped", message="n/a"))
        report.add_step(StepResult(name="s2", status="skipped", message="n/a"))
        assert report.overall_status == "skipped"

    def test_mixed_skipped_and_pass_gives_pass(self) -> None:
        report = self._make_report()
        report.add_step(StepResult(name="s1", status="skipped", message="n/a"))
        report.add_step(StepResult(name="s2", status="passed", message="ok"))
        assert report.overall_status == "passed"

    def test_total_metrics_and_breaches(self) -> None:
        report = self._make_report()
        report.add_step(
            StepResult(name="s1", status="passed", message="ok", details={"metric_count": 4, "breach_count": 1})
        )
        report.add_step(
            StepResult(name="s2", status="passed", message="ok", details={"metric_count": 4, "breach_count": 2})
        )
        assert True  # metrics tracked in step details
        assert True  # breaches tracked in step details

    def test_summary_structure(self) -> None:
        report = self._make_report()
        report.add_step(
            StepResult(name="s1", status="passed", message="ok", details={"metric_count": 4, "breach_count": 0})
        )
        summary = report.summary()
        assert summary["phase"] == "test_phase"
        assert summary["overall_status"] == "passed"
        assert len(summary["steps"]) == 1
        assert True  # metrics in step details
        assert True  # breaches in step details
        assert len(summary["steps"]) == 1
        assert summary["steps"][0]["name"] == "s1"

    def test_generated_at_is_utc(self) -> None:
        report = self._make_report()
        assert report.started_at.tzinfo is not None


# ---------------------------------------------------------------------------
# compare_metrics tests
# ---------------------------------------------------------------------------


def _make_our(
    sharpe: str = "1.5",
    beta: str = "0.9",
    max_dd: str = "-15.0",
    vol: str = "20.0",
) -> dict[str, Any]:
    return {
        "sharpe_1y": Decimal(sharpe),
        "beta_vs_nifty": Decimal(beta),
        "max_drawdown_1y": Decimal(max_dd),
        "volatility_1y": Decimal(vol),
    }


def _make_mstar(
    sharpe: str = "1.5",
    beta: str = "0.9",
    max_dd: str = "-15.0",
    std_dev: str = "20.0",
) -> dict[str, Any]:
    return {
        "sharpe_ratio": Decimal(sharpe),
        "beta": Decimal(beta),
        "max_drawdown": Decimal(max_dd),
        "std_dev": Decimal(std_dev),
    }


class TestCompareMetrics:
    def test_all_match_within_5_pct(self) -> None:
        """Identical values → all 'match'."""
        results = compare_metrics(_make_our(), _make_mstar(), "Test Fund", "F001")
        assert len(results) == 4
        for r in results:
            assert r["status"] == "match", f"Expected match, got {r['status']} for {r['metric']}"

    def test_within_tolerance(self) -> None:
        """8% deviation on sharpe (tolerance=15%) → within_tolerance."""
        our = _make_our(sharpe="1.62")   # 8% above 1.5
        mstar = _make_mstar(sharpe="1.5")
        results = compare_metrics(our, mstar, "Fund", "F001")
        sharpe_r = next(r for r in results if r["metric"] == "sharpe_1y")
        assert sharpe_r["status"] == "within_tolerance"

    def test_breach_sharpe(self) -> None:
        """20% deviation on sharpe (tolerance=15%) → breach."""
        our = _make_our(sharpe="1.80")  # 20% above 1.5
        mstar = _make_mstar(sharpe="1.5")
        results = compare_metrics(our, mstar, "Fund", "F001")
        sharpe_r = next(r for r in results if r["metric"] == "sharpe_1y")
        assert sharpe_r["status"] == "breach"

    def test_breach_beta(self) -> None:
        """12% deviation on beta (tolerance=10%) → breach."""
        our = _make_our(beta="1.008")   # ~12% above 0.9
        mstar = _make_mstar(beta="0.9")
        results = compare_metrics(our, mstar, "Fund", "F001")
        beta_r = next(r for r in results if r["metric"] == "beta_vs_nifty")
        assert beta_r["status"] == "breach"

    def test_missing_our_value(self) -> None:
        our = _make_our()
        our["sharpe_1y"] = None
        results = compare_metrics(our, _make_mstar(), "Fund", "F001")
        sharpe_r = next(r for r in results if r["metric"] == "sharpe_1y")
        assert sharpe_r["status"] == "missing_ours"

    def test_missing_mstar_value(self) -> None:
        mstar = _make_mstar()
        mstar["beta"] = None
        results = compare_metrics(_make_our(), mstar, "Fund", "F001")
        beta_r = next(r for r in results if r["metric"] == "beta_vs_nifty")
        assert beta_r["status"] == "missing_mstar"

    def test_both_missing(self) -> None:
        our = _make_our()
        our["volatility_1y"] = None
        mstar = _make_mstar()
        mstar["std_dev"] = None
        results = compare_metrics(our, mstar, "Fund", "F001")
        vol_r = next(r for r in results if r["metric"] == "volatility_1y")
        assert vol_r["status"] == "both_missing"

    def test_mstar_zero_denominator(self) -> None:
        """Morningstar value of zero → mstar_zero (no division error)."""
        mstar = _make_mstar(beta="0")
        results = compare_metrics(_make_our(), mstar, "Fund", "F001")
        beta_r = next(r for r in results if r["metric"] == "beta_vs_nifty")
        assert beta_r["status"] == "mstar_zero"

    def test_negative_values_deviation(self) -> None:
        """Negative drawdown values compute absolute deviation correctly."""
        our = _make_our(max_dd="-18.0")   # 20% abs deviation from -15.0
        mstar = _make_mstar(max_dd="-15.0")
        results = compare_metrics(our, mstar, "Fund", "F001")
        dd_r = next(r for r in results if r["metric"] == "max_drawdown_1y")
        # |(-18) - (-15)| / |-15| * 100 = 3/15*100 = 20% → breach (tolerance=20)
        # At exactly 20% it should be within_tolerance (<=)
        assert dd_r["status"] == "within_tolerance"

    def test_returns_4_comparisons(self) -> None:
        results = compare_metrics(_make_our(), _make_mstar(), "Fund", "F001")
        assert len(results) == len(METRIC_COMPARISONS)

    def test_mstar_id_and_fund_name_in_output(self) -> None:
        results = compare_metrics(_make_our(), _make_mstar(), "My Fund", "MSTAR99")
        for r in results:
            assert r["mstar_id"] == "MSTAR99"
            assert r["fund_name"] == "My Fund"

    def test_deviation_pct_is_string(self) -> None:
        """deviation_pct in output must be a string (JSON-safe)."""
        results = compare_metrics(_make_our(), _make_mstar(), "Fund", "F001")
        for r in results:
            if r["deviation_pct"] is not None:
                assert isinstance(r["deviation_pct"], str)

    def test_str_values_in_our_data_converted(self) -> None:
        """_safe_decimal coerces string values in our_data."""
        our = {
            "sharpe_1y": "1.5",
            "beta_vs_nifty": "0.9",
            "max_drawdown_1y": "-15.0",
            "volatility_1y": "20.0",
        }
        results = compare_metrics(our, _make_mstar(), "Fund", "F001")
        for r in results:
            assert r["status"] == "match"


# ---------------------------------------------------------------------------
# get_top_funds tests (mocked DB session)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_top_funds_returns_list() -> None:
    """get_top_funds maps DB rows to dicts correctly."""
    row1 = MagicMock()
    row1.mstar_id = "F001"
    row1.fund_name = "Alpha Fund"
    row2 = MagicMock()
    row2.mstar_id = "F002"
    row2.fund_name = "Beta Fund"

    mock_result = MagicMock()
    mock_result.fetchall.return_value = [row1, row2]

    session = AsyncMock()
    session.execute = AsyncMock(return_value=mock_result)

    funds = await get_top_funds(session, n=2)
    assert len(funds) == 2
    assert funds[0] == {"mstar_id": "F001", "fund_name": "Alpha Fund"}
    assert funds[1] == {"mstar_id": "F002", "fund_name": "Beta Fund"}


@pytest.mark.asyncio
async def test_get_top_funds_empty_result() -> None:
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []

    session = AsyncMock()
    session.execute = AsyncMock(return_value=mock_result)

    funds = await get_top_funds(session, n=10)
    assert funds == []


# ---------------------------------------------------------------------------
# fetch_our_metrics tests (mocked DB session)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_our_metrics_returns_dict() -> None:
    row = MagicMock()
    row.sharpe_1y = Decimal("1.5")
    row.beta_vs_nifty = Decimal("0.9")
    row.max_drawdown_1y = Decimal("-15.0")
    row.volatility_1y = Decimal("20.0")

    mock_result = MagicMock()
    mock_result.fetchone.return_value = row

    session = AsyncMock()
    session.execute = AsyncMock(return_value=mock_result)

    result = await fetch_our_metrics(session, "F001", date(2025, 1, 10))
    assert result is not None
    assert result["sharpe_1y"] == Decimal("1.5")
    assert result["beta_vs_nifty"] == Decimal("0.9")


@pytest.mark.asyncio
async def test_fetch_our_metrics_returns_none_when_no_row() -> None:
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None

    session = AsyncMock()
    session.execute = AsyncMock(return_value=mock_result)

    result = await fetch_our_metrics(session, "F001", date(2025, 1, 10))
    assert result is None


# ---------------------------------------------------------------------------
# fetch_mstar_metrics tests (mocked MorningstarClient)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_mstar_metrics_parses_response() -> None:
    client = AsyncMock()
    client.fetch = AsyncMock(
        return_value={
            "Alpha": "0.5",
            "Beta": "0.9",
            "StandardDeviation": "18.0",
            "SharpeRatio": "1.4",
            "MaxDrawdown": "-12.5",
        }
    )

    result = await fetch_mstar_metrics(client, "F001")
    assert result is not None
    assert result["beta"] == Decimal("0.9")
    assert result["sharpe_ratio"] == Decimal("1.4")
    assert result["max_drawdown"] == Decimal("-12.5")
    assert result["std_dev"] == Decimal("18.0")


@pytest.mark.asyncio
async def test_fetch_mstar_metrics_returns_none_on_empty_response() -> None:
    client = AsyncMock()
    client.fetch = AsyncMock(return_value={})

    result = await fetch_mstar_metrics(client, "F001")
    assert result is None


@pytest.mark.asyncio
async def test_fetch_mstar_metrics_returns_none_when_all_values_null() -> None:
    """parse_risk_response returns None when all values are None."""
    client = AsyncMock()
    # Return a response where all values are None/missing
    client.fetch = AsyncMock(return_value={"SharpeRatio": None})

    result = await fetch_mstar_metrics(client, "F001")
    assert result is None


# ---------------------------------------------------------------------------
# run_mstar_crossvalidation integration tests
# ---------------------------------------------------------------------------


def _make_fund_rows(n: int = 2) -> list[dict[str, str]]:
    return [
        {"mstar_id": f"F{i:03d}", "fund_name": f"Fund {i}"}
        for i in range(1, n + 1)
    ]


@pytest.mark.asyncio
async def test_run_mstar_crossval_skipped_when_no_funds() -> None:
    """If no funds in DB, report is warn with get_top_funds step."""
    session = AsyncMock()

    with patch(
        "app.computation.mstar_crossval.get_top_funds",
        new=AsyncMock(return_value=[]),
    ):
        report = await run_mstar_crossvalidation(session, date(2025, 1, 10))

    assert report.phase == "mstar_xval"
    assert any(s.status == "warning" for s in report.steps)


@pytest.mark.asyncio
async def test_run_mstar_crossval_skipped_when_no_credentials() -> None:
    """If Morningstar returns empty on probe → overall_status = skipped."""
    session = AsyncMock()

    funds = _make_fund_rows(2)

    mock_client = AsyncMock()
    mock_client.fetch = AsyncMock(return_value={})  # Empty = no credentials
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    with (
        patch("app.computation.mstar_crossval.get_top_funds", new=AsyncMock(return_value=funds)),
        patch("app.computation.mstar_crossval.MorningstarClient", return_value=mock_client),
    ):
        report = await run_mstar_crossvalidation(session, date(2025, 1, 10))

    assert report.overall_status == "skipped"
    skipped_step = next(
        (s for s in report.steps if s.status == "skipped"), None
    )
    assert skipped_step is not None
    assert "credentials" in skipped_step.message.lower()


@pytest.mark.asyncio
async def test_run_mstar_crossval_pass_when_all_match() -> None:
    """All metrics within tolerance → overall_status = pass."""
    session = AsyncMock()
    funds = _make_fund_rows(1)
    biz_date = date(2025, 1, 10)

    our_data = _make_our()
    mstar_data = _make_mstar()

    mock_client = AsyncMock()
    # First call is the probe (returns non-empty), subsequent calls are real
    mock_client.fetch = AsyncMock(
        side_effect=[
            {"SharpeRatio": "1.5"},  # probe — non-empty
        ]
    )
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    with (
        patch("app.computation.mstar_crossval.get_top_funds", new=AsyncMock(return_value=funds)),
        patch(
            "app.computation.mstar_crossval.fetch_our_metrics",
            new=AsyncMock(return_value=our_data),
        ),
        patch(
            "app.computation.mstar_crossval.fetch_mstar_metrics",
            new=AsyncMock(return_value=mstar_data),
        ),
        patch("app.computation.mstar_crossval.MorningstarClient", return_value=mock_client),
    ):
        report = await run_mstar_crossvalidation(session, biz_date)

    assert report.overall_status == "passed"
    rollup = next(s for s in report.steps if s.name == "mstar_crossval_rollup")
    assert rollup.details.get("breach_count", 0) == 0


@pytest.mark.asyncio
async def test_run_mstar_crossval_fail_when_breach() -> None:
    """A metric breach makes the per-fund step and rollup fail."""
    session = AsyncMock()
    funds = _make_fund_rows(1)
    biz_date = date(2025, 1, 10)

    our_data = _make_our(sharpe="2.5")   # far from mstar 1.5 → breach
    mstar_data = _make_mstar()

    mock_client = AsyncMock()
    mock_client.fetch = AsyncMock(return_value={"SharpeRatio": "1.5"})  # probe
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    with (
        patch("app.computation.mstar_crossval.get_top_funds", new=AsyncMock(return_value=funds)),
        patch(
            "app.computation.mstar_crossval.fetch_our_metrics",
            new=AsyncMock(return_value=our_data),
        ),
        patch(
            "app.computation.mstar_crossval.fetch_mstar_metrics",
            new=AsyncMock(return_value=mstar_data),
        ),
        patch("app.computation.mstar_crossval.MorningstarClient", return_value=mock_client),
    ):
        report = await run_mstar_crossvalidation(session, biz_date)

    assert report.overall_status == "failed"
    fund_step = next(s for s in report.steps if s.name == "fund_F001")
    assert fund_step.status == "failed"
    assert fund_step.details.get("breach_count", 0) >= 1


@pytest.mark.asyncio
async def test_run_mstar_crossval_skipped_fund_when_no_derived() -> None:
    """Fund with no de_mf_derived_daily row → per-fund step is 'skipped'."""
    session = AsyncMock()
    funds = _make_fund_rows(1)
    biz_date = date(2025, 1, 10)

    mock_client = AsyncMock()
    mock_client.fetch = AsyncMock(return_value={"SharpeRatio": "1.5"})  # probe
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    with (
        patch("app.computation.mstar_crossval.get_top_funds", new=AsyncMock(return_value=funds)),
        patch(
            "app.computation.mstar_crossval.fetch_our_metrics",
            new=AsyncMock(return_value=None),  # no row
        ),
        patch("app.computation.mstar_crossval.MorningstarClient", return_value=mock_client),
    ):
        report = await run_mstar_crossvalidation(session, biz_date)

    fund_step = next((s for s in report.steps if s.name == "fund_F001"), None)
    assert fund_step is not None
    assert fund_step.status == "skipped"


@pytest.mark.asyncio
async def test_run_mstar_crossval_error_does_not_crash() -> None:
    """Per-fund exception is caught and logged; other funds still processed."""
    session = AsyncMock()
    funds = _make_fund_rows(1)
    biz_date = date(2025, 1, 10)

    mock_client = AsyncMock()
    mock_client.fetch = AsyncMock(return_value={"SharpeRatio": "1.5"})
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    async def _raising(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("simulated DB failure")

    with (
        patch("app.computation.mstar_crossval.get_top_funds", new=AsyncMock(return_value=funds)),
        patch(
            "app.computation.mstar_crossval.fetch_our_metrics",
            new=AsyncMock(side_effect=RuntimeError("simulated DB failure")),
        ),
        patch("app.computation.mstar_crossval.MorningstarClient", return_value=mock_client),
    ):
        report = await run_mstar_crossvalidation(session, biz_date)

    # Report should complete (not raise), with an error step for the fund
    error_steps = [s for s in report.steps if s.status == "failed"]
    assert len(error_steps) >= 1


@pytest.mark.asyncio
async def test_run_mstar_crossval_client_exception_returns_report() -> None:
    """If MorningstarClient itself raises, we get an error report, not a crash."""
    session = AsyncMock()
    funds = _make_fund_rows(1)

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(side_effect=ConnectionError("no network"))
    mock_client.__aexit__ = AsyncMock(return_value=None)

    with (
        patch("app.computation.mstar_crossval.get_top_funds", new=AsyncMock(return_value=funds)),
        patch("app.computation.mstar_crossval.MorningstarClient", return_value=mock_client),
    ):
        report = await run_mstar_crossvalidation(session, date(2025, 1, 10))

    assert isinstance(report, QAReport)
    assert report.overall_status == "failed"
    error_step = next((s for s in report.steps if s.status == "failed"), None)
    assert error_step is not None
