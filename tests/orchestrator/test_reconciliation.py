"""Tests for cross-source data reconciliation."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.orchestrator.reconciliation import ReconciliationChecker, ReconciliationResult


def _mock_scalar_result(value: object) -> MagicMock:
    result = MagicMock()
    row = MagicMock()
    row.__getitem__ = lambda self, idx: value
    result.fetchone.return_value = row
    return result


def _mock_none_result() -> MagicMock:
    result = MagicMock()
    result.fetchone.return_value = None
    return result


class TestEquityRowCount:
    @pytest.mark.asyncio
    async def test_sufficient_rows_passes(self) -> None:
        checker = ReconciliationChecker()
        session = AsyncMock()
        row = MagicMock()
        row.__getitem__ = lambda self, idx: 1900
        result = MagicMock()
        result.fetchone.return_value = row
        session.execute.return_value = result

        outcome = await checker.check_equity_row_count(session, date(2026, 4, 5))
        assert outcome.passed is True
        assert outcome.severity == "info"
        assert "1900" in outcome.actual

    @pytest.mark.asyncio
    async def test_below_threshold_fails_critical(self) -> None:
        checker = ReconciliationChecker()
        session = AsyncMock()
        row = MagicMock()
        row.__getitem__ = lambda self, idx: 500
        result = MagicMock()
        result.fetchone.return_value = row
        session.execute.return_value = result

        outcome = await checker.check_equity_row_count(session, date(2026, 4, 5))
        assert outcome.passed is False
        assert outcome.severity == "critical"

    @pytest.mark.asyncio
    async def test_exception_returns_failed_result(self) -> None:
        checker = ReconciliationChecker()
        session = AsyncMock()
        session.execute.side_effect = Exception("DB error")

        outcome = await checker.check_equity_row_count(session, date(2026, 4, 5))
        assert outcome.passed is False
        assert outcome.severity == "critical"
        assert "DB error" in outcome.message

    @pytest.mark.asyncio
    async def test_check_name_is_correct(self) -> None:
        checker = ReconciliationChecker()
        session = AsyncMock()
        row = MagicMock()
        row.__getitem__ = lambda self, idx: 2000
        result = MagicMock()
        result.fetchone.return_value = row
        session.execute.return_value = result

        outcome = await checker.check_equity_row_count(session, date(2026, 4, 5))
        assert outcome.check_name == "equity_row_count_sanity"


class TestMFRowCount:
    @pytest.mark.asyncio
    async def test_sufficient_rows_passes(self) -> None:
        checker = ReconciliationChecker()
        session = AsyncMock()
        row = MagicMock()
        row.__getitem__ = lambda self, idx: 8000
        result = MagicMock()
        result.fetchone.return_value = row
        session.execute.return_value = result

        outcome = await checker.check_mf_row_count(session, date(2026, 4, 5))
        assert outcome.passed is True
        assert outcome.severity == "info"

    @pytest.mark.asyncio
    async def test_below_threshold_fails_warning(self) -> None:
        checker = ReconciliationChecker()
        session = AsyncMock()
        row = MagicMock()
        row.__getitem__ = lambda self, idx: 4000
        result = MagicMock()
        result.fetchone.return_value = row
        session.execute.return_value = result

        outcome = await checker.check_mf_row_count(session, date(2026, 4, 5))
        assert outcome.passed is False
        assert outcome.severity == "warning"

    @pytest.mark.asyncio
    async def test_check_name_is_correct(self) -> None:
        checker = ReconciliationChecker()
        session = AsyncMock()
        row = MagicMock()
        row.__getitem__ = lambda self, idx: 6000
        result = MagicMock()
        result.fetchone.return_value = row
        session.execute.return_value = result

        outcome = await checker.check_mf_row_count(session, date(2026, 4, 5))
        assert outcome.check_name == "mf_row_count_sanity"


class TestNSEvsYfinance:
    @pytest.mark.asyncio
    async def test_within_tolerance_passes(self) -> None:
        checker = ReconciliationChecker()
        session = AsyncMock()

        # NSE: 22000, yfinance: 22100 — diff ~ 0.45%, within 2%
        nse_row = MagicMock()
        nse_row.__getitem__ = lambda self, idx: Decimal("22000")
        yf_row = MagicMock()
        yf_row.__getitem__ = lambda self, idx: Decimal("22100")

        nse_result = MagicMock()
        nse_result.fetchone.return_value = nse_row
        yf_result = MagicMock()
        yf_result.fetchone.return_value = yf_row

        session.execute.side_effect = [nse_result, yf_result]

        outcome = await checker.check_nse_vs_yfinance(session, date(2026, 4, 5))
        assert outcome.passed is True
        assert outcome.check_name == "nse_vs_yfinance_nifty50"

    @pytest.mark.asyncio
    async def test_outside_tolerance_fails(self) -> None:
        checker = ReconciliationChecker()
        session = AsyncMock()

        # NSE: 22000, yfinance: 20000 — diff ~ 9%, outside 2%
        nse_row = MagicMock()
        nse_row.__getitem__ = lambda self, idx: Decimal("22000")
        yf_row = MagicMock()
        yf_row.__getitem__ = lambda self, idx: Decimal("20000")

        nse_result = MagicMock()
        nse_result.fetchone.return_value = nse_row
        yf_result = MagicMock()
        yf_result.fetchone.return_value = yf_row

        session.execute.side_effect = [nse_result, yf_result]

        outcome = await checker.check_nse_vs_yfinance(session, date(2026, 4, 5))
        assert outcome.passed is False
        assert outcome.severity == "warning"
        assert outcome.tolerance == "2%"

    @pytest.mark.asyncio
    async def test_missing_nse_data_fails(self) -> None:
        checker = ReconciliationChecker()
        session = AsyncMock()

        nse_result = MagicMock()
        nse_result.fetchone.return_value = None
        session.execute.return_value = nse_result

        outcome = await checker.check_nse_vs_yfinance(session, date(2026, 4, 5))
        assert outcome.passed is False
        assert "not found" in outcome.message.lower()

    @pytest.mark.asyncio
    async def test_zero_nse_close_fails_critical(self) -> None:
        checker = ReconciliationChecker()
        session = AsyncMock()

        nse_row = MagicMock()
        nse_row.__getitem__ = lambda self, idx: Decimal("0")
        yf_row = MagicMock()
        yf_row.__getitem__ = lambda self, idx: Decimal("22000")

        nse_result = MagicMock()
        nse_result.fetchone.return_value = nse_row
        yf_result = MagicMock()
        yf_result.fetchone.return_value = yf_row

        session.execute.side_effect = [nse_result, yf_result]

        outcome = await checker.check_nse_vs_yfinance(session, date(2026, 4, 5))
        assert outcome.passed is False
        assert outcome.severity == "critical"


class TestAMFIvsMorningstar:
    @pytest.mark.asyncio
    async def test_no_matching_funds_fails(self) -> None:
        checker = ReconciliationChecker()
        session = AsyncMock()

        result = MagicMock()
        result.fetchall.return_value = []
        session.execute.return_value = result

        outcome = await checker.check_amfi_vs_morningstar(session, date(2026, 4, 5))
        assert outcome.passed is False
        assert outcome.check_name == "amfi_vs_morningstar_nav"

    @pytest.mark.asyncio
    async def test_all_within_tolerance_passes(self) -> None:
        checker = ReconciliationChecker()
        session = AsyncMock()

        # Two funds, both within 0.1% tolerance
        rows = []
        for i in range(5):
            row = MagicMock()
            row.scheme_code = f"scheme_{i}"
            row.amfi_nav = Decimal("100.00")
            row.ms_nav = Decimal("100.05")  # 0.05% diff
            rows.append(row)

        result = MagicMock()
        result.fetchall.return_value = rows
        session.execute.return_value = result

        outcome = await checker.check_amfi_vs_morningstar(session, date(2026, 4, 5))
        assert outcome.passed is True

    @pytest.mark.asyncio
    async def test_breach_outside_tolerance_fails(self) -> None:
        checker = ReconciliationChecker()
        session = AsyncMock()

        rows = []
        row = MagicMock()
        row.scheme_code = "scheme_breach"
        row.amfi_nav = Decimal("100.00")
        row.ms_nav = Decimal("101.50")  # 1.5% diff — outside 0.1%
        rows.append(row)

        result = MagicMock()
        result.fetchall.return_value = rows
        session.execute.return_value = result

        outcome = await checker.check_amfi_vs_morningstar(session, date(2026, 4, 5))
        assert outcome.passed is False
        assert outcome.severity == "warning"


class TestRunAll:
    @pytest.mark.asyncio
    async def test_run_all_returns_four_results(self) -> None:
        checker = ReconciliationChecker()
        session = AsyncMock()

        # Equity row count
        eq_row = MagicMock()
        eq_row.__getitem__ = lambda self, idx: 2000
        eq_result = MagicMock()
        eq_result.fetchone.return_value = eq_row

        # MF row count
        mf_row = MagicMock()
        mf_row.__getitem__ = lambda self, idx: 8000
        mf_result = MagicMock()
        mf_result.fetchone.return_value = mf_row

        # NSE result
        nse_row = MagicMock()
        nse_row.__getitem__ = lambda self, idx: Decimal("22000")
        nse_result = MagicMock()
        nse_result.fetchone.return_value = nse_row

        # yfinance result
        yf_row = MagicMock()
        yf_row.__getitem__ = lambda self, idx: Decimal("22100")
        yf_result = MagicMock()
        yf_result.fetchone.return_value = yf_row

        # AMFI/MS result
        amfi_result = MagicMock()
        amfi_result.fetchall.return_value = []

        session.execute.side_effect = [
            eq_result,   # equity row count
            mf_result,   # mf row count
            nse_result,  # NSE close
            yf_result,   # yfinance close
            amfi_result, # AMFI vs MS
        ]

        results = await checker.run_all(session, date(2026, 4, 5))
        assert len(results) == 4
        assert all(isinstance(r, ReconciliationResult) for r in results)


class TestReconciliationResult:
    def test_result_attributes(self) -> None:
        result = ReconciliationResult(
            check_name="test_check",
            passed=True,
            severity="info",
            message="All good",
            expected="100",
            actual="100",
            tolerance="2%",
        )
        assert result.check_name == "test_check"
        assert result.passed is True
        assert result.severity == "info"
        assert result.tolerance == "2%"

    def test_failed_result(self) -> None:
        result = ReconciliationResult(
            check_name="failing_check",
            passed=False,
            severity="critical",
            message="Data missing",
        )
        assert result.passed is False
        assert result.expected is None
        assert result.actual is None
