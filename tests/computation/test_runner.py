"""Unit tests for app.computation.runner.

Tests cover:
- run_technicals_for_date logic via mocked AsyncSession
- _persist_sector_rs upsert logic via mocked session
- run_full_computation_pipeline orchestration (dependency skipping on failures)

All DB-touching functions are tested with AsyncMock — no real DB is required.
"""

from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.computation.runner import (
    _persist_sector_rs,
    run_full_computation_pipeline,
    run_technicals_for_date,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_session(price_rows: list | None = None) -> AsyncMock:
    """Build a minimal AsyncSession mock."""
    session = AsyncMock()
    execute_result = MagicMock()
    execute_result.fetchall.return_value = price_rows or []
    session.execute = AsyncMock(return_value=execute_result)
    session.flush = AsyncMock()
    return session


def _make_price_row(
    instrument_id: str,
    row_date: date,
    close_adj: float,
) -> MagicMock:
    row = MagicMock()
    row.instrument_id = uuid.UUID(instrument_id)
    row.date = row_date
    row.close_adj = close_adj
    return row


BUSINESS_DATE = date(2026, 4, 6)
INSTRUMENT_A = "aaaaaaaa-0000-0000-0000-000000000001"
INSTRUMENT_B = "bbbbbbbb-0000-0000-0000-000000000002"


# ---------------------------------------------------------------------------
# run_technicals_for_date tests
# ---------------------------------------------------------------------------

class TestRunTechnicalsForDate:
    @pytest.mark.asyncio
    async def test_no_price_data_returns_zero(self) -> None:
        session = _make_session(price_rows=[])
        result = await run_technicals_for_date(session, BUSINESS_DATE)
        assert result == 0

    @pytest.mark.asyncio
    async def test_instrument_not_on_business_date_is_skipped(self) -> None:
        old_date = date(2026, 4, 1)
        rows = [_make_price_row(INSTRUMENT_A, old_date, 100.0)]
        session = _make_session(price_rows=rows)
        result = await run_technicals_for_date(session, BUSINESS_DATE)
        assert result == 0

    @pytest.mark.asyncio
    async def test_single_instrument_insufficient_history_still_upserts(self) -> None:
        import datetime as dt

        rows = []
        base = date(2026, 3, 1)
        for i in range(30):
            d = base + dt.timedelta(days=i)
            rows.append(_make_price_row(INSTRUMENT_A, d, 100.0 + i))
        rows.append(_make_price_row(INSTRUMENT_A, BUSINESS_DATE, 130.0))

        session = _make_session(price_rows=rows)
        result = await run_technicals_for_date(session, BUSINESS_DATE)
        assert result == 1
        session.execute.assert_called()
        session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_instruments_batch_upserted(self) -> None:
        rows = [
            _make_price_row(INSTRUMENT_A, BUSINESS_DATE, 200.0),
            _make_price_row(INSTRUMENT_B, BUSINESS_DATE, 300.0),
        ]
        session = _make_session(price_rows=rows)
        result = await run_technicals_for_date(session, BUSINESS_DATE)
        assert result == 2


# ---------------------------------------------------------------------------
# _persist_sector_rs tests
# ---------------------------------------------------------------------------

class TestPersistSectorRs:
    @pytest.mark.asyncio
    async def test_empty_dict_returns_zero(self) -> None:
        session = _make_session()
        result = await _persist_sector_rs(session, BUSINESS_DATE, {})
        assert result == 0
        session.flush.assert_not_called()

    @pytest.mark.asyncio
    async def test_persists_sector_rows(self) -> None:
        sector_results = {
            "Technology": {
                "sector_rs": Decimal("1.2345"),
                "pct_above_50dma": Decimal("75.0"),
                "pct_above_200dma": Decimal("70.0"),
                "constituent_count": 5,
            },
            "Banking": {
                "sector_rs": Decimal("-0.5"),
                "pct_above_50dma": Decimal("40.0"),
                "pct_above_200dma": Decimal("35.0"),
                "constituent_count": 8,
            },
        }
        session = _make_session()
        result = await _persist_sector_rs(session, BUSINESS_DATE, sector_results)
        assert result == 2
        session.execute.assert_called_once()
        session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_sector_rs_none_is_handled(self) -> None:
        sector_results = {
            "Healthcare": {
                "sector_rs": None,
                "pct_above_50dma": Decimal("50.0"),
                "pct_above_200dma": Decimal("45.0"),
                "constituent_count": 4,
            },
        }
        session = _make_session()
        result = await _persist_sector_rs(session, BUSINESS_DATE, sector_results)
        assert result == 1


# ---------------------------------------------------------------------------
# run_full_computation_pipeline orchestration tests
# ---------------------------------------------------------------------------

class TestRunFullComputationPipeline:
    @pytest.mark.asyncio
    async def test_all_steps_pass(self) -> None:
        session = _make_session()
        with (
            patch("app.computation.runner.run_technicals_for_date", new=AsyncMock(return_value=500)),
            patch("app.computation.runner.compute_rs_scores", new=AsyncMock(return_value=1500)),
            patch("app.computation.runner.compute_breadth", new=AsyncMock(return_value=1)),
            patch("app.computation.runner.compute_market_regime", new=AsyncMock(return_value="BULL")),
            patch("app.computation.runner.compute_sector_metrics", new=AsyncMock(
                return_value={"Technology": {"sector_rs": Decimal("1.0"), "pct_above_50dma": Decimal("80.0"), "pct_above_200dma": Decimal("70.0"), "constituent_count": 5}}
            )),
            patch("app.computation.runner._persist_sector_rs", new=AsyncMock(return_value=1)),
            patch("app.computation.runner.compute_fund_derived_metrics", new=AsyncMock(return_value=200)),
        ):
            report = await run_full_computation_pipeline(session, BUSINESS_DATE)

        assert report.overall_status == "passed"
        assert report.phase == "compute"
        assert report.business_date == BUSINESS_DATE
        assert len(report.steps) == 6
        step_names = [s.step_name for s in report.steps]
        assert step_names == ["technicals", "rs", "breadth", "regime", "sectors", "fund_derived"]
        assert sum(s.rows_affected for s in report.steps) == 500 + 1500 + 1 + 1 + 1 + 200
        assert report.completed_at is not None

    @pytest.mark.asyncio
    async def test_technicals_failure_skips_all_downstream(self) -> None:
        session = _make_session()
        with patch("app.computation.runner.run_technicals_for_date", new=AsyncMock(side_effect=RuntimeError("DB error"))):
            report = await run_full_computation_pipeline(session, BUSINESS_DATE)

        assert report.overall_status == "failed"
        assert len(report.steps) == 1
        assert report.steps[0].step_name == "technicals"
        assert "DB error" in report.steps[0].errors[0]
        assert report.completed_at is not None

    @pytest.mark.asyncio
    async def test_rs_failure_skips_downstream(self) -> None:
        session = _make_session()
        with (
            patch("app.computation.runner.run_technicals_for_date", new=AsyncMock(return_value=100)),
            patch("app.computation.runner.compute_rs_scores", new=AsyncMock(side_effect=RuntimeError("RS failed"))),
        ):
            report = await run_full_computation_pipeline(session, BUSINESS_DATE)

        assert report.overall_status == "failed"
        assert len(report.steps) == 2
        step_names = [s.step_name for s in report.steps]
        assert "technicals" in step_names
        assert "rs" in step_names
        failed = [s for s in report.steps if s.status == "failed"]
        assert len(failed) == 1
        assert failed[0].step_name == "rs"

    @pytest.mark.asyncio
    async def test_breadth_failure_does_not_skip_regime_or_sectors(self) -> None:
        session = _make_session()
        with (
            patch("app.computation.runner.run_technicals_for_date", new=AsyncMock(return_value=100)),
            patch("app.computation.runner.compute_rs_scores", new=AsyncMock(return_value=200)),
            patch("app.computation.runner.compute_breadth", new=AsyncMock(side_effect=RuntimeError("breadth error"))),
            patch("app.computation.runner.compute_market_regime", new=AsyncMock(return_value="SIDEWAYS")),
            patch("app.computation.runner.compute_sector_metrics", new=AsyncMock(return_value={})),
            patch("app.computation.runner._persist_sector_rs", new=AsyncMock(return_value=0)),
            patch("app.computation.runner.compute_fund_derived_metrics", new=AsyncMock(return_value=50)),
        ):
            report = await run_full_computation_pipeline(session, BUSINESS_DATE)

        assert len(report.steps) == 6
        step_names = [s.step_name for s in report.steps]
        assert "regime" in step_names
        assert "sectors" in step_names
        assert "fund_derived" in step_names
        failed = [s for s in report.steps if s.status == "failed"]
        assert len(failed) == 1
        assert failed[0].step_name == "breadth"

    @pytest.mark.asyncio
    async def test_regime_details_captured(self) -> None:
        session = _make_session()
        with (
            patch("app.computation.runner.run_technicals_for_date", new=AsyncMock(return_value=10)),
            patch("app.computation.runner.compute_rs_scores", new=AsyncMock(return_value=20)),
            patch("app.computation.runner.compute_breadth", new=AsyncMock(return_value=1)),
            patch("app.computation.runner.compute_market_regime", new=AsyncMock(return_value="BEAR")),
            patch("app.computation.runner.compute_sector_metrics", new=AsyncMock(return_value={})),
            patch("app.computation.runner._persist_sector_rs", new=AsyncMock(return_value=0)),
            patch("app.computation.runner.compute_fund_derived_metrics", new=AsyncMock(return_value=0)),
        ):
            report = await run_full_computation_pipeline(session, BUSINESS_DATE)

        regime_step = next(s for s in report.steps if s.step_name == "regime")
        assert regime_step.details["regime"] == "BEAR"
        assert regime_step.rows_affected == 1

    @pytest.mark.asyncio
    async def test_regime_none_sets_rows_zero(self) -> None:
        session = _make_session()
        with (
            patch("app.computation.runner.run_technicals_for_date", new=AsyncMock(return_value=10)),
            patch("app.computation.runner.compute_rs_scores", new=AsyncMock(return_value=20)),
            patch("app.computation.runner.compute_breadth", new=AsyncMock(return_value=1)),
            patch("app.computation.runner.compute_market_regime", new=AsyncMock(return_value=None)),
            patch("app.computation.runner.compute_sector_metrics", new=AsyncMock(return_value={})),
            patch("app.computation.runner._persist_sector_rs", new=AsyncMock(return_value=0)),
            patch("app.computation.runner.compute_fund_derived_metrics", new=AsyncMock(return_value=0)),
        ):
            report = await run_full_computation_pipeline(session, BUSINESS_DATE)

        regime_step = next(s for s in report.steps if s.step_name == "regime")
        assert regime_step.rows_affected == 0

    @pytest.mark.asyncio
    async def test_sectors_details_contains_count(self) -> None:
        sector_dict = {
            "IT": {"sector_rs": Decimal("1.0"), "pct_above_50dma": None, "pct_above_200dma": None, "constituent_count": 3},
            "FMCG": {"sector_rs": Decimal("0.5"), "pct_above_50dma": None, "pct_above_200dma": None, "constituent_count": 4},
        }
        session = _make_session()
        with (
            patch("app.computation.runner.run_technicals_for_date", new=AsyncMock(return_value=10)),
            patch("app.computation.runner.compute_rs_scores", new=AsyncMock(return_value=20)),
            patch("app.computation.runner.compute_breadth", new=AsyncMock(return_value=1)),
            patch("app.computation.runner.compute_market_regime", new=AsyncMock(return_value="BULL")),
            patch("app.computation.runner.compute_sector_metrics", new=AsyncMock(return_value=sector_dict)),
            patch("app.computation.runner._persist_sector_rs", new=AsyncMock(return_value=2)),
            patch("app.computation.runner.compute_fund_derived_metrics", new=AsyncMock(return_value=0)),
        ):
            report = await run_full_computation_pipeline(session, BUSINESS_DATE)

        sectors_step = next(s for s in report.steps if s.step_name == "sectors")
        assert sectors_step.details["sectors_computed"] == 2
        assert sectors_step.rows_affected == 2
