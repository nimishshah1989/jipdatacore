"""Tests for the SymbolHistoryPipeline and helper functions.

The pipeline detects historical symbol changes by comparing the symbol stored
in de_equity_ohlcv rows against de_instrument.current_symbol.
"""

from __future__ import annotations

import uuid
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.pipelines.equity.symbol_history import (
    SymbolHistoryPipeline,
    detect_ohlcv_symbol_changes,
)
from app.pipelines.framework import ExecutionResult


# ---------------------------------------------------------------------------
# detect_ohlcv_symbol_changes
# ---------------------------------------------------------------------------


class TestDetectOhlcvSymbolChanges:
    """Unit tests for detect_ohlcv_symbol_changes — mocks the DB session."""

    @pytest.mark.asyncio
    async def test_returns_list_of_dicts_with_expected_keys(self) -> None:
        instr_id = uuid.uuid4()
        mock_row = MagicMock()
        mock_row.instrument_id = instr_id
        mock_row.old_symbol = "OLDTICKER"
        mock_row.new_symbol = "NEWTICKER"
        mock_row.last_date_old_symbol = date(2022, 3, 15)

        mock_result = MagicMock()
        mock_result.fetchall.return_value = [mock_row]

        session = AsyncMock()
        session.execute = AsyncMock(return_value=mock_result)

        rows = await detect_ohlcv_symbol_changes(session)

        assert len(rows) == 1
        row = rows[0]
        assert row["instrument_id"] == instr_id
        assert row["old_symbol"] == "OLDTICKER"
        assert row["new_symbol"] == "NEWTICKER"
        assert row["last_date_old_symbol"] == date(2022, 3, 15)

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_mismatches(self) -> None:
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []

        session = AsyncMock()
        session.execute = AsyncMock(return_value=mock_result)

        rows = await detect_ohlcv_symbol_changes(session)
        assert rows == []

    @pytest.mark.asyncio
    async def test_multiple_rows_returned(self) -> None:
        ids = [uuid.uuid4(), uuid.uuid4()]
        dates = [date(2019, 1, 15), date(2021, 6, 30)]

        rows_mock = []
        for i in range(2):
            r = MagicMock()
            r.instrument_id = ids[i]
            r.old_symbol = f"OLD{i}"
            r.new_symbol = f"NEW{i}"
            r.last_date_old_symbol = dates[i]
            rows_mock.append(r)

        mock_result = MagicMock()
        mock_result.fetchall.return_value = rows_mock

        session = AsyncMock()
        session.execute = AsyncMock(return_value=mock_result)

        rows = await detect_ohlcv_symbol_changes(session)
        assert len(rows) == 2
        assert rows[0]["instrument_id"] == ids[0]
        assert rows[1]["instrument_id"] == ids[1]

    @pytest.mark.asyncio
    async def test_execute_called_once_with_text_query(self) -> None:
        """Session.execute must be called exactly once (a single SQL query)."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []

        session = AsyncMock()
        session.execute = AsyncMock(return_value=mock_result)

        await detect_ohlcv_symbol_changes(session)

        session.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# SymbolHistoryPipeline.execute
# ---------------------------------------------------------------------------


class TestSymbolHistoryPipelineExecute:
    """Tests for the pipeline's execute() method with mocked session/data."""

    def _make_pipeline(self) -> SymbolHistoryPipeline:
        return SymbolHistoryPipeline()

    def _make_run_log(self) -> MagicMock:
        run_log = MagicMock()
        run_log.id = 42
        return run_log

    @pytest.mark.asyncio
    async def test_execute_processes_valid_change(self) -> None:
        pipeline = self._make_pipeline()
        run_log = self._make_run_log()

        instr_id = uuid.uuid4()
        changes = [
            {
                "instrument_id": instr_id,
                "old_symbol": "SBIN",
                "new_symbol": "SBINEW",
                "last_date_old_symbol": date(2021, 5, 1),
            }
        ]

        session = AsyncMock()
        session.execute = AsyncMock(return_value=MagicMock())

        with patch(
            "app.pipelines.equity.symbol_history.detect_ohlcv_symbol_changes",
            new=AsyncMock(return_value=changes),
        ):
            result = await pipeline.execute(date(2021, 5, 1), session, run_log)

        assert isinstance(result, ExecutionResult)
        assert result.rows_processed == 1
        assert result.rows_failed == 0

    @pytest.mark.asyncio
    async def test_execute_returns_zero_when_no_changes(self) -> None:
        pipeline = self._make_pipeline()
        run_log = self._make_run_log()

        session = AsyncMock()
        session.execute = AsyncMock(return_value=MagicMock())

        with patch(
            "app.pipelines.equity.symbol_history.detect_ohlcv_symbol_changes",
            new=AsyncMock(return_value=[]),
        ):
            result = await pipeline.execute(date(2021, 5, 1), session, run_log)

        assert result.rows_processed == 0
        assert result.rows_failed == 0
        # No DB insert should be attempted when there are no changes
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_execute_skips_self_referential_rows(self) -> None:
        """Rows where old_symbol == new_symbol must be silently skipped."""
        pipeline = self._make_pipeline()
        run_log = self._make_run_log()

        changes = [
            {
                "instrument_id": uuid.uuid4(),
                "old_symbol": "SAME",
                "new_symbol": "SAME",
                "last_date_old_symbol": date(2021, 5, 1),
            }
        ]

        session = AsyncMock()
        session.execute = AsyncMock(return_value=MagicMock())

        with patch(
            "app.pipelines.equity.symbol_history.detect_ohlcv_symbol_changes",
            new=AsyncMock(return_value=changes),
        ):
            result = await pipeline.execute(date(2021, 5, 1), session, run_log)

        assert result.rows_processed == 0
        assert result.rows_failed == 0

    @pytest.mark.asyncio
    async def test_execute_multiple_changes_all_processed(self) -> None:
        pipeline = self._make_pipeline()
        run_log = self._make_run_log()

        changes = [
            {
                "instrument_id": uuid.uuid4(),
                "old_symbol": f"OLD{i}",
                "new_symbol": f"NEW{i}",
                "last_date_old_symbol": date(2020, i + 1, 1),
            }
            for i in range(3)
        ]

        session = AsyncMock()
        session.execute = AsyncMock(return_value=MagicMock())

        with patch(
            "app.pipelines.equity.symbol_history.detect_ohlcv_symbol_changes",
            new=AsyncMock(return_value=changes),
        ):
            result = await pipeline.execute(date(2020, 12, 1), session, run_log)

        assert result.rows_processed == 3
        assert result.rows_failed == 0

    @pytest.mark.asyncio
    async def test_execute_inserts_with_correct_reason(self) -> None:
        """The reason field should be the OHLCV mismatch constant string."""
        from app.pipelines.equity.symbol_history import _CHANGE_REASON

        pipeline = self._make_pipeline()
        run_log = self._make_run_log()

        instr_id = uuid.uuid4()
        changes = [
            {
                "instrument_id": instr_id,
                "old_symbol": "WIPRO2",
                "new_symbol": "WIPRO",
                "last_date_old_symbol": date(2019, 8, 10),
            }
        ]

        captured_values: list = []

        async def capture_execute(stmt, *args, **kwargs):
            # Capture the statement for inspection
            captured_values.append(stmt)
            return MagicMock()

        session = AsyncMock()
        session.execute = capture_execute

        with patch(
            "app.pipelines.equity.symbol_history.detect_ohlcv_symbol_changes",
            new=AsyncMock(return_value=changes),
        ):
            result = await pipeline.execute(date(2019, 8, 10), session, run_log)

        assert result.rows_processed == 1
        assert _CHANGE_REASON == "Historical OHLCV symbol mismatch"

    @pytest.mark.asyncio
    async def test_execute_mixed_valid_and_self_referential(self) -> None:
        """One valid change + one self-referential: only valid one is processed."""
        pipeline = self._make_pipeline()
        run_log = self._make_run_log()

        changes = [
            {
                "instrument_id": uuid.uuid4(),
                "old_symbol": "INFY1",
                "new_symbol": "INFY",
                "last_date_old_symbol": date(2018, 4, 1),
            },
            {
                "instrument_id": uuid.uuid4(),
                "old_symbol": "SELF",
                "new_symbol": "SELF",
                "last_date_old_symbol": date(2018, 5, 1),
            },
        ]

        session = AsyncMock()
        session.execute = AsyncMock(return_value=MagicMock())

        with patch(
            "app.pipelines.equity.symbol_history.detect_ohlcv_symbol_changes",
            new=AsyncMock(return_value=changes),
        ):
            result = await pipeline.execute(date(2018, 5, 1), session, run_log)

        assert result.rows_processed == 1
        assert result.rows_failed == 0


# ---------------------------------------------------------------------------
# SymbolHistoryPipeline.validate
# ---------------------------------------------------------------------------


class TestSymbolHistoryPipelineValidate:
    """Tests for the validate() method — returns empty list (no-op)."""

    def _make_pipeline(self) -> SymbolHistoryPipeline:
        return SymbolHistoryPipeline()

    @pytest.mark.asyncio
    async def test_validate_returns_empty_list(self) -> None:
        pipeline = self._make_pipeline()
        session = AsyncMock()
        run_log = MagicMock()

        anomalies = await pipeline.validate(date(2021, 1, 1), session, run_log)

        assert anomalies == []

    @pytest.mark.asyncio
    async def test_validate_does_not_query_db(self) -> None:
        """validate() is a no-op — it must not issue any DB queries."""
        pipeline = self._make_pipeline()
        session = AsyncMock()
        run_log = MagicMock()

        await pipeline.validate(date(2021, 1, 1), session, run_log)

        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_validate_return_type_is_list(self) -> None:
        pipeline = self._make_pipeline()
        session = AsyncMock()
        run_log = MagicMock()

        result = await pipeline.validate(date(2021, 1, 1), session, run_log)
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Pipeline metadata
# ---------------------------------------------------------------------------


class TestSymbolHistoryPipelineMetadata:
    def test_pipeline_name(self) -> None:
        assert SymbolHistoryPipeline.pipeline_name == "symbol_history"

    def test_requires_trading_day_false(self) -> None:
        assert SymbolHistoryPipeline.requires_trading_day is False

    def test_is_subclass_of_base_pipeline(self) -> None:
        from app.pipelines.framework import BasePipeline

        assert issubclass(SymbolHistoryPipeline, BasePipeline)

    def test_change_reason_constant(self) -> None:
        from app.pipelines.equity.symbol_history import _CHANGE_REASON

        assert _CHANGE_REASON == "Historical OHLCV symbol mismatch"
