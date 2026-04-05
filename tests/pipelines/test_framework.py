"""Tests for BasePipeline orchestration framework."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult, PipelineResult
from app.pipelines.validation import AnomalyRecord


# ---------------------------------------------------------------------------
# Concrete test subclass of BasePipeline
# ---------------------------------------------------------------------------

class _SuccessPipeline(BasePipeline):
    """Minimal pipeline that always succeeds."""

    pipeline_name = "test_success_pipeline"
    requires_trading_day = True
    exchange = "NSE"

    def __init__(self, rows_processed: int = 100, rows_failed: int = 0) -> None:
        self._rows_processed = rows_processed
        self._rows_failed = rows_failed

    async def execute(self, business_date, session, run_log) -> ExecutionResult:
        return ExecutionResult(
            rows_processed=self._rows_processed,
            rows_failed=self._rows_failed,
        )


class _FailingPipeline(BasePipeline):
    """Pipeline that raises an exception during execute."""

    pipeline_name = "test_failing_pipeline"
    requires_trading_day = False

    async def execute(self, business_date, session, run_log) -> ExecutionResult:
        raise RuntimeError("Simulated pipeline failure")


class _AnomalyPipeline(BasePipeline):
    """Pipeline that returns anomalies from validate()."""

    pipeline_name = "test_anomaly_pipeline"
    requires_trading_day = False

    async def execute(self, business_date, session, run_log) -> ExecutionResult:
        return ExecutionResult(rows_processed=50, rows_failed=0)

    async def validate(self, business_date, session, run_log) -> list[AnomalyRecord]:
        return [
            AnomalyRecord(
                entity_type="equity",
                anomaly_type="price_spike",
                severity="high",
                instrument_id=None,
                ticker="RELIANCE",
            )
        ]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session() -> AsyncMock:
    session = AsyncMock()
    session.execute = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    return session


def _make_scalar_result(value) -> MagicMock:
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
    result.scalar_one.return_value = value
    result.scalar.return_value = value
    return result


def _patch_pipeline_dependencies(
    *,
    pipeline_enabled: bool = True,
    is_trading: bool = True,
    lock_acquired: bool = True,
    max_run_number: int = 0,
    anomaly_count: int = 0,
) -> list:
    """Return list of patches to apply for a full pipeline run."""
    return [
        patch("app.pipelines.framework.is_pipeline_enabled", new=AsyncMock(return_value=pipeline_enabled)),
        patch("app.pipelines.framework.is_trading_day", new=AsyncMock(return_value=is_trading)),
        patch("app.pipelines.framework.acquire_pipeline_lock", new=AsyncMock(return_value=lock_acquired)),
        patch("app.pipelines.framework.release_pipeline_lock", new=AsyncMock()),
        patch("app.pipelines.framework.record_anomalies", new=AsyncMock(return_value=anomaly_count)),
    ]


# ---------------------------------------------------------------------------
# Tests: PipelineResult and ExecutionResult dataclasses
# ---------------------------------------------------------------------------

def test_pipeline_result_fields() -> None:
    """PipelineResult dataclass has all expected fields."""
    result = PipelineResult(
        pipeline_name="equity_bhav",
        business_date=date(2025, 1, 15),
        status="success",
        rows_processed=1000,
        rows_failed=0,
        anomalies_detected=0,
        duration_seconds=1.23,
    )
    assert result.pipeline_name == "equity_bhav"
    assert result.status == "success"
    assert result.error is None


def test_execution_result_fields() -> None:
    """ExecutionResult dataclass has all expected fields."""
    result = ExecutionResult(rows_processed=500, rows_failed=10)
    assert result.rows_processed == 500
    assert result.rows_failed == 10
    assert result.source_file_id is None


# ---------------------------------------------------------------------------
# Tests: run() — system flag kill switch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_skips_when_pipeline_disabled(mock_session: AsyncMock) -> None:
    """run() returns skipped status when pipeline is disabled by system flag."""
    patches = _patch_pipeline_dependencies(pipeline_enabled=False)
    pipeline = _SuccessPipeline()

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        result = await pipeline.run(date(2025, 1, 15), mock_session)

    assert result.status == "skipped"
    assert result.rows_processed == 0
    assert "disabled" in result.error.lower()


# ---------------------------------------------------------------------------
# Tests: run() — trading calendar check
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_skips_non_trading_day(mock_session: AsyncMock) -> None:
    """run() returns skipped status on non-trading days when requires_trading_day=True."""
    patches = _patch_pipeline_dependencies(is_trading=False)
    pipeline = _SuccessPipeline()

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        result = await pipeline.run(date(2025, 1, 26), mock_session)

    assert result.status == "skipped"
    assert "Non-trading" in result.error


@pytest.mark.asyncio
async def test_run_proceeds_on_non_trading_day_when_not_required(mock_session: AsyncMock) -> None:
    """run() proceeds even on non-trading days when requires_trading_day=False."""
    patches = _patch_pipeline_dependencies(is_trading=False, anomaly_count=0)
    pipeline = _FailingPipeline()  # requires_trading_day=False

    # Patch session.execute for run_number query
    mock_session.execute.return_value = _make_scalar_result(0)

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        result = await pipeline.run(date(2025, 1, 26), mock_session)

    # Should not be skipped due to trading day — it will fail due to exception
    assert result.status == "failed"


# ---------------------------------------------------------------------------
# Tests: run() — advisory lock
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_skips_when_lock_not_acquired(mock_session: AsyncMock) -> None:
    """run() returns skipped status when advisory lock cannot be acquired."""
    patches = _patch_pipeline_dependencies(lock_acquired=False)
    pipeline = _SuccessPipeline()

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        result = await pipeline.run(date(2025, 1, 15), mock_session)

    assert result.status == "skipped"
    assert "Another instance" in result.error


# ---------------------------------------------------------------------------
# Tests: run() — pipeline log creation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_creates_pipeline_log_entry(mock_session: AsyncMock) -> None:
    """run() creates a DePipelineLog entry with status=running."""
    patches = _patch_pipeline_dependencies()
    pipeline = _SuccessPipeline()

    # run_number query returns 0 (first run)
    mock_session.execute.return_value = _make_scalar_result(0)

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        await pipeline.run(date(2025, 1, 15), mock_session)

    # session.add should have been called (creating run log, then updating it)
    assert mock_session.add.called
    # session.flush should have been called
    assert mock_session.flush.called


@pytest.mark.asyncio
async def test_run_sets_run_number_incrementally(mock_session: AsyncMock) -> None:
    """run() sets run_number to max_existing + 1."""
    patches = _patch_pipeline_dependencies()
    pipeline = _SuccessPipeline()

    added_objects = []
    mock_session.add.side_effect = lambda obj: added_objects.append(obj)
    # Query returns max run_number = 3 (so next should be 4)
    mock_session.execute.return_value = _make_scalar_result(3)

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        await pipeline.run(date(2025, 1, 15), mock_session)

    # Find the DePipelineLog that was added
    log_objects = [o for o in added_objects if isinstance(o, DePipelineLog)]
    assert len(log_objects) >= 1
    assert log_objects[0].run_number == 4


# ---------------------------------------------------------------------------
# Tests: run() — success / partial / failed status logic
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_returns_success_when_no_failures(mock_session: AsyncMock) -> None:
    """run() returns success when rows_failed=0."""
    patches = _patch_pipeline_dependencies()
    pipeline = _SuccessPipeline(rows_processed=100, rows_failed=0)
    mock_session.execute.return_value = _make_scalar_result(0)

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        result = await pipeline.run(date(2025, 1, 15), mock_session)

    assert result.status == "success"
    assert result.rows_processed == 100
    assert result.rows_failed == 0


@pytest.mark.asyncio
async def test_run_returns_partial_when_some_failures(mock_session: AsyncMock) -> None:
    """run() returns partial when rows_failed > 0 but rows_processed > 0."""
    patches = _patch_pipeline_dependencies()
    pipeline = _SuccessPipeline(rows_processed=90, rows_failed=10)
    mock_session.execute.return_value = _make_scalar_result(0)

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        result = await pipeline.run(date(2025, 1, 15), mock_session)

    assert result.status == "partial"
    assert result.rows_processed == 90
    assert result.rows_failed == 10


@pytest.mark.asyncio
async def test_run_returns_failed_when_all_rows_failed(mock_session: AsyncMock) -> None:
    """run() returns failed when rows_processed=0 and rows_failed > 0."""
    patches = _patch_pipeline_dependencies()
    pipeline = _SuccessPipeline(rows_processed=0, rows_failed=50)
    mock_session.execute.return_value = _make_scalar_result(0)

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        result = await pipeline.run(date(2025, 1, 15), mock_session)

    assert result.status == "failed"


@pytest.mark.asyncio
async def test_run_returns_failed_on_exception(mock_session: AsyncMock) -> None:
    """run() returns failed and captures error message when execute() raises."""
    patches = _patch_pipeline_dependencies()
    pipeline = _FailingPipeline()
    mock_session.execute.return_value = _make_scalar_result(0)

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        result = await pipeline.run(date(2025, 1, 15), mock_session)

    assert result.status == "failed"
    assert result.error == "Simulated pipeline failure"
    assert result.rows_processed == 0


# ---------------------------------------------------------------------------
# Tests: run() — anomaly detection
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_records_anomalies_from_validate(mock_session: AsyncMock) -> None:
    """run() calls validate() and records returned anomalies."""
    patches = _patch_pipeline_dependencies(anomaly_count=1)
    pipeline = _AnomalyPipeline()
    mock_session.execute.return_value = _make_scalar_result(0)

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        result = await pipeline.run(date(2025, 1, 15), mock_session)

    assert result.anomalies_detected == 1
    assert result.status == "success"


# ---------------------------------------------------------------------------
# Tests: run() — duration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_populates_duration_seconds(mock_session: AsyncMock) -> None:
    """run() reports a non-negative duration_seconds in PipelineResult."""
    patches = _patch_pipeline_dependencies()
    pipeline = _SuccessPipeline()
    mock_session.execute.return_value = _make_scalar_result(0)

    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        result = await pipeline.run(date(2025, 1, 15), mock_session)

    assert result.duration_seconds >= 0.0
    assert isinstance(result.duration_seconds, float)


# ---------------------------------------------------------------------------
# Tests: run() — pipeline name propagated correctly
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_result_contains_correct_pipeline_name(mock_session: AsyncMock) -> None:
    """run() populates pipeline_name and business_date on PipelineResult."""
    patches = _patch_pipeline_dependencies()
    pipeline = _SuccessPipeline()
    mock_session.execute.return_value = _make_scalar_result(0)

    business_date = date(2025, 3, 31)
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        result = await pipeline.run(business_date, mock_session)

    assert result.pipeline_name == "test_success_pipeline"
    assert result.business_date == business_date
