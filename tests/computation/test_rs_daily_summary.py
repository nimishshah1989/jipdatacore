"""Regression test: nightly compute populates de_rs_daily_summary (Step 13).

Uses AsyncMock — no real DB calls.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from app.computation.rs import populate_rs_daily_summary


def _make_row(instrument_id=None, vs_benchmark="NIFTY 50"):
    """Build a fake row mimicking the SELECT result."""
    row = MagicMock()
    row.date = date(2025, 4, 10)
    row.instrument_id = instrument_id or uuid4()
    row.symbol = "RELIANCE"
    row.sector = "Energy"
    row.vs_benchmark = vs_benchmark
    row.rs_composite = Decimal("1.2345")
    row.rs_1m = Decimal("0.9876")
    row.rs_3m = Decimal("1.5432")
    return row


@pytest.mark.asyncio
async def test_populate_rs_daily_summary_upserts_rows():
    """Step 13: populate_rs_daily_summary writes rows to de_rs_daily_summary."""
    session = AsyncMock()

    rows = [_make_row() for _ in range(3)]
    select_result = MagicMock()
    select_result.fetchall.return_value = rows

    execute_results = [select_result, MagicMock()]
    session.execute = AsyncMock(side_effect=execute_results)

    result = await populate_rs_daily_summary(session, date(2025, 4, 10))

    assert result == 3
    assert session.execute.call_count == 2
    session.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_populate_rs_daily_summary_no_data_returns_zero():
    """If no RS scores exist for the date, return 0."""
    session = AsyncMock()

    select_result = MagicMock()
    select_result.fetchall.return_value = []
    session.execute = AsyncMock(return_value=select_result)

    result = await populate_rs_daily_summary(session, date(2025, 4, 10))

    assert result == 0
    session.flush.assert_not_awaited()


@pytest.mark.asyncio
async def test_populate_rs_daily_summary_batches_large_sets():
    """Verify batching when row count > batch_size."""
    session = AsyncMock()

    rows = [_make_row() for _ in range(1500)]
    select_result = MagicMock()
    select_result.fetchall.return_value = rows

    call_count = 0

    async def mock_execute(stmt, params=None):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return select_result
        return MagicMock()

    session.execute = AsyncMock(side_effect=mock_execute)

    result = await populate_rs_daily_summary(session, date(2025, 4, 10))

    assert result == 1500
    assert session.execute.call_count == 3  # 1 SELECT + 2 INSERT batches


def test_runner_imports_populate_rs_daily_summary():
    """Verify the runner module imports and references populate_rs_daily_summary."""
    import app.computation.runner as runner_mod
    assert hasattr(runner_mod, "populate_rs_daily_summary")
    import inspect
    source = inspect.getsource(runner_mod.run_full_computation_pipeline)
    assert "populate_rs_daily_summary" in source
