"""Tests for pipeline advisory lock guards."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.pipelines.guards import acquire_pipeline_lock, release_pipeline_lock


@pytest.fixture
def mock_session() -> AsyncMock:
    session = AsyncMock()
    session.execute = AsyncMock()
    return session


@pytest.mark.asyncio
async def test_acquire_pipeline_lock_returns_true_when_acquired(mock_session: AsyncMock) -> None:
    """acquire_pipeline_lock returns True when lock is successfully acquired."""
    result_mock = MagicMock()
    result_mock.scalar.return_value = True
    mock_session.execute.return_value = result_mock

    acquired = await acquire_pipeline_lock(mock_session, "equity_bhav", date(2025, 1, 15))

    assert acquired is True
    mock_session.execute.assert_called_once()
    call_args = mock_session.execute.call_args
    # Verify the SQL contains pg_try_advisory_lock
    assert "pg_try_advisory_lock" in str(call_args[0][0])


@pytest.mark.asyncio
async def test_acquire_pipeline_lock_returns_false_when_contention(mock_session: AsyncMock) -> None:
    """acquire_pipeline_lock returns False when lock is already held by another session."""
    result_mock = MagicMock()
    result_mock.scalar.return_value = False
    mock_session.execute.return_value = result_mock

    acquired = await acquire_pipeline_lock(mock_session, "equity_bhav", date(2025, 1, 15))

    assert acquired is False


@pytest.mark.asyncio
async def test_acquire_pipeline_lock_uses_correct_key_format(mock_session: AsyncMock) -> None:
    """acquire_pipeline_lock passes the correct key format to hashtext."""
    result_mock = MagicMock()
    result_mock.scalar.return_value = True
    mock_session.execute.return_value = result_mock

    await acquire_pipeline_lock(mock_session, "mf_nav", date(2025, 3, 31))

    call_args = mock_session.execute.call_args
    params = call_args[0][1]  # Second positional arg is the params dict
    assert params["key"] == "mf_nav:2025-03-31"


@pytest.mark.asyncio
async def test_release_pipeline_lock_executes_unlock(mock_session: AsyncMock) -> None:
    """release_pipeline_lock calls pg_advisory_unlock with the correct key."""
    result_mock = MagicMock()
    mock_session.execute.return_value = result_mock

    await release_pipeline_lock(mock_session, "equity_bhav", date(2025, 1, 15))

    mock_session.execute.assert_called_once()
    call_args = mock_session.execute.call_args
    assert "pg_advisory_unlock" in str(call_args[0][0])
    params = call_args[0][1]
    assert params["key"] == "equity_bhav:2025-01-15"


@pytest.mark.asyncio
async def test_different_pipelines_same_date_have_different_keys(mock_session: AsyncMock) -> None:
    """Two different pipelines on the same date produce different lock keys."""
    result_mock = MagicMock()
    result_mock.scalar.return_value = True
    mock_session.execute.return_value = result_mock

    await acquire_pipeline_lock(mock_session, "pipeline_a", date(2025, 1, 15))
    key_a = mock_session.execute.call_args[0][1]["key"]

    await acquire_pipeline_lock(mock_session, "pipeline_b", date(2025, 1, 15))
    key_b = mock_session.execute.call_args[0][1]["key"]

    assert key_a != key_b


@pytest.mark.asyncio
async def test_same_pipeline_different_dates_have_different_keys(mock_session: AsyncMock) -> None:
    """Same pipeline on different dates produces different lock keys."""
    result_mock = MagicMock()
    result_mock.scalar.return_value = True
    mock_session.execute.return_value = result_mock

    await acquire_pipeline_lock(mock_session, "equity_bhav", date(2025, 1, 15))
    key_jan = mock_session.execute.call_args[0][1]["key"]

    await acquire_pipeline_lock(mock_session, "equity_bhav", date(2025, 1, 16))
    key_feb = mock_session.execute.call_args[0][1]["key"]

    assert key_jan != key_feb
