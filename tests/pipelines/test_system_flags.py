"""Tests for system flags helpers."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.pipelines.system_flags import check_system_flag, is_pipeline_enabled, set_system_flag


@pytest.fixture
def mock_session() -> AsyncMock:
    session = AsyncMock()
    session.execute = AsyncMock()
    return session


def _make_scalar_result(value) -> MagicMock:
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
    return result


@pytest.mark.asyncio
async def test_check_system_flag_returns_true_when_flag_is_true(mock_session: AsyncMock) -> None:
    """check_system_flag returns True when the flag exists and is True."""
    mock_session.execute.return_value = _make_scalar_result(True)

    result = await check_system_flag(mock_session, "some_flag")

    assert result is True


@pytest.mark.asyncio
async def test_check_system_flag_returns_false_when_flag_is_false(mock_session: AsyncMock) -> None:
    """check_system_flag returns False when the flag exists and is False."""
    mock_session.execute.return_value = _make_scalar_result(False)

    result = await check_system_flag(mock_session, "some_flag")

    assert result is False


@pytest.mark.asyncio
async def test_check_system_flag_returns_false_when_flag_missing(mock_session: AsyncMock) -> None:
    """check_system_flag returns False (safe default) when the flag doesn't exist."""
    mock_session.execute.return_value = _make_scalar_result(None)

    result = await check_system_flag(mock_session, "nonexistent_flag")

    assert result is False


@pytest.mark.asyncio
async def test_is_pipeline_enabled_returns_false_when_global_kill_switch_active(
    mock_session: AsyncMock,
) -> None:
    """is_pipeline_enabled returns False when global_kill_switch is True."""
    # First call (global_kill_switch) returns True — kill switch is active
    mock_session.execute.return_value = _make_scalar_result(True)

    result = await is_pipeline_enabled(mock_session, "equity_bhav")

    assert result is False
    # Only one call needed — short-circuit on global kill switch
    mock_session.execute.assert_called_once()


@pytest.mark.asyncio
async def test_is_pipeline_enabled_returns_false_when_pipeline_flag_disabled(
    mock_session: AsyncMock,
) -> None:
    """is_pipeline_enabled returns False when pipeline-specific flag is False."""
    # First call (global_kill_switch) returns False — no global kill
    # Second call (pipeline_equity_bhav_enabled) returns False — disabled
    mock_session.execute.side_effect = [
        _make_scalar_result(False),  # global_kill_switch
        _make_scalar_result(False),  # pipeline_equity_bhav_enabled
    ]

    result = await is_pipeline_enabled(mock_session, "equity_bhav")

    assert result is False
    assert mock_session.execute.call_count == 2


@pytest.mark.asyncio
async def test_is_pipeline_enabled_returns_true_when_no_flags_set(
    mock_session: AsyncMock,
) -> None:
    """is_pipeline_enabled returns True (fail-open) when no flags are configured."""
    # Both flag checks return None (flags don't exist)
    mock_session.execute.side_effect = [
        _make_scalar_result(False),  # global_kill_switch (False = not active)
        _make_scalar_result(None),   # pipeline-specific flag (None = not set, treat as enabled)
    ]

    result = await is_pipeline_enabled(mock_session, "equity_bhav")

    assert result is True


@pytest.mark.asyncio
async def test_is_pipeline_enabled_returns_true_when_pipeline_flag_explicitly_enabled(
    mock_session: AsyncMock,
) -> None:
    """is_pipeline_enabled returns True when pipeline-specific flag is True."""
    mock_session.execute.side_effect = [
        _make_scalar_result(False),  # global_kill_switch
        _make_scalar_result(True),   # pipeline_equity_bhav_enabled = True
    ]

    result = await is_pipeline_enabled(mock_session, "equity_bhav")

    assert result is True


@pytest.mark.asyncio
async def test_set_system_flag_executes_upsert(mock_session: AsyncMock) -> None:
    """set_system_flag executes an upsert statement."""
    mock_session.execute.return_value = MagicMock()

    await set_system_flag(mock_session, "global_kill_switch", True, reason="Emergency stop")

    mock_session.execute.assert_called_once()


@pytest.mark.asyncio
async def test_set_system_flag_false_value(mock_session: AsyncMock) -> None:
    """set_system_flag can set a flag to False."""
    mock_session.execute.return_value = MagicMock()

    await set_system_flag(mock_session, "pipeline_equity_bhav_enabled", False, reason="Maintenance")

    mock_session.execute.assert_called_once()


@pytest.mark.asyncio
async def test_set_system_flag_no_reason(mock_session: AsyncMock) -> None:
    """set_system_flag works when reason is omitted."""
    mock_session.execute.return_value = MagicMock()

    await set_system_flag(mock_session, "some_flag", True)

    mock_session.execute.assert_called_once()
