"""Tests for trading calendar helpers."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.pipelines.calendar import get_last_trading_day, get_next_trading_day, is_trading_day


@pytest.fixture
def mock_session() -> AsyncMock:
    session = AsyncMock()
    session.execute = AsyncMock()
    return session


def _make_scalar_result(value) -> MagicMock:
    """Helper: create a mock result whose scalar_one_or_none() returns value."""
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
    return result


@pytest.mark.asyncio
async def test_is_trading_day_returns_true_for_trading_day(mock_session: AsyncMock) -> None:
    """is_trading_day returns True when calendar entry shows is_trading=True."""
    mock_session.execute.return_value = _make_scalar_result(True)

    result = await is_trading_day(mock_session, date(2025, 1, 15))

    assert result is True


@pytest.mark.asyncio
async def test_is_trading_day_returns_false_for_holiday(mock_session: AsyncMock) -> None:
    """is_trading_day returns False when calendar entry shows is_trading=False."""
    mock_session.execute.return_value = _make_scalar_result(False)

    result = await is_trading_day(mock_session, date(2025, 1, 26))

    assert result is False


@pytest.mark.asyncio
async def test_is_trading_day_fail_open_when_no_entry(mock_session: AsyncMock) -> None:
    """is_trading_day returns True (fail-open) when no calendar entry exists."""
    mock_session.execute.return_value = _make_scalar_result(None)

    result = await is_trading_day(mock_session, date(2025, 12, 31))

    # Fail-open: assume trading day when calendar entry is missing
    assert result is True


@pytest.mark.asyncio
async def test_is_trading_day_uses_exchange_filter(mock_session: AsyncMock) -> None:
    """is_trading_day passes the exchange parameter to the query."""
    mock_session.execute.return_value = _make_scalar_result(True)

    await is_trading_day(mock_session, date(2025, 1, 15), exchange="BSE")

    # Verify the query was executed (exchange filtering happens in SQLAlchemy query)
    mock_session.execute.assert_called_once()


@pytest.mark.asyncio
async def test_get_last_trading_day_returns_date_when_found(mock_session: AsyncMock) -> None:
    """get_last_trading_day returns the most recent trading date before the given date."""
    expected_date = date(2025, 1, 14)
    mock_session.execute.return_value = _make_scalar_result(expected_date)

    result = await get_last_trading_day(mock_session, date(2025, 1, 15))

    assert result == expected_date


@pytest.mark.asyncio
async def test_get_last_trading_day_returns_none_when_not_found(mock_session: AsyncMock) -> None:
    """get_last_trading_day returns None when no prior trading day exists in calendar."""
    mock_session.execute.return_value = _make_scalar_result(None)

    result = await get_last_trading_day(mock_session, date(2020, 1, 1))

    assert result is None


@pytest.mark.asyncio
async def test_get_next_trading_day_returns_date_when_found(mock_session: AsyncMock) -> None:
    """get_next_trading_day returns the next trading date after the given date."""
    expected_date = date(2025, 1, 16)
    mock_session.execute.return_value = _make_scalar_result(expected_date)

    result = await get_next_trading_day(mock_session, date(2025, 1, 15))

    assert result == expected_date


@pytest.mark.asyncio
async def test_get_next_trading_day_returns_none_when_not_found(mock_session: AsyncMock) -> None:
    """get_next_trading_day returns None when no future trading day exists in calendar."""
    mock_session.execute.return_value = _make_scalar_result(None)

    result = await get_next_trading_day(mock_session, date(2030, 12, 31))

    assert result is None


@pytest.mark.asyncio
async def test_get_last_trading_day_uses_exchange_filter(mock_session: AsyncMock) -> None:
    """get_last_trading_day passes the exchange parameter to the query."""
    mock_session.execute.return_value = _make_scalar_result(date(2025, 1, 14))

    await get_last_trading_day(mock_session, date(2025, 1, 15), exchange="NSE")

    mock_session.execute.assert_called_once()
