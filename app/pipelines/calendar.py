"""Trading calendar helpers for pipeline scheduling."""

from __future__ import annotations

from datetime import date

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.instruments import DeTradingCalendar

logger = get_logger(__name__)


async def is_trading_day(
    session: AsyncSession,
    check_date: date,
    exchange: str = "NSE",
) -> bool:
    """Check de_trading_calendar to determine if a date is a trading day.

    If no entry exists for the date, assumes it IS a trading day (fail-open).
    This ensures pipelines don't silently skip data due to missing calendar entries.
    """
    result = await session.execute(
        select(DeTradingCalendar.is_trading).where(
            DeTradingCalendar.date == check_date,
            DeTradingCalendar.exchange == exchange,
        )
    )
    row = result.scalar_one_or_none()

    if row is None:
        logger.warning(
            "trading_calendar_entry_missing",
            check_date=check_date.isoformat(),
            exchange=exchange,
            assumption="trading_day",
        )
        return True

    return bool(row)


async def get_last_trading_day(
    session: AsyncSession,
    before_date: date,
    exchange: str = "NSE",
) -> date | None:
    """Get the most recent trading day strictly before the given date.

    Returns None if no trading day found in the calendar before the given date.
    """
    result = await session.execute(
        select(DeTradingCalendar.date)
        .where(
            DeTradingCalendar.date < before_date,
            DeTradingCalendar.exchange == exchange,
            DeTradingCalendar.is_trading == sa.true(),
        )
        .order_by(DeTradingCalendar.date.desc())
        .limit(1)
    )
    row = result.scalar_one_or_none()
    return row


async def get_next_trading_day(
    session: AsyncSession,
    after_date: date,
    exchange: str = "NSE",
) -> date | None:
    """Get the next trading day strictly after the given date.

    Returns None if no trading day found in the calendar after the given date.
    """
    result = await session.execute(
        select(DeTradingCalendar.date)
        .where(
            DeTradingCalendar.date > after_date,
            DeTradingCalendar.exchange == exchange,
            DeTradingCalendar.is_trading == sa.true(),
        )
        .order_by(DeTradingCalendar.date.asc())
        .limit(1)
    )
    row = result.scalar_one_or_none()
    return row
