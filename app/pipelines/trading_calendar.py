"""Trading calendar pipeline — seeds NSE holiday list and special sessions.

Populates de_trading_calendar with trading days, holidays, and
ad-hoc Saturday special sessions.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.instruments import DeTradingCalendar

logger = get_logger(__name__)

# NSE scheduled holidays (non-exhaustive; seed data for current year)
# Format: (month, day, note)
NSE_FIXED_HOLIDAYS: list[tuple[int, int, str]] = [
    (1, 26, "Republic Day"),
    (8, 15, "Independence Day"),
    (10, 2, "Gandhi Jayanti"),
    (12, 25, "Christmas"),
]

# NSE floating holidays vary year-to-year (Holi, Diwali, Dussehra, etc.)
# These are provided as data-driven input, not hardcoded dates.

EXCHANGE = "NSE"


def generate_calendar_rows(
    year: int,
    holiday_dates: list[date],
    special_saturday_dates: list[date] | None = None,
) -> list[dict[str, Any]]:
    """Generate calendar rows for every day in the given year.

    Args:
        year: The calendar year to populate.
        holiday_dates: List of NSE holiday dates (including Republic Day, etc.).
        special_saturday_dates: List of ad-hoc Saturday trading sessions.

    Returns:
        List of row dicts for de_trading_calendar.
    """
    if special_saturday_dates is None:
        special_saturday_dates = []

    special_saturday_set = set(special_saturday_dates)
    holiday_set = set(holiday_dates)

    rows: list[dict[str, Any]] = []
    current = date(year, 1, 1)
    end = date(year, 12, 31)

    while current <= end:
        weekday = current.weekday()  # Monday=0, Sunday=6
        is_weekend = weekday >= 5  # Saturday=5, Sunday=6

        if is_weekend:
            # Special Saturday sessions are trading days
            if current in special_saturday_set:
                is_trading = True
                notes = "Special Saturday session"
            else:
                is_trading = False
                notes = "Weekend"
        elif current in holiday_set:
            is_trading = False
            notes = "NSE Holiday"
        else:
            is_trading = True
            notes = None

        rows.append(
            {
                "date": current,
                "is_trading": is_trading,
                "exchange": EXCHANGE,
                "notes": notes,
            }
        )

        current += timedelta(days=1)

    return rows


async def upsert_calendar_rows(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> int:
    """Upsert trading calendar rows into de_trading_calendar.

    Returns the number of rows upserted.
    """
    if not rows:
        return 0

    stmt = pg_insert(DeTradingCalendar).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["date"],
        set_={
            "is_trading": stmt.excluded.is_trading,
            "exchange": stmt.excluded.exchange,
            "notes": stmt.excluded.notes,
        },
    )
    await session.execute(stmt)
    return len(rows)


async def populate_trading_calendar(
    session: AsyncSession,
    year: int,
    holiday_dates: list[date] | None = None,
    special_saturday_dates: list[date] | None = None,
) -> int:
    """Populate de_trading_calendar for an entire year.

    This is an idempotent operation — safe to re-run.

    Args:
        session: Async SQLAlchemy session.
        year: The year to populate (e.g. 2026).
        holiday_dates: NSE holiday dates for the year. If None, uses fixed
                       holidays only (Republic Day, Independence Day, etc.).
        special_saturday_dates: Ad-hoc Saturday trading sessions for the year.

    Returns:
        Number of rows inserted/updated.
    """
    if holiday_dates is None:
        # Build fixed holidays for this year
        holiday_dates = []
        for month, day, _ in NSE_FIXED_HOLIDAYS:
            try:
                holiday_dates.append(date(year, month, day))
            except ValueError:
                pass  # Skip invalid dates (e.g. Feb 29 in non-leap years)

    rows = generate_calendar_rows(year, holiday_dates, special_saturday_dates)

    count = await upsert_calendar_rows(session, rows)

    logger.info(
        "trading_calendar_populated",
        year=year,
        rows_upserted=count,
        holidays=len(holiday_dates),
        special_saturdays=len(special_saturday_dates or []),
    )

    return count


async def mark_special_saturday(
    session: AsyncSession,
    session_date: date,
    notes: str | None = "Special Saturday session",
) -> None:
    """Mark a specific Saturday as a trading day (ad-hoc special session).

    Args:
        session: Async SQLAlchemy session.
        session_date: The Saturday date to mark as a trading day.
        notes: Optional description of the special session.
    """
    if session_date.weekday() != 5:
        raise ValueError(
            f"mark_special_saturday: {session_date} is not a Saturday "
            f"(weekday={session_date.weekday()})"
        )

    stmt = pg_insert(DeTradingCalendar).values(
        [
            {
                "date": session_date,
                "is_trading": True,
                "exchange": EXCHANGE,
                "notes": notes,
            }
        ]
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["date"],
        set_={
            "is_trading": True,
            "notes": stmt.excluded.notes,
        },
    )
    await session.execute(stmt)

    logger.info(
        "special_saturday_marked",
        session_date=session_date.isoformat(),
        notes=notes,
    )
