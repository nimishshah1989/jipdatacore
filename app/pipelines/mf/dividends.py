"""MF IDCW dividend ingestion and nav_adj recomputation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.prices import DeMfDividends, DeMfNavDaily

logger = get_logger(__name__)


@dataclass
class DividendRecord:
    """A parsed dividend event for a mutual fund."""

    mstar_id: str
    record_date: date
    dividend_per_unit: Decimal
    nav_before: Optional[Decimal] = None
    nav_after: Optional[Decimal] = None
    source: Optional[str] = None


def compute_adj_factor(nav_before: Decimal, dividend_per_unit: Decimal) -> Optional[Decimal]:
    """Compute the dividend adjustment factor.

    adj_factor = (nav_before - dividend_per_unit) / nav_before

    This factor, when multiplied cumulatively to historical NAVs, produces
    the adjusted NAV series that accounts for dividend payouts.

    Returns None if nav_before is zero or inputs are invalid.
    """
    if nav_before <= Decimal("0"):
        return None
    try:
        return (nav_before - dividend_per_unit) / nav_before
    except (ZeroDivisionError, Exception):
        return None


async def upsert_dividend(
    session: AsyncSession,
    record: DividendRecord,
) -> None:
    """Upsert a single dividend record into de_mf_dividends.

    ON CONFLICT (mstar_id, record_date) DO UPDATE.
    adj_factor is computed from nav_before and dividend_per_unit if nav_before
    is provided.
    """
    adj_factor: Optional[Decimal] = None
    if record.nav_before is not None:
        adj_factor = compute_adj_factor(record.nav_before, record.dividend_per_unit)

    stmt = pg_insert(DeMfDividends).values(
        mstar_id=record.mstar_id,
        record_date=record.record_date,
        dividend_per_unit=record.dividend_per_unit,
        nav_before=record.nav_before,
        nav_after=record.nav_after,
        adj_factor=adj_factor,
        source=record.source,
    )
    stmt = stmt.on_conflict_do_update(
        constraint="uq_mf_dividends",
        set_={
            "dividend_per_unit": stmt.excluded.dividend_per_unit,
            "nav_before": stmt.excluded.nav_before,
            "nav_after": stmt.excluded.nav_after,
            "adj_factor": stmt.excluded.adj_factor,
            "source": stmt.excluded.source,
        },
    )
    await session.execute(stmt)
    logger.info(
        "mf_dividend_upserted",
        mstar_id=record.mstar_id,
        record_date=record.record_date.isoformat(),
        dividend_per_unit=str(record.dividend_per_unit),
    )


async def get_dividends_since(
    session: AsyncSession,
    mstar_id: str,
    from_date: date,
) -> list[DeMfDividends]:
    """Fetch all dividend records for a fund on or after from_date, ordered ASC."""
    result = await session.execute(
        select(DeMfDividends)
        .where(
            DeMfDividends.mstar_id == mstar_id,
            DeMfDividends.record_date >= from_date,
        )
        .order_by(DeMfDividends.record_date.asc())
    )
    return list(result.scalars())


async def recompute_nav_adj(
    session: AsyncSession,
    mstar_id: str,
    from_date: date,
) -> int:
    """Recompute nav_adj from from_date forward for a given fund.

    Algorithm:
    1. Fetch all dividends on or after from_date (already stored in de_mf_dividends)
    2. For each NAV row from from_date onwards, apply cumulative adj_factor
       nav_adj = nav * cumulative_factor

    The cumulative factor starts at 1.0 and is multiplied by each dividend's
    adj_factor on the record_date.

    Returns number of NAV rows updated.
    """
    # Fetch dividends sorted ASC
    dividends = await get_dividends_since(session, mstar_id, from_date)

    # Build a lookup: record_date → adj_factor
    div_factors: dict[date, Decimal] = {}
    for div in dividends:
        if div.adj_factor is not None:
            div_factors[div.record_date] = div.adj_factor

    if not div_factors:
        logger.info(
            "nav_adj_recompute_no_dividends",
            mstar_id=mstar_id,
            from_date=from_date.isoformat(),
        )
        return 0

    # Fetch all NAV rows from from_date onwards
    result = await session.execute(
        select(DeMfNavDaily.nav_date, DeMfNavDaily.nav)
        .where(
            DeMfNavDaily.mstar_id == mstar_id,
            DeMfNavDaily.nav_date >= from_date,
        )
        .order_by(DeMfNavDaily.nav_date.asc())
    )
    nav_rows = result.all()

    if not nav_rows:
        logger.info(
            "nav_adj_recompute_no_nav_rows",
            mstar_id=mstar_id,
            from_date=from_date.isoformat(),
        )
        return 0

    # Walk through rows applying cumulative factor
    cumulative = Decimal("1")
    rows_updated = 0

    for nav_row in nav_rows:
        row_date = nav_row.nav_date
        raw_nav = nav_row.nav

        # Apply dividend factor if this date has a dividend event
        if row_date in div_factors:
            cumulative = cumulative * div_factors[row_date]

        nav_adj = raw_nav * cumulative

        await session.execute(
            sa.update(DeMfNavDaily)
            .where(
                DeMfNavDaily.mstar_id == mstar_id,
                DeMfNavDaily.nav_date == row_date,
            )
            .values(nav_adj=nav_adj)
        )
        rows_updated += 1

    logger.info(
        "nav_adj_recompute_complete",
        mstar_id=mstar_id,
        from_date=from_date.isoformat(),
        rows_updated=rows_updated,
        final_cumulative_factor=str(cumulative),
    )
    return rows_updated


async def ingest_dividends(
    session: AsyncSession,
    records: list[DividendRecord],
) -> tuple[int, int]:
    """Ingest a list of dividend records and trigger nav_adj recomputation.

    For each unique (mstar_id, earliest_record_date), recomputes nav_adj
    from that date forward.

    Returns (records_upserted, recompute_rows_updated).
    """
    if not records:
        return 0, 0

    # Track earliest record_date per fund for recompute
    earliest_by_fund: dict[str, date] = {}
    upserted = 0

    for record in records:
        await upsert_dividend(session, record)
        upserted += 1

        current_earliest = earliest_by_fund.get(record.mstar_id)
        if current_earliest is None or record.record_date < current_earliest:
            earliest_by_fund[record.mstar_id] = record.record_date

    # Flush upserted dividends before recompute
    await session.flush()

    total_recomputed = 0
    for mstar_id, earliest_date in earliest_by_fund.items():
        updated = await recompute_nav_adj(session, mstar_id, earliest_date)
        total_recomputed += updated

    logger.info(
        "dividends_ingest_complete",
        records_upserted=upserted,
        recompute_rows=total_recomputed,
        funds_affected=len(earliest_by_fund),
    )
    return upserted, total_recomputed
