"""MF return computation — 1d through 10y periods."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.prices import DeMfNavDaily

logger = get_logger(__name__)

# Period definitions: (column_name, trading_days, years_for_cagr)
# years_for_cagr=None → simple % return; set to float → CAGR
RETURN_PERIODS: list[tuple[str, int, Optional[float]]] = [
    ("return_1d", 1, None),
    ("return_1w", 5, None),
    ("return_1m", 21, None),
    ("return_3m", 63, None),
    ("return_6m", 126, None),
    ("return_1y", 252, None),
    ("return_3y", 756, 3.0),
    ("return_5y", 1260, 5.0),
    ("return_10y", 2520, 10.0),
]


@dataclass
class NavPoint:
    """NAV value at a specific date."""

    nav_date: date
    nav: Decimal
    mstar_id: str


def compute_simple_return(nav_end: Decimal, nav_start: Decimal) -> Optional[Decimal]:
    """Compute simple percentage return: ((nav_end / nav_start) - 1) * 100.

    Returns None if nav_start is zero or inputs are invalid.
    All arithmetic via Decimal to avoid float rounding.
    """
    if nav_start <= Decimal("0"):
        return None
    try:
        return ((nav_end / nav_start) - Decimal("1")) * Decimal("100")
    except (InvalidOperation, ZeroDivisionError):
        return None


def compute_cagr(nav_end: Decimal, nav_start: Decimal, years: float) -> Optional[Decimal]:
    """Compute CAGR: ((nav_end / nav_start) ^ (1 / years) - 1) * 100.

    Uses float internally for the exponentiation, then converts back to Decimal
    via str() to prevent float contamination of the result.

    Returns None if nav_start is zero, years is zero, or result is invalid.
    """
    if nav_start <= Decimal("0") or years <= 0:
        return None
    try:
        ratio = float(nav_end) / float(nav_start)
        if ratio <= 0:
            return None
        cagr_float = (ratio ** (1.0 / years) - 1.0) * 100.0
        return Decimal(str(round(cagr_float, 4)))
    except (InvalidOperation, ZeroDivisionError, OverflowError, ValueError):
        return None


async def fetch_nav_series(
    session: AsyncSession,
    mstar_id: str,
    as_of_date: date,
    lookback_days: int,
) -> list[NavPoint]:
    """Fetch ordered NAV series for a fund from de_mf_nav_daily.

    Returns rows from (as_of_date - lookback_days trading days) up to and
    including as_of_date, ordered by nav_date ASC.

    Note: 'lookback_days' is the number of trading rows to look back — we
    fetch by LIMIT rather than calendar date arithmetic to get exactly N rows.
    """
    result = await session.execute(
        select(DeMfNavDaily.nav_date, DeMfNavDaily.nav, DeMfNavDaily.mstar_id)
        .where(
            DeMfNavDaily.mstar_id == mstar_id,
            DeMfNavDaily.nav_date <= as_of_date,
            DeMfNavDaily.data_status != "quarantined",
        )
        .order_by(DeMfNavDaily.nav_date.desc())
        .limit(lookback_days + 1)
    )
    rows = result.all()
    # Reverse to get ASC order
    return [NavPoint(nav_date=r.nav_date, nav=r.nav, mstar_id=r.mstar_id) for r in reversed(rows)]


async def compute_returns_for_date(
    session: AsyncSession,
    business_date: date,
    mstar_ids: Optional[list[str]] = None,
) -> tuple[int, int]:
    """Compute and upsert return columns for all funds on business_date.

    For each fund with a NAV on business_date, computes all return periods
    using available historical NAV data and updates de_mf_nav_daily in place.

    If mstar_ids is provided, only computes for those funds (incremental run).
    Returns (rows_updated, rows_failed).
    """
    # Get list of mstar_ids to process
    query = select(DeMfNavDaily.mstar_id).where(
        DeMfNavDaily.nav_date == business_date,
        DeMfNavDaily.data_status != "quarantined",
    )
    if mstar_ids:
        query = query.where(DeMfNavDaily.mstar_id.in_(mstar_ids))

    result = await session.execute(query)
    target_funds = [row.mstar_id for row in result]

    if not target_funds:
        logger.warning("returns_no_funds_for_date", business_date=business_date.isoformat())
        return 0, 0

    # Fetch today's NAVs in bulk
    today_result = await session.execute(
        select(DeMfNavDaily.mstar_id, DeMfNavDaily.nav).where(
            DeMfNavDaily.nav_date == business_date,
            DeMfNavDaily.mstar_id.in_(target_funds),
        )
    )
    today_navs: dict[str, Decimal] = {row.mstar_id: row.nav for row in today_result}

    # Max lookback needed
    max_days = max(days for _, days, _ in RETURN_PERIODS)

    # Fetch historical NAVs in bulk (one query per fund would be N+1 — use bulk)
    hist_result = await session.execute(
        select(DeMfNavDaily.mstar_id, DeMfNavDaily.nav_date, DeMfNavDaily.nav)
        .where(
            DeMfNavDaily.mstar_id.in_(target_funds),
            DeMfNavDaily.nav_date <= business_date,
            DeMfNavDaily.data_status != "quarantined",
        )
        .order_by(DeMfNavDaily.mstar_id, DeMfNavDaily.nav_date.desc())
    )
    # Build dict: mstar_id → list[NavPoint] descending by date
    hist_by_fund: dict[str, list[NavPoint]] = {}
    for row in hist_result:
        pts = hist_by_fund.setdefault(row.mstar_id, [])
        if len(pts) <= max_days:
            pts.append(NavPoint(nav_date=row.nav_date, nav=row.nav, mstar_id=row.mstar_id))

    rows_updated = 0
    rows_failed = 0

    for mstar_id in target_funds:
        nav_end = today_navs.get(mstar_id)
        if nav_end is None:
            rows_failed += 1
            continue

        # Series is ordered desc: index 0 = today, index N = N trading days ago
        series = hist_by_fund.get(mstar_id, [])

        update_vals: dict[str, Optional[Decimal]] = {}

        for col_name, lookback, years in RETURN_PERIODS:
            # We need the NAV exactly `lookback` rows back
            if len(series) > lookback:
                nav_start = series[lookback].nav
                if years is None:
                    ret = compute_simple_return(nav_end, nav_start)
                else:
                    ret = compute_cagr(nav_end, nav_start, years)
                update_vals[col_name] = ret
            else:
                update_vals[col_name] = None

        try:
            await session.execute(
                sa.update(DeMfNavDaily)
                .where(
                    DeMfNavDaily.nav_date == business_date,
                    DeMfNavDaily.mstar_id == mstar_id,
                )
                .values(**update_vals)
            )
            rows_updated += 1
        except Exception as exc:
            logger.error(
                "returns_update_failed",
                mstar_id=mstar_id,
                business_date=business_date.isoformat(),
                error=str(exc),
            )
            rows_failed += 1

    logger.info(
        "returns_compute_complete",
        business_date=business_date.isoformat(),
        rows_updated=rows_updated,
        rows_failed=rows_failed,
    )
    return rows_updated, rows_failed
