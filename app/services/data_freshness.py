"""
Data freshness service.

Checks whether the latest data in a table is fresh (same as the last trading date)
or stale. Returns a DataFreshness enum value and sets X-Data-Freshness header.
"""

from datetime import date
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.middleware.response import DataFreshness

logger = get_logger(__name__)

# Stale threshold: if latest data is older than this many calendar days, mark stale
_STALE_DAYS_EQUITY = 1
_STALE_DAYS_MF = 1
_STALE_DAYS_SLOW = 7  # regime, breadth, macro


async def check_equity_freshness(db: AsyncSession) -> DataFreshness:
    """Check freshness of de_equity_ohlcv validated data."""
    from app.models.prices import DeEquityOhlcv

    result = await db.execute(
        sa.select(sa.func.max(DeEquityOhlcv.date)).where(
            DeEquityOhlcv.data_status == "validated"
        )
    )
    latest: Optional[date] = result.scalar_one_or_none()
    return _freshness_from_date(latest, stale_days=_STALE_DAYS_EQUITY)


async def check_mf_freshness(db: AsyncSession) -> DataFreshness:
    """Check freshness of de_mf_nav_daily validated data."""
    from app.models.prices import DeMfNavDaily

    result = await db.execute(
        sa.select(sa.func.max(DeMfNavDaily.nav_date)).where(
            DeMfNavDaily.data_status == "validated"
        )
    )
    latest: Optional[date] = result.scalar_one_or_none()
    return _freshness_from_date(latest, stale_days=_STALE_DAYS_MF)


async def check_regime_freshness(db: AsyncSession) -> DataFreshness:
    """Check freshness of de_market_regime."""
    from app.models.computed import DeMarketRegime

    result = await db.execute(
        sa.select(sa.func.max(DeMarketRegime.date))
    )
    latest: Optional[date] = result.scalar_one_or_none()
    return _freshness_from_date(latest, stale_days=_STALE_DAYS_SLOW)


async def check_breadth_freshness(db: AsyncSession) -> DataFreshness:
    """Check freshness of de_breadth_daily."""
    from app.models.computed import DeBreadthDaily

    result = await db.execute(
        sa.select(sa.func.max(DeBreadthDaily.date))
    )
    latest: Optional[date] = result.scalar_one_or_none()
    return _freshness_from_date(latest, stale_days=_STALE_DAYS_EQUITY)


async def check_flows_freshness(db: AsyncSession) -> DataFreshness:
    """Check freshness of de_institutional_flows."""
    from app.models.flows import DeInstitutionalFlows

    result = await db.execute(
        sa.select(sa.func.max(DeInstitutionalFlows.date))
    )
    latest: Optional[date] = result.scalar_one_or_none()
    return _freshness_from_date(latest, stale_days=_STALE_DAYS_EQUITY)


def _freshness_from_date(
    latest: Optional[date],
    stale_days: int = 1,
) -> DataFreshness:
    """Determine DataFreshness from latest date vs today."""
    if latest is None:
        return DataFreshness.STALE
    today = date.today()
    delta = (today - latest).days
    # Allow for weekends: stale if more than stale_days + 2 (weekend buffer)
    if delta <= stale_days + 2:
        return DataFreshness.FRESH
    return DataFreshness.STALE
