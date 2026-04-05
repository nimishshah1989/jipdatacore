"""MF Returns Computation Pipeline."""

from datetime import date
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.logging import get_logger

logger = get_logger(__name__)


async def compute_incremental_returns(business_date: date, session: AsyncSession) -> int:
    """Compute rolling returns for mutual funds incrementally.
    Uses window functions over the trading calendar to find precise N-day offsets.
    """
    # This SQL strictly uses trading days to calculate exact point-to-point returns
    query = text("""
        WITH daily_navs AS (
            SELECT 
                r.mstar_id,
                r.nav_date,
                r.nav_adj,
                LAG(r.nav_adj, 1) OVER w as nav_1d,
                LAG(r.nav_adj, 5) OVER w as nav_1w,
                LAG(r.nav_adj, 21) OVER w as nav_1m,
                LAG(r.nav_adj, 63) OVER w as nav_3m,
                LAG(r.nav_adj, 126) OVER w as nav_6m,
                LAG(r.nav_adj, 252) OVER w as nav_1y,
                LAG(r.nav_adj, 756) OVER w as nav_3y,
                LAG(r.nav_adj, 1260) OVER w as nav_5y,
                LAG(r.nav_adj, 2520) OVER w as nav_10y
            FROM de_mf_nav_daily r
            WINDOW w AS (PARTITION BY r.mstar_id ORDER BY r.nav_date)
        )
        UPDATE de_mf_nav_daily dst
        SET 
            return_1d = (src.nav_adj / NULLIF(src.nav_1d, 0) - 1) * 100,
            return_1w = (src.nav_adj / NULLIF(src.nav_1w, 0) - 1) * 100,
            return_1m = (src.nav_adj / NULLIF(src.nav_1m, 0) - 1) * 100,
            return_3m = (src.nav_adj / NULLIF(src.nav_3m, 0) - 1) * 100,
            return_6m = (src.nav_adj / NULLIF(src.nav_6m, 0) - 1) * 100,
            return_1y = (src.nav_adj / NULLIF(src.nav_1y, 0) - 1) * 100,
            return_3y = (POWER((src.nav_adj / NULLIF(src.nav_3y, 0)), 1.0/3.0) - 1) * 100,
            return_5y = (POWER((src.nav_adj / NULLIF(src.nav_5y, 0)), 1.0/5.0) - 1) * 100,
            return_10y = (POWER((src.nav_adj / NULLIF(src.nav_10y, 0)), 1.0/10.0) - 1) * 100
        FROM daily_navs src
        WHERE dst.nav_date = :b_date
          AND dst.mstar_id = src.mstar_id
          AND src.nav_date = :b_date
    """)
    
    result = await session.execute(query, {"b_date": business_date})
    await session.commit()
    logger.info(f"Computed returns for {result.rowcount} mutual funds for {business_date}.")
    return result.rowcount
