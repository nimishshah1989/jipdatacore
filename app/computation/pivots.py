"""Index pivot point computation — classic floor trader pivots."""

from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger

logger = get_logger(__name__)

MAJOR_INDICES = [
    "NIFTY 50", "NIFTY BANK", "NIFTY IT", "NIFTY METAL",
    "NIFTY PHARMA", "NIFTY ENERGY", "NIFTY REALTY",
    "NIFTY AUTO", "NIFTY FMCG", "NIFTY FIN SERVICE",
    "NIFTY MIDCAP 100", "NIFTY SMLCAP 100",
]


async def compute_index_pivots(session: AsyncSession, business_date: date) -> int:
    """Compute classic pivot points for major indices using previous day OHLC.

    Pivot = (H + L + C) / 3
    S1 = 2*P - H,  R1 = 2*P - L
    S2 = P - (H-L), R2 = P + (H-L)
    S3 = L - 2*(H-P), R3 = H + 2*(P-L)

    Returns number of pivot rows upserted.
    """
    result = await session.execute(text("""
        WITH prev_day AS (
            SELECT index_code, high, low, close
            FROM de_index_prices
            WHERE date = (
                SELECT MAX(date) FROM de_index_prices
                WHERE date < :bdate AND index_code = ANY(:indices)
            )
            AND index_code = ANY(:indices)
            AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL
        ),
        pivots AS (
            SELECT
                index_code,
                ROUND(((high + low + close) / 3), 4) AS pivot,
                ROUND((2 * (high + low + close) / 3 - high), 4) AS s1,
                ROUND(((high + low + close) / 3 - (high - low)), 4) AS s2,
                ROUND((low - 2 * (high - (high + low + close) / 3)), 4) AS s3,
                ROUND((2 * (high + low + close) / 3 - low), 4) AS r1,
                ROUND(((high + low + close) / 3 + (high - low)), 4) AS r2,
                ROUND((high + 2 * ((high + low + close) / 3 - low)), 4) AS r3
            FROM prev_day
        )
        INSERT INTO de_index_pivots (date, index_code, pivot, s1, s2, s3, r1, r2, r3)
        SELECT :bdate, index_code, pivot, s1, s2, s3, r1, r2, r3
        FROM pivots
        ON CONFLICT (date, index_code) DO UPDATE SET
            pivot = EXCLUDED.pivot, s1 = EXCLUDED.s1, s2 = EXCLUDED.s2, s3 = EXCLUDED.s3,
            r1 = EXCLUDED.r1, r2 = EXCLUDED.r2, r3 = EXCLUDED.r3
    """), {"bdate": business_date, "indices": MAJOR_INDICES})

    count = result.rowcount
    logger.info("index_pivots_computed", date=str(business_date), rows=count)
    return count
