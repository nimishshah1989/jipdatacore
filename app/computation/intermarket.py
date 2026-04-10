"""Intermarket ratio computation — cross-asset relative analysis."""

from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger

logger = get_logger(__name__)

RATIOS = [
    ("BANKNIFTY_NIFTY", "NIFTY BANK", "NIFTY 50"),
    ("MICROCAP_NIFTY", "NIFTY MICROCAP 250", "NIFTY 50"),
    ("SMALLCAP_NIFTY", "NIFTY SMLCAP 100", "NIFTY 50"),
    ("IT_NIFTY", "NIFTY IT", "NIFTY 50"),
    ("METAL_NIFTY", "NIFTY METAL", "NIFTY 50"),
]


async def compute_intermarket_ratios(session: AsyncSession, business_date: date) -> int:
    """Compute intermarket ratios with SMA(20) and direction.

    For each ratio: value = numerator_close / denominator_close
    SMA(20) of the ratio, direction = rising if value > sma_20 else falling.

    Returns number of ratios upserted.
    """
    count = 0

    for ratio_name, numerator_idx, denominator_idx in RATIOS:
        result = await session.execute(text("""
            WITH ratio_series AS (
                SELECT
                    n.date,
                    CASE WHEN d.close > 0 THEN n.close / d.close ELSE NULL END AS ratio_val
                FROM de_index_prices n
                JOIN de_index_prices d ON d.date = n.date AND d.index_code = :denom
                WHERE n.index_code = :numer
                  AND n.date BETWEEN :start AND :bdate
                ORDER BY n.date
            ),
            with_sma AS (
                SELECT
                    date,
                    ratio_val,
                    AVG(ratio_val) OVER (
                        ORDER BY date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS sma_20
                FROM ratio_series
            )
            INSERT INTO de_intermarket_ratios (date, ratio_name, value, sma_20, direction)
            SELECT
                date,
                :rname,
                ROUND(ratio_val::numeric, 6),
                ROUND(sma_20::numeric, 6),
                CASE
                    WHEN ratio_val > sma_20 THEN 'rising'
                    WHEN ratio_val < sma_20 THEN 'falling'
                    ELSE 'flat'
                END
            FROM with_sma
            WHERE date = :bdate AND ratio_val IS NOT NULL
            ON CONFLICT (date, ratio_name) DO UPDATE SET
                value = EXCLUDED.value,
                sma_20 = EXCLUDED.sma_20,
                direction = EXCLUDED.direction
        """), {
            "rname": ratio_name,
            "numer": numerator_idx,
            "denom": denominator_idx,
            "bdate": business_date,
            "start": business_date - __import__("datetime").timedelta(days=40),
        })

        if result.rowcount > 0:
            count += 1

    logger.info("intermarket_ratios_computed", date=str(business_date), ratios=count)
    return count
