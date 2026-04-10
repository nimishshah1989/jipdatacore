"""Stochastic Oscillator, Disparity Index, Bollinger Width computations."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger

logger = get_logger(__name__)

_D = Decimal


async def compute_stochastic(session: AsyncSession, business_date: date) -> int:
    """Compute Stochastic %K and %D for all instruments on business_date.

    Stochastic(14,3,3):
      raw_k = (close - low_14) / (high_14 - low_14) * 100
      %K = SMA(raw_k, 3)
      %D = SMA(%K, 3)

    Returns number of rows updated.
    """
    # Use SQL window functions for efficient computation
    result = await session.execute(text("""
        WITH ohlcv_window AS (
            SELECT
                e.instrument_id,
                e.date,
                e.close,
                MIN(e.low) OVER (
                    PARTITION BY e.instrument_id
                    ORDER BY e.date
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) AS low_14,
                MAX(e.high) OVER (
                    PARTITION BY e.instrument_id
                    ORDER BY e.date
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) AS high_14,
                COUNT(*) OVER (
                    PARTITION BY e.instrument_id
                    ORDER BY e.date
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) AS window_size
            FROM de_equity_ohlcv e
            WHERE e.date BETWEEN :start_date AND :end_date
        ),
        raw_k AS (
            SELECT
                instrument_id, date,
                CASE WHEN high_14 > low_14 AND window_size >= 14
                    THEN (close - low_14) / (high_14 - low_14) * 100
                    ELSE NULL
                END AS k_raw
            FROM ohlcv_window
        ),
        smoothed_k AS (
            SELECT
                instrument_id, date,
                AVG(k_raw) OVER (
                    PARTITION BY instrument_id
                    ORDER BY date
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) AS stoch_k
            FROM raw_k
        ),
        smoothed_d AS (
            SELECT
                instrument_id, date,
                stoch_k,
                AVG(stoch_k) OVER (
                    PARTITION BY instrument_id
                    ORDER BY date
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) AS stoch_d
            FROM smoothed_k
        )
        UPDATE de_equity_technical_daily t
        SET stochastic_k = ROUND(s.stoch_k::numeric, 4),
            stochastic_d = ROUND(s.stoch_d::numeric, 4)
        FROM smoothed_d s
        WHERE t.date = :bdate
          AND t.instrument_id = s.instrument_id
          AND s.date = :bdate
          AND s.stoch_k IS NOT NULL
    """), {
        "bdate": business_date,
        "start_date": business_date - __import__("datetime").timedelta(days=40),
        "end_date": business_date,
    })

    count = result.rowcount
    logger.info("stochastic_computed", date=str(business_date), rows=count)
    return count


async def compute_disparity(session: AsyncSession, business_date: date) -> int:
    """Compute Disparity Index for all instruments on business_date.

    disparity_20 = ((close - EMA20) / EMA20) * 100
    disparity_50 = ((close - SMA50) / SMA50) * 100

    Uses ema_20 and sma_50 already computed in de_equity_technical_daily.
    Returns number of rows updated.
    """
    result = await session.execute(text("""
        UPDATE de_equity_technical_daily
        SET disparity_20 = CASE
                WHEN ema_20 IS NOT NULL AND ema_20 > 0
                THEN ROUND(((close_adj - ema_20) / ema_20 * 100)::numeric, 4)
                ELSE NULL
            END,
            disparity_50 = CASE
                WHEN sma_50 IS NOT NULL AND sma_50 > 0
                THEN ROUND(((close_adj - sma_50) / sma_50 * 100)::numeric, 4)
                ELSE NULL
            END
        WHERE date = :bdate
          AND close_adj IS NOT NULL
    """), {"bdate": business_date})

    count = result.rowcount
    logger.info("disparity_computed", date=str(business_date), rows=count)
    return count


async def compute_bollinger_width(session: AsyncSession, business_date: date) -> int:
    """Compute Bollinger Band width for squeeze detection.

    BW = ((upper - lower) / middle) * 100
    middle = SMA(20), upper = middle + 2*stddev, lower = middle - 2*stddev
    Simplified: BW = 4 * stddev(20) / SMA(20) * 100

    Returns number of rows updated.
    """
    result = await session.execute(text("""
        WITH stats AS (
            SELECT
                e.instrument_id,
                AVG(e.close) AS sma_20,
                STDDEV_POP(e.close) AS std_20,
                COUNT(*) AS cnt
            FROM de_equity_ohlcv e
            WHERE e.date BETWEEN :start_date AND :bdate
            GROUP BY e.instrument_id
            HAVING COUNT(*) >= 15
        )
        UPDATE de_equity_technical_daily t
        SET bollinger_width = CASE
                WHEN s.sma_20 > 0
                THEN ROUND((4 * s.std_20 / s.sma_20 * 100)::numeric, 4)
                ELSE NULL
            END
        FROM stats s
        WHERE t.date = :bdate
          AND t.instrument_id = s.instrument_id
    """), {
        "bdate": business_date,
        "start_date": business_date - __import__("datetime").timedelta(days=35),
    })

    count = result.rowcount
    logger.info("bollinger_width_computed", date=str(business_date), rows=count)
    return count
