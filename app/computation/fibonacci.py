"""Fibonacci retracement level computation from auto-detected swings."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger

logger = get_logger(__name__)

FIB_LEVELS = [
    ("fib_236", Decimal("0.236")),
    ("fib_382", Decimal("0.382")),
    ("fib_500", Decimal("0.500")),
    ("fib_618", Decimal("0.618")),
    ("fib_786", Decimal("0.786")),
]

MIN_SWING_PCT = Decimal("5")  # minimum 5% swing for equities


async def compute_fib_levels(session: AsyncSession, business_date: date) -> int:
    """Compute Fibonacci retracement levels from recent swings.

    For each instrument:
    1. Get last 120 days of close prices
    2. Find the most recent significant high and low
    3. Compute fib retracement levels between them
    4. Upsert into de_fib_levels

    Returns number of instruments with fib levels computed.
    """
    start_date = business_date - timedelta(days=180)

    # Get all instruments with enough history
    rows = await session.execute(text("""
        SELECT DISTINCT instrument_id
        FROM de_equity_ohlcv
        WHERE date BETWEEN :start AND :end
        GROUP BY instrument_id
        HAVING COUNT(*) >= 60
    """), {"start": start_date, "end": business_date})
    instrument_ids = [r[0] for r in rows.fetchall()]

    if not instrument_ids:
        return 0

    computed = 0

    for inst_id in instrument_ids:
        # Get price series
        series = await session.execute(text("""
            SELECT date, high, low, close
            FROM de_equity_ohlcv
            WHERE instrument_id = :iid AND date BETWEEN :start AND :end
            ORDER BY date
        """), {"iid": str(inst_id), "start": start_date, "end": business_date})

        data = series.fetchall()
        if len(data) < 30:
            continue

        highs = [float(r[1]) for r in data]
        lows = [float(r[2]) for r in data]

        # Find highest high and lowest low in recent data
        max_high = max(highs)
        min_low = min(lows)
        max_idx = highs.index(max_high)
        min_idx = lows.index(min_low)

        # Determine swing direction: if high came first, we retrace down
        swing_range = max_high - min_low
        if swing_range <= 0 or (swing_range / max_high * 100) < float(MIN_SWING_PCT):
            continue

        swing_high = Decimal(str(max_high))
        swing_low = Decimal(str(min_low))
        diff = swing_high - swing_low

        # Compute fib levels (retracement from the swing)
        fib_values = {}
        for col_name, level in FIB_LEVELS:
            if max_idx < min_idx:
                # Downswing: retrace up from the low
                fib_values[col_name] = swing_low + diff * level
            else:
                # Upswing: retrace down from the high
                fib_values[col_name] = swing_high - diff * level

        await session.execute(text("""
            INSERT INTO de_fib_levels
                (date, instrument_id, swing_high, swing_low,
                 fib_236, fib_382, fib_500, fib_618, fib_786)
            VALUES (:dt, :iid, :sh, :sl, :f236, :f382, :f500, :f618, :f786)
            ON CONFLICT (date, instrument_id) DO UPDATE SET
                swing_high = EXCLUDED.swing_high,
                swing_low = EXCLUDED.swing_low,
                fib_236 = EXCLUDED.fib_236,
                fib_382 = EXCLUDED.fib_382,
                fib_500 = EXCLUDED.fib_500,
                fib_618 = EXCLUDED.fib_618,
                fib_786 = EXCLUDED.fib_786
        """), {
            "dt": business_date,
            "iid": str(inst_id),
            "sh": str(swing_high),
            "sl": str(swing_low),
            "f236": str(fib_values["fib_236"]),
            "f382": str(fib_values["fib_382"]),
            "f500": str(fib_values["fib_500"]),
            "f618": str(fib_values["fib_618"]),
            "f786": str(fib_values["fib_786"]),
        })
        computed += 1

    logger.info("fib_levels_computed", date=str(business_date), instruments=computed)
    return computed
