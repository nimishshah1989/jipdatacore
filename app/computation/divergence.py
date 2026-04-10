"""Divergence detection — price vs oscillator divergence signals.

Detects bullish/bearish divergences between price action and RSI/Stochastic.
Triple divergence (strength=3) is Gautam's strongest signal.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from decimal import Decimal
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger

logger = get_logger(__name__)

MIN_SWING_PCT = Decimal("3")  # minimum 3% move for a valid swing


async def detect_divergences(session: AsyncSession, business_date: date) -> int:
    """Detect RSI divergences on daily timeframe for all instruments.

    Simplified approach:
    1. Get last 60 days of close + RSI
    2. Find last 2 swing lows in price and RSI
    3. If price makes lower low but RSI makes higher low → bullish divergence
    4. If price makes higher high but RSI makes lower high → bearish divergence

    Returns number of divergence signals inserted.
    """
    # Get instruments that have stochastic and RSI data
    rows = await session.execute(text("""
        SELECT DISTINCT instrument_id
        FROM de_equity_technical_daily
        WHERE date = :bdate AND stochastic_k IS NOT NULL
    """), {"bdate": business_date})
    instrument_ids = [r[0] for r in rows.fetchall()]

    if not instrument_ids:
        logger.info("divergence_no_instruments", date=str(business_date))
        return 0

    inserted = 0
    start_date = business_date - timedelta(days=90)

    for inst_id in instrument_ids:
        # Get price + RSI series
        series = await session.execute(text("""
            SELECT e.date, e.close, t.stochastic_k
            FROM de_equity_ohlcv e
            JOIN de_equity_technical_daily t
              ON t.date = e.date AND t.instrument_id = e.instrument_id
            WHERE e.instrument_id = :iid
              AND e.date BETWEEN :start AND :end
            ORDER BY e.date
        """), {"iid": str(inst_id), "start": start_date, "end": business_date})

        data = series.fetchall()
        if len(data) < 30:
            continue

        closes = [float(r[1]) for r in data]
        stoch = [float(r[2]) if r[2] else None for r in data]

        # Find swing lows (local minima with minimum depth)
        price_lows = _find_swing_lows(closes, min_depth_pct=float(MIN_SWING_PCT))
        stoch_valid = [s for s in stoch if s is not None]

        if len(price_lows) < 2 or len(stoch_valid) < 30:
            continue

        # Get stochastic values at price swing low indices
        last_two_lows = price_lows[-2:]
        idx1, val1 = last_two_lows[0]
        idx2, val2 = last_two_lows[1]

        stoch1 = stoch[idx1] if idx1 < len(stoch) and stoch[idx1] is not None else None
        stoch2 = stoch[idx2] if idx2 < len(stoch) and stoch[idx2] is not None else None

        if stoch1 is None or stoch2 is None:
            continue

        # Bullish divergence: price lower low, stochastic higher low
        if val2 < val1 and stoch2 > stoch1:
            await _insert_divergence(
                session, business_date, inst_id,
                "daily", "bullish", "stochastic",
                "lower_low", "higher_low", 1,
            )
            inserted += 1

        # Bearish check on highs
        price_highs = _find_swing_highs(closes, min_depth_pct=float(MIN_SWING_PCT))
        if len(price_highs) >= 2:
            h_idx1, h_val1 = price_highs[-2]
            h_idx2, h_val2 = price_highs[-1]
            h_stoch1 = stoch[h_idx1] if h_idx1 < len(stoch) and stoch[h_idx1] is not None else None
            h_stoch2 = stoch[h_idx2] if h_idx2 < len(stoch) and stoch[h_idx2] is not None else None

            if h_stoch1 is not None and h_stoch2 is not None:
                if h_val2 > h_val1 and h_stoch2 < h_stoch1:
                    await _insert_divergence(
                        session, business_date, inst_id,
                        "daily", "bearish", "stochastic",
                        "higher_high", "lower_high", 1,
                    )
                    inserted += 1

    logger.info("divergences_detected", date=str(business_date), signals=inserted)
    return inserted


def _find_swing_lows(
    prices: list[float], min_depth_pct: float = 3.0, lookback: int = 5,
) -> list[tuple[int, float]]:
    """Find local minima in a price series.

    Returns list of (index, price) tuples.
    """
    lows = []
    for i in range(lookback, len(prices) - lookback):
        window = prices[i - lookback : i + lookback + 1]
        if prices[i] == min(window):
            # Verify minimum depth from surrounding peaks
            left_max = max(prices[max(0, i - lookback) : i])
            right_max = max(prices[i + 1 : min(len(prices), i + lookback + 1)])
            peak = max(left_max, right_max)
            depth_pct = (peak - prices[i]) / peak * 100
            if depth_pct >= min_depth_pct:
                lows.append((i, prices[i]))
    return lows


def _find_swing_highs(
    prices: list[float], min_depth_pct: float = 3.0, lookback: int = 5,
) -> list[tuple[int, float]]:
    """Find local maxima in a price series."""
    highs = []
    for i in range(lookback, len(prices) - lookback):
        window = prices[i - lookback : i + lookback + 1]
        if prices[i] == max(window):
            left_min = min(prices[max(0, i - lookback) : i])
            right_min = min(prices[i + 1 : min(len(prices), i + lookback + 1)])
            trough = min(left_min, right_min)
            depth_pct = (prices[i] - trough) / prices[i] * 100
            if depth_pct >= min_depth_pct:
                highs.append((i, prices[i]))
    return highs


async def _insert_divergence(
    session: AsyncSession,
    bdate: date,
    instrument_id,
    timeframe: str,
    div_type: str,
    indicator: str,
    price_dir: str,
    ind_dir: str,
    strength: int,
) -> None:
    """Insert a divergence signal."""
    await session.execute(text("""
        INSERT INTO de_divergence_signals
            (id, date, instrument_id, timeframe, divergence_type, indicator,
             price_direction, indicator_direction, strength)
        VALUES (:id, :dt, :iid, :tf, :dtype, :ind, :pdir, :idir, :str)
    """), {
        "id": str(uuid.uuid4()),
        "dt": bdate,
        "iid": str(instrument_id),
        "tf": timeframe,
        "dtype": div_type,
        "ind": indicator,
        "pdir": price_dir,
        "idir": ind_dir,
        "str": strength,
    })
