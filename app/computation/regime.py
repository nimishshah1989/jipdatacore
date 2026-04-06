"""Market regime classification — BULL / BEAR / SIDEWAYS / RECOVERY.

Component scores (0-100): breadth_score, momentum_score, volume_score,
global_score, fii_score.
If equity data stale: confidence *= 0.5
"""

from __future__ import annotations

import datetime as dt
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.computed import DeMarketRegime

logger = get_logger(__name__)

COMPUTATION_VERSION = 1

# Regime classification thresholds
BULL_THRESHOLD = 60.0
BEAR_THRESHOLD = 40.0
RECOVERY_THRESHOLD = 50.0

# Component weights for confidence score
CONFIDENCE_WEIGHTS = {
    "breadth_score": 0.30,
    "momentum_score": 0.25,
    "volume_score": 0.15,
    "global_score": 0.15,
    "fii_score": 0.15,
}

# Data staleness threshold in trading days
STALE_DATA_DAYS = 2


def classify_regime(
    breadth_score: float,
    momentum_score: float,
    volume_score: float,
    global_score: float,
    fii_score: float,
) -> tuple[str, float]:
    """Classify market regime and compute confidence score.

    Classification logic:
        - confidence = weighted composite of all component scores
        - BULL: confidence >= 60 and breadth_score >= 60
        - BEAR: confidence <= 40 or breadth_score <= 35
        - RECOVERY: 40 < confidence < 60 and momentum_score > breadth_score
        - SIDEWAYS: 40 < confidence <= 60 (default)

    Args:
        breadth_score: Breadth component score (0-100).
        momentum_score: Momentum component score (0-100).
        volume_score: Volume component score (0-100).
        global_score: Global markets component score (0-100).
        fii_score: FII/DII flow component score (0-100).

    Returns:
        Tuple of (regime_string, confidence_score).
    """
    scores = {
        "breadth_score": breadth_score,
        "momentum_score": momentum_score,
        "volume_score": volume_score,
        "global_score": global_score,
        "fii_score": fii_score,
    }

    # Weighted confidence
    confidence = sum(scores[k] * w for k, w in CONFIDENCE_WEIGHTS.items())

    # Classification
    if confidence >= BULL_THRESHOLD and breadth_score >= BULL_THRESHOLD:
        regime = "BULL"
    elif confidence <= BEAR_THRESHOLD or breadth_score <= 35.0:
        regime = "BEAR"
    elif confidence > BEAR_THRESHOLD and momentum_score > breadth_score:
        regime = "RECOVERY"
    else:
        regime = "SIDEWAYS"

    return regime, confidence


async def _fetch_breadth_score(session: AsyncSession, business_date: date) -> Optional[float]:
    """Derive breadth score (0-100) from de_breadth_daily."""
    result = await session.execute(
        sa.text("""
            SELECT
                pct_above_200dma,
                pct_above_50dma,
                ad_ratio
            FROM de_breadth_daily
            WHERE date <= :bdate
            ORDER BY date DESC
            LIMIT 1
        """),
        {"bdate": business_date},
    )
    row = result.fetchone()
    if row is None:
        return None

    # Composite breadth score: average of available components
    components = []
    if row.pct_above_200dma is not None:
        components.append(float(row.pct_above_200dma))
    if row.pct_above_50dma is not None:
        components.append(float(row.pct_above_50dma))
    if row.ad_ratio is not None:
        # Normalise A/D ratio: >1 is bullish, scale to 0-100
        # A/D of 2.0 = ~75, A/D of 0.5 = ~25
        ad = float(row.ad_ratio)
        ad_score = min(100.0, max(0.0, 50.0 + (ad - 1.0) * 25.0))
        components.append(ad_score)

    return sum(components) / len(components) if components else None


async def _fetch_momentum_score(session: AsyncSession, business_date: date) -> Optional[float]:
    """Derive momentum score (0-100) from RS composite scores."""
    result = await session.execute(
        sa.text("""
            SELECT AVG(CAST(rs_composite AS FLOAT)) AS avg_rs
            FROM de_rs_scores
            WHERE date = :bdate
              AND vs_benchmark = 'NIFTY 50'
              AND entity_type = 'equity'
        """),
        {"bdate": business_date},
    )
    row = result.fetchone()
    if row is None or row.avg_rs is None:
        return None

    # RS composite is z-score like; normalise to 0-100
    # Typical range -3 to +3; map to 0-100
    avg_rs = float(row.avg_rs)
    momentum = min(100.0, max(0.0, 50.0 + avg_rs * 10.0))
    return momentum


async def _fetch_volume_score(session: AsyncSession, business_date: date) -> Optional[float]:
    """Derive volume score (0-100) from recent vs historical volumes."""
    # Compute date boundaries in Python to avoid asyncpg interval arithmetic issues
    # Use extra buffer days to account for weekends/holidays
    start_5d = business_date - dt.timedelta(days=10)
    start_20d = business_date - dt.timedelta(days=30)

    result = await session.execute(
        sa.text("""
            WITH recent AS (
                SELECT AVG(CAST(volume AS FLOAT)) AS avg_vol_5d
                FROM de_equity_price_daily
                WHERE date >= :start_5d AND date <= :bdate
                  AND data_status = 'validated'
            ),
            historical AS (
                SELECT AVG(CAST(volume AS FLOAT)) AS avg_vol_20d
                FROM de_equity_price_daily
                WHERE date >= :start_20d AND date < :start_5d
                  AND data_status = 'validated'
            )
            SELECT
                r.avg_vol_5d,
                h.avg_vol_20d
            FROM recent r, historical h
        """),
        {"bdate": business_date, "start_5d": start_5d, "start_20d": start_20d},
    )
    row = result.fetchone()
    if row is None or row.avg_vol_20d is None or row.avg_vol_20d == 0:
        return None

    vol_ratio = float(row.avg_vol_5d or 0) / float(row.avg_vol_20d)
    # Map ratio to 0-100: ratio of 1.5 = ~75, ratio of 0.5 = ~25
    score = min(100.0, max(0.0, 50.0 + (vol_ratio - 1.0) * 50.0))
    return score


async def _fetch_global_score(session: AsyncSession, business_date: date) -> Optional[float]:
    """Derive global markets score from global price data."""
    # Compute date boundary in Python to avoid asyncpg interval arithmetic issues
    start_5d = business_date - dt.timedelta(days=10)

    result = await session.execute(
        sa.text("""
            SELECT
                ticker,
                CAST(close AS FLOAT) AS close,
                CAST(LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS FLOAT) AS prev_close
            FROM de_global_prices
            WHERE date >= :start_5d AND date <= :bdate
            ORDER BY ticker, date DESC
        """),
        {"bdate": business_date, "start_5d": start_5d},
    )
    rows = result.fetchall()

    if not rows:
        return None

    returns = []
    for row in rows:
        if row.close and row.prev_close and row.prev_close != 0:
            ret = (float(row.close) - float(row.prev_close)) / float(row.prev_close)
            returns.append(ret)

    if not returns:
        return None

    avg_return = sum(returns) / len(returns)
    # Map avg daily return to 0-100: 0.5% daily gain = ~75, -0.5% = ~25
    score = min(100.0, max(0.0, 50.0 + avg_return * 5000.0))
    return score


async def _fetch_fii_score(session: AsyncSession, business_date: date) -> Optional[float]:
    """Derive FII/DII flow score from flows data."""
    # Compute date boundary in Python to avoid asyncpg interval arithmetic issues
    start_5d = business_date - dt.timedelta(days=10)

    result = await session.execute(
        sa.text("""
            SELECT
                SUM(CASE WHEN category = 'FII' THEN CAST(net_flow AS FLOAT) ELSE 0 END) AS fii_net,
                SUM(CASE WHEN category = 'DII' THEN CAST(net_flow AS FLOAT) ELSE 0 END) AS dii_net
            FROM de_institutional_flows
            WHERE date >= :start_5d AND date <= :bdate
              AND market_type = 'equity'
        """),
        {"bdate": business_date, "start_5d": start_5d},
    )
    row = result.fetchone()
    if row is None:
        return None

    fii_net = float(row.fii_net or 0)
    dii_net = float(row.dii_net or 0)
    combined = fii_net + dii_net

    # Map net flow to 0-100: positive is bullish
    # Scale: ±5000 crore over 5 days maps to ±25 points
    score = min(100.0, max(0.0, 50.0 + combined / 5000.0 * 25.0))
    return score


async def _check_data_staleness(session: AsyncSession, business_date: date) -> bool:
    """Check if equity price data is stale (no data in last STALE_DATA_DAYS trading days)."""
    result = await session.execute(
        sa.text("""
            SELECT MAX(date) AS last_date
            FROM de_equity_price_daily
            WHERE data_status = 'validated'
        """)
    )
    row = result.fetchone()
    if row is None or row.last_date is None:
        return True

    days_gap = (business_date - row.last_date).days
    return days_gap > STALE_DATA_DAYS


async def compute_market_regime(
    session: AsyncSession,
    business_date: date,
) -> Optional[str]:
    """Compute and persist market regime for business_date.

    Fetches component scores from derived data tables, classifies regime,
    applies staleness penalty, and writes to de_market_regime.

    Args:
        session: Async DB session.
        business_date: Date for which to compute regime.

    Returns:
        Regime string ('BULL'|'BEAR'|'SIDEWAYS'|'RECOVERY'), or None on failure.
    """
    logger.info(
        "regime_compute_start",
        business_date=business_date.isoformat(),
    )

    # Fetch component scores
    breadth_score = await _fetch_breadth_score(session, business_date)
    momentum_score = await _fetch_momentum_score(session, business_date)
    volume_score = await _fetch_volume_score(session, business_date)
    global_score = await _fetch_global_score(session, business_date)
    fii_score = await _fetch_fii_score(session, business_date)

    # Default missing scores to 50 (neutral) for classification
    b_score = breadth_score if breadth_score is not None else 50.0
    m_score = momentum_score if momentum_score is not None else 50.0
    v_score = volume_score if volume_score is not None else 50.0
    g_score = global_score if global_score is not None else 50.0
    f_score = fii_score if fii_score is not None else 50.0

    regime, confidence = classify_regime(b_score, m_score, v_score, g_score, f_score)

    # Apply staleness penalty
    is_stale = await _check_data_staleness(session, business_date)
    if is_stale:
        confidence *= 0.5
        logger.warning(
            "regime_data_stale_penalty_applied",
            business_date=business_date.isoformat(),
        )

    # Build indicator detail JSONB
    indicator_detail: dict[str, Any] = {
        "breadth_score": round(b_score, 2),
        "momentum_score": round(m_score, 2),
        "volume_score": round(v_score, 2),
        "global_score": round(g_score, 2),
        "fii_score": round(f_score, 2),
        "confidence_raw": round(confidence, 2),
        "data_stale": is_stale,
        "missing_components": {
            "breadth": breadth_score is None,
            "momentum": momentum_score is None,
            "volume": volume_score is None,
            "global": global_score is None,
            "fii": fii_score is None,
        },
    }

    computed_at = datetime.now(tz=timezone.utc)

    row_data = {
        "computed_at": computed_at,
        "date": business_date,
        "regime": regime,
        "confidence": Decimal(str(round(confidence, 2))),
        "breadth_score": Decimal(str(round(b_score, 2))),
        "momentum_score": Decimal(str(round(m_score, 2))),
        "volume_score": Decimal(str(round(v_score, 2))),
        "global_score": Decimal(str(round(g_score, 2))),
        "fii_score": Decimal(str(round(f_score, 2))),
        "indicator_detail": indicator_detail,
        "computation_version": COMPUTATION_VERSION,
    }

    stmt = pg_insert(DeMarketRegime).values([row_data])
    stmt = stmt.on_conflict_do_update(
        index_elements=["computed_at"],
        set_={
            "regime": stmt.excluded.regime,
            "confidence": stmt.excluded.confidence,
            "breadth_score": stmt.excluded.breadth_score,
            "momentum_score": stmt.excluded.momentum_score,
            "volume_score": stmt.excluded.volume_score,
            "global_score": stmt.excluded.global_score,
            "fii_score": stmt.excluded.fii_score,
            "indicator_detail": stmt.excluded.indicator_detail,
            "computation_version": stmt.excluded.computation_version,
        },
    )
    await session.execute(stmt)
    await session.flush()

    logger.info(
        "regime_compute_complete",
        business_date=business_date.isoformat(),
        regime=regime,
        confidence=round(confidence, 2),
    )

    return regime
