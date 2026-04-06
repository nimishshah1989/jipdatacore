"""RS (Relative Strength) score computation — Section 5.8 formula.

Lookbacks: 1w=5, 1m=21, 3m=63, 6m=126, 12m=252 trading days.
Composite: rs_1w*0.10 + rs_1m*0.20 + rs_3m*0.30 + rs_6m*0.25 + rs_12m*0.15
Benchmarks: NIFTY 50, NIFTY 500, NIFTY MIDCAP 100
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional

import numpy as np
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.computed import DeRsScores

logger = get_logger(__name__)

# RS composite weights
RS_WEIGHTS: dict[str, float] = {
    "rs_1w": 0.10,
    "rs_1m": 0.20,
    "rs_3m": 0.30,
    "rs_6m": 0.25,
    "rs_12m": 0.15,
}

# Lookback periods in trading days
LOOKBACKS: dict[str, int] = {
    "rs_1w": 5,
    "rs_1m": 21,
    "rs_3m": 63,
    "rs_6m": 126,
    "rs_12m": 252,
}

COMPUTATION_VERSION = 1

# Quarantine threshold: if >5% of universe is quarantined, skip computation
QUARANTINE_THRESHOLD = 0.05

# Supported benchmarks
BENCHMARKS = ["NIFTY 50", "NIFTY 500", "NIFTY MIDCAP 100"]


def _cumreturn(prices: list[float], lookback: int) -> Optional[float]:
    """Compute cumulative return over last `lookback` trading days.

    Formula: cumreturn = (close_adj_today / close_adj_N_days_ago) - 1

    Args:
        prices: Adjusted close prices (chronological, oldest first).
        lookback: Number of trading days.

    Returns:
        Cumulative return as float, or None if insufficient data.
    """
    if len(prices) < lookback + 1:
        return None
    base = prices[-(lookback + 1)]
    if base == 0.0:
        return None
    return prices[-1] / base - 1.0


def _rolling_std(prices: list[float], lookback: int) -> Optional[float]:
    """Compute rolling standard deviation of returns over `lookback` days.

    Args:
        prices: Adjusted close prices (chronological, oldest first).
        lookback: Number of trading days.

    Returns:
        Rolling std of daily returns, or None if insufficient data.
    """
    if len(prices) < lookback + 1:
        return None
    window = prices[-(lookback + 1) :]
    returns = [window[i] / window[i - 1] - 1.0 for i in range(1, len(window))]
    if not returns:
        return None
    arr = np.array(returns, dtype=float)
    std = float(np.std(arr, ddof=1))
    return std if std > 0 else None


def compute_rs_score(
    entity_prices: list[float],
    benchmark_prices: list[float],
    lookback: int,
) -> Optional[float]:
    """Compute RS score for a single lookback period.

    Formula: rs_Nt = (entity_cumreturn_N - benchmark_cumreturn_N) / benchmark_rolling_std_N

    Args:
        entity_prices: Adjusted close prices for the entity.
        benchmark_prices: Adjusted close prices for the benchmark.
        lookback: Lookback in trading days.

    Returns:
        RS score as float, or None if insufficient data.
    """
    entity_cum = _cumreturn(entity_prices, lookback)
    bench_cum = _cumreturn(benchmark_prices, lookback)
    bench_std = _rolling_std(benchmark_prices, lookback)

    if entity_cum is None or bench_cum is None or bench_std is None:
        return None

    return (entity_cum - bench_cum) / bench_std


def compute_rs_composite(rs_scores: dict[str, Optional[float]]) -> Optional[float]:
    """Compute weighted composite RS score.

    Formula: rs_composite = rs_1w*0.10 + rs_1m*0.20 + rs_3m*0.30 + rs_6m*0.25 + rs_12m*0.15

    Args:
        rs_scores: Dict with keys rs_1w, rs_1m, rs_3m, rs_6m, rs_12m.

    Returns:
        Composite score as float, or None if any component is missing.
    """
    total_weight = 0.0
    weighted_sum = 0.0

    for key, weight in RS_WEIGHTS.items():
        val = rs_scores.get(key)
        if val is None:
            # If any lookback is missing, still compute with available ones
            # but track total weight for normalisation
            continue
        weighted_sum += val * weight
        total_weight += weight

    if total_weight == 0.0:
        return None

    # Normalise by available weight (weighted average of available components)
    return weighted_sum / total_weight


async def compute_rs_scores(
    session: AsyncSession,
    business_date: date,
    entity_type: str = "equity",
) -> int:
    """Compute and persist RS scores for all active entities vs all benchmarks.

    Reads validated close_adj prices from de_equity_price_daily (or relevant table).
    Skips computation if >5% of universe is quarantined.
    Writes to de_rs_scores ON CONFLICT DO UPDATE.

    Args:
        session: Async DB session.
        business_date: Date for which to compute scores.
        entity_type: Entity type string (default "equity").

    Returns:
        Number of rows upserted.
    """
    logger.info(
        "rs_scores_compute_start",
        business_date=business_date.isoformat(),
        entity_type=entity_type,
    )

    # Fetch validated status counts to check quarantine threshold
    status_query = sa.text("""
        SELECT data_status, COUNT(*) as cnt
        FROM de_equity_price_daily
        WHERE date = :bdate
        GROUP BY data_status
    """)

    status_result = await session.execute(status_query, {"bdate": business_date})
    status_rows = status_result.fetchall()

    total_count = sum(r.cnt for r in status_rows)
    quarantine_count = sum(r.cnt for r in status_rows if r.data_status == "quarantined")

    if total_count > 0 and (quarantine_count / total_count) > QUARANTINE_THRESHOLD:
        logger.warning(
            "rs_scores_skipped_quarantine_threshold",
            business_date=business_date.isoformat(),
            quarantine_pct=round(quarantine_count / total_count * 100, 2),
        )
        return 0

    # Fetch price history — limit to 300 trading days (~15 months) for rs_12m=252
    import datetime as dt
    # ~300 trading days with buffer for rs_12m=252
    rs_start_date = business_date - dt.timedelta(days=400)

    price_history_query = sa.text("""
        SELECT
            ep.instrument_id::text AS entity_id,
            ep.date,
            CAST(COALESCE(ep.close_adj, ep.close) AS FLOAT) AS close_adj,
            i.symbol
        FROM de_equity_price_daily ep
        JOIN de_instrument i ON i.id = ep.instrument_id
        WHERE ep.data_status = 'validated'
          AND ep.date <= :bdate
          AND ep.date >= :start_date
          AND COALESCE(ep.close_adj, ep.close) IS NOT NULL
        ORDER BY ep.instrument_id, ep.date
    """)

    rows = (await session.execute(price_history_query, {"bdate": business_date, "start_date": rs_start_date})).fetchall()

    if not rows:
        logger.warning("rs_scores_no_price_data", business_date=business_date.isoformat())
        return 0

    # Build price series per entity
    entity_prices: dict[str, dict[str, list]] = {}  # entity_id -> {dates, prices}
    for row in rows:
        eid = row.entity_id
        if eid not in entity_prices:
            entity_prices[eid] = {"dates": [], "prices": [], "symbol": row.symbol}
        entity_prices[eid]["dates"].append(row.date)
        entity_prices[eid]["prices"].append(float(row.close_adj))

    # Fetch benchmark price series by symbol
    benchmark_query = sa.text("""
        SELECT
            ip.index_code AS symbol,
            ip.date,
            CAST(ip.close AS FLOAT) AS close_adj
        FROM de_index_prices ip
        WHERE ip.index_code = ANY(:symbols)
          AND ip.date <= :bdate
          AND ip.close IS NOT NULL
        ORDER BY ip.index_code, ip.date
    """)

    bench_rows = (
        await session.execute(
            benchmark_query,
            {"symbols": list(BENCHMARKS), "bdate": business_date},
        )
    ).fetchall()

    benchmark_prices: dict[str, list[float]] = {}
    for row in bench_rows:
        sym = row.symbol
        if sym not in benchmark_prices:
            benchmark_prices[sym] = []
        benchmark_prices[sym].append(float(row.close_adj))

    if not benchmark_prices:
        logger.warning(
            "rs_scores_no_benchmark_data",
            business_date=business_date.isoformat(),
        )
        return 0

    # Compute RS scores for all entities vs all benchmarks
    upsert_rows: list[dict] = []

    for entity_id, data in entity_prices.items():
        ep = data["prices"]

        for benchmark in BENCHMARKS:
            bp = benchmark_prices.get(benchmark)
            if bp is None:
                continue

            scores: dict[str, Optional[float]] = {}
            for period_name, lookback in LOOKBACKS.items():
                score = compute_rs_score(ep, bp, lookback)
                scores[period_name] = score

            composite = compute_rs_composite(scores)

            upsert_rows.append(
                {
                    "date": business_date,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "vs_benchmark": benchmark,
                    "rs_1w": Decimal(str(round(scores["rs_1w"], 4)))
                    if scores["rs_1w"] is not None
                    else None,
                    "rs_1m": Decimal(str(round(scores["rs_1m"], 4)))
                    if scores["rs_1m"] is not None
                    else None,
                    "rs_3m": Decimal(str(round(scores["rs_3m"], 4)))
                    if scores["rs_3m"] is not None
                    else None,
                    "rs_6m": Decimal(str(round(scores["rs_6m"], 4)))
                    if scores["rs_6m"] is not None
                    else None,
                    "rs_12m": Decimal(str(round(scores["rs_12m"], 4)))
                    if scores["rs_12m"] is not None
                    else None,
                    "rs_composite": Decimal(str(round(composite, 4)))
                    if composite is not None
                    else None,
                    "computation_version": COMPUTATION_VERSION,
                }
            )

    if not upsert_rows:
        return 0

    # Batch upsert
    batch_size = 1000
    total_upserted = 0

    for offset in range(0, len(upsert_rows), batch_size):
        batch = upsert_rows[offset : offset + batch_size]
        stmt = pg_insert(DeRsScores).values(batch)
        stmt = stmt.on_conflict_do_update(
            index_elements=["date", "entity_type", "entity_id", "vs_benchmark"],
            set_={
                "rs_1w": stmt.excluded.rs_1w,
                "rs_1m": stmt.excluded.rs_1m,
                "rs_3m": stmt.excluded.rs_3m,
                "rs_6m": stmt.excluded.rs_6m,
                "rs_12m": stmt.excluded.rs_12m,
                "rs_composite": stmt.excluded.rs_composite,
                "computation_version": stmt.excluded.computation_version,
                "updated_at": sa.func.now(),
            },
        )
        await session.execute(stmt)
        total_upserted += len(batch)

    await session.flush()

    logger.info(
        "rs_scores_compute_complete",
        business_date=business_date.isoformat(),
        rows_upserted=total_upserted,
    )

    return total_upserted
