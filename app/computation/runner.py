"""Computation pipeline runner — orchestrates all computation modules in dependency order.

Dependency graph:
    technicals → rs → breadth → regime
                    → sectors   (after rs)
                    → fund_derived (after rs)

If technicals fails, all downstream steps are skipped.
If rs fails, breadth/regime/sectors/fund_derived are skipped.

All steps are wrapped in try/except so a single failure does not crash the runner.
Results are returned as a QAReport with phase="compute".
"""

from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.computation.breadth import compute_breadth
from app.computation.fund_derived import compute_fund_derived_metrics
from app.computation.qa_types import QAReport, StepResult
from app.computation.regime import compute_market_regime
from app.computation.rs import compute_rs_scores
from app.computation.sectors import compute_sector_metrics
from app.computation.technicals import compute_ema, compute_sma
from app.logging import get_logger
from app.models.computed import DeEquityTechnicalDaily, DeRsScores

logger = get_logger(__name__)

# Number of days of price history to fetch per instrument for technicals
HISTORY_DAYS = 252

# Batch size for technicals upsert (memory management)
TECHNICALS_BATCH_SIZE = 200

# Benchmark used when persisting sector RS into de_rs_scores
SECTOR_BENCHMARK = "NIFTY 50"


async def run_technicals_for_date(
    session: AsyncSession,
    business_date: date,
) -> int:
    """Compute and persist SMA50, SMA200, EMA20, close_adj for all instruments.

    Queries all validated OHLCV rows from de_equity_price_daily for the given
    business_date and the preceding HISTORY_DAYS of history.  For each instrument
    the last row in the window is the one written to de_equity_technical_daily.

    above_50dma and above_200dma are GENERATED ALWAYS columns — they are NOT set.

    Args:
        session: Async DB session.
        business_date: Date for which to compute and store technicals.

    Returns:
        Total number of rows upserted into de_equity_technical_daily.
    """
    logger.info(
        "technicals_run_start",
        business_date=business_date.isoformat(),
        history_days=HISTORY_DAYS,
    )

    # Fetch price history — all instruments, last HISTORY_DAYS+1 days up to business_date
    price_query = sa.text("""
        SELECT
            ep.instrument_id,
            ep.date,
            CAST(ep.close_adj AS FLOAT) AS close_adj
        FROM de_equity_price_daily ep
        WHERE ep.data_status = 'validated'
          AND ep.close_adj IS NOT NULL
          AND ep.date <= :bdate
          AND ep.date >= (:bdate::date - INTERVAL ':days days')
        ORDER BY ep.instrument_id, ep.date
    """)

    # Use parameterised interval substitution safely
    price_query = sa.text("""
        SELECT
            ep.instrument_id,
            ep.date,
            CAST(COALESCE(ep.close_adj, ep.close) AS FLOAT) AS close_adj
        FROM de_equity_price_daily ep
        WHERE ep.data_status = 'validated'
          AND COALESCE(ep.close_adj, ep.close) IS NOT NULL
          AND ep.date <= :bdate
          AND ep.date >= :start_date
        ORDER BY ep.instrument_id, ep.date
    """)

    import datetime as dt
    start_date = business_date - dt.timedelta(days=HISTORY_DAYS + 50)  # small buffer for trading-day gaps

    rows = (
        await session.execute(
            price_query,
            {"bdate": business_date, "start_date": start_date},
        )
    ).fetchall()

    if not rows:
        logger.warning(
            "technicals_no_price_data",
            business_date=business_date.isoformat(),
        )
        return 0

    # Group by instrument_id, maintain chronological order
    instrument_data: dict[str, list[tuple[date, float]]] = {}
    for row in rows:
        iid = str(row.instrument_id)
        if iid not in instrument_data:
            instrument_data[iid] = []
        instrument_data[iid].append((row.date, float(row.close_adj)))

    total_upserted = 0
    instrument_ids = list(instrument_data.keys())

    for batch_start in range(0, len(instrument_ids), TECHNICALS_BATCH_SIZE):
        batch_ids = instrument_ids[batch_start : batch_start + TECHNICALS_BATCH_SIZE]
        upsert_rows: list[dict] = []

        for iid in batch_ids:
            records = instrument_data[iid]
            # records are already sorted by date (ORDER BY in query)
            prices = [p for _, p in records]

            if not prices:
                continue

            # Only compute if the last record is for the requested business_date
            last_date = records[-1][0]
            if last_date != business_date:
                # Instrument has no validated data on this date — skip
                continue

            sma50_series = compute_sma(prices, 50)
            sma200_series = compute_sma(prices, 200)
            ema20_series = compute_ema(prices, 20)

            # Take the last element — corresponds to business_date
            sma_50 = sma50_series[-1]
            sma_200 = sma200_series[-1]
            ema_20 = ema20_series[-1]
            close_adj = Decimal(str(round(prices[-1], 4)))

            upsert_rows.append(
                {
                    "date": business_date,
                    "instrument_id": uuid.UUID(iid),
                    "sma_50": sma_50,
                    "sma_200": sma_200,
                    "ema_20": ema_20,
                    "close_adj": close_adj,
                }
            )

        if not upsert_rows:
            continue

        stmt = pg_insert(DeEquityTechnicalDaily).values(upsert_rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["date", "instrument_id"],
            set_={
                "sma_50": stmt.excluded.sma_50,
                "sma_200": stmt.excluded.sma_200,
                "ema_20": stmt.excluded.ema_20,
                "close_adj": stmt.excluded.close_adj,
                "updated_at": sa.func.now(),
            },
        )
        await session.execute(stmt)
        total_upserted += len(upsert_rows)

    await session.flush()

    logger.info(
        "technicals_run_complete",
        business_date=business_date.isoformat(),
        rows_upserted=total_upserted,
    )

    return total_upserted


async def _persist_sector_rs(
    session: AsyncSession,
    business_date: date,
    sector_results: dict[str, dict],
) -> int:
    """Persist sector RS results from compute_sector_metrics() into de_rs_scores.

    Sector results are stored with entity_type='sector', entity_id=sector_name,
    vs_benchmark=SECTOR_BENCHMARK, and rs_composite from the sector dict.

    Args:
        session: Async DB session.
        business_date: Date of computation.
        sector_results: Dict returned by compute_sector_metrics().

    Returns:
        Number of rows upserted.
    """
    if not sector_results:
        return 0

    upsert_rows = []
    for sector_name, metrics in sector_results.items():
        sector_rs: Optional[Decimal] = metrics.get("sector_rs")
        upsert_rows.append(
            {
                "date": business_date,
                "entity_type": "sector",
                "entity_id": sector_name,
                "vs_benchmark": SECTOR_BENCHMARK,
                "rs_composite": sector_rs,
                "computation_version": 1,
            }
        )

    if not upsert_rows:
        return 0

    stmt = pg_insert(DeRsScores).values(upsert_rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["date", "entity_type", "entity_id", "vs_benchmark"],
        set_={
            "rs_composite": stmt.excluded.rs_composite,
            "computation_version": stmt.excluded.computation_version,
            "updated_at": sa.func.now(),
        },
    )
    await session.execute(stmt)
    await session.flush()

    return len(upsert_rows)


async def run_full_computation_pipeline(
    session: AsyncSession,
    business_date: date,
) -> QAReport:
    """Run all computation modules in dependency order.

    Execution order:
        1. technicals  — SMA/EMA for all equities
        2. rs          — Relative strength scores (needs price history)
        3. breadth     — Market breadth (needs technicals via de_equity_technical_daily)
        4. regime      — Market regime (needs breadth + rs)
        5. sectors     — Sector RS (needs rs)
        6. fund_derived — Fund derived metrics (needs rs + NAV data)

    If technicals fails, steps 2-6 are skipped.
    If rs fails, steps 3-6 are skipped.
    Each step's exception is caught and recorded as a failed StepResult.

    Args:
        session: Async DB session (caller manages transaction).
        business_date: Date for which to run all computations.

    Returns:
        QAReport with phase="compute" containing StepResult for each step.
    """
    report = QAReport(phase="compute", business_date=business_date)

    logger.info(
        "computation_pipeline_start",
        business_date=business_date.isoformat(),
    )

    # --- Step 1: technicals ---
    technicals_passed = False
    try:
        rows = await run_technicals_for_date(session, business_date)
        technicals_passed = True
        report.steps.append(
            StepResult(step_name="technicals", status="passed", rows_affected=rows)
        )
    except Exception as exc:
        await session.rollback()
        report.steps.append(
            StepResult(step_name="technicals",
                status="failed",
                errors=[str(exc)],
            )
        )
        logger.error(
            "computation_step_failed",
            step="technicals",
            business_date=business_date.isoformat(),
            error=str(exc),
        )

    if not technicals_passed:
        logger.warning(
            "computation_pipeline_skipping_downstream",
            reason="technicals_failed",
            skipped=["rs", "breadth", "regime", "sectors", "fund_derived"],
        )
        report.mark_complete()
        return report

    # --- Step 2: rs ---
    rs_passed = False
    try:
        rows = await compute_rs_scores(session, business_date, entity_type="equity")
        rs_passed = True
        report.steps.append(
            StepResult(step_name="rs", status="passed", rows_affected=rows)
        )
    except Exception as exc:
        await session.rollback()
        report.steps.append(
            StepResult(step_name="rs", status="failed", errors=[str(exc)])
        )
        logger.error(
            "computation_step_failed",
            step="rs",
            business_date=business_date.isoformat(),
            error=str(exc),
        )

    if not rs_passed:
        logger.warning(
            "computation_pipeline_skipping_downstream",
            reason="rs_failed",
            skipped=["breadth", "regime", "sectors", "fund_derived"],
        )
        report.mark_complete()
        return report

    # --- Step 3: breadth ---
    try:
        result = await compute_breadth(session, business_date)
        report.steps.append(
            StepResult(step_name="breadth", status="passed", rows_affected=result)
        )
    except Exception as exc:
        await session.rollback()
        report.steps.append(
            StepResult(step_name="breadth", status="failed", errors=[str(exc)])
        )
        logger.error(
            "computation_step_failed",
            step="breadth",
            business_date=business_date.isoformat(),
            error=str(exc),
        )

    # --- Step 4: regime ---
    try:
        regime_label = await compute_market_regime(session, business_date)
        report.steps.append(
            StepResult(step_name="regime",
                status="passed",
                rows_affected=1 if regime_label else 0,
                details={"regime": regime_label},
            )
        )
    except Exception as exc:
        await session.rollback()
        report.steps.append(
            StepResult(step_name="regime", status="failed", errors=[str(exc)])
        )
        logger.error(
            "computation_step_failed",
            step="regime",
            business_date=business_date.isoformat(),
            error=str(exc),
        )

    # --- Step 5: sectors ---
    try:
        sector_dict = await compute_sector_metrics(
            session, business_date, benchmark=SECTOR_BENCHMARK
        )
        persisted = await _persist_sector_rs(session, business_date, sector_dict)
        report.steps.append(
            StepResult(step_name="sectors",
                status="passed",
                rows_affected=persisted,
                details={"sectors_computed": len(sector_dict)},
            )
        )
    except Exception as exc:
        await session.rollback()
        report.steps.append(
            StepResult(step_name="sectors", status="failed", errors=[str(exc)])
        )
        logger.error(
            "computation_step_failed",
            step="sectors",
            business_date=business_date.isoformat(),
            error=str(exc),
        )

    # --- Step 6: fund_derived ---
    try:
        rows = await compute_fund_derived_metrics(
            session, business_date, benchmark=SECTOR_BENCHMARK
        )
        report.steps.append(
            StepResult(step_name="fund_derived", status="passed", rows_affected=rows)
        )
    except Exception as exc:
        await session.rollback()
        report.steps.append(
            StepResult(step_name="fund_derived", status="failed", errors=[str(exc)])
        )
        logger.error(
            "computation_step_failed",
            step="fund_derived",
            business_date=business_date.isoformat(),
            error=str(exc),
        )

    report.mark_complete()

    logger.info(
        "computation_pipeline_complete",
        business_date=business_date.isoformat(),
        overall_status=report.overall_status,
        total_rows=sum(s.rows_affected for s in report.steps),
        failed_steps=[s.step_name for s in report.steps if s.status == "failed"],
    )

    return report
