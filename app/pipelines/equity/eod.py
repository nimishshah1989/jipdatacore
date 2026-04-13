"""EOD Equity Orchestrator pipeline.

Orchestrates the full end-of-day equity ingestion sequence:
  1. master_refresh — sync NSE equity listing
  2. corporate_actions — fetch splits, bonuses, dividends
  3. bhav — BHAV copy OHLCV
  4. delivery — T+1 delivery data
  5. validate — post-insert price/volume spike checks, data status promotion

Extends BasePipeline — the base class handles flags, calendar, locking, logging.
"""

from __future__ import annotations


import uuid
from datetime import date
from decimal import Decimal
from typing import Any


from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.pipelines.equity.bhav import BhavPipeline
from app.pipelines.equity.corporate_actions import CorporateActionsPipeline
from app.pipelines.equity.delivery import DeliveryPipeline
from app.pipelines.equity.master_refresh import MasterRefreshPipeline
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.validation import AnomalyRecord, apply_data_status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)

# Validation thresholds
PRICE_SPIKE_THRESHOLD = Decimal("0.20")   # 20% move without corporate action
VOLUME_SPIKE_MULTIPLIER = Decimal("10")   # 10x rolling 20-day average


class EodOrchestrator(BasePipeline):
    """End-of-day equity data orchestrator.

    Runs: master_refresh → corporate_actions → bhav → delivery → validate.

    The base class `run()` handles flags, calendar checks, and locking.
    This class implements `execute()` which sequences the sub-pipelines,
    and `validate()` which runs post-insert price/volume checks.
    """

    pipeline_name = "equity_eod"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Sequence sub-pipelines for the full EOD run.

        Each sub-pipeline is instantiated and its execute() called directly
        (not run() — we don't want nested orchestration, flags/calendar
        checks already done at this level).

        Args:
            business_date: The trading date.
            session: Async DB session.
            run_log: This pipeline's DePipelineLog entry.

        Returns:
            Aggregated ExecutionResult across all sub-pipelines.
        """
        logger.info("eod_orchestrator_execute_start", business_date=business_date.isoformat())

        total_processed = 0
        total_failed = 0
        last_source_file_id: uuid.UUID | None = None

        # Step 1: Master refresh
        master_result = await self._run_sub_pipeline(
            MasterRefreshPipeline(),
            business_date,
            session,
            run_log,
            step_name="master_refresh",
        )
        total_processed += master_result.rows_processed
        total_failed += master_result.rows_failed

        # Step 2: Corporate actions
        ca_result = await self._run_sub_pipeline(
            CorporateActionsPipeline(),
            business_date,
            session,
            run_log,
            step_name="corporate_actions",
        )
        total_processed += ca_result.rows_processed
        total_failed += ca_result.rows_failed

        # Step 3: BHAV copy (main price data)
        bhav = BhavPipeline()
        bhav_result = await self._run_sub_pipeline(
            bhav,
            business_date,
            session,
            run_log,
            step_name="bhav",
        )
        total_processed += bhav_result.rows_processed
        total_failed += bhav_result.rows_failed
        if bhav_result.source_file_id:
            last_source_file_id = bhav_result.source_file_id

        # Step 4: Delivery data
        delivery_result = await self._run_sub_pipeline(
            DeliveryPipeline(),
            business_date,
            session,
            run_log,
            step_name="delivery",
        )
        total_processed += delivery_result.rows_processed
        total_failed += delivery_result.rows_failed

        logger.info(
            "eod_orchestrator_execute_complete",
            total_processed=total_processed,
            total_failed=total_failed,
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=total_processed,
            rows_failed=total_failed,
            source_file_id=last_source_file_id,
        )

    async def validate(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> list[AnomalyRecord]:
        """Post-insert validation for the full EOD dataset.

        Checks:
        1. Negative OHLC values → critical anomaly
        2. Price range inversion (high < low) → critical anomaly
        3. Price spike > 20% without corporate action → medium anomaly
        4. Volume spike > 10x 20-day rolling avg → low anomaly

        After anomaly detection, promotes data_status:
        - Rows with critical anomalies → quarantined
        - All remaining raw rows → validated

        Args:
            business_date: The trading date.
            session: Async DB session.
            run_log: This pipeline's DePipelineLog entry.

        Returns:
            List of AnomalyRecord objects detected.
        """
        logger.info("eod_validate_start", business_date=business_date.isoformat())
        anomalies: list[AnomalyRecord] = []
        critical_instrument_ids: set[uuid.UUID] = set()

        # Check 1: Negative OHLC values
        negative_rows = await _find_negative_ohlc(session, business_date)
        for row in negative_rows:
            anomalies.append(
                AnomalyRecord(
                    entity_type="equity",
                    anomaly_type="negative_value",
                    severity="critical",
                    instrument_id=row["instrument_id"],
                    expected_range="All OHLC values >= 0",
                    actual_value=row["detail"],
                )
            )
            critical_instrument_ids.add(row["instrument_id"])

        # Check 2: Price range inversion (high < low)
        inverted_rows = await _find_inverted_price_range(session, business_date)
        for row in inverted_rows:
            anomalies.append(
                AnomalyRecord(
                    entity_type="equity",
                    anomaly_type="price_spike",  # using price_spike as closest type
                    severity="critical",
                    instrument_id=row["instrument_id"],
                    expected_range="high >= low",
                    actual_value=f"high={row['high']}, low={row['low']}",
                )
            )
            critical_instrument_ids.add(row["instrument_id"])

        # Check 3: Price spike > 20% without corporate action
        spike_rows = await _find_price_spikes(session, business_date)
        for row in spike_rows:
            instrument_id = row["instrument_id"]
            # Skip if already flagged as critical
            if instrument_id in critical_instrument_ids:
                continue
            anomalies.append(
                AnomalyRecord(
                    entity_type="equity",
                    anomaly_type="price_spike",
                    severity="medium",
                    instrument_id=instrument_id,
                    expected_range=f"Change <= {PRICE_SPIKE_THRESHOLD * 100}% or has corporate action",
                    actual_value=f"change_pct={row['change_pct']}",
                )
            )

        # Check 4: Volume spike > 10x rolling 20d avg
        vol_spike_rows = await _find_volume_spikes(session, business_date)
        for row in vol_spike_rows:
            instrument_id = row["instrument_id"]
            anomalies.append(
                AnomalyRecord(
                    entity_type="equity",
                    anomaly_type="price_spike",  # using price_spike as the valid anomaly_type
                    severity="low",
                    instrument_id=instrument_id,
                    expected_range=f"Volume <= {VOLUME_SPIKE_MULTIPLIER}x rolling 20d avg",
                    actual_value=f"volume={row['volume']}, avg_20d={row['avg_20d']}",
                )
            )

        # Promote data_status: critical instrument_ids → quarantined, rest → validated
        await apply_data_status(
            session,
            table_name="de_equity_ohlcv",
            business_date=business_date,
            pipeline_run_id=run_log.id,
            anomaly_instrument_ids=critical_instrument_ids if critical_instrument_ids else None,
            date_column="date",
        )

        logger.info(
            "eod_validate_complete",
            total_anomalies=len(anomalies),
            critical_count=len(critical_instrument_ids),
            business_date=business_date.isoformat(),
        )

        return anomalies

    async def _run_sub_pipeline(
        self,
        pipeline: BasePipeline,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
        step_name: str,
    ) -> ExecutionResult:
        """Execute a sub-pipeline's execute() directly (not run()).

        Catches exceptions and returns a failed ExecutionResult rather than
        propagating (so one sub-pipeline failure doesn't abort the whole EOD run).

        Args:
            pipeline: Sub-pipeline instance.
            business_date: Trading date.
            session: Async DB session.
            run_log: Parent pipeline's log entry.
            step_name: Name for logging.

        Returns:
            ExecutionResult (may have rows_failed > 0 on error).
        """
        try:
            result = await pipeline.execute(business_date, session, run_log)
            logger.info(
                "eod_sub_pipeline_complete",
                step=step_name,
                rows_processed=result.rows_processed,
                rows_failed=result.rows_failed,
                business_date=business_date.isoformat(),
            )
            return result
        except Exception as exc:
            logger.error(
                "eod_sub_pipeline_failed",
                step=step_name,
                error=str(exc),
                business_date=business_date.isoformat(),
            )
            return ExecutionResult(rows_processed=0, rows_failed=1)


# ---------------------------------------------------------------------------
# Validation query helpers
# ---------------------------------------------------------------------------

async def _find_negative_ohlc(
    session: AsyncSession,
    business_date: date,
) -> list[dict[str, Any]]:
    """Find equity OHLCV rows with any negative price value.

    Args:
        session: Async DB session.
        business_date: The trading date.

    Returns:
        List of dicts with instrument_id and detail string.
    """
    result = await session.execute(
        text(
            """
            SELECT instrument_id,
                   CONCAT('open=', open, ' high=', high, ' low=', low, ' close=', close) AS detail
            FROM de_equity_ohlcv
            WHERE date = :bdate
              AND (open < 0 OR high < 0 OR low < 0 OR close < 0)
            """
        ),
        {"bdate": business_date},
    )
    return [
        {"instrument_id": row.instrument_id, "detail": row.detail}
        for row in result
    ]


async def _find_inverted_price_range(
    session: AsyncSession,
    business_date: date,
) -> list[dict[str, Any]]:
    """Find OHLCV rows where high < low (inverted price range).

    Args:
        session: Async DB session.
        business_date: The trading date.

    Returns:
        List of dicts with instrument_id, high, low.
    """
    result = await session.execute(
        text(
            """
            SELECT instrument_id, high, low
            FROM de_equity_ohlcv
            WHERE date = :bdate
              AND high IS NOT NULL
              AND low IS NOT NULL
              AND high < low
            """
        ),
        {"bdate": business_date},
    )
    return [
        {"instrument_id": row.instrument_id, "high": str(row.high), "low": str(row.low)}
        for row in result
    ]


async def _find_price_spikes(
    session: AsyncSession,
    business_date: date,
) -> list[dict[str, Any]]:
    """Find rows where |close - prev_close| / prev_close > 20% with no corporate action.

    Uses a CTE to get today and yesterday's close prices per instrument,
    then filters for > 20% moves, excluding instruments with corporate actions on that date.

    Args:
        session: Async DB session.
        business_date: The trading date.

    Returns:
        List of dicts with instrument_id and change_pct string.
    """
    result = await session.execute(
        text(
            """
            WITH today AS (
                SELECT instrument_id, close
                FROM de_equity_ohlcv
                WHERE date = :bdate
                  AND close IS NOT NULL
            ),
            prev AS (
                SELECT DISTINCT ON (instrument_id)
                    instrument_id, close AS prev_close
                FROM de_equity_ohlcv
                WHERE date < :bdate
                  AND close IS NOT NULL
                  AND close > 0
                ORDER BY instrument_id, date DESC
            ),
            corp_action_instruments AS (
                SELECT DISTINCT instrument_id
                FROM de_corporate_actions
                WHERE ex_date = :bdate
            )
            SELECT t.instrument_id,
                   ROUND(ABS(t.close - p.prev_close) / p.prev_close * 100, 4) AS change_pct
            FROM today t
            JOIN prev p ON t.instrument_id = p.instrument_id
            WHERE p.prev_close > 0
              AND ABS(t.close - p.prev_close) / p.prev_close > :threshold
              AND t.instrument_id NOT IN (SELECT instrument_id FROM corp_action_instruments)
            """
        ),
        {
            "bdate": business_date,
            "threshold": PRICE_SPIKE_THRESHOLD,
        },
    )
    return [
        {"instrument_id": row.instrument_id, "change_pct": str(row.change_pct)}
        for row in result
    ]


async def _find_volume_spikes(
    session: AsyncSession,
    business_date: date,
) -> list[dict[str, Any]]:
    """Find rows where volume > 10x the 20-day rolling average volume.

    Args:
        session: Async DB session.
        business_date: The trading date.

    Returns:
        List of dicts with instrument_id, volume, avg_20d.
    """
    result = await session.execute(
        text(
            """
            WITH today AS (
                SELECT instrument_id, volume
                FROM de_equity_ohlcv
                WHERE date = :bdate
                  AND volume IS NOT NULL
                  AND volume > 0
            ),
            avg_20d AS (
                SELECT instrument_id,
                       AVG(volume) AS avg_volume
                FROM de_equity_ohlcv
                WHERE date < :bdate
                  AND date >= :bdate - INTERVAL '30 days'
                  AND volume IS NOT NULL
                  AND volume > 0
                GROUP BY instrument_id
                HAVING COUNT(*) >= 5
            )
            SELECT t.instrument_id, t.volume, ROUND(a.avg_volume, 0) AS avg_20d
            FROM today t
            JOIN avg_20d a ON t.instrument_id = a.instrument_id
            WHERE a.avg_volume > 0
              AND t.volume > a.avg_volume * :multiplier
            """
        ),
        {
            "bdate": business_date,
            "multiplier": VOLUME_SPIKE_MULTIPLIER,
        },
    )
    return [
        {
            "instrument_id": row.instrument_id,
            "volume": str(row.volume),
            "avg_20d": str(row.avg_20d),
        }
        for row in result
    ]
