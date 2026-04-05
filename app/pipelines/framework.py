"""Core pipeline base class — all data ingestion pipelines inherit from BasePipeline."""

from __future__ import annotations


import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timezone

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.pipelines.calendar import is_trading_day
from app.pipelines.guards import acquire_pipeline_lock, release_pipeline_lock
from app.pipelines.system_flags import is_pipeline_enabled
from app.pipelines.validation import AnomalyRecord, record_anomalies

logger = get_logger(__name__)


@dataclass
class ExecutionResult:
    """Result returned by a pipeline's execute() method."""

    rows_processed: int
    rows_failed: int
    source_file_id: uuid.UUID | None = None


@dataclass
class PipelineResult:
    """Final result returned by BasePipeline.run()."""

    pipeline_name: str
    business_date: date
    status: str  # success/partial/failed/skipped
    rows_processed: int
    rows_failed: int
    anomalies_detected: int
    duration_seconds: float
    error: str | None = None


class BasePipeline(ABC):
    """Abstract base class for all JIP data ingestion pipelines.

    Subclasses must define:
      - pipeline_name (class attribute)
      - execute() method

    Subclasses may override:
      - validate() method for custom anomaly detection
      - requires_trading_day (default True)
      - exchange (default "NSE")

    Orchestration flow in run():
      1. Check system flags (global kill switch + pipeline-specific)
      2. Check trading calendar (skip if not a trading day and requires_trading_day=True)
      3. Acquire advisory lock (skip if already running for same date)
      4. Create pipeline log entry (status=running)
      5. Call self.execute() — actual ingestion logic
      6. Run self.validate() — anomaly detection
      7. Update pipeline log (status=success/partial/failed)
      8. Release advisory lock
    """

    pipeline_name: str  # Must be set by subclass
    requires_trading_day: bool = True
    exchange: str = "NSE"

    async def run(
        self,
        business_date: date,
        session: AsyncSession,
    ) -> PipelineResult:
        """Main entry point. Orchestrates the full pipeline run."""
        start_time = time.monotonic()
        lock_acquired = False

        logger.info(
            "pipeline_run_start",
            pipeline_name=self.pipeline_name,
            business_date=business_date.isoformat(),
        )

        # Step 1: Check system flags
        enabled = await is_pipeline_enabled(session, self.pipeline_name)
        if not enabled:
            logger.warning(
                "pipeline_skipped_flag_disabled",
                pipeline_name=self.pipeline_name,
                business_date=business_date.isoformat(),
            )
            return PipelineResult(
                pipeline_name=self.pipeline_name,
                business_date=business_date,
                status="skipped",
                rows_processed=0,
                rows_failed=0,
                anomalies_detected=0,
                duration_seconds=time.monotonic() - start_time,
                error="Pipeline disabled by system flag",
            )

        # Step 2: Check trading calendar
        if self.requires_trading_day:
            trading = await is_trading_day(session, business_date, self.exchange)
            if not trading:
                logger.info(
                    "pipeline_skipped_non_trading_day",
                    pipeline_name=self.pipeline_name,
                    business_date=business_date.isoformat(),
                    exchange=self.exchange,
                )
                return PipelineResult(
                    pipeline_name=self.pipeline_name,
                    business_date=business_date,
                    status="skipped",
                    rows_processed=0,
                    rows_failed=0,
                    anomalies_detected=0,
                    duration_seconds=time.monotonic() - start_time,
                    error=f"Non-trading day on {self.exchange}",
                )

        # Step 3: Acquire advisory lock
        lock_acquired = await acquire_pipeline_lock(session, self.pipeline_name, business_date)
        if not lock_acquired:
            logger.warning(
                "pipeline_skipped_lock_contention",
                pipeline_name=self.pipeline_name,
                business_date=business_date.isoformat(),
            )
            return PipelineResult(
                pipeline_name=self.pipeline_name,
                business_date=business_date,
                status="skipped",
                rows_processed=0,
                rows_failed=0,
                anomalies_detected=0,
                duration_seconds=time.monotonic() - start_time,
                error="Another instance is already running this pipeline for this date",
            )

        run_log: DePipelineLog | None = None

        try:
            # Step 4: Create pipeline log entry
            run_log = await self._create_run_log(session, business_date)

            # Step 5: Execute the pipeline
            exec_result: ExecutionResult = await self.execute(business_date, session, run_log)

            # Step 6: Run post-ingestion validation
            anomalies: list[AnomalyRecord] = await self.validate(business_date, session, run_log)
            anomaly_count = await record_anomalies(session, self.pipeline_name, business_date, anomalies)

            # Step 7: Determine final status
            if exec_result.rows_failed > 0 and exec_result.rows_processed > 0:
                final_status = "partial"
            elif exec_result.rows_failed > 0 and exec_result.rows_processed == 0:
                final_status = "failed"
            else:
                final_status = "success"

            duration = time.monotonic() - start_time
            await self._finalize_run_log(
                session,
                run_log,
                status=final_status,
                rows_processed=exec_result.rows_processed,
                rows_failed=exec_result.rows_failed,
                duration_seconds=duration,
            )

            logger.info(
                "pipeline_run_complete",
                pipeline_name=self.pipeline_name,
                business_date=business_date.isoformat(),
                status=final_status,
                rows_processed=exec_result.rows_processed,
                rows_failed=exec_result.rows_failed,
                anomalies=anomaly_count,
                duration_seconds=round(duration, 3),
            )

            return PipelineResult(
                pipeline_name=self.pipeline_name,
                business_date=business_date,
                status=final_status,
                rows_processed=exec_result.rows_processed,
                rows_failed=exec_result.rows_failed,
                anomalies_detected=anomaly_count,
                duration_seconds=duration,
            )

        except Exception as exc:
            duration = time.monotonic() - start_time
            error_detail = str(exc)

            logger.error(
                "pipeline_run_failed",
                pipeline_name=self.pipeline_name,
                business_date=business_date.isoformat(),
                error=error_detail,
                duration_seconds=round(duration, 3),
            )

            if run_log is not None:
                await self._finalize_run_log(
                    session,
                    run_log,
                    status="failed",
                    rows_processed=0,
                    rows_failed=0,
                    duration_seconds=duration,
                    error_detail=error_detail,
                )

            return PipelineResult(
                pipeline_name=self.pipeline_name,
                business_date=business_date,
                status="failed",
                rows_processed=0,
                rows_failed=0,
                anomalies_detected=0,
                duration_seconds=duration,
                error=error_detail,
            )

        finally:
            # Step 8: Release advisory lock
            if lock_acquired:
                await release_pipeline_lock(session, self.pipeline_name, business_date)

    @abstractmethod
    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Subclasses implement actual ingestion logic here.

        Must return an ExecutionResult with rows_processed and rows_failed counts.
        Raise an exception to mark the run as failed.
        """
        ...

    async def validate(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> list[AnomalyRecord]:
        """Override in subclass for custom validation rules.

        Default implementation is a no-op (returns empty list).
        Return a list of AnomalyRecord objects for each detected anomaly.
        """
        return []

    async def _create_run_log(
        self,
        session: AsyncSession,
        business_date: date,
    ) -> DePipelineLog:
        """Create a DePipelineLog entry with status=running."""
        # Determine run_number: max existing + 1 for this pipeline+date
        result = await session.execute(
            select(sa.func.coalesce(sa.func.max(DePipelineLog.run_number), 0)).where(
                DePipelineLog.pipeline_name == self.pipeline_name,
                DePipelineLog.business_date == business_date,
            )
        )
        max_run = result.scalar_one() or 0
        run_number = max_run + 1

        run_log = DePipelineLog(
            pipeline_name=self.pipeline_name,
            business_date=business_date,
            run_number=run_number,
            status="running",
            started_at=datetime.now(tz=timezone.utc),
        )
        session.add(run_log)
        await session.flush()  # Populate auto-generated id

        logger.info(
            "pipeline_log_created",
            pipeline_name=self.pipeline_name,
            business_date=business_date.isoformat(),
            run_number=run_number,
            log_id=run_log.id,
        )
        return run_log

    async def _finalize_run_log(
        self,
        session: AsyncSession,
        run_log: DePipelineLog,
        status: str,
        rows_processed: int,
        rows_failed: int,
        duration_seconds: float,
        error_detail: str | None = None,
    ) -> None:
        """Update the pipeline log entry with final status and counts."""
        run_log.status = status
        run_log.completed_at = datetime.now(tz=timezone.utc)
        run_log.rows_processed = rows_processed
        run_log.rows_failed = rows_failed
        run_log.error_detail = error_detail
        session.add(run_log)
        await session.flush()
