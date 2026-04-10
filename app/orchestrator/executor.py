"""Unified pipeline executor — wires DAG, retry, SLA, alerts, and reconciliation.

This is the single entry point for all pipeline execution. The trigger API
calls PipelineExecutor methods; PipelineExecutor calls DAG executor, which
calls BasePipeline.run() for each pipeline, with retry on transient errors.

After execution: SLA checks, reconciliation, and failure alerts.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.orchestrator.alerts import AlertManager
from app.orchestrator.dag import DAGExecutor, PipelineNode, PipelineState
from app.orchestrator.retry import (
    RetryCategory,
    RetryPolicy,
    classify_exception,
    execute_with_retry,
)
from app.orchestrator.sla import SLAChecker
from app.pipelines.framework import PipelineResult
from app.pipelines.registry import (
    get_pipeline,
    get_schedule,
    is_computation_script,
    resolve_name,
)

logger = get_logger(__name__)


async def pipeline_runner(
    name: str,
    business_date: date,
    session: AsyncSession,
) -> PipelineResult:
    """Execute a single pipeline with automatic retry on transient errors.

    This is the callback passed to DAGExecutor.execute().
    """
    if is_computation_script(name):
        # Computation scripts run as subprocess — import here to avoid circular
        from app.api.v1.pipeline_trigger import _run_computation_script
        result = await _run_computation_script(name, business_date)
        return PipelineResult(
            pipeline_name=name,
            business_date=business_date,
            status=result.status,
            rows_processed=result.rows_processed,
            rows_failed=result.rows_failed,
            anomalies_detected=0,
            duration_seconds=result.duration_seconds,
            error=result.error,
        )

    resolved = resolve_name(name)
    pipeline = get_pipeline(resolved)
    if pipeline is None:
        return PipelineResult(
            pipeline_name=name,
            business_date=business_date,
            status="failed",
            rows_processed=0,
            rows_failed=0,
            anomalies_detected=0,
            duration_seconds=0,
            error=f"Pipeline '{name}' (resolved: '{resolved}') not found in registry",
        )

    async def _run() -> PipelineResult:
        return await pipeline.run(business_date, session)

    try:
        return await execute_with_retry(
            coro_factory=_run,
            policy=RetryPolicy.transient(),
            pipeline_name=name,
        )
    except Exception as exc:
        category = classify_exception(exc)
        if category == RetryCategory.TRANSIENT:
            # Retry exhausted — already logged by execute_with_retry
            pass
        return PipelineResult(
            pipeline_name=name,
            business_date=business_date,
            status="failed",
            rows_processed=0,
            rows_failed=0,
            anomalies_detected=0,
            duration_seconds=0,
            error=str(exc),
        )


class PipelineExecutor:
    """Unified executor that ties together DAG, SLA, alerts, retry, and reconciliation.

    Usage:
        executor = PipelineExecutor(alert_manager, sla_checker)
        results = await executor.run_schedule("eod", date.today(), session)
    """

    def __init__(
        self,
        alert_manager: AlertManager | None = None,
        sla_checker: SLAChecker | None = None,
    ) -> None:
        self.alert_manager = alert_manager or AlertManager()
        self.sla_checker = sla_checker or SLAChecker()
        self.dag_executor = DAGExecutor()

    async def run_schedule(
        self,
        schedule_name: str,
        business_date: date,
        session: AsyncSession,
    ) -> list[PipelineResult]:
        """Run a schedule group through DAG with full orchestration."""
        pipeline_names = get_schedule(schedule_name)
        if pipeline_names is None:
            logger.error("schedule_not_found", schedule=schedule_name)
            return []

        logger.info(
            "executor_schedule_start",
            schedule=schedule_name,
            business_date=business_date.isoformat(),
            pipelines=pipeline_names,
        )

        # Run through DAG (handles dependencies, Track A/B isolation)
        nodes = await self.dag_executor.execute(
            pipeline_names=pipeline_names,
            business_date=business_date,
            session=session,
            pipeline_runner=pipeline_runner,
        )

        # Convert nodes to PipelineResult list
        results = self._nodes_to_results(nodes, business_date)

        # Alert on failures
        await self._alert_failures(nodes, business_date)

        # SLA check
        await self._check_sla(business_date, session)

        # Reconciliation after EOD
        if schedule_name == "eod":
            await self._run_reconciliation(business_date, session)

        logger.info(
            "executor_schedule_complete",
            schedule=schedule_name,
            business_date=business_date.isoformat(),
            total=len(results),
            succeeded=sum(1 for r in results if r.status in ("success", "partial")),
            failed=sum(1 for r in results if r.status == "failed"),
        )

        return results

    async def run_single(
        self,
        pipeline_name: str,
        business_date: date,
        session: AsyncSession,
    ) -> PipelineResult:
        """Run a single pipeline with retry and alerting."""
        result = await pipeline_runner(pipeline_name, business_date, session)

        if result.status == "failed" and result.error:
            await self.alert_manager.send_pipeline_failure(
                pipeline_name=pipeline_name,
                error=result.error,
                business_date=business_date.isoformat(),
            )

        return result

    async def _alert_failures(
        self,
        nodes: dict[str, PipelineNode],
        business_date: date,
    ) -> None:
        """Send alerts for any failed pipeline nodes."""
        for node in nodes.values():
            if node.state == PipelineState.FAILED:
                await self.alert_manager.send_pipeline_failure(
                    pipeline_name=node.name,
                    error=node.error or "Unknown error",
                    business_date=business_date.isoformat(),
                )

    async def _check_sla(
        self,
        business_date: date,
        session: AsyncSession,
    ) -> None:
        """Run SLA checks and alert on breaches."""
        try:
            breaches = await self.sla_checker.check(session, business_date)
            for breach in breaches:
                await self.alert_manager.send_sla_breach(breach)
        except Exception as exc:
            logger.error("sla_check_error", error=str(exc))

    async def _run_reconciliation(
        self,
        business_date: date,
        session: AsyncSession,
    ) -> None:
        """Run reconciliation checks after EOD and alert on failures."""
        try:
            from app.orchestrator.reconciliation import ReconciliationChecker

            checker = ReconciliationChecker()
            results = await checker.run_all(business_date, session)

            for result in results:
                if not result.passed:
                    await self.alert_manager.send_reconciliation_failure(
                        check_name=result.check_name,
                        details=result.message,
                        business_date=business_date.isoformat(),
                        severity=result.severity,
                    )
        except Exception as exc:
            logger.error("reconciliation_error", error=str(exc))

    @staticmethod
    def _nodes_to_results(
        nodes: dict[str, PipelineNode],
        business_date: date,
    ) -> list[PipelineResult]:
        """Convert DAG PipelineNodes to PipelineResult list."""
        results: list[PipelineResult] = []
        for node in nodes.values():
            duration = 0.0
            if node.started_at and node.completed_at:
                duration = (node.completed_at - node.started_at).total_seconds()

            results.append(
                PipelineResult(
                    pipeline_name=node.name,
                    business_date=business_date,
                    status=node.state.value,
                    rows_processed=node.rows_processed,
                    rows_failed=0,
                    anomalies_detected=0,
                    duration_seconds=duration,
                    error=node.error,
                )
            )
        return results
