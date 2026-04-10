# Pipeline Chunk 2: Wire Orchestration Layer

**Layer:** 1
**Dependencies:** Pipeline Chunk 1
**Complexity:** Medium
**Status:** pending

## Overview
Connect the existing dead code: DAG executor, SLA monitor, retry policies, alerts,
and reconciliation. All the code exists in `app/orchestrator/` — it just needs to be
wired together and called from the trigger API.

## Files to Create
- `app/orchestrator/executor.py` — Unified executor that ties DAG + SLA + alerts + retry

## Files to Modify
- `app/orchestrator/dag.py` — Add pipeline_runner implementation
- `app/orchestrator/alerts.py` — Wire to config for Slack webhook + SMTP
- `app/orchestrator/sla.py` — Add missing SLA configs
- `app/config.py` — Add SMTP + alert config fields
- `app/main.py` — Initialize AlertManager + SLA checker in lifespan
- `app/api/v1/pipeline_trigger.py` — Use executor instead of direct pipeline calls

## Detailed Spec

### 1. Pipeline Runner (`app/orchestrator/executor.py`)

The DAG executor expects a `pipeline_runner` callable. Build it:

```python
async def pipeline_runner(
    pipeline_name: str,
    business_date: date,
    session: AsyncSession,
) -> PipelineResult:
    """Instantiate pipeline from registry and run with retry."""
    pipeline = get_pipeline(pipeline_name)
    if pipeline is None:
        # Check if it's a computation script
        return await run_computation_script(pipeline_name, business_date)

    # Wrap with retry policy
    async def _run():
        return await pipeline.run(business_date, session)

    return await execute_with_auto_retry(_run)
```

### 2. Unified Executor (`app/orchestrator/executor.py`)

```python
class PipelineExecutor:
    def __init__(self, alert_manager: AlertManager, sla_checker: SLAChecker):
        self.alert_manager = alert_manager
        self.sla_checker = sla_checker
        self.dag_executor = DAGExecutor()
        self.recon_checker = ReconciliationChecker()

    async def run_schedule(
        self, schedule_name: str, business_date: date, session: AsyncSession
    ) -> list[PipelineResult]:
        """Run a schedule group through DAG with full orchestration."""
        pipelines = get_schedule(schedule_name)

        # Run through DAG (handles dependencies, Track A/B isolation)
        dag_run = await self.dag_executor.execute(
            pipeline_names=pipelines,
            business_date=business_date,
            session=session,
            pipeline_runner=pipeline_runner,
        )

        # Check for failures → alert
        for node in dag_run.nodes.values():
            if node.state == PipelineState.FAILED:
                await self.alert_manager.send_pipeline_failure(
                    pipeline_name=node.name,
                    business_date=business_date,
                    error=node.error or "Unknown error",
                )

        # Run reconciliation after EOD
        if schedule_name == "eod":
            recon_results = await self.recon_checker.run_all(business_date, session)
            for result in recon_results:
                if not result.passed:
                    await self.alert_manager.send_reconciliation_failure(result)

        # SLA check
        breaches = await self.sla_checker.check(business_date, session)
        for breach in breaches:
            await self.alert_manager.send_sla_breach(breach)

        return [
            PipelineResult(
                pipeline_name=n.name,
                business_date=business_date,
                status=n.state.value,
                rows_processed=n.rows_processed,
                rows_failed=0,
                anomalies_detected=0,
                duration_seconds=n.duration or 0,
                error=n.error,
            )
            for n in dag_run.nodes.values()
        ]
```

### 3. Alert Manager Initialization

In `app/main.py` lifespan:
```python
alert_manager = AlertManager(
    slack_webhook_url=settings.slack_webhook_url,
    smtp_host=settings.smtp_host,
    smtp_port=settings.smtp_port,
    smtp_user=settings.smtp_user,
    smtp_password=settings.smtp_password,
)
```

Store as app.state for dependency injection into trigger endpoints.

### 4. SLA Additions

Add missing SLAs to `SLAChecker.DEFAULT_SLAS`:
- amfi_nav: 19:30 IST (warning)
- fii_dii_flows: 10:00 IST (warning)
- yfinance_global: 19:30 IST (warning)
- fred_macro: 19:30 IST (warning)
- equity_technicals: 20:00 IST (warning)
- mf_derived: 22:00 IST (warning)
- etf_technicals: 23:30 IST (warning)

### 5. Config Additions

```python
# SMTP (for email alerts)
smtp_host: str = ""
smtp_port: int = 587
smtp_user: str = ""
smtp_password: str = ""
alert_from_email: str = "jip-alerts@jslwealth.in"

# Feature flags
enable_sla_checks: bool = True
enable_reconciliation: bool = True
enable_slack_alerts: bool = True
```

### 6. Update Trigger API

Modify `pipeline_trigger.py` to use `PipelineExecutor` instead of direct pipeline calls:
- Inject PipelineExecutor via `app.state.executor`
- `trigger/{schedule_name}` calls `executor.run_schedule()`
- `trigger/single/{name}` calls `executor.run_single()`
- Backfill uses `executor.run_schedule()` per date

## Acceptance Criteria
- [ ] Schedule trigger runs pipelines through DAG with dependency resolution
- [ ] Track A failure (equity) skips RS/regime but not MF/global
- [ ] Failed pipelines trigger Slack alert (if webhook configured)
- [ ] SLA breaches detected and alerted
- [ ] Reconciliation runs after EOD schedule
- [ ] Retry policy applies: 3 attempts for transient errors, fail-fast for persistent
- [ ] All execution goes through BasePipeline.run() (locking, logging, validation)
- [ ] AlertManager reads config from environment

## Risk
- DAG executor's dependency graph may not match the schedule groups exactly
- Reconciliation queries may fail if tables are empty (first-time setup)
- Slack webhook may not be configured — alerts should degrade gracefully (log only)
