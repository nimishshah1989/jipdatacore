"""Pipeline trigger API — execute pipelines via HTTP.

POST /api/v1/pipeline/trigger/{schedule_name}     — run a schedule group
POST /api/v1/pipeline/trigger/single/{name}        — run one pipeline
POST /api/v1/pipeline/trigger/backfill             — catch up date range
GET  /api/v1/pipeline/trigger/status/{job_id}      — poll background job
GET  /api/v1/pipeline/trigger/schedules            — list schedule groups
GET  /api/v1/pipeline/trigger/pipelines            — list all pipelines

Auth: X-Pipeline-Key header (API key, not JWT).
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Annotated, Optional

import sqlalchemy as sa
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, verify_pipeline_key
from app.logging import get_logger
from app.pipelines.framework import PipelineResult
from app.pipelines.registry import (
    get_computation_module,
    get_pipeline,
    get_schedule,
    is_computation_script,
    is_special_handler,
    list_computation_scripts,
    list_pipelines,
    list_schedules,
    resolve_name,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/pipeline", tags=["pipeline-trigger"])


# ---------------------------------------------------------------------------
# Background job tracking
# ---------------------------------------------------------------------------

@dataclass
class PipelineJob:
    job_id: str
    status: str = "pending"  # pending, running, completed, failed
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    schedule_name: str | None = None
    pipeline_names: list[str] = field(default_factory=list)
    business_dates: list[str] = field(default_factory=list)
    results: list[dict] = field(default_factory=list)
    dates_done: int = 0
    dates_total: int = 0
    error: str | None = None


_jobs: dict[str, PipelineJob] = {}

# Prune jobs older than 24 hours
_PRUNE_AGE_SECONDS = 86400


def _prune_old_jobs() -> None:
    now = datetime.now(tz=timezone.utc)
    expired = [
        jid for jid, job in _jobs.items()
        if (now - job.created_at).total_seconds() > _PRUNE_AGE_SECONDS
        and job.status in ("completed", "failed")
    ]
    for jid in expired:
        del _jobs[jid]


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------

class BackfillRequest(BaseModel):
    pipeline_names: list[str] = Field(..., min_length=1, description="Pipeline or schedule names")
    start_date: date = Field(..., description="Start date (inclusive)")
    end_date: date = Field(..., description="End date (inclusive)")


class PipelineResultResponse(BaseModel):
    pipeline_name: str
    business_date: str
    status: str
    rows_processed: int = 0
    rows_failed: int = 0
    duration_seconds: float = 0
    error: str | None = None


# ---------------------------------------------------------------------------
# Pipeline execution helpers
# ---------------------------------------------------------------------------

async def _run_special_handler(
    name: str,
    business_date: date,
) -> PipelineResultResponse:
    """Run a special inline handler (validate, goldilocks, reconciliation)."""
    import time as _time

    from app.db.session import async_session_factory

    t0 = _time.monotonic()

    if name == "__validate_ohlcv__":
        # Validate raw OHLCV: UPDATE data_status raw → validated for today
        try:
            async with async_session_factory() as sess:
                async with sess.begin():
                    result = await sess.execute(
                        sa.text(
                            "UPDATE de_equity_ohlcv SET data_status = 'validated' "
                            "WHERE date = :bdate AND data_status = 'raw'"
                        ),
                        {"bdate": business_date},
                    )
                    rows_affected = result.rowcount
            duration = _time.monotonic() - t0
            logger.info(
                "validate_ohlcv_done",
                rows_validated=rows_affected,
                business_date=business_date.isoformat(),
            )
            return PipelineResultResponse(
                pipeline_name=name,
                business_date=business_date.isoformat(),
                status="success",
                rows_processed=rows_affected,
                duration_seconds=round(duration, 3),
            )
        except Exception as exc:
            logger.error("validate_ohlcv_error", error=str(exc))
            return PipelineResultResponse(
                pipeline_name=name,
                business_date=business_date.isoformat(),
                status="failed",
                error=str(exc),
            )

    elif name == "__goldilocks_compute__":
        # Run goldilocks scraper + PDF extraction + LLM extraction as subprocesses
        steps = [
            ("goldilocks_scraper", [sys.executable, "-m", "scripts.ingest.goldilocks_scraper", "--mode", "daily"]),
            ("pdf_extraction", [sys.executable, "-m", "scripts.ingest.extract_goldilocks_pdfs"]),
            ("llm_extraction", [sys.executable, "-m", "scripts.ingest.run_goldilocks_extraction", "--max-docs", "10"]),
        ]
        errors: list[str] = []
        for step_name, cmd in steps:
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=600)
                if proc.returncode != 0:
                    err = stderr.decode()[-500:] if stderr else f"Exit code {proc.returncode}"
                    errors.append(f"{step_name}: {err}")
                    logger.error("goldilocks_step_failed", step=step_name, error=err)
                else:
                    logger.info("goldilocks_step_done", step=step_name)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                errors.append(f"{step_name}: timed out after 600s")
                logger.error("goldilocks_step_timeout", step=step_name)
            except Exception as exc:
                errors.append(f"{step_name}: {exc}")
                logger.error("goldilocks_step_error", step=step_name, error=str(exc))

        duration = _time.monotonic() - t0
        return PipelineResultResponse(
            pipeline_name=name,
            business_date=business_date.isoformat(),
            status="success" if not errors else "failed",
            duration_seconds=round(duration, 3),
            error="; ".join(errors) if errors else None,
        )

    elif name == "__reconciliation__":
        try:
            from app.orchestrator.reconciliation import ReconciliationChecker

            async with async_session_factory() as sess:
                checker = ReconciliationChecker(sess)
                await checker.run_all()
            duration = _time.monotonic() - t0
            return PipelineResultResponse(
                pipeline_name=name,
                business_date=business_date.isoformat(),
                status="success",
                duration_seconds=round(duration, 3),
            )
        except Exception as exc:
            logger.error("reconciliation_error", error=str(exc))
            return PipelineResultResponse(
                pipeline_name=name,
                business_date=business_date.isoformat(),
                status="failed",
                error=str(exc),
            )

    return PipelineResultResponse(
        pipeline_name=name,
        business_date=business_date.isoformat(),
        status="failed",
        error=f"Unknown special handler: {name}",
    )


async def _run_single_pipeline(
    name: str,
    business_date: date,
    session: AsyncSession | None = None,
) -> PipelineResultResponse:
    """Run a single pipeline or computation script and return result.

    Each pipeline gets its own isolated DB session to prevent transaction
    poisoning when one pipeline fails mid-transaction.
    """
    from app.db.session import async_session_factory

    if is_special_handler(name):
        return await _run_special_handler(name, business_date)

    if is_computation_script(name):
        return await _run_computation_script(name, business_date)

    resolved = resolve_name(name)
    pipeline = get_pipeline(resolved)

    if pipeline is None:
        logger.warning("pipeline_not_found", name=name, resolved=resolved)
        return PipelineResultResponse(
            pipeline_name=name,
            business_date=business_date.isoformat(),
            status="failed",
            error=f"Pipeline '{name}' not found in registry",
        )

    try:
        # Each pipeline gets its own session to isolate transaction failures
        async with async_session_factory() as isolated_session:
            result: PipelineResult = await pipeline.run(business_date, isolated_session)
            await isolated_session.commit()
        return PipelineResultResponse(
            pipeline_name=name,
            business_date=business_date.isoformat(),
            status=result.status,
            rows_processed=result.rows_processed,
            rows_failed=result.rows_failed,
            duration_seconds=round(result.duration_seconds, 3),
            error=result.error,
        )
    except Exception as exc:
        logger.error("pipeline_execution_error", pipeline=name, error=str(exc))
        return PipelineResultResponse(
            pipeline_name=name,
            business_date=business_date.isoformat(),
            status="failed",
            error=str(exc),
        )


async def _run_computation_script(
    name: str,
    business_date: date,
) -> PipelineResultResponse:
    """Run a standalone computation script as a subprocess."""
    module = get_computation_module(name)
    if module is None:
        return PipelineResultResponse(
            pipeline_name=name,
            business_date=business_date.isoformat(),
            status="failed",
            error=f"Computation script '{name}' not found",
        )

    cmd = [sys.executable, "-m", module]

    # Map script-specific CLI arguments
    if name in ("equity_technicals_sql", "equity_technicals_pandas"):
        cmd.extend(["--start-date", business_date.isoformat()])
    elif name in ("relative_strength",):
        cmd.extend(["--entity-type", "all", "--start-date", business_date.isoformat()])
    elif name in ("market_breadth", "regime_detection"):
        cmd.extend(["--start-date", business_date.isoformat()])
    elif name in ("mf_derived",):
        cmd.extend(["--start-date", business_date.isoformat()])
    elif name in ("etf_technicals", "global_technicals"):
        cmd.extend(["--filter-date", business_date.isoformat()])
    elif name in ("etf_rs", "global_rs"):
        cmd.extend(["--compute-start", business_date.isoformat()])
    elif name == "full_runner":
        cmd.extend(["--date", business_date.isoformat(), "--step", "all"])

    logger.info("computation_script_start", name=name, module=module, cmd=cmd)

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=1800)

        if proc.returncode == 0:
            logger.info(
                "computation_script_success",
                name=name,
                stdout_tail=stdout.decode()[-500:] if stdout else "",
            )
            return PipelineResultResponse(
                pipeline_name=name,
                business_date=business_date.isoformat(),
                status="success",
            )
        else:
            error_msg = stderr.decode()[-1000:] if stderr else f"Exit code {proc.returncode}"
            logger.error("computation_script_failed", name=name, error=error_msg)
            return PipelineResultResponse(
                pipeline_name=name,
                business_date=business_date.isoformat(),
                status="failed",
                error=error_msg,
            )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        logger.error("computation_script_timeout", name=name)
        return PipelineResultResponse(
            pipeline_name=name,
            business_date=business_date.isoformat(),
            status="failed",
            error="Script timed out after 30 minutes",
        )
    except Exception as exc:
        logger.error("computation_script_error", name=name, error=str(exc))
        return PipelineResultResponse(
            pipeline_name=name,
            business_date=business_date.isoformat(),
            status="failed",
            error=str(exc),
        )


# ---------------------------------------------------------------------------
# Background backfill runner
# ---------------------------------------------------------------------------

async def _run_backfill(job: PipelineJob) -> None:
    """Background task: run pipelines for each date in range."""
    from app.db.session import async_session_factory

    job.status = "running"

    try:
        for date_str in job.business_dates:
            bdate = date.fromisoformat(date_str)
            date_results: list[dict] = []

            async with async_session_factory() as session:
                for pname in job.pipeline_names:
                    result = await _run_single_pipeline(pname, bdate, session)
                    date_results.append(result.model_dump())

                await session.commit()

            job.results.extend(date_results)
            job.dates_done += 1

            logger.info(
                "backfill_date_complete",
                job_id=job.job_id,
                business_date=date_str,
                dates_done=job.dates_done,
                dates_total=job.dates_total,
            )

        job.status = "completed"
    except Exception as exc:
        job.status = "failed"
        job.error = str(exc)
        logger.error("backfill_failed", job_id=job.job_id, error=str(exc))


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post(
    "/trigger/{schedule_name}",
    status_code=status.HTTP_200_OK,
    summary="Trigger a schedule group",
)
async def trigger_schedule(
    schedule_name: str,
    request: Request,
    _key: Annotated[bool, Depends(verify_pipeline_key)],
    db: Annotated[AsyncSession, Depends(get_db)],
    business_date: Optional[date] = Query(default=None, description="Business date (defaults to today)"),
):
    """Run all pipelines in a schedule group.

    Uses PipelineExecutor (DAG + retry + SLA + alerts) if available,
    otherwise falls back to sequential execution.
    """
    pipeline_names = get_schedule(schedule_name)
    if pipeline_names is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schedule '{schedule_name}' not found. Available: {list(list_schedules().keys())}",
        )

    bdate = business_date or date.today()

    # Use PipelineExecutor if initialized (wired in main.py lifespan)
    executor = getattr(request.app.state, "executor", None)
    if executor is not None:
        from app.orchestrator.executor import PipelineExecutor
        assert isinstance(executor, PipelineExecutor)

        pipeline_results = await executor.run_schedule(schedule_name, bdate, db)
        await db.commit()

        return {
            "schedule_name": schedule_name,
            "business_date": bdate.isoformat(),
            "orchestrated": True,
            "pipelines": [
                {
                    "pipeline_name": r.pipeline_name,
                    "business_date": r.business_date.isoformat(),
                    "status": r.status,
                    "rows_processed": r.rows_processed,
                    "rows_failed": r.rows_failed,
                    "duration_seconds": round(r.duration_seconds, 3),
                    "error": r.error,
                }
                for r in pipeline_results
            ],
        }

    # Fallback: sequential execution without orchestration
    results: list[dict] = []

    logger.info(
        "schedule_trigger_start",
        schedule=schedule_name,
        business_date=bdate.isoformat(),
        pipelines=pipeline_names,
    )

    for pname in pipeline_names:
        result = await _run_single_pipeline(pname, bdate, db)
        results.append(result.model_dump())

    await db.commit()

    return {
        "schedule_name": schedule_name,
        "business_date": bdate.isoformat(),
        "orchestrated": False,
        "pipelines": results,
    }


@router.post(
    "/trigger/single/{pipeline_name}",
    status_code=status.HTTP_200_OK,
    summary="Trigger a single pipeline",
)
async def trigger_single(
    pipeline_name: str,
    _key: Annotated[bool, Depends(verify_pipeline_key)],
    db: Annotated[AsyncSession, Depends(get_db)],
    business_date: Optional[date] = Query(default=None, description="Business date (defaults to today)"),
):
    """Run a single pipeline by name."""
    resolved = resolve_name(pipeline_name)

    # Check if it's a known pipeline, computation script, or special handler
    if (
        not is_special_handler(pipeline_name)
        and not is_computation_script(pipeline_name)
        and get_pipeline(resolved) is None
    ):
        all_names = list_pipelines() + list_computation_scripts()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline '{pipeline_name}' not found. Available: {all_names}",
        )

    bdate = business_date or date.today()
    result = await _run_single_pipeline(pipeline_name, bdate, db)
    await db.commit()

    return result.model_dump()


@router.post(
    "/trigger/backfill",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Backfill pipelines for a date range (background)",
)
async def trigger_backfill(
    body: BackfillRequest,
    background_tasks: BackgroundTasks,
    _key: Annotated[bool, Depends(verify_pipeline_key)],
):
    """Queue a backfill job for multiple dates. Returns job_id for polling."""
    if body.end_date < body.start_date:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="end_date must be >= start_date",
        )

    day_count = (body.end_date - body.start_date).days + 1
    if day_count > 90:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Max 90 days per backfill, requested {day_count}",
        )

    # Generate date list
    from datetime import timedelta
    dates = [
        (body.start_date + timedelta(days=i)).isoformat()
        for i in range(day_count)
    ]

    _prune_old_jobs()

    job = PipelineJob(
        job_id=str(uuid.uuid4()),
        schedule_name="backfill",
        pipeline_names=body.pipeline_names,
        business_dates=dates,
        dates_total=day_count,
    )
    _jobs[job.job_id] = job

    background_tasks.add_task(_run_backfill, job)

    logger.info(
        "backfill_queued",
        job_id=job.job_id,
        pipelines=body.pipeline_names,
        start_date=body.start_date.isoformat(),
        end_date=body.end_date.isoformat(),
        date_count=day_count,
    )

    return {
        "job_id": job.job_id,
        "status": "pending",
        "dates_total": day_count,
        "pipeline_count": len(body.pipeline_names),
    }


@router.get(
    "/trigger/status/{job_id}",
    status_code=status.HTTP_200_OK,
    summary="Check backfill job status",
)
async def get_job_status(
    job_id: str,
    _key: Annotated[bool, Depends(verify_pipeline_key)],
):
    """Poll a background backfill job for progress."""
    job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found (may have expired after 24h)",
        )

    return {
        "job_id": job.job_id,
        "status": job.status,
        "dates_done": job.dates_done,
        "dates_total": job.dates_total,
        "pipeline_names": job.pipeline_names,
        "error": job.error,
        "results": job.results[-20:],  # Last 20 results to avoid huge responses
    }


@router.get(
    "/trigger/schedules",
    status_code=status.HTTP_200_OK,
    summary="List all schedule groups",
)
async def get_schedules(
    _key: Annotated[bool, Depends(verify_pipeline_key)],
):
    """Return all schedule groups and their pipeline lists."""
    return {"schedules": list_schedules()}


@router.get(
    "/trigger/pipelines",
    status_code=status.HTTP_200_OK,
    summary="List all registered pipelines",
)
async def get_pipelines(
    _key: Annotated[bool, Depends(verify_pipeline_key)],
):
    """Return all registered pipeline names and computation scripts."""
    return {
        "pipelines": list_pipelines(),
        "computation_scripts": list_computation_scripts(),
    }
