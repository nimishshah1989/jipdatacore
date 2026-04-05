"""Standalone Pipeline Monitoring Dashboard — FastAPI app on port 8099.

Serves the HTML/JS/CSS dashboard and a set of thin JSON endpoints that
proxy / aggregate data from the main engine at localhost:8010.

Start with:
    uvicorn dashboard.app:app --host 127.0.0.1 --port 8099 --reload
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import structlog
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from dashboard.api import (
    SLA_DEADLINES,
    PIPELINE_SLA_MAP,
    compute_sla_status,
    fetch_anomalies,
    fetch_pipeline_runs,
    fetch_system_health,
    format_indian_number,
    format_ist_date,
    format_ist_datetime,
)

logger = structlog.get_logger(__name__)

DASHBOARD_DIR = Path(__file__).parent
STATIC_DIR = DASHBOARD_DIR / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[type-arg]
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            structlog.get_level_from_name(os.environ.get("LOG_LEVEL", "INFO"))
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    logger.info("dashboard_starting", port=8099, engine_url=os.environ.get("ENGINE_BASE_URL", "http://localhost:8010"))
    yield
    logger.info("dashboard_shutdown")


app = FastAPI(
    title="JIP Pipeline Dashboard",
    version="1.0.0",
    description="Pipeline monitoring dashboard for JIP Data Engine",
    docs_url="/api/docs",
    redoc_url=None,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8099", "http://127.0.0.1:8099"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---------------------------------------------------------------------------
# Page routes
# ---------------------------------------------------------------------------


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    """Serve the main dashboard HTML page."""
    index_path = DASHBOARD_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(str(index_path), media_type="text/html")


# ---------------------------------------------------------------------------
# Data API routes
# ---------------------------------------------------------------------------


@app.get("/api/pipeline-runs")
async def pipeline_runs(
    business_date: Optional[date] = Query(default=None, description="Filter by business date (YYYY-MM-DD)"),
    date_from: Optional[date] = Query(default=None, description="History range start (YYYY-MM-DD)"),
    date_to: Optional[date] = Query(default=None, description="History range end (YYYY-MM-DD)"),
) -> JSONResponse:
    """Return pipeline run logs enriched with SLA status.

    Proxies GET /api/v1/admin/pipeline-runs on the main engine and
    injects `sla` metadata for each run.
    """
    raw = await fetch_pipeline_runs(
        business_date=business_date,
        date_from=date_from,
        date_to=date_to,
    )

    runs: list[dict[str, Any]] = raw.get("runs", [])
    enriched: list[dict[str, Any]] = []
    for run in runs:
        completed_at: Optional[datetime] = None
        if run.get("completed_at"):
            try:
                completed_at = datetime.fromisoformat(run["completed_at"])
            except (ValueError, TypeError):
                pass

        bd: Optional[date] = None
        if run.get("business_date"):
            try:
                bd = date.fromisoformat(run["business_date"])
            except (ValueError, TypeError):
                pass

        sla = compute_sla_status(
            pipeline_name=run.get("pipeline_name", ""),
            completed_at=completed_at,
            status=run.get("status", ""),
            reference_date=bd,
        )

        # Human-readable timestamps
        display: dict[str, Any] = {}
        if run.get("started_at"):
            try:
                display["started_at_fmt"] = format_ist_datetime(datetime.fromisoformat(run["started_at"]))
            except (ValueError, TypeError):
                display["started_at_fmt"] = run["started_at"]
        if run.get("completed_at"):
            try:
                display["completed_at_fmt"] = format_ist_datetime(datetime.fromisoformat(run["completed_at"]))
            except (ValueError, TypeError):
                display["completed_at_fmt"] = run["completed_at"]
        if run.get("business_date"):
            try:
                display["business_date_fmt"] = format_ist_date(date.fromisoformat(run["business_date"]))
            except (ValueError, TypeError):
                display["business_date_fmt"] = run["business_date"]

        rows_processed = run.get("rows_processed")
        rows_failed = run.get("rows_failed")
        if rows_processed is not None:
            display["rows_processed_fmt"] = format_indian_number(rows_processed)
        if rows_failed is not None:
            display["rows_failed_fmt"] = format_indian_number(rows_failed)

        enriched.append({**run, "sla": sla, "display": display})

    return JSONResponse(content={"runs": enriched, "error": raw.get("error")})


@app.get("/api/anomalies")
async def anomalies(
    business_date: Optional[date] = Query(default=None, description="Filter by business date"),
    severity: Optional[str] = Query(default=None, description="Filter by severity: critical|warning|info"),
) -> JSONResponse:
    """Return today's anomalies grouped by severity."""
    if severity is not None and severity not in ("critical", "warning", "info"):
        raise HTTPException(status_code=422, detail="severity must be critical, warning, or info")

    raw = await fetch_anomalies(business_date=business_date, severity=severity)
    anomaly_list: list[dict[str, Any]] = raw.get("anomalies", [])

    grouped: dict[str, list[dict[str, Any]]] = {"critical": [], "warning": [], "info": []}
    for item in anomaly_list:
        sev = item.get("severity", "info")
        if sev in grouped:
            grouped[sev].append(item)
        else:
            grouped["info"].append(item)

    return JSONResponse(
        content={
            "grouped": grouped,
            "counts": {k: len(v) for k, v in grouped.items()},
            "total": len(anomaly_list),
            "error": raw.get("error"),
        }
    )


@app.get("/api/system-health")
async def system_health() -> JSONResponse:
    """Return system health metrics (Redis, DB connections, disk)."""
    data = await fetch_system_health()
    return JSONResponse(content=data)


@app.get("/api/sla-config")
async def sla_config() -> JSONResponse:
    """Return the static SLA deadline configuration used by the frontend."""
    return JSONResponse(
        content={
            "deadlines": SLA_DEADLINES,
            "pipeline_map": PIPELINE_SLA_MAP,
        }
    )


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe for the dashboard service itself."""
    return {"status": "healthy", "service": "dashboard", "version": "1.0.0"}
