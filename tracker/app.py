"""Standalone Live Tracker Dashboard — FastAPI app on port 8098.

Serves a live-updating HTML dashboard with DIRECT database access to
de_pipeline_log, showing ingestion status for target and recent pipelines.

Start with:
    uvicorn tracker.app:app --host 0.0.0.0 --port 8098 --reload
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import sqlalchemy as sa
import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from tracker.computation_api import router as computation_router, set_session_factory

logger = structlog.get_logger(__name__)

TRACKER_DIR = Path(__file__).parent

IST = timezone(timedelta(hours=5, minutes=30))

# Target pipelines to prominently track
TARGET_PIPELINES: list[dict[str, str]] = [
    {"name": "mf_category_flows", "display_name": "MF Category Flows"},
    {"name": "market_cap_history", "display_name": "Market Cap History"},
    {"name": "symbol_history", "display_name": "Symbol History"},
]

# ---------------------------------------------------------------------------
# Database engine — created at startup from env / .env
# ---------------------------------------------------------------------------

_engine: Any = None
_session_factory: Any = None


def _get_database_url() -> str:
    """Read DATABASE_URL from environment, falling back to app config."""
    url = os.environ.get("DATABASE_URL") or os.environ.get("database_url")
    if url:
        # Convert sync URLs to async if necessary
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql+asyncpg://", 1)
        return url

    # Try to load from .env file in project root
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key in ("DATABASE_URL", "database_url"):
                if val.startswith("postgresql://"):
                    val = val.replace("postgresql://", "postgresql+asyncpg://", 1)
                elif val.startswith("postgresql+psycopg2://"):
                    val = val.replace("postgresql+psycopg2://", "postgresql+asyncpg://", 1)
                return val

    # Fall back to app settings
    try:
        from app.config import get_settings  # noqa: PLC0415

        settings = get_settings()
        return settings.database_url
    except Exception:
        pass

    return "postgresql+asyncpg://jip_admin:password@localhost:5432/data_engine"


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[type-arg]
    """Configure logging and create the DB engine on startup."""
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

    global _engine, _session_factory
    db_url = _get_database_url()
    _engine = create_async_engine(
        db_url,
        pool_size=3,
        max_overflow=5,
        pool_pre_ping=True,
        echo=False,
    )
    _session_factory = async_sessionmaker(
        _engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    set_session_factory(_session_factory)
    logger.info("tracker_starting", port=8098, db_url=db_url.split("@")[-1])  # hide creds
    yield
    if _engine is not None:
        await _engine.dispose()
    logger.info("tracker_shutdown")


app = FastAPI(
    title="JIP Live Tracker",
    version="1.0.0",
    description="Live ingestion tracker dashboard for JIP Data Engine",
    docs_url="/api/docs",
    redoc_url=None,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(computation_router)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_ist(dt: Optional[datetime]) -> Optional[str]:
    """Return ISO-8601 string in IST for a datetime, or None."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST).isoformat()


def _duration_seconds(
    started_at: Optional[datetime],
    completed_at: Optional[datetime],
) -> Optional[float]:
    """Return elapsed seconds between two datetimes, or None."""
    if started_at is None:
        return None
    end = completed_at or datetime.now(tz=timezone.utc)
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    delta = end - started_at
    return round(delta.total_seconds(), 1)


def _row_to_dict(row: Any) -> dict[str, Any]:
    """Convert a SQLAlchemy Row to a serialisable dict."""
    started_at: Optional[datetime] = row.started_at
    completed_at: Optional[datetime] = row.completed_at
    business_date = row.business_date

    return {
        "name": row.pipeline_name,
        "status": row.status,
        "rows_processed": row.rows_processed,
        "rows_failed": row.rows_failed,
        "duration_seconds": _duration_seconds(started_at, completed_at),
        "started_at": _format_ist(started_at),
        "completed_at": _format_ist(completed_at),
        "business_date": business_date.isoformat() if business_date is not None else None,
        "error_detail": row.error_detail,
    }


async def _query_latest_for_pipeline(
    session: AsyncSession,
    pipeline_name: str,
) -> Optional[dict[str, Any]]:
    """Return the most recent log row for the given pipeline, or None."""
    stmt = sa.text(
        """
        SELECT pipeline_name, status, rows_processed, rows_failed,
               started_at, completed_at, business_date, error_detail
        FROM de_pipeline_log
        WHERE pipeline_name = :name
        ORDER BY started_at DESC NULLS LAST
        LIMIT 1
        """
    )
    result = await session.execute(stmt, {"name": pipeline_name})
    row = result.fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


async def _query_recent_runs(session: AsyncSession) -> list[dict[str, Any]]:
    """Return all pipeline runs started in the last 24 hours, most recent first."""
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=24)
    stmt = sa.text(
        """
        SELECT pipeline_name, status, rows_processed, rows_failed,
               started_at, completed_at, business_date, error_detail
        FROM de_pipeline_log
        WHERE started_at >= :cutoff
        ORDER BY started_at DESC
        LIMIT 200
        """
    )
    result = await session.execute(stmt, {"cutoff": cutoff})
    rows = result.fetchall()
    return [_row_to_dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    """Serve the tracker HTML page."""
    html_path = TRACKER_DIR / "tracker.html"
    if not html_path.exists():
        from fastapi import HTTPException  # noqa: PLC0415

        raise HTTPException(status_code=404, detail="tracker.html not found")
    return FileResponse(str(html_path), media_type="text/html")


@app.get("/api/status")
async def get_status() -> JSONResponse:
    """Return latest pipeline run status for target and recent pipelines.

    Response shape::

        {
            "target_pipelines": [...],
            "recent_runs": [...],
            "server_time_ist": "2026-04-06T14:30:15+05:30"
        }
    """
    if _session_factory is None:
        return JSONResponse(
            status_code=503,
            content={"error": "Database not initialised"},
        )

    server_time_ist = datetime.now(tz=IST).isoformat()

    target_results: list[dict[str, Any]] = []
    recent_runs: list[dict[str, Any]] = []

    try:
        async with _session_factory() as session:
            for pipeline_cfg in TARGET_PIPELINES:
                row = await _query_latest_for_pipeline(session, pipeline_cfg["name"])
                if row is not None:
                    row["display_name"] = pipeline_cfg["display_name"]
                    target_results.append(row)
                else:
                    # Pipeline has never run — show pending placeholder
                    target_results.append(
                        {
                            "name": pipeline_cfg["name"],
                            "display_name": pipeline_cfg["display_name"],
                            "status": "pending",
                            "rows_processed": None,
                            "rows_failed": None,
                            "duration_seconds": None,
                            "started_at": None,
                            "completed_at": None,
                            "business_date": None,
                            "error_detail": None,
                        }
                    )

            recent_runs = await _query_recent_runs(session)

        logger.info(
            "status_fetched",
            target_count=len(target_results),
            recent_count=len(recent_runs),
        )

    except Exception as exc:
        logger.error("status_fetch_error", error=str(exc))
        return JSONResponse(
            status_code=500,
            content={
                "error": str(exc),
                "target_pipelines": target_results,
                "recent_runs": recent_runs,
                "server_time_ist": server_time_ist,
            },
        )

    return JSONResponse(
        content={
            "target_pipelines": target_results,
            "recent_runs": recent_runs,
            "server_time_ist": server_time_ist,
        }
    )


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe for the tracker service."""
    return {"status": "healthy", "service": "tracker", "version": "1.0.0"}
