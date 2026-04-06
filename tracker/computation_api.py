"""Computation QA API — serves data for computation_tracker.html."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import sqlalchemy as sa
import structlog
from fastapi import APIRouter
from fastapi.responses import FileResponse, JSONResponse

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/computation", tags=["computation"])

IST = timezone(timedelta(hours=5, minutes=30))

# ---------------------------------------------------------------------------
# Session factory — injected by app.py at startup
# ---------------------------------------------------------------------------

_get_session: Any = None


def set_session_factory(factory: Any) -> None:
    """Inject the session factory created by the tracker app lifespan."""
    global _get_session
    _get_session = factory


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

COMPUTATION_STEPS = [
    "technicals",
    "rs",
    "breadth",
    "regime",
    "sectors",
    "fund_derived",
]


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
    return round((end - started_at).total_seconds(), 1)


def _parse_json_field(raw: Any) -> Any:
    """Safely parse a JSON field that may already be a dict/list or a string."""
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def _map_status(status: str) -> str:
    """Normalise pipeline log statuses to QA tracker vocabulary."""
    mapping = {
        "success": "passed",
        "completed": "passed",
        "failed": "failed",
        "error": "failed",
        "running": "running",
        "pending": "pending",
        "warning": "warning",
        "partial": "warning",
    }
    return mapping.get(status.lower(), status.lower()) if status else "pending"


def _build_pipeline_step(
    step: str,
    row: Any,
) -> dict[str, Any]:
    """Build a pipeline_status entry from a de_pipeline_log row (or None)."""
    if row is None:
        return {
            "step": step,
            "status": "pending",
            "rows": None,
            "duration_s": None,
            "started_at": None,
            "completed_at": None,
            "errors": [],
        }
    track_status = _parse_json_field(row.track_status) or {}
    errors: list[str] = []
    if row.error_detail:
        errors = [row.error_detail] if isinstance(row.error_detail, str) else list(row.error_detail)

    return {
        "step": step,
        "status": _map_status(row.status or ""),
        "rows": row.rows_processed,
        "duration_s": _duration_seconds(row.started_at, row.completed_at),
        "started_at": _format_ist(row.started_at),
        "completed_at": _format_ist(row.completed_at),
        "errors": errors,
        "track_status": track_status,
    }


def _extract_qa_checks(track_status: dict[str, Any], phase: str) -> list[dict[str, Any]]:
    """Extract QA check entries from a track_status JSONB blob."""
    checks: list[dict[str, Any]] = []
    raw = track_status.get(phase) or track_status.get("checks") or []
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            checks.append(
                {
                    "check": item.get("check") or item.get("name") or "unknown",
                    "status": _map_status(item.get("status", "pending")),
                    "severity": item.get("severity", "info"),
                    "message": item.get("message") or item.get("msg") or "",
                    "details": item.get("details") or {},
                }
            )
    return checks


def _extract_spot_checks(track_status: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract spot-check validation results from track_status."""
    raw = track_status.get("spot_checks") or []
    results = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        results.append(
            {
                "symbol": item.get("symbol", ""),
                "metric": item.get("metric", ""),
                "computed": str(item.get("computed", "")),
                "expected": str(item.get("expected", "")),
                "source": item.get("source", ""),
                "deviation_pct": str(item.get("deviation_pct", "")),
                "status": item.get("status", "unknown"),
            }
        )
    return results


def _extract_mstar_crossval(track_status: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract Morningstar cross-validation results from track_status."""
    raw = track_status.get("mstar_crossval") or track_status.get("morningstar") or []
    results = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        results.append(
            {
                "fund": item.get("fund") or item.get("fund_name", ""),
                "mstar_id": item.get("mstar_id", ""),
                "metric": item.get("metric", ""),
                "ours": str(item.get("ours") or item.get("our_value", "")),
                "morningstar": str(item.get("morningstar") or item.get("mstar_value", "")),
                "deviation_pct": str(item.get("deviation_pct", "")),
                "tolerance_pct": str(item.get("tolerance_pct", "15")),
                "status": item.get("status", "unknown"),
            }
        )
    return results


# ---------------------------------------------------------------------------
# Database queries
# ---------------------------------------------------------------------------


async def _fetch_latest_pipeline(session: Any, pipeline_name: str) -> Any:
    """Return the most recent de_pipeline_log row for a given pipeline_name."""
    stmt = sa.text(
        """
        SELECT pipeline_name, status, rows_processed, rows_failed,
               started_at, completed_at, business_date, error_detail,
               track_status
        FROM de_pipeline_log
        WHERE pipeline_name = :name
        ORDER BY started_at DESC NULLS LAST
        LIMIT 1
        """
    )
    result = await session.execute(stmt, {"name": pipeline_name})
    return result.fetchone()


async def _fetch_recent_anomalies(session: Any) -> list[dict[str, Any]]:
    """Return recent unresolved anomalies from de_data_anomalies."""
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=48)
    stmt = sa.text(
        """
        SELECT anomaly_type, entity_id, entity_type, value, severity,
               detected_at, notes
        FROM de_data_anomalies
        WHERE is_resolved = false
          AND detected_at >= :cutoff
        ORDER BY detected_at DESC
        LIMIT 100
        """
    )
    try:
        result = await session.execute(stmt, {"cutoff": cutoff})
        rows = result.fetchall()
    except Exception as exc:
        logger.warning("anomaly_query_failed", error=str(exc))
        return []

    bad_data = []
    for r in rows:
        bad_data.append(
            {
                "type": r.anomaly_type or "unknown",
                "symbol": r.entity_id or r.entity_type or "",
                "value": str(r.value) if r.value is not None else "",
                "severity": r.severity or "medium",
                "flagged_at": _format_ist(r.detected_at),
                "notes": r.notes or "",
            }
        )
    return bad_data


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/tracker", include_in_schema=False)
async def computation_tracker_page() -> FileResponse:
    """Serve the computation tracker HTML page."""
    html_path = Path(__file__).parent / "computation_tracker.html"
    if not html_path.exists():
        from fastapi import HTTPException  # noqa: PLC0415

        raise HTTPException(status_code=404, detail="computation_tracker.html not found")
    return FileResponse(str(html_path), media_type="text/html")


@router.get("/status")
async def get_computation_status() -> JSONResponse:
    """Return computation pipeline status, QA results, spot-checks, and cross-validation.

    Response shape::

        {
            "business_date": "2026-04-04",
            "server_time_ist": "2026-04-06T14:30:15+05:30",
            "pipeline_status": [...],
            "pre_qa": [...],
            "post_qa": [...],
            "spot_checks": [...],
            "mstar_crossval": [...],
            "bad_data": [...]
        }
    """
    if _get_session is None:
        return JSONResponse(
            status_code=503,
            content={"error": "Database not initialised"},
        )

    server_time_ist = datetime.now(tz=IST).isoformat()

    pipeline_status: list[dict[str, Any]] = []
    pre_qa: list[dict[str, Any]] = []
    post_qa: list[dict[str, Any]] = []
    spot_checks: list[dict[str, Any]] = []
    mstar_crossval: list[dict[str, Any]] = []
    bad_data: list[dict[str, Any]] = []
    business_date: Optional[str] = None

    try:
        async with _get_session() as session:
            # 1. Pipeline step statuses (individual step pipelines)
            for step in COMPUTATION_STEPS:
                row = await _fetch_latest_pipeline(session, f"computation_{step}")
                pipeline_status.append(_build_pipeline_step(step, row))

            # 2. Pre-QA: look for computation_pre_qa or fall back to computation_runner track_status
            pre_qa_row = await _fetch_latest_pipeline(session, "computation_pre_qa")
            if pre_qa_row is not None:
                ts = _parse_json_field(pre_qa_row.track_status) or {}
                pre_qa = _extract_qa_checks(ts, "pre_qa")
                if pre_qa_row.business_date is not None:
                    business_date = pre_qa_row.business_date.isoformat()

            # 3. Post-QA
            post_qa_row = await _fetch_latest_pipeline(session, "computation_post_qa")
            if post_qa_row is not None:
                ts = _parse_json_field(post_qa_row.track_status) or {}
                post_qa = _extract_qa_checks(ts, "post_qa")

            # 4. Spot checks
            spot_check_row = await _fetch_latest_pipeline(session, "computation_spot_check")
            if spot_check_row is not None:
                ts = _parse_json_field(spot_check_row.track_status) or {}
                spot_checks = _extract_spot_checks(ts)

            # 5. Morningstar cross-validation
            mstar_row = await _fetch_latest_pipeline(session, "computation_mstar_xval")
            if mstar_row is not None:
                ts = _parse_json_field(mstar_row.track_status) or {}
                mstar_crossval = _extract_mstar_crossval(ts)

            # 6. Fall back: try the computation_runner omnibus row for any missing data
            runner_row = await _fetch_latest_pipeline(session, "computation_runner")
            if runner_row is not None:
                runner_ts = _parse_json_field(runner_row.track_status) or {}
                if not pre_qa:
                    pre_qa = _extract_qa_checks(runner_ts, "pre_qa")
                if not post_qa:
                    post_qa = _extract_qa_checks(runner_ts, "post_qa")
                if not spot_checks:
                    spot_checks = _extract_spot_checks(runner_ts)
                if not mstar_crossval:
                    mstar_crossval = _extract_mstar_crossval(runner_ts)
                if business_date is None and runner_row.business_date is not None:
                    business_date = runner_row.business_date.isoformat()
                # Fill in step statuses from runner track_status if individual rows missing
                steps_ts = runner_ts.get("steps") or {}
                for i, step_entry in enumerate(pipeline_status):
                    if step_entry["status"] == "pending" and step_entry["step"] in steps_ts:
                        step_data = steps_ts[step_entry["step"]]
                        if isinstance(step_data, dict):
                            pipeline_status[i]["status"] = _map_status(
                                step_data.get("status", "pending")
                            )
                            pipeline_status[i]["rows"] = step_data.get("rows")
                            pipeline_status[i]["duration_s"] = step_data.get("duration_s")

            # 7. Bad data anomalies
            bad_data = await _fetch_recent_anomalies(session)

        # Determine business_date from first available pipeline if still None
        if business_date is None:
            for step_entry in pipeline_status:
                started = step_entry.get("started_at")
                if started:
                    try:
                        dt = datetime.fromisoformat(started)
                        business_date = dt.astimezone(IST).date().isoformat()
                    except ValueError:
                        pass
                    break

        logger.info(
            "computation_status_fetched",
            steps=len(pipeline_status),
            pre_qa=len(pre_qa),
            post_qa=len(post_qa),
            spot_checks=len(spot_checks),
            mstar_crossval=len(mstar_crossval),
            bad_data=len(bad_data),
        )

    except Exception as exc:
        logger.error("computation_status_error", error=str(exc))
        return JSONResponse(
            status_code=500,
            content={
                "error": str(exc),
                "business_date": business_date,
                "server_time_ist": server_time_ist,
                "pipeline_status": pipeline_status,
                "pre_qa": pre_qa,
                "post_qa": post_qa,
                "spot_checks": spot_checks,
                "mstar_crossval": mstar_crossval,
                "bad_data": bad_data,
            },
        )

    return JSONResponse(
        content={
            "business_date": business_date,
            "server_time_ist": server_time_ist,
            "pipeline_status": pipeline_status,
            "pre_qa": pre_qa,
            "post_qa": post_qa,
            "spot_checks": spot_checks,
            "mstar_crossval": mstar_crossval,
            "bad_data": bad_data,
        }
    )
