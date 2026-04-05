"""Thin API layer for the dashboard — fetches data from the main engine (port 8010).

All endpoints are GET-only and proxy/aggregate data from localhost:8010/api/v1/admin/*.
No direct DB access; the dashboard is a pure consumer of the main engine's admin API.
"""

from __future__ import annotations

import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

import httpx
import structlog

logger = structlog.get_logger(__name__)

# Main engine base URL — overridable via env for tests
ENGINE_BASE_URL: str = os.environ.get("ENGINE_BASE_URL", "http://localhost:8010")
ENGINE_TIMEOUT: float = float(os.environ.get("ENGINE_TIMEOUT", "10.0"))

# SLA deadlines in HH:MM (IST)
SLA_DEADLINES: dict[str, str] = {
    "pre_market": "08:00",
    "equity_eod": "19:30",
    "mf_nav": "22:30",
    "fii_dii": "20:00",
    "rs": "23:00",
    "regime": "23:30",
}

# Canonical pipeline → SLA key mapping
PIPELINE_SLA_MAP: dict[str, str] = {
    "bhav_copy": "equity_eod",
    "corporate_actions": "equity_eod",
    "nse_indices": "equity_eod",
    "amfi_nav": "mf_nav",
    "mf_master": "mf_nav",
    "fii_dii_flows": "fii_dii",
    "global_equities": "equity_eod",
    "fred_macro": "equity_eod",
    "rs_computation": "rs",
    "regime_detection": "regime",
    "breadth_computation": "rs",
    "pre_market_scan": "pre_market",
}


def _get_client() -> httpx.AsyncClient:
    """Return a configured async HTTP client pointing at the main engine."""
    return httpx.AsyncClient(
        base_url=ENGINE_BASE_URL,
        timeout=ENGINE_TIMEOUT,
        headers={"Content-Type": "application/json"},
    )


async def fetch_pipeline_runs(
    business_date: Optional[date] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> dict[str, Any]:
    """Fetch pipeline run logs from the main engine.

    Returns raw JSON from GET /api/v1/admin/pipeline-runs.
    Falls back to an empty structure on connection error so the dashboard
    degrades gracefully when the engine is unreachable.
    """
    params: dict[str, str] = {}
    if business_date is not None:
        params["business_date"] = business_date.isoformat()
    if date_from is not None:
        params["date_from"] = date_from.isoformat()
    if date_to is not None:
        params["date_to"] = date_to.isoformat()

    async with _get_client() as client:
        try:
            resp = await client.get("/api/v1/admin/pipeline-runs", params=params)
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()
            logger.info("fetch_pipeline_runs_ok", status=resp.status_code, params=params)
            return data
        except httpx.HTTPStatusError as exc:
            logger.error(
                "fetch_pipeline_runs_http_error",
                status=exc.response.status_code,
                detail=exc.response.text[:200],
            )
            return {"runs": [], "error": str(exc)}
        except httpx.RequestError as exc:
            logger.error("fetch_pipeline_runs_connection_error", error=str(exc))
            return {"runs": [], "error": "Engine unreachable"}


async def fetch_anomalies(
    business_date: Optional[date] = None,
    severity: Optional[str] = None,
) -> dict[str, Any]:
    """Fetch today's anomalies from GET /api/v1/admin/anomalies."""
    params: dict[str, str] = {}
    if business_date is not None:
        params["business_date"] = business_date.isoformat()
    if severity is not None:
        params["severity"] = severity

    async with _get_client() as client:
        try:
            resp = await client.get("/api/v1/admin/anomalies", params=params)
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()
            logger.info("fetch_anomalies_ok", status=resp.status_code)
            return data
        except httpx.HTTPStatusError as exc:
            logger.error("fetch_anomalies_http_error", status=exc.response.status_code)
            return {"anomalies": [], "error": str(exc)}
        except httpx.RequestError as exc:
            logger.error("fetch_anomalies_connection_error", error=str(exc))
            return {"anomalies": [], "error": "Engine unreachable"}


async def fetch_system_health() -> dict[str, Any]:
    """Fetch system health from GET /api/v1/admin/health."""
    async with _get_client() as client:
        try:
            resp = await client.get("/api/v1/admin/health")
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()
            logger.info("fetch_system_health_ok", status=resp.status_code)
            return data
        except httpx.HTTPStatusError as exc:
            logger.error("fetch_system_health_http_error", status=exc.response.status_code)
            return {"healthy": False, "error": str(exc)}
        except httpx.RequestError as exc:
            logger.error("fetch_system_health_connection_error", error=str(exc))
            return {"healthy": False, "error": "Engine unreachable"}


def compute_sla_status(
    pipeline_name: str,
    completed_at: Optional[datetime],
    status: str,
    reference_date: Optional[date] = None,
) -> dict[str, Any]:
    """Compute SLA status for a single pipeline run.

    Args:
        pipeline_name: Canonical pipeline identifier.
        completed_at: Timezone-aware datetime when the run finished, or None.
        status: Pipeline run status string.
        reference_date: The business date; defaults to today (IST).

    Returns:
        Dict with keys: sla_key, deadline_str, met (bool | None), overdue_minutes (int | None).
    """
    sla_key = PIPELINE_SLA_MAP.get(pipeline_name)
    if sla_key is None:
        return {"sla_key": None, "deadline_str": None, "met": None, "overdue_minutes": None}

    deadline_str = SLA_DEADLINES[sla_key]
    hh, mm = (int(x) for x in deadline_str.split(":"))

    # Build IST deadline datetime
    import zoneinfo  # stdlib 3.9+

    ist = zoneinfo.ZoneInfo("Asia/Kolkata")
    if reference_date is None:
        reference_date = datetime.now(tz=ist).date()

    deadline_dt = datetime(
        reference_date.year,
        reference_date.month,
        reference_date.day,
        hh,
        mm,
        0,
        tzinfo=ist,
    )

    if status in ("running", "pending"):
        # Check if already overdue
        now_ist = datetime.now(tz=ist)
        if now_ist > deadline_dt:
            overdue = int((now_ist - deadline_dt).total_seconds() / 60)
            return {
                "sla_key": sla_key,
                "deadline_str": deadline_str,
                "met": False,
                "overdue_minutes": overdue,
            }
        return {"sla_key": sla_key, "deadline_str": deadline_str, "met": None, "overdue_minutes": None}

    if completed_at is None:
        return {"sla_key": sla_key, "deadline_str": deadline_str, "met": None, "overdue_minutes": None}

    # Ensure completed_at is timezone-aware
    if completed_at.tzinfo is None:
        completed_at = completed_at.replace(tzinfo=ist)
    else:
        completed_at = completed_at.astimezone(ist)

    met = completed_at <= deadline_dt
    overdue_minutes: Optional[int] = None
    if not met:
        overdue_minutes = int((completed_at - deadline_dt).total_seconds() / 60)

    return {
        "sla_key": sla_key,
        "deadline_str": deadline_str,
        "met": met,
        "overdue_minutes": overdue_minutes,
    }


def format_indian_number(value: int | Decimal) -> str:
    """Format an integer/Decimal using Indian lakh/crore notation.

    Examples:
        1234 -> "1,234"
        123456 -> "1,23,456"
        12345678 -> "1,23,45,678"
    """
    n = int(value)
    negative = n < 0
    n = abs(n)
    s = str(n)
    if len(s) <= 3:
        result = s
    else:
        # Last 3 digits, then groups of 2
        last3 = s[-3:]
        rest = s[:-3]
        groups: list[str] = []
        while len(rest) > 2:
            groups.append(rest[-2:])
            rest = rest[:-2]
        if rest:
            groups.append(rest)
        groups.reverse()
        result = ",".join(groups) + "," + last3
    return ("-" if negative else "") + result


def format_ist_datetime(dt: datetime) -> str:
    """Return a DD-MMM-YYYY HH:MM IST string from a timezone-aware datetime."""
    import zoneinfo

    ist = zoneinfo.ZoneInfo("Asia/Kolkata")
    local = dt.astimezone(ist)
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    return f"{local.day:02d}-{months[local.month - 1]}-{local.year} {local.hour:02d}:{local.minute:02d} IST"


def format_ist_date(d: date) -> str:
    """Return a DD-MMM-YYYY string from a date object."""
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    return f"{d.day:02d}-{months[d.month - 1]}-{d.year}"
