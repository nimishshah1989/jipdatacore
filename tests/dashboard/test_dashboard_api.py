"""Tests for dashboard API endpoints and helper functions.

Covers:
- GET /health
- GET /api/pipeline-runs
- GET /api/anomalies (including severity validation)
- GET /api/system-health
- GET /api/sla-config
- GET / (HTML page)
- compute_sla_status()
- format_indian_number()
- format_ist_datetime() / format_ist_date()
- fetch_* functions (mocked httpx)
"""

from __future__ import annotations

import zoneinfo
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

IST = zoneinfo.ZoneInfo("Asia/Kolkata")


# ---------------------------------------------------------------------------
# App fixture
# ---------------------------------------------------------------------------

@pytest.fixture
async def client():
    """Async client for the dashboard app (no real engine connection)."""
    from dashboard.app import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ---------------------------------------------------------------------------
# Helper: fake engine responses
# ---------------------------------------------------------------------------

def _make_mock_response(status_code: int, payload: dict[str, Any]) -> MagicMock:
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = payload
    mock.raise_for_status = MagicMock()
    return mock


FAKE_RUNS: list[dict[str, Any]] = [
    {
        "id": 1,
        "pipeline_name": "bhav_copy",
        "business_date": "2026-04-04",
        "run_number": 1,
        "status": "success",
        "started_at": "2026-04-04T19:10:00+05:30",
        "completed_at": "2026-04-04T19:25:00+05:30",
        "rows_processed": 123456,
        "rows_failed": 0,
        "error_detail": None,
    },
    {
        "id": 2,
        "pipeline_name": "amfi_nav",
        "business_date": "2026-04-04",
        "run_number": 1,
        "status": "failed",
        "started_at": "2026-04-04T22:00:00+05:30",
        "completed_at": "2026-04-04T22:05:00+05:30",
        "rows_processed": 500,
        "rows_failed": 500,
        "error_detail": "Connection timeout",
    },
]

FAKE_ANOMALIES: list[dict[str, Any]] = [
    {
        "id": 10,
        "severity": "critical",
        "title": "Missing BHAV data",
        "detail": "NSE BHAV file not received",
        "detected_at": "2026-04-04T19:30:00+05:30",
    },
    {
        "id": 11,
        "severity": "warning",
        "title": "Slow pipeline",
        "detail": "amfi_nav took 2x expected time",
        "detected_at": "2026-04-04T22:10:00+05:30",
    },
    {
        "id": 12,
        "severity": "info",
        "title": "Holiday detected",
        "detail": "Market closed on 04-Apr-2026",
        "detected_at": "2026-04-04T08:00:00+05:30",
    },
]

FAKE_HEALTH: dict[str, Any] = {
    "healthy": True,
    "redis_ping": True,
    "db_connections": 12,
    "db_max_connections": 100,
    "disk_used_pct": 42.5,
    "disk_free_gb": 58.0,
    "engine_version": "2.0.0",
    "engine_uptime_human": "3d 4h",
}


# ---------------------------------------------------------------------------
# Tests: /health
# ---------------------------------------------------------------------------

async def test_dashboard_health_returns_200(client: AsyncClient) -> None:
    resp = await client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "healthy"
    assert body["service"] == "dashboard"


# ---------------------------------------------------------------------------
# Tests: GET /
# ---------------------------------------------------------------------------

async def test_index_returns_html(client: AsyncClient) -> None:
    resp = await client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert b"JIP Data Engine" in resp.content


# ---------------------------------------------------------------------------
# Tests: /api/sla-config
# ---------------------------------------------------------------------------

async def test_sla_config_returns_all_keys(client: AsyncClient) -> None:
    resp = await client.get("/api/sla-config")
    assert resp.status_code == 200
    body = resp.json()
    assert "deadlines" in body
    assert "pipeline_map" in body
    for key in ("pre_market", "equity_eod", "mf_nav", "fii_dii", "rs", "regime"):
        assert key in body["deadlines"]


# ---------------------------------------------------------------------------
# Tests: /api/pipeline-runs
# ---------------------------------------------------------------------------

@patch("dashboard.app.fetch_pipeline_runs")
async def test_pipeline_runs_returns_enriched(mock_fetch: AsyncMock, client: AsyncClient) -> None:
    mock_fetch.return_value = {"runs": FAKE_RUNS, "error": None}
    resp = await client.get("/api/pipeline-runs")
    assert resp.status_code == 200
    body = resp.json()
    runs = body["runs"]
    assert len(runs) == 2

    # Enrichment: sla and display keys present
    assert "sla" in runs[0]
    assert "display" in runs[0]

    # bhav_copy succeeded at 19:25 IST, SLA deadline 19:30 IST → met
    assert runs[0]["sla"]["sla_key"] == "equity_eod"

    # display keys formatted
    assert runs[0]["display"]["rows_processed_fmt"] == "1,23,456"
    assert "2026" in runs[0]["display"]["business_date_fmt"]


@patch("dashboard.app.fetch_pipeline_runs")
async def test_pipeline_runs_with_date_param(mock_fetch: AsyncMock, client: AsyncClient) -> None:
    mock_fetch.return_value = {"runs": [], "error": None}
    resp = await client.get("/api/pipeline-runs?business_date=2026-04-04")
    assert resp.status_code == 200
    mock_fetch.assert_called_once()
    call_kwargs = mock_fetch.call_args.kwargs
    assert call_kwargs["business_date"] == date(2026, 4, 4)


@patch("dashboard.app.fetch_pipeline_runs")
async def test_pipeline_runs_engine_error_returns_gracefully(
    mock_fetch: AsyncMock, client: AsyncClient
) -> None:
    mock_fetch.return_value = {"runs": [], "error": "Engine unreachable"}
    resp = await client.get("/api/pipeline-runs")
    assert resp.status_code == 200
    body = resp.json()
    assert body["error"] == "Engine unreachable"
    assert body["runs"] == []


# ---------------------------------------------------------------------------
# Tests: /api/anomalies
# ---------------------------------------------------------------------------

@patch("dashboard.app.fetch_anomalies")
async def test_anomalies_groups_by_severity(mock_fetch: AsyncMock, client: AsyncClient) -> None:
    mock_fetch.return_value = {"anomalies": FAKE_ANOMALIES, "error": None}
    resp = await client.get("/api/anomalies")
    assert resp.status_code == 200
    body = resp.json()
    assert body["counts"]["critical"] == 1
    assert body["counts"]["warning"] == 1
    assert body["counts"]["info"] == 1
    assert body["total"] == 3


@patch("dashboard.app.fetch_anomalies")
async def test_anomalies_filter_by_severity(mock_fetch: AsyncMock, client: AsyncClient) -> None:
    mock_fetch.return_value = {"anomalies": FAKE_ANOMALIES, "error": None}
    resp = await client.get("/api/anomalies?severity=critical")
    assert resp.status_code == 200
    mock_fetch.assert_called_once()
    assert mock_fetch.call_args.kwargs["severity"] == "critical"


async def test_anomalies_invalid_severity_returns_422(client: AsyncClient) -> None:
    resp = await client.get("/api/anomalies?severity=urgent")
    assert resp.status_code == 422


@patch("dashboard.app.fetch_anomalies")
async def test_anomalies_unknown_severity_bucketed_to_info(
    mock_fetch: AsyncMock, client: AsyncClient
) -> None:
    mock_fetch.return_value = {
        "anomalies": [{"id": 99, "severity": "unknown", "title": "?"}],
        "error": None,
    }
    resp = await client.get("/api/anomalies")
    assert resp.status_code == 200
    body = resp.json()
    assert body["counts"]["info"] == 1


# ---------------------------------------------------------------------------
# Tests: /api/system-health
# ---------------------------------------------------------------------------

@patch("dashboard.app.fetch_system_health")
async def test_system_health_proxied(mock_fetch: AsyncMock, client: AsyncClient) -> None:
    mock_fetch.return_value = FAKE_HEALTH
    resp = await client.get("/api/system-health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["redis_ping"] is True
    assert body["db_connections"] == 12


@patch("dashboard.app.fetch_system_health")
async def test_system_health_engine_down(mock_fetch: AsyncMock, client: AsyncClient) -> None:
    mock_fetch.return_value = {"healthy": False, "error": "Engine unreachable"}
    resp = await client.get("/api/system-health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["healthy"] is False


# ---------------------------------------------------------------------------
# Unit tests: compute_sla_status
# ---------------------------------------------------------------------------

def test_compute_sla_status_met() -> None:
    from dashboard.api import compute_sla_status

    completed = datetime(2026, 4, 4, 19, 25, 0, tzinfo=IST)  # 19:25 < 19:30
    result = compute_sla_status("bhav_copy", completed, "success", date(2026, 4, 4))
    assert result["met"] is True
    assert result["sla_key"] == "equity_eod"
    assert result["deadline_str"] == "19:30"
    assert result["overdue_minutes"] is None


def test_compute_sla_status_missed() -> None:
    from dashboard.api import compute_sla_status

    completed = datetime(2026, 4, 4, 20, 0, 0, tzinfo=IST)  # 20:00 > 19:30 → +30m
    result = compute_sla_status("bhav_copy", completed, "success", date(2026, 4, 4))
    assert result["met"] is False
    assert result["overdue_minutes"] == 30


def test_compute_sla_status_no_completed_at_past_deadline() -> None:
    """Pending pipeline with no completed_at for a past business date is overdue."""
    from dashboard.api import compute_sla_status

    # April 4 deadline (19:30 IST) is in the past as of today (2026-04-05)
    result = compute_sla_status("bhav_copy", None, "pending", date(2026, 4, 4))
    # Should be overdue (met=False) or still in-flight (met=None) depending on wall clock
    # We only assert that met is not True (pipeline never completed)
    assert result["met"] is not True
    assert result["sla_key"] == "equity_eod"


def test_compute_sla_status_no_completed_at_future_date() -> None:
    """Pending pipeline for a future business date with deadline not yet reached."""
    from dashboard.api import compute_sla_status

    # Use a far-future date so the deadline is always in the future
    future_date = date(2030, 12, 31)
    result = compute_sla_status("bhav_copy", None, "pending", future_date)
    # Deadline not reached yet — met should be None (in-flight)
    assert result["met"] is None
    assert result["sla_key"] == "equity_eod"


def test_compute_sla_status_unknown_pipeline() -> None:
    from dashboard.api import compute_sla_status

    result = compute_sla_status("nonexistent_pipeline", None, "success", date(2026, 4, 4))
    assert result["sla_key"] is None
    assert result["met"] is None


def test_compute_sla_status_naive_datetime_treated_as_ist() -> None:
    from dashboard.api import compute_sla_status

    # Naive datetime before deadline — should be treated as IST and marked met
    naive_completed = datetime(2026, 4, 4, 19, 0, 0)  # naive, 19:00
    result = compute_sla_status("bhav_copy", naive_completed, "success", date(2026, 4, 4))
    assert result["met"] is True


def test_compute_sla_status_mf_nav() -> None:
    from dashboard.api import compute_sla_status

    completed = datetime(2026, 4, 4, 22, 15, 0, tzinfo=IST)  # 22:15 < 22:30
    result = compute_sla_status("amfi_nav", completed, "success", date(2026, 4, 4))
    assert result["met"] is True
    assert result["sla_key"] == "mf_nav"


# ---------------------------------------------------------------------------
# Unit tests: format_indian_number
# ---------------------------------------------------------------------------

def test_format_indian_number_small() -> None:
    from dashboard.api import format_indian_number

    assert format_indian_number(999) == "999"
    assert format_indian_number(1000) == "1,000"


def test_format_indian_number_lakh() -> None:
    from dashboard.api import format_indian_number

    assert format_indian_number(100000) == "1,00,000"
    assert format_indian_number(123456) == "1,23,456"


def test_format_indian_number_crore() -> None:
    from dashboard.api import format_indian_number

    assert format_indian_number(12345678) == "1,23,45,678"


def test_format_indian_number_negative() -> None:
    from dashboard.api import format_indian_number

    assert format_indian_number(-123456) == "-1,23,456"


def test_format_indian_number_decimal() -> None:
    from dashboard.api import format_indian_number

    assert format_indian_number(Decimal("1000000")) == "10,00,000"


def test_format_indian_number_zero() -> None:
    from dashboard.api import format_indian_number

    assert format_indian_number(0) == "0"


# ---------------------------------------------------------------------------
# Unit tests: format_ist_datetime / format_ist_date
# ---------------------------------------------------------------------------

def test_format_ist_datetime_utc_input() -> None:
    from dashboard.api import format_ist_datetime
    import zoneinfo

    utc = zoneinfo.ZoneInfo("UTC")
    dt = datetime(2026, 4, 4, 13, 30, 0, tzinfo=utc)  # 13:30 UTC = 19:00 IST
    result = format_ist_datetime(dt)
    assert "04-Apr-2026" in result
    assert "19:00 IST" in result


def test_format_ist_date_known() -> None:
    from dashboard.api import format_ist_date

    d = date(2026, 4, 4)
    assert format_ist_date(d) == "04-Apr-2026"


def test_format_ist_date_jan() -> None:
    from dashboard.api import format_ist_date

    d = date(2026, 1, 1)
    assert format_ist_date(d) == "01-Jan-2026"


# ---------------------------------------------------------------------------
# Unit tests: fetch_* functions (httpx mocking)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_pipeline_runs_success() -> None:
    from dashboard.api import fetch_pipeline_runs

    mock_resp = _make_mock_response(200, {"runs": FAKE_RUNS})

    with patch("dashboard.api._get_client") as mock_client_factory:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client_factory.return_value = mock_client

        result = await fetch_pipeline_runs(business_date=date(2026, 4, 4))
        assert result["runs"] == FAKE_RUNS
        mock_client.get.assert_called_once()
        args, kwargs = mock_client.get.call_args
        assert kwargs["params"]["business_date"] == "2026-04-04"


@pytest.mark.asyncio
async def test_fetch_pipeline_runs_connection_error() -> None:
    import httpx
    from dashboard.api import fetch_pipeline_runs

    with patch("dashboard.api._get_client") as mock_client_factory:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(side_effect=httpx.ConnectError("refused"))
        mock_client_factory.return_value = mock_client

        result = await fetch_pipeline_runs()
        assert result["runs"] == []
        assert "error" in result


@pytest.mark.asyncio
async def test_fetch_anomalies_success() -> None:
    from dashboard.api import fetch_anomalies

    payload = {"anomalies": FAKE_ANOMALIES}
    mock_resp = _make_mock_response(200, payload)

    with patch("dashboard.api._get_client") as mock_client_factory:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client_factory.return_value = mock_client

        result = await fetch_anomalies()
        assert len(result["anomalies"]) == 3


@pytest.mark.asyncio
async def test_fetch_system_health_success() -> None:
    from dashboard.api import fetch_system_health

    mock_resp = _make_mock_response(200, FAKE_HEALTH)

    with patch("dashboard.api._get_client") as mock_client_factory:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client_factory.return_value = mock_client

        result = await fetch_system_health()
        assert result["redis_ping"] is True


@pytest.mark.asyncio
async def test_fetch_system_health_http_error() -> None:
    import httpx
    from dashboard.api import fetch_system_health

    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.text = "Internal Server Error"
    exc = httpx.HTTPStatusError("500", request=MagicMock(), response=mock_resp)

    with patch("dashboard.api._get_client") as mock_client_factory:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_resp_ok = MagicMock()
        mock_resp_ok.raise_for_status = MagicMock(side_effect=exc)
        mock_resp_ok.json = MagicMock(return_value={})
        mock_client.get = AsyncMock(return_value=mock_resp_ok)
        mock_client_factory.return_value = mock_client

        result = await fetch_system_health()
        assert result["healthy"] is False
        assert "error" in result
