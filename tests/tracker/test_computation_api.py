"""Tests for tracker/computation_api.py."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

from tracker.computation_api import (
    IST,
    _build_pipeline_step,
    _duration_seconds,
    _extract_mstar_crossval,
    _extract_qa_checks,
    _extract_spot_checks,
    _format_ist,
    _map_status,
    _parse_json_field,
    router,
    set_session_factory,
)


# ---------------------------------------------------------------------------
# App fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def tracker_app() -> FastAPI:
    """Minimal FastAPI app with the computation router included."""
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
async def tracker_client(tracker_app: FastAPI) -> AsyncClient:
    async with AsyncClient(
        transport=ASGITransport(app=tracker_app), base_url="http://test"
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# Unit tests: _parse_json_field
# ---------------------------------------------------------------------------


def test_parse_json_field_with_dict_returns_dict():
    data = {"key": "value"}
    assert _parse_json_field(data) == data


def test_parse_json_field_with_list_returns_list():
    data = [1, 2, 3]
    assert _parse_json_field(data) == data


def test_parse_json_field_with_json_string_parses_correctly():
    raw = '{"check": "coverage", "status": "passed"}'
    result = _parse_json_field(raw)
    assert result == {"check": "coverage", "status": "passed"}


def test_parse_json_field_with_none_returns_none():
    assert _parse_json_field(None) is None


def test_parse_json_field_with_invalid_string_returns_none():
    assert _parse_json_field("not valid json {{{") is None


# ---------------------------------------------------------------------------
# Unit tests: _map_status
# ---------------------------------------------------------------------------


def test_map_status_success_returns_passed():
    assert _map_status("success") == "passed"


def test_map_status_completed_returns_passed():
    assert _map_status("completed") == "passed"


def test_map_status_failed_returns_failed():
    assert _map_status("failed") == "failed"


def test_map_status_error_returns_failed():
    assert _map_status("error") == "failed"


def test_map_status_running_returns_running():
    assert _map_status("running") == "running"


def test_map_status_partial_returns_warning():
    assert _map_status("partial") == "warning"


def test_map_status_unknown_string_is_lowercased():
    assert _map_status("RUNNING") == "running"


def test_map_status_pending_returns_pending():
    assert _map_status("pending") == "pending"


# ---------------------------------------------------------------------------
# Unit tests: _format_ist
# ---------------------------------------------------------------------------


def test_format_ist_with_none_returns_none():
    assert _format_ist(None) is None


def test_format_ist_with_utc_datetime_returns_ist_string():
    dt = datetime(2026, 4, 6, 9, 0, 0, tzinfo=timezone.utc)
    result = _format_ist(dt)
    assert result is not None
    # UTC 09:00 = IST 14:30
    assert "+05:30" in result


def test_format_ist_with_naive_datetime_assumes_utc():
    dt = datetime(2026, 4, 6, 9, 0, 0)  # naive
    result = _format_ist(dt)
    assert result is not None
    assert "+05:30" in result


# ---------------------------------------------------------------------------
# Unit tests: _duration_seconds
# ---------------------------------------------------------------------------


def test_duration_seconds_with_none_started_returns_none():
    assert _duration_seconds(None, None) is None


def test_duration_seconds_computes_elapsed():
    start = datetime(2026, 4, 6, 9, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 6, 9, 0, 30, tzinfo=timezone.utc)
    result = _duration_seconds(start, end)
    assert result == 30.0


def test_duration_seconds_with_no_completed_uses_now():
    start = datetime.now(tz=timezone.utc) - timedelta(seconds=10)
    result = _duration_seconds(start, None)
    assert result is not None
    assert result >= 10.0


def test_duration_seconds_naive_datetimes_handled():
    start = datetime(2026, 4, 6, 9, 0, 0)  # naive
    end = datetime(2026, 4, 6, 9, 1, 0)    # naive
    result = _duration_seconds(start, end)
    assert result == 60.0


# ---------------------------------------------------------------------------
# Unit tests: _build_pipeline_step
# ---------------------------------------------------------------------------


def test_build_pipeline_step_with_none_row_returns_pending():
    result = _build_pipeline_step("technicals", None)
    assert result["step"] == "technicals"
    assert result["status"] == "pending"
    assert result["rows"] is None
    assert result["duration_s"] is None
    assert result["errors"] == []


def test_build_pipeline_step_with_row_maps_fields():
    row = MagicMock()
    row.status = "success"
    row.rows_processed = 1847
    row.started_at = datetime(2026, 4, 6, 9, 0, 0, tzinfo=timezone.utc)
    row.completed_at = datetime(2026, 4, 6, 9, 0, 12, tzinfo=timezone.utc)
    row.error_detail = None
    row.track_status = None

    result = _build_pipeline_step("technicals", row)
    assert result["step"] == "technicals"
    assert result["status"] == "passed"
    assert result["rows"] == 1847
    assert result["duration_s"] == 12.0


def test_build_pipeline_step_error_detail_added_to_errors():
    row = MagicMock()
    row.status = "failed"
    row.rows_processed = 0
    row.started_at = datetime(2026, 4, 6, 9, 0, 0, tzinfo=timezone.utc)
    row.completed_at = datetime(2026, 4, 6, 9, 0, 5, tzinfo=timezone.utc)
    row.error_detail = "Database timeout"
    row.track_status = None

    result = _build_pipeline_step("rs", row)
    assert result["status"] == "failed"
    assert "Database timeout" in result["errors"]


# ---------------------------------------------------------------------------
# Unit tests: _extract_qa_checks
# ---------------------------------------------------------------------------


def test_extract_qa_checks_from_list_in_track_status():
    ts = {
        "pre_qa": [
            {"check": "ohlcv_coverage", "status": "success", "severity": "critical",
             "message": "1847 rows", "details": {"validated_rows": 1847}},
        ]
    }
    result = _extract_qa_checks(ts, "pre_qa")
    assert len(result) == 1
    assert result[0]["check"] == "ohlcv_coverage"
    assert result[0]["status"] == "passed"
    assert result[0]["severity"] == "critical"
    assert result[0]["message"] == "1847 rows"


def test_extract_qa_checks_empty_when_phase_missing():
    result = _extract_qa_checks({}, "pre_qa")
    assert result == []


def test_extract_qa_checks_skips_non_dict_items():
    ts = {"pre_qa": ["invalid", {"check": "c1", "status": "passed"}]}
    result = _extract_qa_checks(ts, "pre_qa")
    assert len(result) == 1
    assert result[0]["check"] == "c1"


# ---------------------------------------------------------------------------
# Unit tests: _extract_spot_checks
# ---------------------------------------------------------------------------


def test_extract_spot_checks_returns_list():
    ts = {
        "spot_checks": [
            {"symbol": "RELIANCE", "metric": "sma_50", "computed": "2847.12",
             "expected": "2851.30", "source": "yfinance",
             "deviation_pct": "0.15", "status": "match"},
        ]
    }
    result = _extract_spot_checks(ts)
    assert len(result) == 1
    assert result[0]["symbol"] == "RELIANCE"
    assert result[0]["metric"] == "sma_50"
    assert result[0]["deviation_pct"] == "0.15"
    assert result[0]["status"] == "match"


def test_extract_spot_checks_empty_when_key_missing():
    result = _extract_spot_checks({})
    assert result == []


def test_extract_spot_checks_converts_numeric_to_str():
    ts = {
        "spot_checks": [
            {"symbol": "TCS", "metric": "rsi", "computed": 62.5,
             "expected": 63.1, "source": "yfinance",
             "deviation_pct": 0.95, "status": "match"},
        ]
    }
    result = _extract_spot_checks(ts)
    assert result[0]["computed"] == "62.5"
    assert result[0]["deviation_pct"] == "0.95"


# ---------------------------------------------------------------------------
# Unit tests: _extract_mstar_crossval
# ---------------------------------------------------------------------------


def test_extract_mstar_crossval_returns_list():
    ts = {
        "mstar_crossval": [
            {"fund": "HDFC Flexi Cap", "mstar_id": "F00000Y0HH",
             "metric": "sharpe_1y", "ours": "1.23", "morningstar": "1.18",
             "deviation_pct": "4.24", "tolerance_pct": "15",
             "status": "within_tolerance"},
        ]
    }
    result = _extract_mstar_crossval(ts)
    assert len(result) == 1
    assert result[0]["fund"] == "HDFC Flexi Cap"
    assert result[0]["status"] == "within_tolerance"
    assert result[0]["tolerance_pct"] == "15"


def test_extract_mstar_crossval_falls_back_to_morningstar_key():
    ts = {
        "morningstar": [
            {"fund": "Axis Bluechip", "mstar_id": "XXX",
             "metric": "alpha_1y", "ours": "2.1", "morningstar": "2.0",
             "deviation_pct": "5.0", "tolerance_pct": "10",
             "status": "within_tolerance"},
        ]
    }
    result = _extract_mstar_crossval(ts)
    assert len(result) == 1
    assert result[0]["fund"] == "Axis Bluechip"


def test_extract_mstar_crossval_empty_when_key_missing():
    result = _extract_mstar_crossval({})
    assert result == []


# ---------------------------------------------------------------------------
# Unit tests: set_session_factory
# ---------------------------------------------------------------------------


def test_set_session_factory_sets_module_level_variable():
    """set_session_factory injects the factory into the module."""
    import tracker.computation_api as api_mod

    sentinel = object()
    set_session_factory(sentinel)
    assert api_mod._get_session is sentinel

    # Reset to None so other tests are not affected
    set_session_factory(None)


# ---------------------------------------------------------------------------
# Integration tests: API endpoints (mocked DB)
# ---------------------------------------------------------------------------


def _make_mock_row(
    pipeline_name: str = "computation_technicals",
    status: str = "success",
    rows_processed: int = 1847,
    rows_failed: int = 0,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    business_date: Any = None,
    error_detail: str | None = None,
    track_status: Any = None,
) -> MagicMock:
    row = MagicMock()
    row.pipeline_name = pipeline_name
    row.status = status
    row.rows_processed = rows_processed
    row.rows_failed = rows_failed
    row.started_at = started_at or datetime(2026, 4, 6, 9, 0, 0, tzinfo=timezone.utc)
    row.completed_at = completed_at or datetime(2026, 4, 6, 9, 0, 12, tzinfo=timezone.utc)
    row.business_date = business_date
    row.error_detail = error_detail
    row.track_status = track_status
    return row


@pytest.fixture
def mock_session_factory(monkeypatch):
    """Return a factory that yields a mock AsyncSession."""
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    session.execute = AsyncMock()

    factory = MagicMock(return_value=session)

    import tracker.computation_api as api_mod
    monkeypatch.setattr(api_mod, "_get_session", factory)
    return session


async def test_get_computation_status_no_session_returns_503(tracker_client):
    """When session factory is None, endpoint returns 503."""
    import tracker.computation_api as api_mod
    original = api_mod._get_session
    api_mod._get_session = None
    try:
        resp = await tracker_client.get("/api/computation/status")
        assert resp.status_code == 503
        assert "not initialised" in resp.json()["error"]
    finally:
        api_mod._get_session = original


async def test_get_computation_status_returns_200_with_empty_data(
    tracker_client, mock_session_factory
):
    """With all DB rows returning None, status returns 200 with empty arrays."""
    result_mock = MagicMock()
    result_mock.fetchone.return_value = None
    result_mock.fetchall.return_value = []
    mock_session_factory.execute.return_value = result_mock

    resp = await tracker_client.get("/api/computation/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "pipeline_status" in data
    assert "pre_qa" in data
    assert "post_qa" in data
    assert "spot_checks" in data
    assert "mstar_crossval" in data
    assert "bad_data" in data
    assert "server_time_ist" in data

    # All steps should be pending when DB has no data
    for step in data["pipeline_status"]:
        assert step["status"] == "pending"


async def test_get_computation_status_pipeline_steps_present(
    tracker_client, mock_session_factory
):
    """Response always includes all 6 computation pipeline steps."""
    result_mock = MagicMock()
    result_mock.fetchone.return_value = None
    result_mock.fetchall.return_value = []
    mock_session_factory.execute.return_value = result_mock

    resp = await tracker_client.get("/api/computation/status")
    data = resp.json()
    step_names = [s["step"] for s in data["pipeline_status"]]
    for expected in ["technicals", "rs", "breadth", "regime", "sectors", "fund_derived"]:
        assert expected in step_names


async def test_get_computation_status_with_db_row_returns_passed(
    tracker_client, mock_session_factory
):
    """A successful DB row for a step should produce status=passed in response."""
    row = _make_mock_row(pipeline_name="computation_technicals", status="success")
    result_mock = MagicMock()

    call_count = 0

    async def execute_side_effect(stmt, params=None):
        nonlocal call_count
        call_count += 1
        mock_res = MagicMock()
        # Return our row for the first call (technicals step), None for all others
        if call_count == 1:
            mock_res.fetchone.return_value = row
        else:
            mock_res.fetchone.return_value = None
        mock_res.fetchall.return_value = []
        return mock_res

    mock_session_factory.execute = AsyncMock(side_effect=execute_side_effect)

    resp = await tracker_client.get("/api/computation/status")
    data = resp.json()
    technicals_step = next(s for s in data["pipeline_status"] if s["step"] == "technicals")
    assert technicals_step["status"] == "passed"
    assert technicals_step["rows"] == 1847


async def test_computation_tracker_page_returns_404_when_html_missing(tracker_client):
    """If HTML file does not exist, /tracker returns 404."""
    import tracker.computation_api as api_mod
    from pathlib import Path

    with patch.object(Path, "exists", return_value=False):
        resp = await tracker_client.get("/api/computation/tracker")
        assert resp.status_code == 404


async def test_get_computation_status_db_error_returns_500(
    tracker_client, mock_session_factory
):
    """When DB raises an exception, endpoint returns 500 with error message."""
    mock_session_factory.execute.side_effect = Exception("connection refused")

    resp = await tracker_client.get("/api/computation/status")
    assert resp.status_code == 500
    assert "connection refused" in resp.json()["error"]


async def test_get_computation_status_response_has_correct_keys(
    tracker_client, mock_session_factory
):
    """Response JSON must contain all required top-level keys."""
    result_mock = MagicMock()
    result_mock.fetchone.return_value = None
    result_mock.fetchall.return_value = []
    mock_session_factory.execute.return_value = result_mock

    resp = await tracker_client.get("/api/computation/status")
    data = resp.json()

    required_keys = [
        "business_date", "server_time_ist", "pipeline_status",
        "pre_qa", "post_qa", "spot_checks", "mstar_crossval", "bad_data",
    ]
    for key in required_keys:
        assert key in data, f"Missing key: {key}"
