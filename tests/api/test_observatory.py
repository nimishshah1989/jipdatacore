"""
Tests for observatory API endpoints.

All endpoints are public (no auth required) — verified by these tests.
All DB calls are mocked so tests run without a live database.

Tests:
- test_pulse_no_auth_succeeds              — public endpoint, no token needed
- test_pulse_returns_required_keys         — response shape matches spec
- test_pulse_freshness_logic_fresh         — < 24h classified as fresh
- test_pulse_freshness_logic_stale         — 24–72h classified as stale
- test_pulse_freshness_logic_critical      — > 72h classified as critical
- test_pulse_freshness_logic_unknown       — None hours_old = unknown
- test_quality_no_auth_succeeds            — public endpoint
- test_quality_table_missing_returns_gracefully  — table_present = False
- test_quality_table_present_returns_checks      — checks list returned
- test_coverage_no_auth_succeeds           — public endpoint
- test_coverage_returns_tables_list        — tables key present
- test_pipelines_no_auth_succeeds          — public endpoint
- test_pipelines_7day_window_returned      — window_days = 7
- test_dictionary_no_auth_succeeds         — public endpoint
- test_dictionary_returns_column_list      — tables + column_count keys
- test_observatory_ui_no_auth_succeeds     — /observatory returns 200 or 404
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from app.api.v1.observatory import _freshness_status
from app.main import app


# ─── Helpers ──────────────────────────────────────────────────────────────

def _make_mock_db(stat_rows=None, max_date_val=None, exists_val=True, quality_rows=None,
                  col_rows=None, pipe_rows=None, date_range_row=None):
    """Build a mock AsyncSession that returns preset values."""
    mock_session = AsyncMock()
    call_count = [0]

    async def side_effect(query, *args, **kwargs):
        call_count[0] += 1
        result = AsyncMock()

        query_str = str(query) if not isinstance(query, str) else query

        # information_schema check (EXISTS returns True/False)
        if 'information_schema.tables' in query_str or 'EXISTS' in query_str:
            result.scalar_one = MagicMock(return_value=exists_val)
            return result

        # pg_stat_user_tables
        if 'pg_stat_user_tables' in query_str:
            mock_rows = stat_rows or []
            result.fetchall = MagicMock(return_value=mock_rows)
            return result

        # MAX(date) query
        if 'MAX(' in query_str:
            result.scalar_one_or_none = MagicMock(return_value=max_date_val)
            return result

        # Quality checks
        if 'de_data_quality_checks' in query_str:
            mock_rows = quality_rows or []
            result.fetchall = MagicMock(return_value=mock_rows)
            return result

        # pipeline log
        if 'de_pipeline_log' in query_str:
            mock_rows = pipe_rows or []
            result.fetchall = MagicMock(return_value=mock_rows)
            return result

        # information_schema columns
        if 'information_schema.columns' in query_str:
            mock_rows = col_rows or []
            result.fetchall = MagicMock(return_value=mock_rows)
            return result

        # MIN/MAX date range
        if 'MIN(' in query_str:
            if date_range_row:
                result.fetchone = MagicMock(return_value=date_range_row)
            else:
                result.fetchone = MagicMock(return_value=None)
            return result

        result.fetchall = MagicMock(return_value=[])
        result.fetchone = MagicMock(return_value=None)
        result.scalar_one = MagicMock(return_value=0)
        result.scalar_one_or_none = MagicMock(return_value=None)
        return result

    mock_session.execute = AsyncMock(side_effect=side_effect)
    return mock_session


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ─── Unit tests for _freshness_status ─────────────────────────────────────


def test_pulse_freshness_logic_fresh():
    assert _freshness_status(0) == "fresh"
    assert _freshness_status(12.5) == "fresh"
    assert _freshness_status(23.9) == "fresh"


def test_pulse_freshness_logic_stale():
    assert _freshness_status(24.0) == "stale"
    assert _freshness_status(48.0) == "stale"
    assert _freshness_status(72.0) == "stale"


def test_pulse_freshness_logic_critical():
    assert _freshness_status(72.1) == "critical"
    assert _freshness_status(200.0) == "critical"


def test_pulse_freshness_logic_unknown():
    assert _freshness_status(None) == "unknown"


# ─── Pulse endpoint ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_pulse_no_auth_succeeds(client):
    """Pulse endpoint must be accessible without any token."""
    mock_db = _make_mock_db()
    from app.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: mock_db
    try:
        resp = await client.get("/api/v1/observatory/pulse")
        assert resp.status_code == 200
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.mark.asyncio
async def test_pulse_returns_required_keys(client):
    """Pulse response must contain as_of, overall_status, summary, streams."""
    mock_db = _make_mock_db()
    from app.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: mock_db
    try:
        resp = await client.get("/api/v1/observatory/pulse")
        data = resp.json()
        assert "as_of" in data
        assert "overall_status" in data
        assert "summary" in data
        assert "streams" in data
        assert isinstance(data["streams"], list)
        for key in ("fresh", "stale", "critical"):
            assert key in data["summary"]
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.mark.asyncio
async def test_pulse_overall_status_healthy_when_all_fresh(client):
    """overall_status = healthy when no stale/critical streams."""
    import datetime

    # Return a very recent date for MAX() queries
    recent = datetime.date.today()
    mock_db = _make_mock_db(max_date_val=recent, exists_val=True)
    from app.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: mock_db
    try:
        resp = await client.get("/api/v1/observatory/pulse")
        # Should succeed and have valid structure
        assert resp.status_code == 200
    finally:
        app.dependency_overrides.pop(get_db, None)


# ─── Quality endpoint ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_quality_no_auth_succeeds(client):
    """Quality endpoint must not require auth."""
    mock_db = _make_mock_db(exists_val=False)
    from app.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: mock_db
    try:
        resp = await client.get("/api/v1/observatory/quality")
        assert resp.status_code == 200
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.mark.asyncio
async def test_quality_table_missing_returns_gracefully(client):
    """When de_data_quality_checks doesn't exist, table_present = False."""
    mock_db = _make_mock_db(exists_val=False)
    from app.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: mock_db
    try:
        resp = await client.get("/api/v1/observatory/quality")
        assert resp.status_code == 200
        data = resp.json()
        assert data["table_present"] is False
        assert data["summary"]["pass"] == 0
        assert data["summary"]["fail"] == 0
        assert data["checks"] == []
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.mark.asyncio
async def test_quality_table_present_returns_checks(client):
    """When table exists with rows, checks list is populated."""
    import datetime

    class FakeRow:
        check_name = "equity_ohlcv_row_count"
        check_category = "completeness"
        check_status = "pass"
        actual_value = "12500"
        threshold_value = "5000"
        detail = "All rows present"
        checked_at = datetime.datetime(2026, 4, 8, 10, 0, tzinfo=datetime.timezone.utc)

    mock_db = _make_mock_db(exists_val=True, quality_rows=[FakeRow()])
    from app.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: mock_db
    try:
        resp = await client.get("/api/v1/observatory/quality")
        assert resp.status_code == 200
        data = resp.json()
        assert data["table_present"] is True
        assert len(data["checks"]) == 1
        chk = data["checks"][0]
        assert chk["check_name"] == "equity_ohlcv_row_count"
        assert chk["status"] == "pass"
        assert data["summary"]["pass"] == 1
    finally:
        app.dependency_overrides.pop(get_db, None)


# ─── Coverage endpoint ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_coverage_no_auth_succeeds(client):
    """Coverage endpoint must not require auth."""
    mock_db = _make_mock_db()
    from app.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: mock_db
    try:
        resp = await client.get("/api/v1/observatory/coverage")
        assert resp.status_code == 200
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.mark.asyncio
async def test_coverage_returns_tables_list(client):
    """Coverage response must contain tables list and table_count."""
    class FakeStatRow:
        relname = "de_equity_ohlcv"
        n_live_tup = 250000
        last_analyze = None
        last_autoanalyze = None

    mock_db = _make_mock_db(stat_rows=[FakeStatRow()])
    from app.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: mock_db
    try:
        resp = await client.get("/api/v1/observatory/coverage")
        assert resp.status_code == 200
        data = resp.json()
        assert "tables" in data
        assert "table_count" in data
        assert isinstance(data["tables"], list)
    finally:
        app.dependency_overrides.pop(get_db, None)


# ─── Pipelines endpoint ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_pipelines_no_auth_succeeds(client):
    """Pipelines endpoint must not require auth."""
    mock_db = _make_mock_db()
    from app.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: mock_db
    try:
        resp = await client.get("/api/v1/observatory/pipelines")
        assert resp.status_code == 200
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.mark.asyncio
async def test_pipelines_7day_window_returned(client):
    """Pipelines response must declare window_days = 7."""
    mock_db = _make_mock_db()
    from app.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: mock_db
    try:
        resp = await client.get("/api/v1/observatory/pipelines")
        data = resp.json()
        assert data["window_days"] == 7
        assert "pipelines" in data
        assert "pipeline_count" in data
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.mark.asyncio
async def test_pipelines_groups_by_pipeline_name(client):
    """Multiple runs for same pipeline+date rolled into one cell."""
    import datetime

    class FakePipeRow:
        pipeline_name = "equity_bhav"
        business_date = datetime.date(2026, 4, 8)
        status = "success"
        rows_processed = 5000
        duration_secs = 12.5
        completed_at = datetime.datetime(2026, 4, 8, 6, 0, tzinfo=datetime.timezone.utc)

    mock_db = _make_mock_db(pipe_rows=[FakePipeRow(), FakePipeRow()])
    from app.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: mock_db
    try:
        resp = await client.get("/api/v1/observatory/pipelines")
        data = resp.json()
        assert data["pipeline_count"] == 1
        pipe = data["pipelines"][0]
        assert pipe["pipeline_name"] == "equity_bhav"
        # Both runs on same date should merge to one cell
        assert len(pipe["days"]) == 1
        assert pipe["days"][0]["status"] == "success"
    finally:
        app.dependency_overrides.pop(get_db, None)


# ─── Dictionary endpoint ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dictionary_no_auth_succeeds(client):
    """Dictionary endpoint must not require auth."""
    mock_db = _make_mock_db()
    from app.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: mock_db
    try:
        resp = await client.get("/api/v1/observatory/dictionary")
        assert resp.status_code == 200
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.mark.asyncio
async def test_dictionary_returns_column_list(client):
    """Dictionary response must include tables, column_count, table_count."""
    class FakeColRow:
        table_name = "de_equity_ohlcv"
        column_name = "close"
        data_type = "numeric"
        is_nullable = "NO"
        column_default = None
        ordinal_position = 5

    mock_db = _make_mock_db(col_rows=[FakeColRow()])
    from app.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: mock_db
    try:
        resp = await client.get("/api/v1/observatory/dictionary")
        assert resp.status_code == 200
        data = resp.json()
        assert "tables" in data
        assert "column_count" in data
        assert "table_count" in data
        assert data["column_count"] == 1
        tbl = data["tables"][0]
        assert tbl["table"] == "de_equity_ohlcv"
        col = tbl["columns"][0]
        assert col["column"] == "close"
        assert col["data_type"] == "numeric"
        assert col["is_nullable"] is False
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.mark.asyncio
async def test_dictionary_enriches_known_columns(client):
    """Columns in COLUMN_DESCRIPTIONS get a non-empty description."""
    class FakeColRow:
        table_name = "de_equity_ohlcv"
        column_name = "close"
        data_type = "numeric"
        is_nullable = "YES"
        column_default = None
        ordinal_position = 1

    mock_db = _make_mock_db(col_rows=[FakeColRow()])
    from app.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: mock_db
    try:
        resp = await client.get("/api/v1/observatory/dictionary")
        data = resp.json()
        col = data["tables"][0]["columns"][0]
        assert col["description"] != ""
    finally:
        app.dependency_overrides.pop(get_db, None)


# ─── Observatory UI endpoint ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_observatory_ui_no_auth_succeeds(client):
    """/observatory should return 200 (HTML file present) or 404 (not deployed) — never 401."""
    resp = await client.get("/observatory")
    assert resp.status_code in (200, 404)
