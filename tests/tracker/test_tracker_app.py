"""Tests for tracker/app.py — the live ingestion tracker on port 8098.

All database queries are mocked so tests run without a real Postgres instance.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

# ---------------------------------------------------------------------------
# Helpers re-imported directly to avoid coupling to DB engine init
# ---------------------------------------------------------------------------
import tracker.app as tracker_module
from tracker.app import (
    IST,
    TARGET_PIPELINES,
    _duration_seconds,
    _format_ist,
    _get_database_url,
    app,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
async def client():
    """Async test client for the tracker app. Bypasses lifespan so no DB needed."""
    # Inject a fake session factory so /api/status can be called
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ---------------------------------------------------------------------------
# Unit tests: pure helpers
# ---------------------------------------------------------------------------

class TestFormatIst:
    def test_none_returns_none(self) -> None:
        assert _format_ist(None) is None

    def test_naive_utc_converted(self) -> None:
        # 2026-04-06 09:00:00 UTC  →  14:30:00+05:30
        dt = datetime(2026, 4, 6, 9, 0, 0, tzinfo=None)
        result = _format_ist(dt)
        assert result is not None
        # The naive datetime is treated as UTC, then shifted to IST (+5:30)
        assert "14:30" in result

    def test_aware_utc_converted(self) -> None:
        dt = datetime(2026, 4, 6, 9, 0, 0, tzinfo=timezone.utc)
        result = _format_ist(dt)
        assert result is not None
        assert "+05:30" in result

    def test_already_ist_preserved(self) -> None:
        dt = datetime(2026, 4, 6, 14, 30, 0, tzinfo=IST)
        result = _format_ist(dt)
        assert result is not None
        assert "14:30" in result


class TestDurationSeconds:
    def test_both_none_returns_none(self) -> None:
        assert _duration_seconds(None, None) is None

    def test_started_none_returns_none(self) -> None:
        assert _duration_seconds(None, datetime(2026, 4, 6, tzinfo=timezone.utc)) is None

    def test_no_completed_uses_now(self) -> None:
        # Started 10 seconds ago
        started = datetime.now(tz=timezone.utc) - timedelta(seconds=10)
        result = _duration_seconds(started, None)
        assert result is not None
        assert 9 <= result <= 15  # small tolerance for test execution time

    def test_explicit_range(self) -> None:
        started = datetime(2026, 4, 6, 10, 0, 0, tzinfo=timezone.utc)
        ended   = datetime(2026, 4, 6, 10, 1, 30, tzinfo=timezone.utc)  # 90 seconds
        assert _duration_seconds(started, ended) == 90.0

    def test_naive_datetimes_handled(self) -> None:
        started = datetime(2026, 4, 6, 10, 0, 0)
        ended   = datetime(2026, 4, 6, 10, 0, 45)
        assert _duration_seconds(started, ended) == 45.0


class TestGetDatabaseUrl:
    def test_falls_back_to_asyncpg_scheme(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("database_url", raising=False)
        url = _get_database_url()
        assert url.startswith("postgresql+asyncpg://")

    def test_env_var_sync_converted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pw@host/db")
        url = _get_database_url()
        assert url.startswith("postgresql+asyncpg://")

    def test_env_var_psycopg2_converted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg2://user:pw@host/db")
        url = _get_database_url()
        assert url.startswith("postgresql+asyncpg://")

    def test_env_var_asyncpg_unchanged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pw@host/db")
        url = _get_database_url()
        assert url == "postgresql+asyncpg://user:pw@host/db"


# ---------------------------------------------------------------------------
# Integration tests: HTTP endpoints (DB mocked)
# ---------------------------------------------------------------------------

def _make_fake_row(
    pipeline_name: str = "test_pipeline",
    status: str = "success",
    rows_processed: int = 100,
    rows_failed: int = 0,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    business_date: Any = None,
    error_detail: str | None = None,
) -> MagicMock:
    """Build a MagicMock that mimics a SQLAlchemy Row."""
    row = MagicMock()
    row.pipeline_name  = pipeline_name
    row.status         = status
    row.rows_processed = rows_processed
    row.rows_failed    = rows_failed
    row.started_at     = started_at or datetime(2026, 4, 6, 8, 0, 0, tzinfo=timezone.utc)
    row.completed_at   = completed_at or datetime(2026, 4, 6, 8, 1, 0, tzinfo=timezone.utc)
    row.business_date  = business_date
    row.error_detail   = error_detail
    return row


class TestHealthEndpoint:
    @pytest.mark.asyncio
    async def test_health_returns_200(self, client: AsyncClient) -> None:
        resp = await client.get("/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_health_body(self, client: AsyncClient) -> None:
        resp = await client.get("/health")
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["service"] == "tracker"


class TestStatusEndpoint:
    @pytest.mark.asyncio
    async def test_status_503_when_no_session_factory(self, client: AsyncClient) -> None:
        """When _session_factory is None the endpoint returns 503."""
        original = tracker_module._session_factory
        tracker_module._session_factory = None
        try:
            resp = await client.get("/api/status")
            assert resp.status_code == 503
            assert "not initialised" in resp.json()["error"].lower()
        finally:
            tracker_module._session_factory = original

    @pytest.mark.asyncio
    async def test_status_returns_target_and_recent(self, client: AsyncClient) -> None:
        """With a mocked session factory, /api/status returns the expected shape."""
        # Build mock rows for target pipelines
        target_rows = {
            "mf_category_flows":  _make_fake_row("mf_category_flows", "success", 35),
            "market_cap_history": _make_fake_row("market_cap_history", "running", 10),
            "symbol_history":     _make_fake_row("symbol_history",     "failed",  0),
        }
        recent_row = _make_fake_row("bhav_copy", "success", 500)

        # Mock _query_latest_for_pipeline and _query_recent_runs
        async def fake_latest(session: Any, name: str):  # noqa: ANN001
            row = target_rows.get(name)
            if row is None:
                return None
            from tracker.app import _row_to_dict  # noqa: PLC0415
            return _row_to_dict(row)

        async def fake_recent(session: Any):  # noqa: ANN001
            from tracker.app import _row_to_dict  # noqa: PLC0415
            return [_row_to_dict(recent_row)]

        # Build a fake async context manager for the session factory
        fake_session = AsyncMock()
        fake_ctx = AsyncMock()
        fake_ctx.__aenter__ = AsyncMock(return_value=fake_session)
        fake_ctx.__aexit__  = AsyncMock(return_value=False)

        fake_factory = MagicMock(return_value=fake_ctx)

        original_factory = tracker_module._session_factory
        original_latest  = tracker_module._query_latest_for_pipeline
        original_recent  = tracker_module._query_recent_runs

        tracker_module._session_factory             = fake_factory
        tracker_module._query_latest_for_pipeline   = fake_latest
        tracker_module._query_recent_runs           = fake_recent

        try:
            resp = await client.get("/api/status")
            assert resp.status_code == 200
            data = resp.json()

            # Top-level keys present
            assert "target_pipelines" in data
            assert "recent_runs" in data
            assert "server_time_ist" in data

            # server_time_ist is a valid ISO-8601 string with IST offset
            assert "+05:30" in data["server_time_ist"]

            # Three target pipelines always returned (one per TARGET_PIPELINES entry)
            assert len(data["target_pipelines"]) == len(TARGET_PIPELINES)

            # Verify mf_category_flows row
            mf = next(p for p in data["target_pipelines"] if p["name"] == "mf_category_flows")
            assert mf["status"] == "success"
            assert mf["rows_processed"] == 35
            assert mf["display_name"] == "MF Category Flows"

            # Recent runs present
            assert len(data["recent_runs"]) == 1
            assert data["recent_runs"][0]["name"] == "bhav_copy"

        finally:
            tracker_module._session_factory             = original_factory
            tracker_module._query_latest_for_pipeline   = original_latest
            tracker_module._query_recent_runs           = original_recent

    @pytest.mark.asyncio
    async def test_status_pending_placeholder_for_missing_pipeline(
        self, client: AsyncClient
    ) -> None:
        """Pipelines with no DB rows get a pending placeholder."""

        async def fake_latest_none(session: Any, name: str):  # noqa: ANN001
            return None  # simulate pipeline never ran

        async def fake_recent_empty(session: Any):  # noqa: ANN001
            return []

        fake_session = AsyncMock()
        fake_ctx = AsyncMock()
        fake_ctx.__aenter__ = AsyncMock(return_value=fake_session)
        fake_ctx.__aexit__  = AsyncMock(return_value=False)
        fake_factory = MagicMock(return_value=fake_ctx)

        original_factory = tracker_module._session_factory
        original_latest  = tracker_module._query_latest_for_pipeline
        original_recent  = tracker_module._query_recent_runs

        tracker_module._session_factory             = fake_factory
        tracker_module._query_latest_for_pipeline   = fake_latest_none
        tracker_module._query_recent_runs           = fake_recent_empty

        try:
            resp = await client.get("/api/status")
            assert resp.status_code == 200
            data = resp.json()
            for p in data["target_pipelines"]:
                assert p["status"] == "pending"
                assert p["rows_processed"] is None
        finally:
            tracker_module._session_factory             = original_factory
            tracker_module._query_latest_for_pipeline   = original_latest
            tracker_module._query_recent_runs           = original_recent

    @pytest.mark.asyncio
    async def test_status_500_on_db_error(self, client: AsyncClient) -> None:
        """DB exceptions are caught and returned as HTTP 500 with error key."""

        fake_session = AsyncMock()
        fake_ctx = AsyncMock()
        fake_ctx.__aenter__ = AsyncMock(return_value=fake_session)
        fake_ctx.__aexit__  = AsyncMock(return_value=False)
        fake_factory = MagicMock(return_value=fake_ctx)

        async def exploding_latest(session: Any, name: str):  # noqa: ANN001
            raise RuntimeError("DB connection refused")

        async def fake_recent_empty(session: Any):  # noqa: ANN001
            return []

        original_factory = tracker_module._session_factory
        original_latest  = tracker_module._query_latest_for_pipeline
        original_recent  = tracker_module._query_recent_runs

        tracker_module._session_factory             = fake_factory
        tracker_module._query_latest_for_pipeline   = exploding_latest
        tracker_module._query_recent_runs           = fake_recent_empty

        try:
            resp = await client.get("/api/status")
            assert resp.status_code == 500
            data = resp.json()
            assert "error" in data
            assert "DB connection refused" in data["error"]
        finally:
            tracker_module._session_factory             = original_factory
            tracker_module._query_latest_for_pipeline   = original_latest
            tracker_module._query_recent_runs           = original_recent


class TestIndexRoute:
    @pytest.mark.asyncio
    async def test_index_returns_html(self, client: AsyncClient) -> None:
        """GET / returns the tracker.html file."""
        resp = await client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert b"JIP Data Ingestion Tracker" in resp.content
