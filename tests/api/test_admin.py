"""
Tests for admin API endpoints.

Tests:
- test_pipeline_status_no_token_returns_401
- test_pipeline_status_non_admin_returns_403
- test_pipeline_status_admin_returns_envelope
- test_anomalies_admin_returns_envelope
- test_resolve_anomaly_not_found_returns_404
- test_resolve_anomaly_admin_succeeds
- test_resolve_anomaly_already_resolved_returns_422
- test_data_override_admin_creates_flag
- test_pipeline_replay_admin_queues_job
- test_system_flag_admin_sets_flag
"""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.api.deps import get_db
from app.main import app
from app.middleware.auth import create_access_token


def _auth_headers(admin: bool = False) -> dict:
    platform = "admin" if admin else "marketpulse"
    token, _ = create_access_token(platform)
    return {"Authorization": f"Bearer {token}"}


def _make_mock_db(side_effects: list) -> AsyncMock:
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(side_effect=side_effects)
    begin_ctx = AsyncMock()
    begin_ctx.__aenter__ = AsyncMock(return_value=None)
    begin_ctx.__aexit__ = AsyncMock(return_value=False)
    mock_session.begin = MagicMock(return_value=begin_ctx)
    mock_session.add = MagicMock()
    return mock_session


def _make_mock_redis() -> AsyncMock:
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock(return_value=True)
    return redis


# ---- Auth enforcement ----


@pytest.mark.asyncio
async def test_pipeline_status_no_token_returns_401(client):
    """No token should return 401."""
    response = await client.get("/api/v1/admin/pipeline/status")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_pipeline_status_non_admin_returns_403(client):
    """Non-admin token should return 403."""
    mock_db = _make_mock_db([])

    async def _override_db():
        yield mock_db

    app.dependency_overrides[get_db] = _override_db
    try:
        response = await client.get(
            "/api/v1/admin/pipeline/status",
            headers=_auth_headers(admin=False),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 403


# ---- Pipeline status ----


@pytest.mark.asyncio
async def test_pipeline_status_admin_returns_envelope(client):
    """Admin can retrieve pipeline status."""
    count_result = MagicMock()
    count_result.scalar_one.return_value = 0
    rows_result = MagicMock()
    rows_result.scalars.return_value.all.return_value = []

    mock_db = _make_mock_db([count_result, rows_result])

    async def _override_db():
        yield mock_db

    app.dependency_overrides[get_db] = _override_db
    try:
        response = await client.get(
            "/api/v1/admin/pipeline/status",
            headers=_auth_headers(admin=True),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert "pagination" in body


# ---- Anomalies ----


@pytest.mark.asyncio
async def test_anomalies_admin_returns_envelope(client):
    """Admin can list anomalies."""
    count_result = MagicMock()
    count_result.scalar_one.return_value = 0
    rows_result = MagicMock()
    rows_result.scalars.return_value.all.return_value = []

    mock_db = _make_mock_db([count_result, rows_result])

    async def _override_db():
        yield mock_db

    app.dependency_overrides[get_db] = _override_db
    try:
        response = await client.get(
            "/api/v1/admin/anomalies",
            headers=_auth_headers(admin=True),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body


@pytest.mark.asyncio
async def test_resolve_anomaly_not_found_returns_404(client):
    """Non-existent anomaly returns 404."""
    fake_id = uuid.uuid4()
    result = MagicMock()
    result.scalar_one_or_none.return_value = None

    mock_db = _make_mock_db([result])

    async def _override_db():
        yield mock_db

    app.dependency_overrides[get_db] = _override_db
    try:
        response = await client.post(
            f"/api/v1/admin/anomalies/{fake_id}/resolve",
            json={"resolution_note": "Investigating"},
            headers=_auth_headers(admin=True),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_resolve_anomaly_already_resolved_returns_422(client):
    """Already-resolved anomaly returns 422."""
    fake_id = uuid.uuid4()

    from app.models.prices import DeDataAnomalies

    mock_anomaly = MagicMock(spec=DeDataAnomalies)
    mock_anomaly.id = fake_id
    mock_anomaly.is_resolved = True

    result = MagicMock()
    result.scalar_one_or_none.return_value = mock_anomaly

    mock_db = _make_mock_db([result])

    async def _override_db():
        yield mock_db

    app.dependency_overrides[get_db] = _override_db
    try:
        response = await client.post(
            f"/api/v1/admin/anomalies/{fake_id}/resolve",
            json={"resolution_note": "Done"},
            headers=_auth_headers(admin=True),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert "already resolved" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_resolve_anomaly_admin_succeeds(client):
    """Admin can resolve an open anomaly."""
    fake_id = uuid.uuid4()

    from app.models.prices import DeDataAnomalies

    mock_anomaly = MagicMock(spec=DeDataAnomalies)
    mock_anomaly.id = fake_id
    mock_anomaly.is_resolved = False

    select_result = MagicMock()
    select_result.scalar_one_or_none.return_value = mock_anomaly
    update_result = MagicMock()

    mock_db = _make_mock_db([select_result, update_result])

    async def _override_db():
        yield mock_db

    app.dependency_overrides[get_db] = _override_db
    try:
        response = await client.post(
            f"/api/v1/admin/anomalies/{fake_id}/resolve",
            json={"resolution_note": "Fixed upstream data issue"},
            headers=_auth_headers(admin=True),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["resolved"] is True


# ---- Data override ----


@pytest.mark.asyncio
async def test_data_override_admin_creates_flag(client):
    """Admin can create/update a system flag via data override."""
    existing_result = MagicMock()
    existing_result.scalar_one_or_none.return_value = None  # flag doesn't exist

    mock_db = _make_mock_db([existing_result])

    async def _override_db():
        yield mock_db

    app.dependency_overrides[get_db] = _override_db
    try:
        response = await client.post(
            "/api/v1/admin/data/override",
            json={"flag_key": "halt_equity_pipeline", "value": True, "reason": "Test override"},
            headers=_auth_headers(admin=True),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["flag_key"] == "halt_equity_pipeline"
    assert body["data"]["value"] is True


# ---- Pipeline replay ----


@pytest.mark.asyncio
async def test_pipeline_replay_admin_queues_job(client):
    """Admin can queue a pipeline replay."""
    run_num_result = MagicMock()
    run_num_result.scalar_one.return_value = 2  # 2 existing runs

    mock_db = _make_mock_db([run_num_result])

    async def _override_db():
        yield mock_db

    app.dependency_overrides[get_db] = _override_db
    try:
        response = await client.post(
            "/api/v1/admin/pipeline/replay",
            json={
                "pipeline_name": "equity_bhav",
                "business_date": "2026-04-04",
                "reason": "Data corruption fix",
            },
            headers=_auth_headers(admin=True),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 202
    body = response.json()
    assert body["data"]["pipeline_name"] == "equity_bhav"
    assert body["data"]["run_number"] == 3
    assert body["data"]["status"] == "pending"


# ---- System flag ----


@pytest.mark.asyncio
async def test_system_flag_admin_sets_flag(client):
    """Admin can set a system flag."""
    existing_result = MagicMock()
    existing_result.scalar_one_or_none.return_value = None

    mock_db = _make_mock_db([existing_result])

    async def _override_db():
        yield mock_db

    app.dependency_overrides[get_db] = _override_db
    try:
        response = await client.post(
            "/api/v1/admin/system/flag",
            json={"key": "maintenance_mode", "value": False, "reason": "Maintenance done"},
            headers=_auth_headers(admin=True),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["key"] == "maintenance_mode"
    assert body["data"]["value"] is False
