"""
Tests for MF API endpoints.

Tests:
- test_get_mf_nav_no_token_returns_401
- test_get_mf_nav_fund_not_found_returns_404
- test_get_mf_nav_returns_envelope
- test_get_mf_universe_returns_envelope
- test_get_mf_category_flows_returns_envelope
- test_get_mf_derived_fund_not_found_returns_404
- test_get_mf_derived_returns_envelope
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.api.deps import get_db, get_redis
from app.main import app
from app.middleware.auth import create_access_token
from app.middleware.response import DataFreshness


def _auth_headers() -> dict:
    token, _ = create_access_token("marketpulse")
    return {"Authorization": f"Bearer {token}"}


def _make_mock_db(side_effects: list) -> AsyncMock:
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(side_effect=side_effects)
    return mock_session


def _make_mock_redis(cached: str | None = None) -> AsyncMock:
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=cached)
    redis.set = AsyncMock(return_value=True)
    return redis


# ---- /mf/nav tests ----


@pytest.mark.asyncio
async def test_get_mf_nav_no_token_returns_401(client):
    """Missing token should return 401."""
    response = await client.get("/api/v1/mf/nav/F0GBR04S23")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_get_mf_nav_fund_not_found_returns_404(client):
    """Unknown mstar_id should return 404."""
    mock_db = _make_mock_db([])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    with patch("app.api.v1.mf._assert_mf_exists") as mock_assert:
        from fastapi import HTTPException, status

        mock_assert.side_effect = HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Fund 'FAKE123' not found",
        )

        app.dependency_overrides[get_db] = _override_db
        app.dependency_overrides[get_redis] = _override_redis
        try:
            response = await client.get(
                "/api/v1/mf/nav/FAKE123",
                headers=_auth_headers(),
            )
        finally:
            app.dependency_overrides.clear()

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_get_mf_nav_returns_envelope(client):
    """Valid fund with mocked DB returns envelope structure."""
    from app.models.instruments import DeMfMaster

    mock_fund = MagicMock(spec=DeMfMaster)
    count_result = MagicMock()
    count_result.scalar_one.return_value = 0
    rows_result = MagicMock()
    rows_result.scalars.return_value.all.return_value = []

    mock_db = _make_mock_db([count_result, rows_result])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    with patch("app.api.v1.mf._assert_mf_exists", new_callable=AsyncMock) as mock_assert, \
         patch("app.api.v1.mf.check_mf_freshness", new_callable=AsyncMock) as mock_fresh:

        mock_assert.return_value = mock_fund
        mock_fresh.return_value = DataFreshness.FRESH

        app.dependency_overrides[get_db] = _override_db
        app.dependency_overrides[get_redis] = _override_redis
        try:
            response = await client.get(
                "/api/v1/mf/nav/F0GBR04S23",
                headers=_auth_headers(),
            )
        finally:
            app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert "meta" in body
    assert "pagination" in body


# ---- /mf/universe tests ----


@pytest.mark.asyncio
async def test_get_mf_universe_returns_envelope(client):
    """Universe endpoint returns envelope."""
    count_result = MagicMock()
    count_result.scalar_one.return_value = 0
    rows_result = MagicMock()
    rows_result.scalars.return_value.all.return_value = []

    mock_db = _make_mock_db([count_result, rows_result])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_redis] = _override_redis
    try:
        response = await client.get(
            "/api/v1/mf/universe",
            headers=_auth_headers(),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert "pagination" in body


# ---- /mf/category-flows tests ----


@pytest.mark.asyncio
async def test_get_mf_category_flows_returns_envelope(client):
    """Category flows endpoint returns envelope."""
    count_result = MagicMock()
    count_result.scalar_one.return_value = 0
    rows_result = MagicMock()
    rows_result.scalars.return_value.all.return_value = []

    mock_db = _make_mock_db([count_result, rows_result])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_redis] = _override_redis
    try:
        response = await client.get(
            "/api/v1/mf/category-flows",
            headers=_auth_headers(),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body


# ---- /mf/derived tests ----


@pytest.mark.asyncio
async def test_get_mf_derived_fund_not_found_returns_404(client):
    """Unknown fund in derived endpoint returns 404."""
    mock_db = _make_mock_db([])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    with patch("app.api.v1.mf._assert_mf_exists") as mock_assert:
        from fastapi import HTTPException, status

        mock_assert.side_effect = HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Fund 'GHOST' not found",
        )

        app.dependency_overrides[get_db] = _override_db
        app.dependency_overrides[get_redis] = _override_redis
        try:
            response = await client.get(
                "/api/v1/mf/derived/GHOST",
                headers=_auth_headers(),
            )
        finally:
            app.dependency_overrides.clear()

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_mf_derived_returns_envelope(client):
    """Derived endpoint returns latest returns data."""
    from app.models.instruments import DeMfMaster
    from app.models.prices import DeMfNavDaily

    mock_fund = MagicMock(spec=DeMfMaster)
    mock_nav = MagicMock(spec=DeMfNavDaily)
    mock_nav.nav_date = date(2026, 4, 4)
    mock_nav.nav = Decimal("120.50")
    mock_nav.return_1d = Decimal("0.5")
    mock_nav.return_1w = Decimal("1.2")
    mock_nav.return_1m = Decimal("3.4")
    mock_nav.return_3m = Decimal("8.1")
    mock_nav.return_6m = Decimal("12.5")
    mock_nav.return_1y = Decimal("25.0")
    mock_nav.return_3y = Decimal("60.0")
    mock_nav.return_5y = Decimal("80.0")
    mock_nav.return_10y = Decimal("150.0")
    mock_nav.nav_52wk_high = Decimal("130.0")
    mock_nav.nav_52wk_low = Decimal("100.0")

    row_result = MagicMock()
    row_result.scalar_one_or_none.return_value = mock_nav

    mock_db = _make_mock_db([row_result])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    with patch("app.api.v1.mf._assert_mf_exists", new_callable=AsyncMock) as mock_assert, \
         patch("app.api.v1.mf.check_mf_freshness", new_callable=AsyncMock) as mock_fresh:

        mock_assert.return_value = mock_fund
        mock_fresh.return_value = DataFreshness.FRESH

        app.dependency_overrides[get_db] = _override_db
        app.dependency_overrides[get_redis] = _override_redis
        try:
            response = await client.get(
                "/api/v1/mf/derived/F0GBR04S23",
                headers=_auth_headers(),
            )
        finally:
            app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert body["data"]["return_1y"] is not None
