"""
Tests for equity API endpoints.

Tests:
- test_get_equity_ohlcv_no_token_returns_401
- test_get_equity_ohlcv_symbol_not_found_returns_404
- test_get_equity_ohlcv_returns_envelope
- test_get_equity_universe_returns_envelope
- test_get_equity_universe_invalid_index_filter_returns_422
- test_get_rs_stocks_returns_envelope
- test_get_rs_sectors_returns_envelope
- test_get_rs_stock_not_found_returns_404
- test_get_rs_stock_returns_envelope
"""

from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.api.deps import get_db, get_redis
from app.main import app
from app.middleware.auth import create_access_token
from app.middleware.response import DataFreshness


def _make_token(admin: bool = False) -> str:
    platform = "admin" if admin else "marketpulse"
    token, _ = create_access_token(platform)
    return token


def _auth_headers(admin: bool = False) -> dict:
    return {"Authorization": f"Bearer {_make_token(admin=admin)}"}


def _make_mock_db(side_effects: list) -> AsyncMock:
    """Create an async DB session mock with given execute side effects."""
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(side_effect=side_effects)
    return mock_session


def _make_mock_redis(cached: str | None = None) -> AsyncMock:
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=cached)
    redis.set = AsyncMock(return_value=True)
    return redis


# ---- OHLCV tests ----


@pytest.mark.asyncio
async def test_get_equity_ohlcv_no_token_returns_401(client):
    """Missing token should return 401."""
    response = await client.get("/api/v1/equity/ohlcv/RELIANCE")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_get_equity_ohlcv_symbol_not_found_returns_404(client):
    """Unknown symbol should return 404."""
    mock_db = _make_mock_db([])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    with patch("app.api.v1.equity.resolve_symbol_or_404") as mock_resolve:
        from fastapi import HTTPException, status

        mock_resolve.side_effect = HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Symbol 'FAKESTOCK' not found",
        )

        app.dependency_overrides[get_db] = _override_db
        app.dependency_overrides[get_redis] = _override_redis
        try:
            response = await client.get(
                "/api/v1/equity/ohlcv/FAKESTOCK",
                headers=_auth_headers(),
            )
        finally:
            app.dependency_overrides.clear()

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_get_equity_ohlcv_returns_envelope(client):
    """Valid symbol with mocked DB should return envelope with data/meta/pagination keys."""
    mock_instrument_id = uuid.uuid4()

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

    with patch("app.api.v1.equity.resolve_symbol_or_404", new_callable=AsyncMock) as mock_resolve, \
         patch("app.api.v1.equity.check_equity_freshness", new_callable=AsyncMock) as mock_fresh:

        mock_resolve.return_value = mock_instrument_id
        mock_fresh.return_value = DataFreshness.FRESH

        app.dependency_overrides[get_db] = _override_db
        app.dependency_overrides[get_redis] = _override_redis
        try:
            response = await client.get(
                "/api/v1/equity/ohlcv/RELIANCE",
                headers=_auth_headers(),
            )
        finally:
            app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert "meta" in body
    assert "pagination" in body


# ---- Universe tests ----


@pytest.mark.asyncio
async def test_get_equity_universe_invalid_index_filter_returns_422(client):
    """Invalid index_filter param should return 422."""
    mock_db = _make_mock_db([])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_redis] = _override_redis
    try:
        response = await client.get(
            "/api/v1/equity/universe?index_filter=invalid_index",
            headers=_auth_headers(),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_equity_universe_returns_envelope(client):
    """Universe endpoint should return envelope structure."""
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
            "/api/v1/equity/universe",
            headers=_auth_headers(),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert "meta" in body
    assert "pagination" in body


# ---- RS tests ----


@pytest.mark.asyncio
async def test_get_rs_stocks_returns_envelope(client):
    """RS stocks endpoint should return envelope."""
    date_result = MagicMock()
    date_result.scalar_one_or_none.return_value = date(2026, 4, 4)
    count_result = MagicMock()
    count_result.scalar_one.return_value = 0
    rows_result = MagicMock()
    rows_result.scalars.return_value.all.return_value = []

    mock_db = _make_mock_db([date_result, count_result, rows_result])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_redis] = _override_redis
    try:
        response = await client.get("/api/v1/rs/stocks", headers=_auth_headers())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body


@pytest.mark.asyncio
async def test_get_rs_sectors_returns_envelope(client):
    """RS sectors endpoint should return envelope."""
    date_result = MagicMock()
    date_result.scalar_one_or_none.return_value = date(2026, 4, 4)
    rows_result = MagicMock()
    rows_result.all.return_value = []

    mock_db = _make_mock_db([date_result, rows_result])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_redis] = _override_redis
    try:
        response = await client.get("/api/v1/rs/sectors", headers=_auth_headers())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_get_rs_stock_not_found_returns_404(client):
    """Unknown symbol in RS stock endpoint should return 404."""
    mock_db = _make_mock_db([])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    with patch("app.api.v1.equity.resolve_symbol_or_404") as mock_resolve:
        from fastapi import HTTPException, status as hstatus

        mock_resolve.side_effect = HTTPException(
            status_code=hstatus.HTTP_404_NOT_FOUND,
            detail="Symbol 'GHOST' not found",
        )

        app.dependency_overrides[get_db] = _override_db
        app.dependency_overrides[get_redis] = _override_redis
        try:
            response = await client.get(
                "/api/v1/rs/stock/GHOST",
                headers=_auth_headers(),
            )
        finally:
            app.dependency_overrides.clear()

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_rs_stock_returns_envelope(client):
    """RS stock endpoint should return full RS data for known symbol."""
    mock_instrument_id = uuid.uuid4()

    from app.models.computed import DeRsScores

    mock_rs = MagicMock(spec=DeRsScores)
    mock_rs.date = date(2026, 4, 4)
    mock_rs.vs_benchmark = "NIFTY_50"
    mock_rs.rs_1w = Decimal("55.0")
    mock_rs.rs_1m = Decimal("60.0")
    mock_rs.rs_3m = Decimal("65.0")
    mock_rs.rs_6m = Decimal("70.0")
    mock_rs.rs_12m = Decimal("72.0")
    mock_rs.rs_composite = Decimal("64.5")
    mock_rs.computation_version = 1

    date_result = MagicMock()
    date_result.scalar_one_or_none.return_value = date(2026, 4, 4)
    row_result = MagicMock()
    row_result.scalar_one_or_none.return_value = mock_rs

    mock_db = _make_mock_db([date_result, row_result])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    with patch("app.api.v1.equity.resolve_symbol_or_404", new_callable=AsyncMock) as mock_resolve:
        mock_resolve.return_value = mock_instrument_id

        app.dependency_overrides[get_db] = _override_db
        app.dependency_overrides[get_redis] = _override_redis
        try:
            response = await client.get(
                "/api/v1/rs/stock/RELIANCE",
                headers=_auth_headers(),
            )
        finally:
            app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert body["data"]["rs_composite"] is not None
