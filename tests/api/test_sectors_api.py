"""Tests for sectors API endpoints."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.api.deps import get_db, get_redis
from app.main import app
from app.middleware.auth import create_access_token


def _auth_headers() -> dict:
    token, _ = create_access_token("sectors_test")
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


@pytest.mark.asyncio
async def test_sector_breadth_no_token_returns_401(client):
    response = await client.get("/api/v1/sectors/breadth")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_sector_breadth_returns_envelope(client):
    from datetime import date
    from decimal import Decimal

    max_date_result = MagicMock()
    max_date_result.scalar_one_or_none.return_value = date(2026, 4, 14)

    row_data = MagicMock()
    row_data.mappings.return_value.all.return_value = [
        {
            "date": date(2026, 4, 14),
            "sector": "Energy",
            "stocks_total": 25,
            "stocks_above_50dma": 18,
            "stocks_above_200dma": 15,
            "stocks_above_20ema": 20,
            "pct_above_50dma": Decimal("72.00"),
            "pct_above_200dma": Decimal("60.00"),
            "pct_above_20ema": Decimal("80.00"),
            "stocks_rsi_overbought": 5,
            "stocks_rsi_oversold": 2,
            "stocks_macd_bullish": 14,
            "breadth_regime": "bullish",
        },
    ]

    mock_db = _make_mock_db([max_date_result, row_data])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_redis] = _override_redis
    try:
        response = await client.get(
            "/api/v1/sectors/breadth", headers=_auth_headers()
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert "meta" in body
    assert len(body["data"]) == 1
    assert body["data"][0]["sector"] == "Energy"
    assert body["data"][0]["pct_above_50dma"] is not None


@pytest.mark.asyncio
async def test_sector_breadth_no_data_returns_empty(client):
    max_date_result = MagicMock()
    max_date_result.scalar_one_or_none.return_value = None

    mock_db = _make_mock_db([max_date_result])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_redis] = _override_redis
    try:
        response = await client.get(
            "/api/v1/sectors/breadth", headers=_auth_headers()
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["data"] == []


@pytest.mark.asyncio
async def test_sector_breadth_history_returns_envelope(client):
    from datetime import date
    from decimal import Decimal

    count_result = MagicMock()
    count_result.scalar_one.return_value = 1

    row_data = MagicMock()
    row_data.mappings.return_value.all.return_value = [
        {
            "date": date(2026, 4, 14),
            "sector": "IT",
            "stocks_total": 30,
            "stocks_above_50dma": 10,
            "stocks_above_200dma": 8,
            "stocks_above_20ema": 12,
            "pct_above_50dma": Decimal("33.33"),
            "pct_above_200dma": Decimal("26.67"),
            "pct_above_20ema": Decimal("40.00"),
            "stocks_rsi_overbought": 2,
            "stocks_rsi_oversold": 8,
            "stocks_macd_bullish": 5,
            "breadth_regime": "neutral",
        },
    ]

    mock_db = _make_mock_db([count_result, row_data])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_redis] = _override_redis
    try:
        response = await client.get(
            "/api/v1/sectors/breadth/history?sector=IT&window=1y",
            headers=_auth_headers(),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert "pagination" in body
    assert body["pagination"]["total_count"] == 1


@pytest.mark.asyncio
async def test_sector_breadth_history_invalid_window(client):
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
            "/api/v1/sectors/breadth/history?sector=IT&window=99y",
            headers=_auth_headers(),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
