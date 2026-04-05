"""
Tests for market API endpoints.

Tests:
- test_regime_current_no_token_returns_401
- test_regime_current_returns_envelope
- test_regime_current_no_data_returns_stale_meta
- test_regime_history_returns_envelope
- test_breadth_latest_returns_envelope
- test_breadth_history_returns_envelope
- test_indices_list_returns_envelope
- test_indices_list_invalid_category_returns_422
- test_index_history_not_found_returns_404
- test_index_history_returns_envelope
- test_global_indices_returns_envelope
- test_global_macro_returns_envelope
"""

from __future__ import annotations

from datetime import date, datetime, timezone
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


# ---- Regime tests ----


@pytest.mark.asyncio
async def test_regime_current_no_token_returns_401(client):
    response = await client.get("/api/v1/regime/current")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_regime_current_no_data_returns_stale_meta(client):
    """No regime data should return stale meta."""
    result = MagicMock()
    result.scalar_one_or_none.return_value = None

    mock_db = _make_mock_db([result])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_redis] = _override_redis
    try:
        response = await client.get("/api/v1/regime/current", headers=_auth_headers())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["data_freshness"] == "stale"


@pytest.mark.asyncio
async def test_regime_current_returns_envelope(client):
    """Regime current returns full envelope with regime data."""
    from app.models.computed import DeMarketRegime

    mock_regime = MagicMock(spec=DeMarketRegime)
    mock_regime.date = date(2026, 4, 4)
    mock_regime.computed_at = datetime(2026, 4, 4, 10, 0, 0, tzinfo=timezone.utc)
    mock_regime.regime = "BULL"
    mock_regime.confidence = Decimal("75.0")
    mock_regime.breadth_score = Decimal("68.0")
    mock_regime.momentum_score = Decimal("72.0")
    mock_regime.volume_score = Decimal("60.0")
    mock_regime.global_score = Decimal("55.0")
    mock_regime.fii_score = Decimal("65.0")
    mock_regime.indicator_detail = None
    mock_regime.computation_version = 1

    result = MagicMock()
    result.scalar_one_or_none.return_value = mock_regime

    mock_db = _make_mock_db([result])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    with patch("app.api.v1.market.check_regime_freshness", new_callable=AsyncMock) as mock_fresh:
        mock_fresh.return_value = DataFreshness.FRESH

        app.dependency_overrides[get_db] = _override_db
        app.dependency_overrides[get_redis] = _override_redis
        try:
            response = await client.get("/api/v1/regime/current", headers=_auth_headers())
        finally:
            app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert body["data"]["regime"] == "BULL"


@pytest.mark.asyncio
async def test_regime_history_returns_envelope(client):
    """Regime history returns envelope with pagination."""
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
        response = await client.get("/api/v1/regime/history", headers=_auth_headers())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert "pagination" in body


# ---- Breadth tests ----


@pytest.mark.asyncio
async def test_breadth_latest_returns_envelope(client):
    """Breadth latest returns snapshot."""
    from app.models.computed import DeBreadthDaily

    mock_breadth = MagicMock(spec=DeBreadthDaily)
    mock_breadth.date = date(2026, 4, 4)
    mock_breadth.advance = 1200
    mock_breadth.decline = 700
    mock_breadth.unchanged = 100
    mock_breadth.total_stocks = 2000
    mock_breadth.ad_ratio = Decimal("1.71")
    mock_breadth.pct_above_200dma = Decimal("65.0")
    mock_breadth.pct_above_50dma = Decimal("58.0")
    mock_breadth.new_52w_highs = 45
    mock_breadth.new_52w_lows = 12

    result = MagicMock()
    result.scalar_one_or_none.return_value = mock_breadth

    mock_db = _make_mock_db([result])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    with patch("app.api.v1.market.check_breadth_freshness", new_callable=AsyncMock) as mock_fresh:
        mock_fresh.return_value = DataFreshness.FRESH

        app.dependency_overrides[get_db] = _override_db
        app.dependency_overrides[get_redis] = _override_redis
        try:
            response = await client.get("/api/v1/breadth/latest", headers=_auth_headers())
        finally:
            app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert body["data"]["advance"] == 1200


@pytest.mark.asyncio
async def test_breadth_history_returns_envelope(client):
    """Breadth history returns paginated results."""
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
        response = await client.get("/api/v1/breadth/history", headers=_auth_headers())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200


# ---- Indices tests ----


@pytest.mark.asyncio
async def test_indices_list_returns_envelope(client):
    """Indices list returns master data."""
    result = MagicMock()
    result.scalars.return_value.all.return_value = []

    mock_db = _make_mock_db([result])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_redis] = _override_redis
    try:
        response = await client.get("/api/v1/indices/list", headers=_auth_headers())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body


@pytest.mark.asyncio
async def test_indices_list_invalid_category_returns_422(client):
    """Invalid category param returns 422."""
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
            "/api/v1/indices/list?category=invalid_cat",
            headers=_auth_headers(),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_index_history_not_found_returns_404(client):
    """Unknown index code returns 404."""
    result = MagicMock()
    result.scalar_one_or_none.return_value = None

    mock_db = _make_mock_db([result])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_redis] = _override_redis
    try:
        response = await client.get(
            "/api/v1/indices/FAKE_IDX/history",
            headers=_auth_headers(),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_index_history_returns_envelope(client):
    """Valid index code returns OHLCV envelope."""
    from app.models.instruments import DeIndexMaster

    mock_idx = MagicMock(spec=DeIndexMaster)
    mock_idx.index_code = "NIFTY_50"

    idx_result = MagicMock()
    idx_result.scalar_one_or_none.return_value = mock_idx
    count_result = MagicMock()
    count_result.scalar_one.return_value = 0
    rows_result = MagicMock()
    rows_result.scalars.return_value.all.return_value = []

    mock_db = _make_mock_db([idx_result, count_result, rows_result])
    mock_redis = _make_mock_redis()

    async def _override_db():
        yield mock_db

    async def _override_redis():
        return mock_redis

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_redis] = _override_redis
    try:
        response = await client.get(
            "/api/v1/indices/NIFTY_50/history",
            headers=_auth_headers(),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert "pagination" in body


# ---- Global endpoints ----


@pytest.mark.asyncio
async def test_global_indices_returns_envelope(client):
    """Global indices latest returns envelope."""
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
        response = await client.get("/api/v1/global/indices", headers=_auth_headers())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_global_macro_returns_envelope(client):
    """Global macro endpoint returns paginated data."""
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
        response = await client.get("/api/v1/global/macro", headers=_auth_headers())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
