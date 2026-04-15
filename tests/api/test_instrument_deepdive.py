"""Tests for the instrument deepdive endpoint.

Golden response snapshot tests for RELIANCE, TCS, HDFCBANK.
Missing-data graceful degradation. Unknown symbol → 404.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from app.api.deps import get_db, get_redis
from app.main import app
from app.middleware.auth import create_access_token

IST = timezone(timedelta(hours=5, minutes=30))

REQUIRED_SECTIONS = [
    "instrument",
    "fundamentals",
    "price",
    "technicals",
    "risk",
    "relative_strength",
    "sector_peers",
    "recent_news",
    "meta",
]


def _auth_headers() -> dict:
    token, _ = create_access_token("marketpulse")
    return {"Authorization": f"Bearer {token}"}


def _fake_instrument(symbol: str = "RELIANCE", sector: str = "Oil & Gas"):
    inst = type("Inst", (), {
        "id": uuid.uuid4(),
        "current_symbol": symbol,
        "isin": "INE002A01018",
        "company_name": f"{symbol} Industries Ltd",
        "sector": sector,
        "industry": "Refineries",
        "listing_date": date(1995, 11, 29),
    })()
    return inst


def _fake_fundamentals():
    return type("Fund", (), {
        "as_of_date": date(2026, 4, 15),
        "market_cap_cr": Decimal("1926475.00"),
        "pe_ratio": Decimal("24.3000"),
        "pb_ratio": Decimal("2.8000"),
        "peg_ratio": None,
        "ev_ebitda": None,
        "roe_pct": Decimal("11.2000"),
        "roce_pct": Decimal("13.5000"),
        "operating_margin_pct": Decimal("18.9000"),
        "net_margin_pct": Decimal("9.1000"),
        "debt_to_equity": Decimal("0.4200"),
        "interest_coverage": None,
        "eps_ttm": Decimal("98.1000"),
        "book_value": Decimal("850.0000"),
        "face_value": Decimal("10.00"),
        "dividend_per_share": Decimal("10.0000"),
        "dividend_yield_pct": Decimal("0.3900"),
        "promoter_holding_pct": Decimal("50.30"),
        "pledged_pct": Decimal("0.00"),
        "fii_holding_pct": Decimal("22.10"),
        "dii_holding_pct": Decimal("16.70"),
        "revenue_growth_yoy_pct": Decimal("8.4000"),
        "profit_growth_yoy_pct": Decimal("11.2000"),
        "high_52w": Decimal("3100.0000"),
        "low_52w": Decimal("2180.0000"),
    })()


def _fake_ohlcv_rows():
    base = date(2026, 4, 13)
    close_vals = [Decimal("2950"), Decimal("2937")]
    for i in range(251):
        close_vals.append(Decimal("2380") + Decimal(str(i * 2)))
    rows = []
    for i, c in enumerate(close_vals):
        r = type("R", (), {"date": base - timedelta(days=i), "close": c})()
        rows.append(r)
    return rows


def _fake_technical():
    return type("Tech", (), {
        "date": date(2026, 4, 13),
        "sma_20": Decimal("2920.1000"),
        "sma_50": Decimal("2850.5000"),
        "sma_200": Decimal("2700.0000"),
        "ema_20": Decimal("2930.0000"),
        "ema_50": Decimal("2860.0000"),
        "rsi_14": Decimal("58.3000"),
        "macd_line": Decimal("12.4000"),
        "macd_signal": Decimal("10.1000"),
        "bollinger_upper": Decimal("2980.0000"),
        "bollinger_lower": Decimal("2830.0000"),
        "atr_14": Decimal("42.1000"),
        "adx_14": Decimal("24.5000"),
        "above_50dma": True,
        "above_200dma": True,
        "sharpe_1y": Decimal("0.8200"),
        "sharpe_3y": Decimal("0.6500"),
        "sharpe_5y": Decimal("0.5400"),
        "sortino_1y": Decimal("1.1200"),
        "max_drawdown_1y": Decimal("-0.1800"),
        "beta_3y": Decimal("1.0800"),
        "treynor_3y": Decimal("0.1200"),
        "downside_risk_3y": Decimal("0.1500"),
    })()


def _fake_rs_rows():
    return [
        type("RS", (), {
            "vs_benchmark": "NIFTY_50",
            "rs_composite": Decimal("65.0000"),
            "date": date(2026, 4, 13),
        })(),
        type("RS", (), {
            "vs_benchmark": "SECTOR",
            "rs_composite": Decimal("72.0000"),
            "date": date(2026, 4, 13),
        })(),
    ]


# ---- Tests ----


@pytest.mark.asyncio
async def test_deepdive_unknown_symbol_returns_404(client):
    response = await client.get("/api/v1/instrument/NOSUCHSTOCK", headers=_auth_headers())
    assert response.status_code == 404
    assert "symbol not found" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_deepdive_no_token_returns_401(client):
    response = await client.get("/api/v1/instrument/RELIANCE")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_deepdive_returns_all_sections(client):
    """Golden snapshot: all 7+2 top-level keys present."""
    instrument = _fake_instrument()
    iid = instrument.id

    with (
        patch("app.api.v1.instrument_deepdive.resolve_symbol", return_value=iid),
        patch(
            "app.services.instrument_deepdive_service._fetch_instrument",
            return_value=instrument,
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_fundamentals",
            return_value=None,
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_price",
            return_value=None,
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_technicals_and_risk",
            return_value=(None, None),
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_relative_strength",
            return_value=None,
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_sector_peers",
            return_value=[],
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_recent_news",
            return_value=[],
        ),
        patch(
            "app.services.instrument_deepdive_service.db_face_value",
            create=True,
        ),
    ):
        mock_db = AsyncMock()
        mock_result = AsyncMock()
        mock_result.scalar_one_or_none = lambda: None
        mock_db.execute = AsyncMock(return_value=mock_result)

        async def _override_db():
            yield mock_db

        async def _override_redis():
            return AsyncMock(get=AsyncMock(return_value=None), set=AsyncMock())

        app.dependency_overrides[get_db] = _override_db
        app.dependency_overrides[get_redis] = _override_redis

        try:
            response = await client.get(
                "/api/v1/instrument/RELIANCE", headers=_auth_headers()
            )
        finally:
            app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    for section in REQUIRED_SECTIONS:
        assert section in body, f"Missing section: {section}"


@pytest.mark.asyncio
async def test_deepdive_missing_fundamentals_returns_null_not_404(client):
    """Symbol with no fundamentals returns null, not 404."""
    instrument = _fake_instrument()
    iid = instrument.id

    with (
        patch("app.api.v1.instrument_deepdive.resolve_symbol", return_value=iid),
        patch(
            "app.services.instrument_deepdive_service._fetch_instrument",
            return_value=instrument,
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_fundamentals",
            return_value=None,
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_price",
            return_value=None,
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_technicals_and_risk",
            return_value=(None, None),
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_relative_strength",
            return_value=None,
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_sector_peers",
            return_value=[],
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_recent_news",
            return_value=[],
        ),
    ):
        mock_db = AsyncMock()
        mock_result = AsyncMock()
        mock_result.scalar_one_or_none = lambda: None
        mock_db.execute = AsyncMock(return_value=mock_result)

        async def _override_db():
            yield mock_db

        async def _override_redis():
            return AsyncMock(get=AsyncMock(return_value=None), set=AsyncMock())

        app.dependency_overrides[get_db] = _override_db
        app.dependency_overrides[get_redis] = _override_redis

        try:
            response = await client.get(
                "/api/v1/instrument/RELIANCE", headers=_auth_headers()
            )
        finally:
            app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["fundamentals"] is None
    assert body["price"] is None
    assert body["technicals"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol", ["RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK"])
async def test_deepdive_golden_snapshot_symbols(client, symbol):
    """Golden snapshot for 5 test symbols — verifies structure."""
    instrument = _fake_instrument(symbol=symbol)
    iid = instrument.id

    with (
        patch("app.api.v1.instrument_deepdive.resolve_symbol", return_value=iid),
        patch(
            "app.services.instrument_deepdive_service._fetch_instrument",
            return_value=instrument,
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_fundamentals",
            return_value=None,
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_price",
            return_value=None,
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_technicals_and_risk",
            return_value=(None, None),
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_relative_strength",
            return_value=None,
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_sector_peers",
            return_value=[],
        ),
        patch(
            "app.services.instrument_deepdive_service._fetch_recent_news",
            return_value=[],
        ),
    ):
        mock_db = AsyncMock()
        mock_result = AsyncMock()
        mock_result.scalar_one_or_none = lambda: None
        mock_db.execute = AsyncMock(return_value=mock_result)

        async def _override_db():
            yield mock_db

        async def _override_redis():
            return AsyncMock(get=AsyncMock(return_value=None), set=AsyncMock())

        app.dependency_overrides[get_db] = _override_db
        app.dependency_overrides[get_redis] = _override_redis

        try:
            response = await client.get(
                f"/api/v1/instrument/{symbol}", headers=_auth_headers()
            )
        finally:
            app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["instrument"]["symbol"] == symbol
    for section in REQUIRED_SECTIONS:
        assert section in body
    assert "completeness_pct" in body["meta"]
    assert "data_as_of" in body["meta"]
