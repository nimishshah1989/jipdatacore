"""Tests for NSE index prices pipeline and India VIX pipeline."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.pipelines.indices.nse_indices import (
    NseIndicesPipeline,
    _parse_index_row,
    _safe_decimal,
    upsert_index_prices,
)
from app.pipelines.indices.vix import (
    IndiaVixPipeline,
    upsert_vix_value,
)


# ---------------------------------------------------------------------------
# _safe_decimal tests
# ---------------------------------------------------------------------------


def test_safe_decimal_valid_number_returns_decimal() -> None:
    result = _safe_decimal("12345.67")
    assert result == Decimal("12345.67")


def test_safe_decimal_int_returns_decimal() -> None:
    result = _safe_decimal(100)
    assert result == Decimal("100")


def test_safe_decimal_none_returns_none() -> None:
    result = _safe_decimal(None)
    assert result is None


def test_safe_decimal_invalid_string_returns_none() -> None:
    result = _safe_decimal("N/A")
    assert result is None


def test_safe_decimal_empty_string_returns_none() -> None:
    result = _safe_decimal("")
    assert result is None


# ---------------------------------------------------------------------------
# _parse_index_row tests
# ---------------------------------------------------------------------------

BUSINESS_DATE = date(2026, 4, 5)


def test_parse_index_row_valid_record_returns_dict() -> None:
    record = {
        "indexSymbol": "NIFTY 50",
        "open": "22500.00",
        "high": "22750.50",
        "low": "22400.00",
        "last": "22650.00",
        "pe": "20.5",
        "pb": "3.2",
        "dy": "1.5",
    }
    result = _parse_index_row(record, BUSINESS_DATE)
    assert result is not None
    assert result["date"] == BUSINESS_DATE
    assert result["index_code"] == "NIFTY 50"
    assert result["close"] == Decimal("22650.00")
    assert result["open"] == Decimal("22500.00")
    assert result["pe_ratio"] == Decimal("20.5")
    assert result["pb_ratio"] == Decimal("3.2")
    assert result["div_yield"] == Decimal("1.5")


def test_parse_index_row_missing_close_returns_none() -> None:
    record = {
        "indexSymbol": "NIFTY 50",
        "open": "22500.00",
        "last": None,
    }
    result = _parse_index_row(record, BUSINESS_DATE)
    assert result is None


def test_parse_index_row_missing_symbol_returns_none() -> None:
    record = {
        "last": "22650.00",
    }
    result = _parse_index_row(record, BUSINESS_DATE)
    assert result is None


def test_parse_index_row_uses_index_field_as_fallback() -> None:
    record = {
        "index": "BANK NIFTY",
        "last": "48000.00",
    }
    result = _parse_index_row(record, BUSINESS_DATE)
    assert result is not None
    assert result["index_code"] == "BANK NIFTY"


def test_parse_index_row_index_code_is_uppercase() -> None:
    record = {
        "indexSymbol": "nifty midcap 100",
        "last": "50000.00",
    }
    result = _parse_index_row(record, BUSINESS_DATE)
    assert result is not None
    assert result["index_code"] == "NIFTY MIDCAP 100"


def test_parse_index_row_optional_fields_are_none_when_missing() -> None:
    record = {
        "indexSymbol": "NIFTY 50",
        "last": "22650.00",
    }
    result = _parse_index_row(record, BUSINESS_DATE)
    assert result is not None
    assert result["pe_ratio"] is None
    assert result["pb_ratio"] is None
    assert result["div_yield"] is None


# ---------------------------------------------------------------------------
# upsert_index_prices tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_index_prices_empty_list_returns_zero_zero() -> None:
    mock_session = AsyncMock()
    rows_processed, rows_failed = await upsert_index_prices(mock_session, [])
    assert rows_processed == 0
    assert rows_failed == 0
    mock_session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_index_prices_calls_execute_with_rows() -> None:
    mock_session = AsyncMock()
    rows = [
        {
            "date": BUSINESS_DATE,
            "index_code": "NIFTY 50",
            "open": Decimal("22500.00"),
            "high": Decimal("22750.00"),
            "low": Decimal("22400.00"),
            "close": Decimal("22650.00"),
            "pe_ratio": None,
            "pb_ratio": None,
            "div_yield": None,
        }
    ]
    rows_processed, rows_failed = await upsert_index_prices(mock_session, rows)
    assert rows_processed == 1
    assert rows_failed == 0
    mock_session.execute.assert_called_once()


@pytest.mark.asyncio
async def test_upsert_index_prices_multiple_rows_returns_count() -> None:
    mock_session = AsyncMock()
    rows = [
        {
            "date": BUSINESS_DATE,
            "index_code": f"INDEX_{i}",
            "open": Decimal("1000.00"),
            "high": Decimal("1100.00"),
            "low": Decimal("900.00"),
            "close": Decimal("1050.00"),
            "pe_ratio": None,
            "pb_ratio": None,
            "div_yield": None,
        }
        for i in range(10)
    ]
    rows_processed, rows_failed = await upsert_index_prices(mock_session, rows)
    assert rows_processed == 10
    assert rows_failed == 0


# ---------------------------------------------------------------------------
# NseIndicesPipeline.execute tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_nse_indices_execute_success_returns_rows_processed() -> None:
    """Test that execute() processes valid NSE API response correctly."""
    mock_api_data = [
        {
            "indexSymbol": "NIFTY 50",
            "open": "22500",
            "high": "22750",
            "low": "22400",
            "last": "22650",
            "pe": "20.5",
            "pb": "3.2",
            "dy": "1.5",
        },
        {
            "indexSymbol": "NIFTY BANK",
            "open": "48000",
            "high": "48500",
            "low": "47500",
            "last": "48200",
            "pe": None,
            "pb": None,
            "dy": None,
        },
    ]

    mock_session = AsyncMock()
    mock_run_log = MagicMock()

    with patch(
        "app.pipelines.indices.nse_indices._fetch_all_indices",
        new_callable=AsyncMock,
        return_value=mock_api_data,
    ):
        pipeline = NseIndicesPipeline()
        result = await pipeline.execute(BUSINESS_DATE, mock_session, mock_run_log)

    assert result.rows_processed == 2
    assert result.rows_failed == 0


@pytest.mark.asyncio
async def test_nse_indices_execute_skips_invalid_records() -> None:
    """Test that records with missing close are skipped."""
    mock_api_data = [
        {
            "indexSymbol": "NIFTY 50",
            "last": "22650",  # valid
        },
        {
            "indexSymbol": "NIFTY BANK",
            "last": None,  # missing close — should be skipped
        },
        {
            # Missing indexSymbol — should be skipped
            "last": "50000",
        },
    ]

    mock_session = AsyncMock()
    mock_run_log = MagicMock()

    with patch(
        "app.pipelines.indices.nse_indices._fetch_all_indices",
        new_callable=AsyncMock,
        return_value=mock_api_data,
    ):
        pipeline = NseIndicesPipeline()
        result = await pipeline.execute(BUSINESS_DATE, mock_session, mock_run_log)

    assert result.rows_processed == 1
    assert result.rows_failed == 0


# ---------------------------------------------------------------------------
# upsert_vix_value tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_vix_value_calls_execute() -> None:
    mock_session = AsyncMock()
    vix_value = Decimal("14.50")
    await upsert_vix_value(mock_session, BUSINESS_DATE, vix_value)
    mock_session.execute.assert_called_once()


# ---------------------------------------------------------------------------
# IndiaVixPipeline.execute tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_india_vix_execute_success_returns_one_row() -> None:
    """Test execute() when VIX value is found."""
    mock_session = AsyncMock()
    mock_run_log = MagicMock()

    with patch(
        "app.pipelines.indices.vix._fetch_vix_value",
        new_callable=AsyncMock,
        return_value=Decimal("14.50"),
    ):
        pipeline = IndiaVixPipeline()
        result = await pipeline.execute(BUSINESS_DATE, mock_session, mock_run_log)

    assert result.rows_processed == 1
    assert result.rows_failed == 0


@pytest.mark.asyncio
async def test_india_vix_execute_not_found_returns_zero_processed() -> None:
    """Test execute() when VIX value is not found in API response."""
    mock_session = AsyncMock()
    mock_run_log = MagicMock()

    with patch(
        "app.pipelines.indices.vix._fetch_vix_value",
        new_callable=AsyncMock,
        return_value=None,
    ):
        pipeline = IndiaVixPipeline()
        result = await pipeline.execute(BUSINESS_DATE, mock_session, mock_run_log)

    assert result.rows_processed == 0
    assert result.rows_failed == 1


# ---------------------------------------------------------------------------
# NseIndicesPipeline pipeline_name and config tests
# ---------------------------------------------------------------------------


def test_nse_indices_pipeline_name() -> None:
    assert NseIndicesPipeline.pipeline_name == "nse_indices"


def test_nse_indices_requires_trading_day() -> None:
    assert NseIndicesPipeline.requires_trading_day is True


def test_india_vix_pipeline_name() -> None:
    assert IndiaVixPipeline.pipeline_name == "india_vix"


def test_india_vix_requires_trading_day() -> None:
    assert IndiaVixPipeline.requires_trading_day is True
