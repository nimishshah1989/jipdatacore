"""Tests for yfinance pipeline, FRED pipeline, and trading calendar."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.pipelines.global_data.fred_pipeline import (
    FredPipeline,
    _get_latest_observation,
    _safe_decimal,
    upsert_macro_values,
)
from app.pipelines.global_data.yfinance_pipeline import (
    ALL_TICKERS,
    COMMODITY_FX_TICKERS,
    GLOBAL_INDEX_TICKERS,
    YfinancePipeline,
    _safe_decimal as yf_safe_decimal,
    parse_yfinance_download,
    upsert_global_prices,
)
from app.pipelines.trading_calendar import (
    generate_calendar_rows,
    mark_special_saturday,
    populate_trading_calendar,
    upsert_calendar_rows,
)


BUSINESS_DATE = date(2026, 4, 5)


# ---------------------------------------------------------------------------
# yfinance _safe_decimal tests
# ---------------------------------------------------------------------------


def test_yf_safe_decimal_valid_float_returns_decimal() -> None:
    result = yf_safe_decimal(22500.75)
    assert isinstance(result, Decimal)
    assert result == Decimal("22500.75")


def test_yf_safe_decimal_none_returns_none() -> None:
    result = yf_safe_decimal(None)
    assert result is None


def test_yf_safe_decimal_nan_returns_none() -> None:
    import math

    result = yf_safe_decimal(math.nan)
    assert result is None


def test_yf_safe_decimal_inf_returns_none() -> None:
    import math

    result = yf_safe_decimal(math.inf)
    assert result is None


# ---------------------------------------------------------------------------
# Ticker list tests
# ---------------------------------------------------------------------------


def test_all_tickers_contains_global_indices() -> None:
    assert "^GSPC" in ALL_TICKERS
    assert "^IXIC" in ALL_TICKERS
    assert "^N225" in ALL_TICKERS


def test_all_tickers_contains_commodities() -> None:
    assert "GC=F" in ALL_TICKERS
    assert "CL=F" in ALL_TICKERS


def test_all_tickers_contains_fx_pairs() -> None:
    assert "USDINR=X" in ALL_TICKERS
    assert "EURUSD=X" in ALL_TICKERS


def test_all_tickers_total_count() -> None:
    assert len(ALL_TICKERS) == len(GLOBAL_INDEX_TICKERS) + len(COMMODITY_FX_TICKERS)


# ---------------------------------------------------------------------------
# parse_yfinance_download tests
# ---------------------------------------------------------------------------


def test_parse_yfinance_download_empty_df_returns_empty() -> None:
    mock_df = MagicMock()
    mock_df.empty = True
    result = parse_yfinance_download(mock_df, BUSINESS_DATE, ["^GSPC"])
    assert result == []


def test_parse_yfinance_download_none_returns_empty() -> None:
    result = parse_yfinance_download(None, BUSINESS_DATE, ["^GSPC"])
    assert result == []


# ---------------------------------------------------------------------------
# upsert_global_prices tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_global_prices_empty_returns_zero() -> None:
    mock_session = AsyncMock()
    rows_processed, rows_failed = await upsert_global_prices(mock_session, [])
    assert rows_processed == 0
    assert rows_failed == 0
    mock_session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_global_prices_calls_execute() -> None:
    mock_session = AsyncMock()
    rows = [
        {
            "date": BUSINESS_DATE,
            "ticker": "^GSPC",
            "open": Decimal("5200.00"),
            "high": Decimal("5250.00"),
            "low": Decimal("5180.00"),
            "close": Decimal("5230.00"),
            "volume": 1000000,
        }
    ]
    rows_processed, rows_failed = await upsert_global_prices(mock_session, rows)
    assert rows_processed == 1
    assert rows_failed == 0
    mock_session.execute.assert_called_once()


# ---------------------------------------------------------------------------
# YfinancePipeline.execute tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_yfinance_execute_calls_upsert_with_rows() -> None:
    """Test execute() calls upsert with parsed rows."""
    mock_rows = [
        {
            "date": BUSINESS_DATE,
            "ticker": "^GSPC",
            "open": Decimal("5200.00"),
            "high": Decimal("5250.00"),
            "low": Decimal("5180.00"),
            "close": Decimal("5230.00"),
            "volume": 1000000,
        }
    ]
    mock_session = AsyncMock()
    mock_run_log = MagicMock()

    with patch(
        "app.pipelines.global_data.yfinance_pipeline.fetch_global_prices",
        new_callable=AsyncMock,
        return_value=mock_rows,
    ):
        pipeline = YfinancePipeline()
        result = await pipeline.execute(BUSINESS_DATE, mock_session, mock_run_log)

    assert result.rows_processed == 1
    assert result.rows_failed == 0


@pytest.mark.asyncio
async def test_yfinance_execute_empty_response_returns_zero() -> None:
    """Test execute() when yfinance returns no data."""
    mock_session = AsyncMock()
    mock_run_log = MagicMock()

    with patch(
        "app.pipelines.global_data.yfinance_pipeline.fetch_global_prices",
        new_callable=AsyncMock,
        return_value=[],
    ):
        pipeline = YfinancePipeline()
        result = await pipeline.execute(BUSINESS_DATE, mock_session, mock_run_log)

    assert result.rows_processed == 0
    assert result.rows_failed == 0


def test_yfinance_pipeline_name() -> None:
    assert YfinancePipeline.pipeline_name == "yfinance_global"


def test_yfinance_requires_trading_day_false() -> None:
    """yfinance runs every day regardless of NSE trading calendar."""
    assert YfinancePipeline.requires_trading_day is False


# ---------------------------------------------------------------------------
# FRED _safe_decimal tests
# ---------------------------------------------------------------------------


def test_fred_safe_decimal_valid_number() -> None:
    result = _safe_decimal("4.75")
    assert result == Decimal("4.75")


def test_fred_safe_decimal_dot_returns_none() -> None:
    result = _safe_decimal(".")
    assert result is None


def test_fred_safe_decimal_empty_returns_none() -> None:
    result = _safe_decimal("")
    assert result is None


# ---------------------------------------------------------------------------
# _get_latest_observation tests
# ---------------------------------------------------------------------------


def test_get_latest_observation_returns_most_recent_before_date() -> None:
    observations = [
        {"date": "2026-04-03", "value": "4.50"},
        {"date": "2026-04-04", "value": "4.55"},
        {"date": "2026-04-05", "value": "4.60"},
        {"date": "2026-04-06", "value": "4.65"},  # future — should be excluded
    ]
    result = _get_latest_observation(observations, BUSINESS_DATE)
    assert result is not None
    obs_date, value = result
    assert obs_date == date(2026, 4, 5)
    assert value == Decimal("4.60")


def test_get_latest_observation_skips_missing_values() -> None:
    observations = [
        {"date": "2026-04-04", "value": "."},  # FRED missing marker
        {"date": "2026-04-03", "value": "4.50"},
    ]
    result = _get_latest_observation(observations, BUSINESS_DATE)
    assert result is not None
    obs_date, value = result
    assert obs_date == date(2026, 4, 3)
    assert value == Decimal("4.50")


def test_get_latest_observation_all_future_returns_none() -> None:
    observations = [
        {"date": "2026-04-10", "value": "4.50"},
        {"date": "2026-04-11", "value": "4.55"},
    ]
    result = _get_latest_observation(observations, BUSINESS_DATE)
    assert result is None


def test_get_latest_observation_empty_list_returns_none() -> None:
    result = _get_latest_observation([], BUSINESS_DATE)
    assert result is None


# ---------------------------------------------------------------------------
# upsert_macro_values tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_macro_values_empty_returns_zero() -> None:
    mock_session = AsyncMock()
    rows_processed, rows_failed = await upsert_macro_values(mock_session, [])
    assert rows_processed == 0
    assert rows_failed == 0
    mock_session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_macro_values_calls_execute() -> None:
    mock_session = AsyncMock()
    rows = [
        {"date": BUSINESS_DATE, "ticker": "DGS10", "value": Decimal("4.50")},
        {"date": BUSINESS_DATE, "ticker": "FEDFUNDS", "value": Decimal("5.25")},
    ]
    rows_processed, rows_failed = await upsert_macro_values(mock_session, rows)
    assert rows_processed == 2
    assert rows_failed == 0
    mock_session.execute.assert_called_once()


# ---------------------------------------------------------------------------
# FredPipeline.execute tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fred_execute_no_api_key_raises() -> None:
    """Test execute() raises ValueError when FRED_API_KEY not set."""
    mock_session = AsyncMock()
    mock_run_log = MagicMock()

    with patch(
        "app.pipelines.global_data.fred_pipeline.get_settings",
        return_value=MagicMock(fred_api_key=""),
    ):
        pipeline = FredPipeline()
        with pytest.raises(ValueError, match="FRED_API_KEY not configured"):
            await pipeline.execute(BUSINESS_DATE, mock_session, mock_run_log)


@pytest.mark.asyncio
async def test_fred_execute_success_upserts_all_series() -> None:
    """Test execute() fetches and upserts all FRED series."""
    mock_observations = [
        {"date": "2026-04-05", "value": "4.50"},
    ]
    mock_session = AsyncMock()
    mock_run_log = MagicMock()

    with (
        patch(
            "app.pipelines.global_data.fred_pipeline.get_settings",
            return_value=MagicMock(fred_api_key="test_key_abc"),
        ),
        patch(
            "app.pipelines.global_data.fred_pipeline.fetch_fred_series",
            new_callable=AsyncMock,
            return_value=mock_observations,
        ),
    ):
        pipeline = FredPipeline()
        result = await pipeline.execute(BUSINESS_DATE, mock_session, mock_run_log)

    # 6 FRED series, all return data
    assert result.rows_processed == 6
    assert result.rows_failed == 0


@pytest.mark.asyncio
async def test_fred_execute_partial_failure_counts_failed() -> None:
    """Test execute() counts failed series but continues with others."""
    import httpx

    mock_request = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 429

    call_count = 0

    async def _side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise httpx.HTTPStatusError("429", request=mock_request, response=mock_response)
        return [{"date": "2026-04-05", "value": "4.50"}]

    mock_session = AsyncMock()
    mock_run_log = MagicMock()

    with (
        patch(
            "app.pipelines.global_data.fred_pipeline.get_settings",
            return_value=MagicMock(fred_api_key="test_key"),
        ),
        patch(
            "app.pipelines.global_data.fred_pipeline.fetch_fred_series",
            side_effect=_side_effect,
        ),
    ):
        pipeline = FredPipeline()
        result = await pipeline.execute(BUSINESS_DATE, mock_session, mock_run_log)

    # 1 failed + 5 succeeded
    assert result.rows_failed == 1
    assert result.rows_processed == 5


def test_fred_pipeline_name() -> None:
    assert FredPipeline.pipeline_name == "fred_macro"


def test_fred_requires_trading_day_false() -> None:
    assert FredPipeline.requires_trading_day is False


# ---------------------------------------------------------------------------
# generate_calendar_rows tests
# ---------------------------------------------------------------------------


def test_generate_calendar_rows_produces_365_or_366_rows() -> None:
    rows = generate_calendar_rows(2026, holiday_dates=[])
    assert len(rows) in (365, 366)


def test_generate_calendar_rows_weekends_are_non_trading() -> None:
    rows = generate_calendar_rows(2026, holiday_dates=[])
    for row in rows:
        d = row["date"]
        if d.weekday() >= 5:  # Weekend
            if row["date"] not in []:
                assert row["is_trading"] is False


def test_generate_calendar_rows_holidays_are_non_trading() -> None:
    holidays = [date(2026, 1, 26), date(2026, 8, 15)]
    rows = generate_calendar_rows(2026, holiday_dates=holidays)
    row_map = {r["date"]: r for r in rows}

    assert row_map[date(2026, 1, 26)]["is_trading"] is False
    assert row_map[date(2026, 8, 15)]["is_trading"] is False


def test_generate_calendar_rows_weekday_non_holiday_is_trading() -> None:
    rows = generate_calendar_rows(2026, holiday_dates=[])
    row_map = {r["date"]: r for r in rows}

    # April 6, 2026 is a Monday (trading day)
    assert row_map[date(2026, 4, 6)]["is_trading"] is True


def test_generate_calendar_rows_special_saturday_is_trading() -> None:
    special = [date(2026, 4, 4)]  # Saturday
    rows = generate_calendar_rows(2026, holiday_dates=[], special_saturday_dates=special)
    row_map = {r["date"]: r for r in rows}

    assert row_map[date(2026, 4, 4)]["is_trading"] is True
    assert row_map[date(2026, 4, 4)]["notes"] == "Special Saturday session"


# ---------------------------------------------------------------------------
# upsert_calendar_rows tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_calendar_rows_empty_returns_zero() -> None:
    mock_session = AsyncMock()
    count = await upsert_calendar_rows(mock_session, [])
    assert count == 0
    mock_session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_calendar_rows_calls_execute() -> None:
    mock_session = AsyncMock()
    rows = [
        {"date": BUSINESS_DATE, "is_trading": True, "exchange": "NSE", "notes": None}
    ]
    count = await upsert_calendar_rows(mock_session, rows)
    assert count == 1
    mock_session.execute.assert_called_once()


# ---------------------------------------------------------------------------
# populate_trading_calendar tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_populate_trading_calendar_returns_365_or_366() -> None:
    mock_session = AsyncMock()
    count = await populate_trading_calendar(mock_session, year=2026)
    assert count in (365, 366)


@pytest.mark.asyncio
async def test_populate_trading_calendar_with_custom_holidays() -> None:
    mock_session = AsyncMock()
    holidays = [date(2026, 1, 26), date(2026, 4, 14)]
    count = await populate_trading_calendar(
        mock_session, year=2026, holiday_dates=holidays
    )
    assert count == 365


# ---------------------------------------------------------------------------
# mark_special_saturday tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mark_special_saturday_valid_saturday_calls_execute() -> None:
    mock_session = AsyncMock()
    saturday = date(2026, 4, 4)  # Saturday
    await mark_special_saturday(mock_session, saturday)
    mock_session.execute.assert_called_once()


@pytest.mark.asyncio
async def test_mark_special_saturday_non_saturday_raises_value_error() -> None:
    mock_session = AsyncMock()
    monday = date(2026, 4, 6)  # Monday
    with pytest.raises(ValueError, match="not a Saturday"):
        await mark_special_saturday(mock_session, monday)
