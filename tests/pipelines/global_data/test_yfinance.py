"""Tests for yfinance pipeline, FRED pipeline, and trading calendar."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.pipelines.global_data.fred_pipeline import (
    FRED_SERIES,
    FRED_SERIES_METADATA,
    FredPipeline,
    _get_latest_observation,
    _safe_decimal,
    upsert_macro_values,
)
from app.pipelines.global_data.yfinance_pipeline import (
    ALL_TICKERS,
    BOND_TICKERS,
    COMMODITY_EXTRA_TICKERS,
    COMMODITY_FX_TICKERS,
    CRYPTO_TICKERS,
    FOREX_EXTRA_TICKERS,
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
    expected = (
        len(GLOBAL_INDEX_TICKERS)
        + len(COMMODITY_FX_TICKERS)
        + len(BOND_TICKERS)
        + len(COMMODITY_EXTRA_TICKERS)
        + len(FOREX_EXTRA_TICKERS)
        + len(CRYPTO_TICKERS)
    )
    assert len(ALL_TICKERS) == expected


# ---------------------------------------------------------------------------
# New ticker group tests
# ---------------------------------------------------------------------------


def test_bond_tickers_contains_treasury_yields() -> None:
    assert "^TNX" in BOND_TICKERS   # 10-Year
    assert "^TYX" in BOND_TICKERS   # 30-Year
    assert "^IRX" in BOND_TICKERS   # 13-Week T-Bill
    assert "^FVX" in BOND_TICKERS   # 5-Year


def test_bond_tickers_count() -> None:
    assert len(BOND_TICKERS) == 4


def test_commodity_extra_tickers_contains_key_futures() -> None:
    assert "HG=F" in COMMODITY_EXTRA_TICKERS   # Copper
    assert "NG=F" in COMMODITY_EXTRA_TICKERS   # Natural Gas
    assert "ZC=F" in COMMODITY_EXTRA_TICKERS   # Corn
    assert "PL=F" in COMMODITY_EXTRA_TICKERS   # Platinum


def test_commodity_extra_tickers_count() -> None:
    assert len(COMMODITY_EXTRA_TICKERS) == 8


def test_forex_extra_tickers_contains_major_pairs() -> None:
    assert "GBPUSD=X" in FOREX_EXTRA_TICKERS
    assert "AUDUSD=X" in FOREX_EXTRA_TICKERS
    assert "USDCAD=X" in FOREX_EXTRA_TICKERS
    assert "USDMXN=X" in FOREX_EXTRA_TICKERS


def test_forex_extra_tickers_count() -> None:
    assert len(FOREX_EXTRA_TICKERS) == 7


def test_crypto_tickers_contains_bitcoin_and_ethereum() -> None:
    assert "BTC-USD" in CRYPTO_TICKERS
    assert "ETH-USD" in CRYPTO_TICKERS


def test_crypto_tickers_count() -> None:
    assert len(CRYPTO_TICKERS) == 2


def test_all_tickers_no_duplicates() -> None:
    assert len(ALL_TICKERS) == len(set(ALL_TICKERS))


def test_all_tickers_contains_bonds() -> None:
    assert "^TNX" in ALL_TICKERS
    assert "^TYX" in ALL_TICKERS


def test_all_tickers_contains_extra_commodities() -> None:
    assert "HG=F" in ALL_TICKERS
    assert "NG=F" in ALL_TICKERS


def test_all_tickers_contains_extra_forex() -> None:
    assert "GBPUSD=X" in ALL_TICKERS
    assert "USDKRW=X" in ALL_TICKERS


def test_all_tickers_contains_crypto() -> None:
    assert "BTC-USD" in ALL_TICKERS
    assert "ETH-USD" in ALL_TICKERS


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

    # All FRED series return data — count matches FRED_SERIES length
    assert result.rows_processed == len(FRED_SERIES)
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

    # 1 failed + (len(FRED_SERIES) - 1) succeeded
    assert result.rows_failed == 1
    assert result.rows_processed == len(FRED_SERIES) - 1


def test_fred_pipeline_name() -> None:
    assert FredPipeline.pipeline_name == "fred_macro"


def test_fred_requires_trading_day_false() -> None:
    assert FredPipeline.requires_trading_day is False


# ---------------------------------------------------------------------------
# FRED_SERIES and FRED_SERIES_METADATA expansion tests
# ---------------------------------------------------------------------------


def test_fred_series_count_expanded() -> None:
    """FRED_SERIES should now contain ~54 series (well above the original 6)."""
    assert len(FRED_SERIES) >= 50


def test_fred_series_metadata_keys_match_series_list() -> None:
    """Every entry in FRED_SERIES must have a matching entry in FRED_SERIES_METADATA."""
    assert set(FRED_SERIES) == set(FRED_SERIES_METADATA.keys())


def test_fred_series_metadata_structure() -> None:
    """Each metadata value must be a 3-tuple of (name, unit, frequency)."""
    for ticker, meta in FRED_SERIES_METADATA.items():
        assert isinstance(meta, tuple), f"{ticker}: metadata must be a tuple"
        assert len(meta) == 3, f"{ticker}: tuple must have 3 elements (name, unit, frequency)"
        name, unit, frequency = meta
        assert isinstance(name, str) and name, f"{ticker}: name must be non-empty string"
        assert isinstance(unit, str) and unit, f"{ticker}: unit must be non-empty string"
        assert isinstance(frequency, str) and frequency, f"{ticker}: frequency must be non-empty string"


def test_fred_series_metadata_valid_frequencies() -> None:
    """All frequencies must match the de_macro_master CHECK constraint."""
    allowed = {"daily", "weekly", "monthly", "quarterly", "annual"}
    for ticker, (_, _, frequency) in FRED_SERIES_METADATA.items():
        assert frequency in allowed, (
            f"{ticker}: frequency '{frequency}' not in allowed set {allowed}"
        )


def test_fred_series_ticker_lengths_within_varchar20() -> None:
    """All tickers must fit in VARCHAR(20) — the de_macro_master column size."""
    for ticker in FRED_SERIES:
        assert len(ticker) <= 20, f"Ticker '{ticker}' exceeds VARCHAR(20): {len(ticker)} chars"


def test_fred_series_contains_us_treasury_curve() -> None:
    """Full US Treasury yield curve (1M to 30Y) must be present."""
    expected = ["DGS1MO", "DGS3MO", "DGS6MO", "DGS1", "DGS2", "DGS3", "DGS5", "DGS7", "DGS10", "DGS20", "DGS30"]
    for ticker in expected:
        assert ticker in FRED_SERIES, f"Missing Treasury series: {ticker}"


def test_fred_series_contains_us_macro_indicators() -> None:
    """Key US macro indicators must be present."""
    expected = ["CPIAUCSL", "CPILFESL", "PCEPI", "PCEPILFE", "UNRATE", "PAYEMS",
                "INDPRO", "HOUST", "UMCSENT", "JTSJOL", "PPIFIS"]
    for ticker in expected:
        assert ticker in FRED_SERIES, f"Missing US macro series: {ticker}"


def test_fred_series_contains_financial_indicators() -> None:
    """Key US financial indicators must be present."""
    expected = ["FEDFUNDS", "T10Y2Y", "T10Y3M", "BAMLH0A0HYM2", "VIXCLS"]
    for ticker in expected:
        assert ticker in FRED_SERIES, f"Missing financial series: {ticker}"


def test_fred_series_contains_global_bond_yields() -> None:
    """All 10 OECD country bond yield series must be present."""
    expected = [
        "IRLTLT01DEM156N",  # Germany
        "IRLTLT01JPM156N",  # Japan
        "IRLTLT01GBM156N",  # UK
        "IRLTLT01FRM156N",  # France
        "IRLTLT01ITM156N",  # Italy
        "IRLTLT01CAM156N",  # Canada
        "IRLTLT01AUM156N",  # Australia
        "IRLTLT01KRM156N",  # South Korea
        "IRLTLT01BRM156N",  # Brazil
        "IRLTLT01INM156N",  # India
    ]
    for ticker in expected:
        assert ticker in FRED_SERIES, f"Missing global bond yield series: {ticker}"


def test_fred_series_daily_tickers_have_daily_frequency() -> None:
    """Known daily series must be tagged as 'daily' in metadata."""
    daily_tickers = ["DGS10", "DGS2", "T10Y2Y", "T10Y3M", "VIXCLS", "BAMLH0A0HYM2"]
    for ticker in daily_tickers:
        _, _, frequency = FRED_SERIES_METADATA[ticker]
        assert frequency == "daily", f"{ticker}: expected daily, got {frequency}"


def test_fred_series_monthly_tickers_have_monthly_frequency() -> None:
    """Known monthly series must be tagged as 'monthly' in metadata."""
    monthly_tickers = ["CPIAUCSL", "UNRATE", "PAYEMS", "INDPRO", "HOUST"]
    for ticker in monthly_tickers:
        _, _, frequency = FRED_SERIES_METADATA[ticker]
        assert frequency == "monthly", f"{ticker}: expected monthly, got {frequency}"


def test_fred_series_global_bond_yields_have_monthly_frequency() -> None:
    """OECD global bond yield series are published monthly."""
    global_bond_tickers = [
        "IRLTLT01DEM156N", "IRLTLT01JPM156N", "IRLTLT01GBM156N",
        "IRLTLT01FRM156N", "IRLTLT01INM156N",
    ]
    for ticker in global_bond_tickers:
        _, _, frequency = FRED_SERIES_METADATA[ticker]
        assert frequency == "monthly", f"{ticker}: expected monthly, got {frequency}"


def test_fred_series_no_duplicates() -> None:
    """FRED_SERIES list must not contain duplicate tickers."""
    assert len(FRED_SERIES) == len(set(FRED_SERIES)), "FRED_SERIES contains duplicate tickers"


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
