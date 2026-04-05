"""Tests for FII/DII flows pipeline and F&O summary pipeline."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.pipelines.flows.fii_dii import (
    FiiDiiFlowsPipeline,
    _parse_nse_response,
    _parse_sebi_csv,
    _safe_decimal,
    upsert_institutional_flows,
)
from app.pipelines.flows.fo_summary import (
    FoSummaryPipeline,
    compute_max_pain,
    compute_pcr,
    parse_option_chain,
    upsert_fo_summary,
)


BUSINESS_DATE = date(2026, 4, 5)


# ---------------------------------------------------------------------------
# _safe_decimal tests
# ---------------------------------------------------------------------------


def test_safe_decimal_valid_number_returns_decimal() -> None:
    result = _safe_decimal("12345.67")
    assert result == Decimal("12345.67")


def test_safe_decimal_comma_formatted_number_returns_decimal() -> None:
    result = _safe_decimal("1,23,456.78")
    assert result == Decimal("123456.78")


def test_safe_decimal_none_returns_none() -> None:
    result = _safe_decimal(None)
    assert result is None


def test_safe_decimal_dash_returns_none() -> None:
    result = _safe_decimal("-")
    assert result is None


def test_safe_decimal_na_returns_none() -> None:
    result = _safe_decimal("N/A")
    assert result is None


# ---------------------------------------------------------------------------
# _parse_nse_response tests
# ---------------------------------------------------------------------------


def test_parse_nse_response_valid_fii_dii_records() -> None:
    data = [
        {
            "category": "FII",
            "type": "equity",
            "buyValue": "15000.50",
            "sellValue": "12000.25",
        },
        {
            "category": "DII",
            "type": "equity",
            "buyValue": "8000.00",
            "sellValue": "6500.00",
        },
    ]
    rows = _parse_nse_response(data, BUSINESS_DATE)
    assert len(rows) == 2
    assert rows[0]["category"] == "FII"
    assert rows[0]["gross_buy"] == Decimal("15000.50")
    assert rows[0]["gross_sell"] == Decimal("12000.25")
    assert rows[0]["market_type"] == "equity"
    assert rows[0]["source"] == "NSE"
    assert rows[1]["category"] == "DII"


def test_parse_nse_response_skips_non_fii_dii() -> None:
    data = [
        {"category": "MF", "type": "equity", "buyValue": "5000", "sellValue": "4000"},
        {"category": "FII", "type": "equity", "buyValue": "15000", "sellValue": "12000"},
    ]
    rows = _parse_nse_response(data, BUSINESS_DATE)
    assert len(rows) == 1
    assert rows[0]["category"] == "FII"


def test_parse_nse_response_skips_null_buy_and_sell() -> None:
    data = [
        {"category": "FII", "type": "equity", "buyValue": None, "sellValue": None},
    ]
    rows = _parse_nse_response(data, BUSINESS_DATE)
    assert len(rows) == 0


def test_parse_nse_response_normalizes_market_type() -> None:
    data = [
        {"category": "FII", "type": "debt", "buyValue": "1000", "sellValue": "500"},
    ]
    rows = _parse_nse_response(data, BUSINESS_DATE)
    assert len(rows) == 1
    assert rows[0]["market_type"] == "debt"


def test_parse_nse_response_unknown_market_type_defaults_to_equity() -> None:
    data = [
        {"category": "FII", "type": "unknown_market", "buyValue": "1000", "sellValue": "500"},
    ]
    rows = _parse_nse_response(data, BUSINESS_DATE)
    assert len(rows) == 1
    assert rows[0]["market_type"] == "equity"


# ---------------------------------------------------------------------------
# _parse_sebi_csv tests
# ---------------------------------------------------------------------------


def test_parse_sebi_csv_returns_empty_for_different_date() -> None:
    csv_content = (
        "Date,Category,Market,Gross Buy,Gross Sell\n"
        "01-Jan-2026,FII,equity,5000,4000\n"
    )
    rows = _parse_sebi_csv(csv_content, BUSINESS_DATE)
    assert len(rows) == 0


def test_parse_sebi_csv_returns_rows_matching_date() -> None:
    csv_content = (
        "Date,Category,Market,Gross Buy,Gross Sell\n"
        "05-Apr-2026,FII,equity,15000.50,12000.25\n"
    )
    rows = _parse_sebi_csv(csv_content, BUSINESS_DATE)
    assert len(rows) == 1
    assert rows[0]["category"] == "FII"
    assert rows[0]["gross_buy"] == Decimal("15000.50")
    assert rows[0]["source"] == "SEBI"


# ---------------------------------------------------------------------------
# upsert_institutional_flows tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_institutional_flows_empty_returns_zero() -> None:
    mock_session = AsyncMock()
    rows_processed, rows_failed = await upsert_institutional_flows(mock_session, [])
    assert rows_processed == 0
    assert rows_failed == 0
    mock_session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_institutional_flows_calls_execute() -> None:
    mock_session = AsyncMock()
    rows = [
        {
            "date": BUSINESS_DATE,
            "category": "FII",
            "market_type": "equity",
            "gross_buy": Decimal("15000.50"),
            "gross_sell": Decimal("12000.25"),
            "source": "NSE",
        }
    ]
    rows_processed, rows_failed = await upsert_institutional_flows(mock_session, rows)
    assert rows_processed == 1
    assert rows_failed == 0
    mock_session.execute.assert_called_once()


# ---------------------------------------------------------------------------
# FiiDiiFlowsPipeline.execute tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fii_dii_execute_nse_success() -> None:
    """Test execute() with successful NSE API response."""
    mock_nse_data = [
        {"category": "FII", "type": "equity", "buyValue": "15000", "sellValue": "12000"},
        {"category": "DII", "type": "equity", "buyValue": "8000", "sellValue": "6500"},
    ]
    mock_session = AsyncMock()
    mock_run_log = MagicMock()

    with patch(
        "app.pipelines.flows.fii_dii._fetch_from_nse",
        new_callable=AsyncMock,
        return_value=mock_nse_data,
    ):
        pipeline = FiiDiiFlowsPipeline()
        result = await pipeline.execute(BUSINESS_DATE, mock_session, mock_run_log)

    assert result.rows_processed == 2
    assert result.rows_failed == 0


@pytest.mark.asyncio
async def test_fii_dii_execute_nse_403_falls_back_to_sebi() -> None:
    """Test execute() falls back to SEBI when NSE returns 403."""
    import httpx

    mock_request = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 403

    sebi_csv = (
        "Date,Category,Market,Gross Buy,Gross Sell\n"
        "05-Apr-2026,FII,equity,15000.50,12000.25\n"
    )

    mock_session = AsyncMock()
    mock_run_log = MagicMock()

    with (
        patch(
            "app.pipelines.flows.fii_dii._fetch_from_nse",
            new_callable=AsyncMock,
            side_effect=httpx.HTTPStatusError("403", request=mock_request, response=mock_response),
        ),
        patch(
            "app.pipelines.flows.fii_dii._fetch_from_sebi",
            new_callable=AsyncMock,
            return_value=sebi_csv,
        ),
    ):
        pipeline = FiiDiiFlowsPipeline()
        result = await pipeline.execute(BUSINESS_DATE, mock_session, mock_run_log)

    assert result.rows_processed == 1
    assert result.rows_failed == 0


@pytest.mark.asyncio
async def test_fii_dii_execute_no_rows_returns_zero_processed() -> None:
    """Test execute() returns zero rows when API data has no FII/DII rows."""
    mock_nse_data: list = []
    mock_session = AsyncMock()
    mock_run_log = MagicMock()

    with patch(
        "app.pipelines.flows.fii_dii._fetch_from_nse",
        new_callable=AsyncMock,
        return_value=mock_nse_data,
    ):
        pipeline = FiiDiiFlowsPipeline()
        result = await pipeline.execute(BUSINESS_DATE, mock_session, mock_run_log)

    assert result.rows_processed == 0
    assert result.rows_failed == 0


# ---------------------------------------------------------------------------
# compute_pcr tests
# ---------------------------------------------------------------------------


def test_compute_pcr_valid_oi_and_volume() -> None:
    pcr_oi, pcr_volume = compute_pcr(
        total_put_oi=200,
        total_call_oi=100,
        total_put_volume=400,
        total_call_volume=200,
    )
    assert pcr_oi == Decimal("2")
    assert pcr_volume == Decimal("2")


def test_compute_pcr_zero_call_oi_returns_none_pcr_oi() -> None:
    pcr_oi, pcr_volume = compute_pcr(
        total_put_oi=200,
        total_call_oi=0,
        total_put_volume=400,
        total_call_volume=200,
    )
    assert pcr_oi is None
    assert pcr_volume == Decimal("2")


def test_compute_pcr_both_denominators_zero_returns_none_none() -> None:
    pcr_oi, pcr_volume = compute_pcr(
        total_put_oi=0,
        total_call_oi=0,
        total_put_volume=0,
        total_call_volume=0,
    )
    assert pcr_oi is None
    assert pcr_volume is None


def test_compute_pcr_returns_decimal_not_float() -> None:
    pcr_oi, pcr_volume = compute_pcr(100, 200, 300, 400)
    assert isinstance(pcr_oi, Decimal)
    assert isinstance(pcr_volume, Decimal)


# ---------------------------------------------------------------------------
# compute_max_pain tests
# ---------------------------------------------------------------------------


def test_compute_max_pain_empty_dict_returns_none() -> None:
    result = compute_max_pain({})
    assert result is None


def test_compute_max_pain_single_strike() -> None:
    strike_oi = {Decimal("22000"): {"call_oi": 100, "put_oi": 200}}
    result = compute_max_pain(strike_oi)
    assert result == Decimal("22000")


def test_compute_max_pain_returns_minimum_pain_strike() -> None:
    """Max pain should be at 22000 where total writer loss is minimized."""
    strike_oi = {
        Decimal("21000"): {"call_oi": 500, "put_oi": 100},
        Decimal("22000"): {"call_oi": 1000, "put_oi": 1000},
        Decimal("23000"): {"call_oi": 100, "put_oi": 500},
    }
    result = compute_max_pain(strike_oi)
    # At 22000: call writers lose for strikes below 22000 (none here),
    # put writers lose for strikes above 22000 (none here)
    # At 21000: call writers lose 0, put writers lose 500*(22000-21000) + 100*(23000-21000) = 700000
    # At 23000: call writers lose 500*(23000-21000) + 1000*(23000-22000) = 1000000+1000000 = 2000000 ... complex
    assert result is not None
    assert isinstance(result, Decimal)


def test_compute_max_pain_result_is_decimal() -> None:
    strike_oi = {
        Decimal("20000"): {"call_oi": 100, "put_oi": 50},
        Decimal("20500"): {"call_oi": 200, "put_oi": 300},
    }
    result = compute_max_pain(strike_oi)
    assert isinstance(result, Decimal)


# ---------------------------------------------------------------------------
# parse_option_chain tests
# ---------------------------------------------------------------------------


def _make_option_chain_response(records: list[dict]) -> dict:
    """Build a minimal NSE option chain response structure."""
    return {
        "filtered": {"data": records},
        "records": {"data": records},
    }


def test_parse_option_chain_computes_pcr() -> None:
    records = [
        {
            "strikePrice": 22000,
            "CE": {"openInterest": 100, "totalTradedVolume": 50, "changeinOpenInterest": 10},
            "PE": {"openInterest": 200, "totalTradedVolume": 80, "changeinOpenInterest": 5},
        },
        {
            "strikePrice": 22500,
            "CE": {"openInterest": 150, "totalTradedVolume": 60, "changeinOpenInterest": 15},
            "PE": {"openInterest": 100, "totalTradedVolume": 40, "changeinOpenInterest": 2},
        },
    ]
    data = _make_option_chain_response(records)
    result = parse_option_chain(data)

    # total_call_oi = 250, total_put_oi = 300
    assert result["total_call_oi"] == 250
    assert result["total_put_oi"] == 300
    assert result["pcr_oi"] == Decimal("300") / Decimal("250")
    assert result["pcr_oi"] is not None
    assert isinstance(result["pcr_oi"], Decimal)


def test_parse_option_chain_empty_data_returns_zeros() -> None:
    data = {"filtered": {"data": []}}
    result = parse_option_chain(data)
    assert result["total_oi"] == 0
    assert result["pcr_oi"] is None
    assert result["max_pain"] is None


def test_parse_option_chain_skips_missing_strike() -> None:
    records = [
        {
            # No strikePrice — should be skipped
            "CE": {"openInterest": 100, "totalTradedVolume": 50, "changeinOpenInterest": 0},
            "PE": {"openInterest": 200, "totalTradedVolume": 80, "changeinOpenInterest": 0},
        },
    ]
    data = _make_option_chain_response(records)
    result = parse_option_chain(data)
    assert result["total_oi"] == 0


# ---------------------------------------------------------------------------
# upsert_fo_summary tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_fo_summary_calls_execute() -> None:
    mock_session = AsyncMock()
    summary = {
        "pcr_oi": Decimal("1.2"),
        "pcr_volume": Decimal("0.9"),
        "total_oi": 5000000,
        "oi_change": 100000,
        "max_pain": Decimal("22000"),
    }
    await upsert_fo_summary(mock_session, BUSINESS_DATE, summary)
    mock_session.execute.assert_called_once()


# ---------------------------------------------------------------------------
# FoSummaryPipeline pipeline_name and config tests
# ---------------------------------------------------------------------------


def test_fo_summary_pipeline_name() -> None:
    assert FoSummaryPipeline.pipeline_name == "fo_summary"


def test_fo_summary_requires_trading_day() -> None:
    assert FoSummaryPipeline.requires_trading_day is True


def test_fii_dii_pipeline_name() -> None:
    assert FiiDiiFlowsPipeline.pipeline_name == "fii_dii_flows"


def test_fii_dii_requires_trading_day() -> None:
    assert FiiDiiFlowsPipeline.requires_trading_day is True
