"""Tests for Morningstar holdings pipeline — parsing, ISIN resolution, upsert."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

import pytest

from app.pipelines.morningstar.holdings import (
    HOLDINGS_DATAPOINTS,
    HoldingsPipeline,
    _safe_decimal,
    parse_holdings_response,
    resolve_and_mark,
    upsert_holdings_batch,
)


# ---------------------------------------------------------------------------
# _safe_decimal
# ---------------------------------------------------------------------------

def test_safe_decimal_valid() -> None:
    assert _safe_decimal("45.32") == Decimal("45.32")


def test_safe_decimal_none_returns_none() -> None:
    assert _safe_decimal(None) is None


def test_safe_decimal_invalid_returns_none() -> None:
    assert _safe_decimal("N/A") is None


def test_safe_decimal_is_not_float() -> None:
    result = _safe_decimal("1.23456789")
    assert isinstance(result, Decimal)
    # Must not be a float
    assert not isinstance(result, float)


# ---------------------------------------------------------------------------
# parse_holdings_response
# ---------------------------------------------------------------------------

SAMPLE_HOLDINGS_DATA: dict = {
    "HoldingDate": "2026-03-31",
    "Holdings": [
        {
            "ExternalId": "INF205K01UP5",
            "HoldingName": "Aditya Birla Sun Life Flexi Cap Fund",
            "Weighting": "5.23",
            "SharesHeld": "15000",
            "MarketValue": "8723456.78",
            "GlobalSectorCode": "FINL",
        },
        {
            "ISIN": "INF179K01VU7",
            "Name": "HDFC Flexi Cap",
            "Weight": "3.10",
            "Shares": "7500",
            "Value": "4200000.00",
            "SectorCode": "TECH",
        },
        {
            # No ISIN — bond/cash holding
            "HoldingName": "Government Bond 2030",
            "Weighting": "1.50",
        },
    ],
}


def test_parse_holdings_response_extracts_all_holdings() -> None:
    result = parse_holdings_response("F0GBR04M30", SAMPLE_HOLDINGS_DATA, date(2026, 3, 31))
    assert len(result) == 3


def test_parse_holdings_response_uses_api_holding_date() -> None:
    """HoldingDate from response overrides passed report_date."""
    result = parse_holdings_response("F0GBR04M30", SAMPLE_HOLDINGS_DATA, date(2026, 4, 1))
    assert all(r["as_of_date"] == date(2026, 3, 31) for r in result)


def test_parse_holdings_response_extracts_external_id_isin() -> None:
    result = parse_holdings_response("F0GBR04M30", SAMPLE_HOLDINGS_DATA, date(2026, 3, 31))
    isins = {r["isin"] for r in result}
    assert "INF205K01UP5" in isins
    assert "INF179K01VU7" in isins


def test_parse_holdings_response_null_isin_allowed() -> None:
    """Holdings without ISIN have isin=None — allowed per spec."""
    result = parse_holdings_response("F0GBR04M30", SAMPLE_HOLDINGS_DATA, date(2026, 3, 31))
    no_isin = [r for r in result if r["isin"] is None]
    assert len(no_isin) == 1


def test_parse_holdings_response_weight_is_decimal() -> None:
    result = parse_holdings_response("F0GBR04M30", SAMPLE_HOLDINGS_DATA, date(2026, 3, 31))
    row = next(r for r in result if r["isin"] == "INF205K01UP5")
    assert isinstance(row["weight_pct"], Decimal)
    assert row["weight_pct"] == Decimal("5.23")


def test_parse_holdings_response_market_value_is_decimal() -> None:
    result = parse_holdings_response("F0GBR04M30", SAMPLE_HOLDINGS_DATA, date(2026, 3, 31))
    row = next(r for r in result if r["isin"] == "INF205K01UP5")
    assert isinstance(row["market_value"], Decimal)


def test_parse_holdings_response_shares_held_is_int() -> None:
    result = parse_holdings_response("F0GBR04M30", SAMPLE_HOLDINGS_DATA, date(2026, 3, 31))
    row = next(r for r in result if r["isin"] == "INF205K01UP5")
    assert isinstance(row["shares_held"], int)
    assert row["shares_held"] == 15000


def test_parse_holdings_response_initial_is_mapped_false() -> None:
    """is_mapped is always False after parse; set True only after ISIN resolution."""
    result = parse_holdings_response("F0GBR04M30", SAMPLE_HOLDINGS_DATA, date(2026, 3, 31))
    assert all(r["is_mapped"] is False for r in result)


def test_parse_holdings_response_empty_data_returns_empty() -> None:
    result = parse_holdings_response("F0GBR04M30", {}, date(2026, 3, 31))
    assert result == []


def test_parse_holdings_response_non_list_holdings_returns_empty() -> None:
    result = parse_holdings_response("F0GBR04M30", {"Holdings": "bad"}, date(2026, 3, 31))
    assert result == []


def test_parse_holdings_response_fallback_to_report_date_on_bad_holding_date() -> None:
    """Invalid HoldingDate in response falls back to passed report_date."""
    data = {"HoldingDate": "invalid", "Holdings": [{"HoldingName": "Bond"}]}
    result = parse_holdings_response("F0001", data, date(2026, 4, 1))
    assert result[0]["as_of_date"] == date(2026, 4, 1)


# ---------------------------------------------------------------------------
# resolve_and_mark
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_resolve_and_mark_sets_instrument_id_and_is_mapped() -> None:
    """Resolved ISINs get instrument_id set and is_mapped = True."""
    instrument_id = uuid.uuid4()
    rows = [
        {
            "isin": "INF205K01UP5",
            "is_mapped": False,
            "instrument_id": None,
        }
    ]

    with patch(
        "app.pipelines.morningstar.holdings.resolve_isin_batch",
        new_callable=AsyncMock,
        return_value={"INF205K01UP5": instrument_id},
    ):
        session = AsyncMock()
        result = await resolve_and_mark(session, rows)

    assert result[0]["instrument_id"] == instrument_id
    assert result[0]["is_mapped"] is True


@pytest.mark.asyncio
async def test_resolve_and_mark_unresolved_isin_keeps_none() -> None:
    """Unresolved ISIN keeps instrument_id=None and is_mapped=False."""
    rows = [
        {
            "isin": "INF999UNKNOWN",
            "is_mapped": False,
            "instrument_id": None,
        }
    ]

    with patch(
        "app.pipelines.morningstar.holdings.resolve_isin_batch",
        new_callable=AsyncMock,
        return_value={"INF999UNKNOWN": None},
    ):
        session = AsyncMock()
        result = await resolve_and_mark(session, rows)

    assert result[0]["instrument_id"] is None
    assert result[0]["is_mapped"] is False


@pytest.mark.asyncio
async def test_resolve_and_mark_empty_rows_returns_empty() -> None:
    session = AsyncMock()
    result = await resolve_and_mark(session, [])
    assert result == []


# ---------------------------------------------------------------------------
# upsert_holdings_batch (mock session)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upsert_holdings_batch_empty_returns_zero() -> None:
    session = AsyncMock()
    count = await upsert_holdings_batch(session, [])
    assert count == 0
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_holdings_batch_executes_for_isin_rows() -> None:
    """Rows with ISIN trigger a pg_insert execute."""
    session = AsyncMock()
    rows = [
        {
            "mstar_id": "F0001",
            "as_of_date": date(2026, 3, 31),
            "isin": "INF205K01UP5",
            "holding_name": "Test",
            "weight_pct": Decimal("5.0"),
            "shares_held": 100,
            "market_value": Decimal("50000.00"),
            "sector_code": "FINL",
            "is_mapped": False,
            "instrument_id": None,
        }
    ]

    count = await upsert_holdings_batch(session, rows)

    assert count == 1
    session.execute.assert_called_once()


@pytest.mark.asyncio
async def test_upsert_holdings_batch_executes_for_no_isin_rows() -> None:
    """Rows without ISIN still trigger an insert (on_conflict_do_nothing)."""
    session = AsyncMock()
    rows = [
        {
            "mstar_id": "F0001",
            "as_of_date": date(2026, 3, 31),
            "isin": None,
            "holding_name": "Government Bond",
            "weight_pct": Decimal("2.0"),
            "shares_held": None,
            "market_value": None,
            "sector_code": None,
            "is_mapped": False,
            "instrument_id": None,
        }
    ]

    count = await upsert_holdings_batch(session, rows)

    assert count == 1
    session.execute.assert_called_once()


# ---------------------------------------------------------------------------
# HoldingsPipeline.execute (mock integration)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_holdings_pipeline_execute_sums_holding_rows() -> None:
    """execute() returns total holdings upserted across all funds."""
    from app.pipelines.morningstar.client import MorningstarClient

    mock_client = AsyncMock(spec=MorningstarClient)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.fetch = AsyncMock(return_value={"Holdings": [
        {"ExternalId": "INF001", "HoldingName": "Fund A", "Weighting": "5.0"},
    ]})

    session = AsyncMock()
    session.flush = AsyncMock()
    run_log = MagicMock()

    pipeline = HoldingsPipeline(client=mock_client)

    with patch(
        "app.pipelines.morningstar.holdings.load_target_universe",
        new_callable=AsyncMock,
        return_value=["F0001", "F0002"],
    ), patch(
        "app.pipelines.morningstar.holdings.resolve_and_mark",
        new_callable=AsyncMock,
        side_effect=lambda s, rows: rows,
    ), patch(
        "app.pipelines.morningstar.holdings.upsert_holdings_batch",
        new_callable=AsyncMock,
        return_value=3,
    ):
        result = await pipeline.execute(date(2026, 4, 1), session, run_log)

    # 2 funds × 3 holdings each = 6
    assert result.rows_processed == 6
    assert result.rows_failed == 0


@pytest.mark.asyncio
async def test_holdings_pipeline_empty_universe_returns_zero() -> None:
    session = AsyncMock()
    run_log = MagicMock()
    pipeline = HoldingsPipeline()

    with patch(
        "app.pipelines.morningstar.holdings.load_target_universe",
        new_callable=AsyncMock,
        return_value=[],
    ):
        result = await pipeline.execute(date(2026, 4, 1), session, run_log)

    assert result.rows_processed == 0
    assert result.rows_failed == 0


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

def test_holdings_datapoints_includes_holdings_key() -> None:
    assert "Holdings" in HOLDINGS_DATAPOINTS
