"""Tests for Morningstar fund master pipeline — parsing, update logic, inactivation."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.pipelines.morningstar.fund_master import (
    FUND_MASTER_DATAPOINTS,
    INACTIVE_THRESHOLD_DAYS,
    FundMasterPipeline,
    _safe_date,
    _safe_decimal,
    mark_fund_inactive_if_stale,
    parse_fund_master_response,
    update_fund_master_row,
)


# ---------------------------------------------------------------------------
# _safe_decimal
# ---------------------------------------------------------------------------

def test_safe_decimal_valid_string() -> None:
    assert _safe_decimal("1.5") == Decimal("1.5")


def test_safe_decimal_valid_int() -> None:
    assert _safe_decimal(2) == Decimal("2")


def test_safe_decimal_valid_float_via_str() -> None:
    # Must go via str() — no float precision bleeding
    result = _safe_decimal(0.1)
    assert isinstance(result, Decimal)


def test_safe_decimal_none_returns_none() -> None:
    assert _safe_decimal(None) is None


def test_safe_decimal_invalid_string_returns_none() -> None:
    assert _safe_decimal("not-a-number") is None


def test_safe_decimal_empty_string_returns_none() -> None:
    assert _safe_decimal("") is None


# ---------------------------------------------------------------------------
# _safe_date
# ---------------------------------------------------------------------------

def test_safe_date_iso_string() -> None:
    assert _safe_date("2020-01-15") == date(2020, 1, 15)


def test_safe_date_datetime_object() -> None:
    dt = date(2021, 6, 1)
    assert _safe_date(dt) == date(2021, 6, 1)


def test_safe_date_with_time_component() -> None:
    # API sometimes returns full datetime strings
    assert _safe_date("2020-01-15T00:00:00") == date(2020, 1, 15)


def test_safe_date_invalid_returns_none() -> None:
    assert _safe_date("not-a-date") is None


def test_safe_date_none_returns_none() -> None:
    assert _safe_date(None) is None


def test_safe_date_empty_string_returns_none() -> None:
    assert _safe_date("") is None


# ---------------------------------------------------------------------------
# parse_fund_master_response
# ---------------------------------------------------------------------------

def test_parse_fund_master_response_full_data() -> None:
    """All known fields are parsed correctly."""
    data = {
        "Name": "Axis Bluechip Fund Regular Growth",
        "CategoryName": "India OE Equity Large Cap",
        "BroadCategoryGroup": "Equity",
        "NetExpenseRatio": "1.56",
        "ManagerName": "Shreyas Devalkar",
        "Benchmark": "S&P BSE 100 TRI",
        "InceptionDate": "2010-01-05",
    }

    result = parse_fund_master_response("F0GBR04M30", data)

    assert result is not None
    assert result["mstar_id"] == "F0GBR04M30"
    assert result["fund_name"] == "Axis Bluechip Fund Regular Growth"
    assert result["category_name"] == "India OE Equity Large Cap"
    assert result["broad_category"] == "Equity"
    assert result["expense_ratio"] == Decimal("1.56")
    assert result["primary_benchmark"] == "S&P BSE 100 TRI"
    assert result["inception_date"] == date(2010, 1, 5)
    assert result["investment_strategy"] == "Shreyas Devalkar"


def test_parse_fund_master_response_empty_data_returns_none() -> None:
    assert parse_fund_master_response("F0GBR04M30", {}) is None


def test_parse_fund_master_response_partial_data() -> None:
    """Partial data is parsed; missing keys return None."""
    data = {"Name": "Parag Parikh Flexi Cap Fund"}
    result = parse_fund_master_response("F000003Y31", data)

    assert result is not None
    assert result["fund_name"] == "Parag Parikh Flexi Cap Fund"
    assert result["category_name"] is None
    assert result["expense_ratio"] is None


def test_parse_fund_master_response_invalid_expense_ratio() -> None:
    """Non-numeric expense ratio is silently coerced to None."""
    data = {"Name": "Test Fund", "NetExpenseRatio": "N/A"}
    result = parse_fund_master_response("F000001", data)
    assert result is not None
    assert result["expense_ratio"] is None


def test_parse_fund_master_response_expense_ratio_is_decimal_not_float() -> None:
    """expense_ratio must be Decimal, never float."""
    data = {"NetExpenseRatio": "1.23"}
    result = parse_fund_master_response("F000002", data)
    assert isinstance(result["expense_ratio"], Decimal)


# ---------------------------------------------------------------------------
# update_fund_master_row (mock session)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_fund_master_row_executes_update() -> None:
    """update_fund_master_row calls session.execute with update statement."""
    session = AsyncMock()
    fields = {
        "mstar_id": "F0GBR04M30",
        "category_name": "India OE Equity Large Cap",
        "expense_ratio": Decimal("1.50"),
    }

    await update_fund_master_row(session, fields)

    session.execute.assert_called_once()


@pytest.mark.asyncio
async def test_update_fund_master_row_skips_none_values() -> None:
    """Fields that are None are excluded from the update."""
    session = AsyncMock()
    fields = {
        "mstar_id": "F0GBR04M30",
        "category_name": None,
        "expense_ratio": None,
        "fund_name": None,
        "broad_category": None,
        "primary_benchmark": None,
        "inception_date": None,
        "investment_strategy": None,
    }

    # All non-mstar_id fields are None → no update should be executed
    await update_fund_master_row(session, fields)
    session.execute.assert_not_called()


# ---------------------------------------------------------------------------
# mark_fund_inactive_if_stale (mock session)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mark_fund_inactive_if_stale_marks_old_fund() -> None:
    """Fund last updated >30 days ago and 404'd → marked inactive."""
    session = AsyncMock()
    stale_date = datetime.now(tz=timezone.utc) - timedelta(days=45)

    mock_result = MagicMock()
    mock_result.one_or_none.return_value = (stale_date, True)
    session.execute = AsyncMock(return_value=mock_result)

    marked = await mark_fund_inactive_if_stale(session, "F0GBR04M30")

    assert marked is True
    # Second call is the UPDATE
    assert session.execute.call_count == 2


@pytest.mark.asyncio
async def test_mark_fund_inactive_if_stale_keeps_recent_fund_active() -> None:
    """Fund last updated <30 days ago → not marked inactive."""
    session = AsyncMock()
    recent_date = datetime.now(tz=timezone.utc) - timedelta(days=5)

    mock_result = MagicMock()
    mock_result.one_or_none.return_value = (recent_date, True)
    session.execute = AsyncMock(return_value=mock_result)

    marked = await mark_fund_inactive_if_stale(session, "F0GBR04M30")

    assert marked is False
    # Only 1 call — SELECT only, no UPDATE
    assert session.execute.call_count == 1


@pytest.mark.asyncio
async def test_mark_fund_inactive_if_stale_unknown_mstar_id_returns_false() -> None:
    """Unknown mstar_id (not in DB) returns False without error."""
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.one_or_none.return_value = None
    session.execute = AsyncMock(return_value=mock_result)

    marked = await mark_fund_inactive_if_stale(session, "UNKNOWN_ID")
    assert marked is False


@pytest.mark.asyncio
async def test_mark_fund_inactive_already_inactive_returns_false() -> None:
    """Already inactive fund is not double-updated."""
    session = AsyncMock()
    stale_date = datetime.now(tz=timezone.utc) - timedelta(days=60)

    mock_result = MagicMock()
    mock_result.one_or_none.return_value = (stale_date, False)  # is_active=False
    session.execute = AsyncMock(return_value=mock_result)

    marked = await mark_fund_inactive_if_stale(session, "F0GBR04M30")
    assert marked is False
    assert session.execute.call_count == 1  # Only SELECT


# ---------------------------------------------------------------------------
# FundMasterPipeline.execute (integration with mock client + session)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fund_master_pipeline_execute_counts_processed_and_failed() -> None:
    """execute() processes funds and returns correct row counts."""
    from app.pipelines.morningstar.client import MorningstarClient

    # Mock universe
    session = AsyncMock()
    mock_universe_result = MagicMock()
    mock_universe_result.fetchall.return_value = [
        ("F0GBR04M30",),
        ("F000003Y31",),
        ("F_NOTFOUND",),
    ]

    def execute_side_effect(stmt):
        # Universe query returns fetchall
        return mock_universe_result

    session.execute = AsyncMock(side_effect=execute_side_effect)
    session.flush = AsyncMock()

    # Mock client
    mock_client = AsyncMock(spec=MorningstarClient)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    async def fetch_side_effect(id_type, identifier, datapoints):
        if identifier == "F_NOTFOUND":
            return {}
        return {"Name": f"Fund {identifier}", "CategoryName": "Equity"}

    mock_client.fetch = AsyncMock(side_effect=fetch_side_effect)

    run_log = MagicMock()
    pipeline = FundMasterPipeline(client=mock_client)

    with patch(
        "app.pipelines.morningstar.fund_master.load_target_universe",
        new_callable=AsyncMock,
        return_value=["F0GBR04M30", "F000003Y31", "F_NOTFOUND"],
    ), patch(
        "app.pipelines.morningstar.fund_master.update_fund_master_row",
        new_callable=AsyncMock,
    ), patch(
        "app.pipelines.morningstar.fund_master.mark_fund_inactive_if_stale",
        new_callable=AsyncMock,
        return_value=False,
    ):
        result = await pipeline.execute(date(2026, 4, 6), session, run_log)

    assert result.rows_processed == 2
    assert result.rows_failed == 1


@pytest.mark.asyncio
async def test_fund_master_pipeline_empty_universe_returns_zero() -> None:
    """Empty universe returns ExecutionResult(0, 0) without calling client."""
    session = AsyncMock()
    run_log = MagicMock()
    pipeline = FundMasterPipeline()

    with patch(
        "app.pipelines.morningstar.fund_master.load_target_universe",
        new_callable=AsyncMock,
        return_value=[],
    ):
        result = await pipeline.execute(date(2026, 4, 6), session, run_log)

    assert result.rows_processed == 0
    assert result.rows_failed == 0


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

def test_fund_master_datapoints_includes_required_fields() -> None:
    """FUND_MASTER_DATAPOINTS must include key fields."""
    required = {"Name", "CategoryName", "NetExpenseRatio", "InceptionDate", "Benchmark"}
    assert required.issubset(set(FUND_MASTER_DATAPOINTS))


def test_inactive_threshold_is_30_days() -> None:
    assert INACTIVE_THRESHOLD_DAYS == 30
