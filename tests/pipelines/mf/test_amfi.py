"""Tests for AMFI NAV parsing, filtering, and upsert logic."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.pipelines.mf.amfi import (
    AmfiNavRow,
    compute_checksum,
    filter_universe,
    parse_amfi_date,
    parse_amfi_nav_content,
    validate_freshness,
    upsert_nav_rows,
    build_amfi_code_to_mstar_map,
)


# ---------------------------------------------------------------------------
# compute_checksum
# ---------------------------------------------------------------------------

def test_compute_checksum_returns_64_char_hex() -> None:
    data = b"hello world"
    result = compute_checksum(data)
    assert len(result) == 64
    assert all(c in "0123456789abcdef" for c in result)


def test_compute_checksum_deterministic() -> None:
    data = b"same content"
    assert compute_checksum(data) == compute_checksum(data)


def test_compute_checksum_differs_for_different_content() -> None:
    assert compute_checksum(b"a") != compute_checksum(b"b")


# ---------------------------------------------------------------------------
# parse_amfi_date
# ---------------------------------------------------------------------------

def test_parse_amfi_date_standard_format() -> None:
    result = parse_amfi_date("05-Apr-2026")
    assert result == date(2026, 4, 5)


def test_parse_amfi_date_slash_format() -> None:
    result = parse_amfi_date("05/04/2026")
    assert result == date(2026, 4, 5)


def test_parse_amfi_date_invalid_returns_none() -> None:
    result = parse_amfi_date("not-a-date")
    assert result is None


def test_parse_amfi_date_empty_returns_none() -> None:
    result = parse_amfi_date("")
    assert result is None


# ---------------------------------------------------------------------------
# parse_amfi_nav_content
# ---------------------------------------------------------------------------

SAMPLE_AMFI_CONTENT = """Open Ended Schemes(Equity Scheme - Flexi Cap Fund)

Aditya Birla Sun Life AMC Limited
120503;INF205K01UP5;-;Aditya Birla Sun Life Flexi Cap Fund - Growth - Regular Plan;579.2345;05-Apr-2026
120504;INF205K01UQ3;-;Aditya Birla Sun Life Flexi Cap Fund - IDCW - Regular Plan;45.6789;05-Apr-2026

HDFC Asset Management Company Limited
119551;INF179K01VU7;-;HDFC Flexi Cap Fund - Growth Option;890.1234;05-Apr-2026

Open Ended Schemes(Debt Scheme - Low Duration Fund)
119552;INF179K01VV5;-;HDFC Low Duration Fund - Regular Plan - Growth;19.5678;05-Apr-2026
"""


def test_parse_amfi_nav_content_returns_list_of_rows() -> None:
    rows = parse_amfi_nav_content(SAMPLE_AMFI_CONTENT)
    assert len(rows) == 4


def test_parse_amfi_nav_content_correct_values() -> None:
    rows = parse_amfi_nav_content(SAMPLE_AMFI_CONTENT)
    first = rows[0]
    assert first.amfi_code == "120503"
    assert first.nav == Decimal("579.2345")
    assert first.nav_date == date(2026, 4, 5)
    assert first.scheme_name == "Aditya Birla Sun Life Flexi Cap Fund - Growth - Regular Plan"


def test_parse_amfi_nav_content_skips_blank_lines() -> None:
    content = "\n\n120503;INF;-;Test Fund Growth;100.0;05-Apr-2026\n\n"
    rows = parse_amfi_nav_content(content)
    assert len(rows) == 1


def test_parse_amfi_nav_content_skips_section_headers() -> None:
    content = "Open Ended Schemes(Equity)\nAditya Birla AMC\n120503;-;-;Test Fund;100.0;05-Apr-2026"
    rows = parse_amfi_nav_content(content)
    assert len(rows) == 1


def test_parse_amfi_nav_content_skips_invalid_nav() -> None:
    content = "120503;-;-;Test Fund;N.A.;05-Apr-2026"
    rows = parse_amfi_nav_content(content)
    assert len(rows) == 0


def test_parse_amfi_nav_content_skips_zero_nav() -> None:
    content = "120503;-;-;Test Fund;0.0;05-Apr-2026"
    rows = parse_amfi_nav_content(content)
    assert len(rows) == 0


def test_parse_amfi_nav_content_nav_is_decimal_not_float() -> None:
    content = "120503;-;-;Test Fund;579.2345;05-Apr-2026"
    rows = parse_amfi_nav_content(content)
    assert isinstance(rows[0].nav, Decimal)


def test_parse_amfi_nav_content_normalizes_dash_isin_to_none() -> None:
    content = "120503;-;-;Test Fund;100.0;05-Apr-2026"
    rows = parse_amfi_nav_content(content)
    assert rows[0].isin_div_payout is None
    assert rows[0].isin_div_reinvestment is None


def test_parse_amfi_nav_content_preserves_valid_isin() -> None:
    content = "120503;INF205K01UP5;INF205K01UQ3;Test Fund;100.0;05-Apr-2026"
    rows = parse_amfi_nav_content(content)
    assert rows[0].isin_div_payout == "INF205K01UP5"
    assert rows[0].isin_div_reinvestment == "INF205K01UQ3"


def test_parse_amfi_nav_content_skips_invalid_date() -> None:
    content = "120503;-;-;Test Fund;100.0;invalid-date"
    rows = parse_amfi_nav_content(content)
    assert len(rows) == 0


# ---------------------------------------------------------------------------
# filter_universe
# ---------------------------------------------------------------------------

def _make_row(amfi_code: str, scheme_name: str, nav: str = "100.0") -> AmfiNavRow:
    return AmfiNavRow(
        amfi_code=amfi_code,
        isin_div_payout=None,
        isin_div_reinvestment=None,
        scheme_name=scheme_name,
        nav=Decimal(nav),
        nav_date=date(2026, 4, 5),
    )


def test_filter_universe_includes_equity_growth_regular() -> None:
    rows = [_make_row("1001", "HDFC Equity Fund - Growth - Regular Plan")]
    result = filter_universe(rows)
    assert len(result) == 1


def test_filter_universe_excludes_idcw_plans() -> None:
    rows = [_make_row("1002", "HDFC Equity Fund - IDCW - Regular Plan")]
    result = filter_universe(rows)
    assert len(result) == 0


def test_filter_universe_excludes_non_equity_funds() -> None:
    rows = [_make_row("1003", "HDFC Low Duration Fund - Regular Plan - Growth")]
    result = filter_universe(rows)
    assert len(result) == 0


def test_filter_universe_excludes_dividend_plans() -> None:
    rows = [_make_row("1004", "Axis Equity Fund - Dividend - Regular Plan")]
    result = filter_universe(rows)
    assert len(result) == 0


def test_filter_universe_includes_elss() -> None:
    rows = [_make_row("1005", "Axis ELSS Tax Saver Fund - Growth - Regular Plan")]
    result = filter_universe(rows)
    assert len(result) == 1


def test_filter_universe_returns_empty_for_empty_input() -> None:
    assert filter_universe([]) == []


# ---------------------------------------------------------------------------
# validate_freshness
# ---------------------------------------------------------------------------

def test_validate_freshness_passes_when_conditions_met() -> None:
    today = date(2026, 4, 5)
    rows = [
        _make_row(str(i), f"Fund {i} Equity Growth Regular")
        for i in range(1001)
    ]
    is_valid, _ = validate_freshness(rows, today)
    assert is_valid is True


def test_validate_freshness_fails_when_too_few_rows() -> None:
    today = date(2026, 4, 5)
    rows = [_make_row(str(i), f"Fund {i}") for i in range(500)]
    is_valid, reason = validate_freshness(rows, today)
    assert is_valid is False
    assert "500" in reason


def test_validate_freshness_fails_when_date_mismatch() -> None:
    today = date(2026, 4, 5)
    stale_date = date(2026, 4, 4)
    rows = [
        AmfiNavRow(
            amfi_code=str(i),
            isin_div_payout=None,
            isin_div_reinvestment=None,
            scheme_name=f"Fund {i}",
            nav=Decimal("100"),
            nav_date=stale_date,
        )
        for i in range(1001)
    ]
    is_valid, reason = validate_freshness(rows, today)
    assert is_valid is False
    assert today.isoformat() in reason


# ---------------------------------------------------------------------------
# upsert_nav_rows
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upsert_nav_rows_returns_counts() -> None:
    """Rows with mapping are counted; rows without mapping are skipped."""
    session = AsyncMock()
    session.execute = AsyncMock()

    rows = [
        AmfiNavRow("100", None, None, "Fund A Equity Growth", Decimal("150.0"), date(2026, 4, 5)),
        AmfiNavRow("200", None, None, "Fund B Equity Growth", Decimal("200.0"), date(2026, 4, 5)),
        AmfiNavRow("999", None, None, "Fund No Mapping",      Decimal("50.0"),  date(2026, 4, 5)),
    ]
    amfi_to_mstar = {"100": "MSTAR001", "200": "MSTAR002"}

    inserted, skipped = await upsert_nav_rows(session, rows, amfi_to_mstar, 42)
    assert inserted == 2
    assert skipped == 1
    assert session.execute.called


@pytest.mark.asyncio
async def test_upsert_nav_rows_empty_rows_returns_zero() -> None:
    session = AsyncMock()
    inserted, skipped = await upsert_nav_rows(session, [], {}, 1)
    assert inserted == 0
    assert skipped == 0


# ---------------------------------------------------------------------------
# build_amfi_code_to_mstar_map
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_build_amfi_code_to_mstar_map_returns_dict() -> None:
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([
        ("101010", "MSTAR_X"),
        ("202020", "MSTAR_Y"),
    ]))
    session.execute = AsyncMock(return_value=mock_result)

    mapping = await build_amfi_code_to_mstar_map(session)
    assert mapping == {"101010": "MSTAR_X", "202020": "MSTAR_Y"}


@pytest.mark.asyncio
async def test_build_amfi_code_to_mstar_map_empty_returns_empty_dict() -> None:
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([]))
    session.execute = AsyncMock(return_value=mock_result)

    mapping = await build_amfi_code_to_mstar_map(session)
    assert mapping == {}
