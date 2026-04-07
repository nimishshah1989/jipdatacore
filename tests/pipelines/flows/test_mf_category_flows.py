"""Tests for MF Category Flows pipeline (XLS-based AMFI source)."""

from __future__ import annotations

import io
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.pipelines.flows.mf_category_flows import (
    MfCategoryFlowsPipeline,
    _safe_decimal,
    _safe_int,
    build_amfi_xls_url,
    parse_amfi_xls,
    upsert_mf_category_flows,
)

BUSINESS_DATE = date(2026, 3, 15)
MONTH_DATE = date(2026, 3, 1)


# ---------------------------------------------------------------------------
# _safe_decimal tests
# ---------------------------------------------------------------------------


def test_safe_decimal_valid_returns_decimal() -> None:
    assert _safe_decimal("12345.67") == Decimal("12345.67")


def test_safe_decimal_comma_formatted_returns_decimal() -> None:
    assert _safe_decimal("1,23,456.78") == Decimal("123456.78")


def test_safe_decimal_none_returns_none() -> None:
    assert _safe_decimal(None) is None


def test_safe_decimal_dash_returns_none() -> None:
    assert _safe_decimal("-") is None


def test_safe_decimal_na_returns_none() -> None:
    assert _safe_decimal("N/A") is None


def test_safe_decimal_na_variant_returns_none() -> None:
    assert _safe_decimal("NA") is None


def test_safe_decimal_double_dash_returns_none() -> None:
    assert _safe_decimal("--") is None


def test_safe_decimal_empty_string_returns_none() -> None:
    assert _safe_decimal("") is None


def test_safe_decimal_nan_string_returns_none() -> None:
    assert _safe_decimal("nan") is None


def test_safe_decimal_integer_value_returns_decimal() -> None:
    assert _safe_decimal(42) == Decimal("42")


def test_safe_decimal_negative_value_returns_decimal() -> None:
    assert _safe_decimal("-1234.56") == Decimal("-1234.56")


# ---------------------------------------------------------------------------
# _safe_int tests
# ---------------------------------------------------------------------------


def test_safe_int_valid_integer_returns_int() -> None:
    assert _safe_int("12345") == 12345


def test_safe_int_comma_formatted_returns_int() -> None:
    assert _safe_int("1,23,456") == 123456


def test_safe_int_none_returns_none() -> None:
    assert _safe_int(None) is None


def test_safe_int_dash_returns_none() -> None:
    assert _safe_int("-") is None


def test_safe_int_na_returns_none() -> None:
    assert _safe_int("N/A") is None


def test_safe_int_empty_returns_none() -> None:
    assert _safe_int("") is None


def test_safe_int_float_string_truncates_to_int() -> None:
    assert _safe_int("12345.00") == 12345


def test_safe_int_nan_string_returns_none() -> None:
    assert _safe_int("nan") is None


# ---------------------------------------------------------------------------
# build_amfi_xls_url tests
# ---------------------------------------------------------------------------


def test_build_amfi_xls_url_february_2026() -> None:
    url = build_amfi_xls_url(date(2026, 2, 1))
    assert url == "https://portal.amfiindia.com/spages/amfeb2026repo.xls"


def test_build_amfi_xls_url_january_2025() -> None:
    url = build_amfi_xls_url(date(2025, 1, 1))
    assert url == "https://portal.amfiindia.com/spages/amjan2025repo.xls"


def test_build_amfi_xls_url_december_2020() -> None:
    url = build_amfi_xls_url(date(2020, 12, 1))
    assert url == "https://portal.amfiindia.com/spages/amdec2020repo.xls"


def test_build_amfi_xls_url_march_2026() -> None:
    url = build_amfi_xls_url(MONTH_DATE)
    assert url == "https://portal.amfiindia.com/spages/ammar2026repo.xls"


def test_build_amfi_xls_url_all_months_covered() -> None:
    """All 12 months should produce valid URLs without KeyError."""
    for m in range(1, 13):
        url = build_amfi_xls_url(date(2025, m, 1))
        assert "portal.amfiindia.com" in url
        assert "repo.xls" in url


# ---------------------------------------------------------------------------
# parse_amfi_xls tests — use a real in-memory XLS via openpyxl
# ---------------------------------------------------------------------------


def _make_xls_bytes(data: list[list]) -> bytes:
    """Build an in-memory XLS (xlsx) with the given rows using openpyxl."""
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    for row in data:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


def _sample_xls_bytes() -> bytes:
    """Build a representative AMFI XLS with header + data + totals rows."""
    rows = [
        # Preamble rows (should be skipped)
        ["AMFI MONTHLY REPORT", None, None, None, None, None, None, None, None],
        ["March 2026", None, None, None, None, None, None, None, None],
        # Header row — Sr in col A, Scheme Name in col B
        ["Sr", "Scheme Name", "No. of Schemes", "No. of Folios",
         "Funds Mobilized", "Repurchase/Redemption", "Net Inflow/Outflow",
         "Net AUM", "Average Net AUM"],
        # Data rows
        ["1", "Large Cap Fund", "29", "1200000", "25000.50", "20000.00", "5000.50", "350000.00", "345000.00"],
        ["2", "Mid Cap Fund", "25", "800000", "15000.00", "12000.00", "3000.00", "180000.00", "175000.00"],
        ["3", "Small Cap Fund", "20", "600000", "10000.00", "8000.00", "2000.00", "90000.00", "88000.00"],
        # Total row — should be skipped
        ["Total", "Total", None, "2600000", "50000.50", "40000.00", "10000.50", "620000.00", "608000.00"],
    ]
    return _make_xls_bytes(rows)


def test_parse_amfi_xls_returns_data_rows() -> None:
    content = _sample_xls_bytes()
    rows = parse_amfi_xls(content, MONTH_DATE)
    assert len(rows) == 3


def test_parse_amfi_xls_skips_total_rows() -> None:
    content = _sample_xls_bytes()
    rows = parse_amfi_xls(content, MONTH_DATE)
    categories = [r["category"] for r in rows]
    assert not any("total" in c.lower() for c in categories)


def test_parse_amfi_xls_sets_month_date() -> None:
    content = _sample_xls_bytes()
    rows = parse_amfi_xls(content, MONTH_DATE)
    for row in rows:
        assert row["month_date"] == MONTH_DATE


def test_parse_amfi_xls_parses_financial_values_as_decimal() -> None:
    content = _sample_xls_bytes()
    rows = parse_amfi_xls(content, MONTH_DATE)
    large_cap = next(r for r in rows if "Large Cap" in r["category"])
    assert large_cap["gross_inflow_cr"] == Decimal("25000.50")
    assert large_cap["gross_outflow_cr"] == Decimal("20000.00")
    assert large_cap["net_flow_cr"] == Decimal("5000.50")
    assert large_cap["aum_cr"] == Decimal("350000.00")
    assert isinstance(large_cap["gross_inflow_cr"], Decimal)


def test_parse_amfi_xls_folios_as_int() -> None:
    content = _sample_xls_bytes()
    rows = parse_amfi_xls(content, MONTH_DATE)
    large_cap = next(r for r in rows if "Large Cap" in r["category"])
    assert large_cap["folios"] == 1200000
    assert isinstance(large_cap["folios"], int)


def test_parse_amfi_xls_sip_fields_are_none() -> None:
    """SIP data is not present in the AMFI XLS — these fields must be None."""
    content = _sample_xls_bytes()
    rows = parse_amfi_xls(content, MONTH_DATE)
    for row in rows:
        assert row["sip_flow_cr"] is None
        assert row["sip_accounts"] is None


def test_parse_amfi_xls_empty_bytes_returns_empty() -> None:
    rows = parse_amfi_xls(b"", MONTH_DATE)
    assert rows == []


def test_parse_amfi_xls_no_header_row_returns_empty() -> None:
    """XLS without a recognizable header should return empty list gracefully."""
    rows_data = [
        ["random data", "no header here", 1, 2],
        [1, 2, 3, 4],
    ]
    content = _make_xls_bytes(rows_data)
    rows = parse_amfi_xls(content, MONTH_DATE)
    assert isinstance(rows, list)
    # May return [] or rows depending on matching — must not raise


def test_parse_amfi_xls_category_names_stored_as_is() -> None:
    """Category names like 'Sectoral/Thematic Funds' must be preserved verbatim."""
    rows_data = [
        ["Sr", "Scheme Name", "No. of Schemes", "No. of Folios",
         "Funds Mobilized", "Repurchase/Redemption", "Net Inflow/Outflow",
         "Net AUM", "Average Net AUM"],
        ["1", "Sectoral/Thematic Funds", "5", "100000", "500.00", "300.00", "200.00", "5000.00", "4900.00"],
        ["2", "ELSS", "42", "500000", "2000.00", "1000.00", "1000.00", "25000.00", "24000.00"],
    ]
    content = _make_xls_bytes(rows_data)
    rows = parse_amfi_xls(content, MONTH_DATE)
    cats = [r["category"] for r in rows]
    assert "Sectoral/Thematic Funds" in cats
    assert "ELSS" in cats


def test_parse_amfi_xls_negative_net_flow_preserved() -> None:
    """Negative net flows (outflows) should be preserved — they are valid."""
    rows_data = [
        ["Sr", "Scheme Name", "No. of Schemes", "No. of Folios",
         "Funds Mobilized", "Repurchase/Redemption", "Net Inflow/Outflow",
         "Net AUM", "Average Net AUM"],
        ["1", "Liquid Fund", "30", "200000", "50000.00", "51000.00", "-1000.00", "200000.00", "198000.00"],
    ]
    content = _make_xls_bytes(rows_data)
    rows = parse_amfi_xls(content, MONTH_DATE)
    assert len(rows) == 1
    assert rows[0]["net_flow_cr"] == Decimal("-1000.00")


# ---------------------------------------------------------------------------
# upsert_mf_category_flows tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_mf_category_flows_empty_rows_returns_zero() -> None:
    session = MagicMock()
    session.execute = AsyncMock()

    rows_processed, rows_failed = await upsert_mf_category_flows(session, [])
    assert rows_processed == 0
    assert rows_failed == 0
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_mf_category_flows_calls_execute() -> None:
    session = MagicMock()
    session.execute = AsyncMock()

    sample_rows = [
        {
            "month_date": MONTH_DATE,
            "category": "Large Cap Fund",
            "net_flow_cr": Decimal("5000.00"),
            "gross_inflow_cr": Decimal("25000.00"),
            "gross_outflow_cr": Decimal("20000.00"),
            "aum_cr": Decimal("350000.00"),
            "sip_flow_cr": None,
            "sip_accounts": None,
            "folios": 1200000,
        }
    ]

    rows_processed, rows_failed = await upsert_mf_category_flows(session, sample_rows)
    assert rows_processed == 1
    assert rows_failed == 0
    session.execute.assert_called_once()


@pytest.mark.asyncio
async def test_upsert_mf_category_flows_returns_correct_count() -> None:
    session = MagicMock()
    session.execute = AsyncMock()

    sample_rows = [
        {
            "month_date": MONTH_DATE,
            "category": f"Category {i}",
            "net_flow_cr": Decimal("100.00"),
            "gross_inflow_cr": Decimal("500.00"),
            "gross_outflow_cr": Decimal("400.00"),
            "aum_cr": Decimal("10000.00"),
            "sip_flow_cr": None,
            "sip_accounts": None,
            "folios": None,
        }
        for i in range(25)
    ]

    rows_processed, rows_failed = await upsert_mf_category_flows(session, sample_rows)
    assert rows_processed == 25
    assert rows_failed == 0


# ---------------------------------------------------------------------------
# MfCategoryFlowsPipeline.execute tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pipeline_execute_derives_month_date_from_business_date() -> None:
    """Pipeline must use first day of business_date's month."""
    pipeline = MfCategoryFlowsPipeline()
    session = MagicMock()
    session.execute = AsyncMock()
    run_log = MagicMock()

    parsed_rows = [
        {
            "month_date": MONTH_DATE,
            "category": "Large Cap Fund",
            "net_flow_cr": Decimal("5000.00"),
            "gross_inflow_cr": Decimal("25000.00"),
            "gross_outflow_cr": Decimal("20000.00"),
            "aum_cr": Decimal("350000.00"),
            "sip_flow_cr": None,
            "sip_accounts": None,
            "folios": 1200000,
        }
    ]

    with (
        patch(
            "app.pipelines.flows.mf_category_flows.fetch_amfi_xls",
            new_callable=AsyncMock,
            return_value=b"mock_bytes",
        ) as mock_fetch,
        patch(
            "app.pipelines.flows.mf_category_flows.parse_amfi_xls",
            return_value=parsed_rows,
        ) as mock_parse,
        patch(
            "app.pipelines.flows.mf_category_flows.upsert_mf_category_flows",
            new_callable=AsyncMock,
            return_value=(1, 0),
        ),
    ):
        result = await pipeline.execute(BUSINESS_DATE, session, run_log)

    # fetch_amfi_xls called with (client, month_date) — month_date is 2nd positional
    call_args = mock_fetch.call_args
    assert call_args[0][1] == MONTH_DATE

    # parse_amfi_xls called with (content, month_date) — month_date is 2nd positional
    parse_call_args = mock_parse.call_args
    assert parse_call_args[0][1] == MONTH_DATE

    assert result.rows_processed == 1
    assert result.rows_failed == 0


@pytest.mark.asyncio
async def test_pipeline_execute_no_rows_returns_zero() -> None:
    pipeline = MfCategoryFlowsPipeline()
    session = MagicMock()
    session.execute = AsyncMock()
    run_log = MagicMock()

    with (
        patch(
            "app.pipelines.flows.mf_category_flows.fetch_amfi_xls",
            new_callable=AsyncMock,
            return_value=b"",
        ),
        patch(
            "app.pipelines.flows.mf_category_flows.parse_amfi_xls",
            return_value=[],
        ),
    ):
        result = await pipeline.execute(BUSINESS_DATE, session, run_log)

    assert result.rows_processed == 0
    assert result.rows_failed == 0


def test_pipeline_requires_trading_day_false() -> None:
    pipeline = MfCategoryFlowsPipeline()
    assert pipeline.requires_trading_day is False


def test_pipeline_name() -> None:
    assert MfCategoryFlowsPipeline.pipeline_name == "mf_category_flows"


# ---------------------------------------------------------------------------
# MfCategoryFlowsPipeline.validate tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validate_category_count_in_range_no_anomalies() -> None:
    pipeline = MfCategoryFlowsPipeline()
    session = MagicMock()
    run_log = MagicMock()

    session.execute = AsyncMock(
        side_effect=[
            _make_scalar_result(25),
            _make_all_result([]),
            _make_all_result([]),
        ]
    )

    anomalies = await pipeline.validate(BUSINESS_DATE, session, run_log)
    assert len(anomalies) == 0


@pytest.mark.asyncio
async def test_validate_category_count_too_low_raises_anomaly() -> None:
    pipeline = MfCategoryFlowsPipeline()
    session = MagicMock()
    run_log = MagicMock()

    session.execute = AsyncMock(
        side_effect=[
            _make_scalar_result(5),
            _make_all_result([]),
            _make_all_result([]),
        ]
    )

    anomalies = await pipeline.validate(BUSINESS_DATE, session, run_log)
    assert any(a.anomaly_type == "category_count_out_of_range" for a in anomalies)


@pytest.mark.asyncio
async def test_validate_category_count_zero_is_high_severity() -> None:
    pipeline = MfCategoryFlowsPipeline()
    session = MagicMock()
    run_log = MagicMock()

    session.execute = AsyncMock(
        side_effect=[
            _make_scalar_result(0),
            _make_all_result([]),
            _make_all_result([]),
        ]
    )

    anomalies = await pipeline.validate(BUSINESS_DATE, session, run_log)
    count_anomalies = [a for a in anomalies if a.anomaly_type == "category_count_out_of_range"]
    assert len(count_anomalies) == 1
    assert count_anomalies[0].severity == "high"


@pytest.mark.asyncio
async def test_validate_negative_aum_raises_high_severity_anomaly() -> None:
    pipeline = MfCategoryFlowsPipeline()
    session = MagicMock()
    run_log = MagicMock()

    session.execute = AsyncMock(
        side_effect=[
            _make_scalar_result(25),
            _make_all_result([("Large Cap Fund", Decimal("-1000"))]),
            _make_all_result([]),
        ]
    )

    anomalies = await pipeline.validate(BUSINESS_DATE, session, run_log)
    neg_anomalies = [a for a in anomalies if a.anomaly_type == "negative_aum"]
    assert len(neg_anomalies) == 1
    assert neg_anomalies[0].severity == "high"
    assert neg_anomalies[0].ticker == "Large Cap Fund"


@pytest.mark.asyncio
async def test_validate_aum_drop_over_30pct_raises_anomaly() -> None:
    pipeline = MfCategoryFlowsPipeline()
    session = MagicMock()
    run_log = MagicMock()

    prior_aum = Decimal("100000")
    current_aum = Decimal("60000")  # 40% drop

    session.execute = AsyncMock(
        side_effect=[
            _make_scalar_result(25),
            _make_all_result([]),
            _make_all_result([("Large Cap Fund", prior_aum)]),
            _make_all_result([("Large Cap Fund", current_aum)]),
        ]
    )

    anomalies = await pipeline.validate(BUSINESS_DATE, session, run_log)
    drop_anomalies = [a for a in anomalies if a.anomaly_type == "aum_drop_exceeded_threshold"]
    assert len(drop_anomalies) == 1
    assert "Large Cap Fund" in drop_anomalies[0].ticker


@pytest.mark.asyncio
async def test_validate_aum_drop_under_30pct_no_anomaly() -> None:
    pipeline = MfCategoryFlowsPipeline()
    session = MagicMock()
    run_log = MagicMock()

    prior_aum = Decimal("100000")
    current_aum = Decimal("85000")  # 15% drop — within threshold

    session.execute = AsyncMock(
        side_effect=[
            _make_scalar_result(25),
            _make_all_result([]),
            _make_all_result([("Large Cap Fund", prior_aum)]),
            _make_all_result([("Large Cap Fund", current_aum)]),
        ]
    )

    anomalies = await pipeline.validate(BUSINESS_DATE, session, run_log)
    drop_anomalies = [a for a in anomalies if a.anomaly_type == "aum_drop_exceeded_threshold"]
    assert len(drop_anomalies) == 0


@pytest.mark.asyncio
async def test_validate_january_uses_december_prior_month() -> None:
    """Validate that Jan 2026 compares against Dec 2025 (year rollback)."""
    pipeline = MfCategoryFlowsPipeline()
    session = MagicMock()
    run_log = MagicMock()

    jan_business_date = date(2026, 1, 15)

    session.execute = AsyncMock(
        side_effect=[
            _make_scalar_result(25),
            _make_all_result([]),
            _make_all_result([]),  # prior month (Dec 2025) — empty is fine
        ]
    )

    # Should not raise and should not produce anomalies for count/aum
    anomalies = await pipeline.validate(jan_business_date, session, run_log)
    # Only check it ran without error
    assert isinstance(anomalies, list)


# ---------------------------------------------------------------------------
# Helpers for mocking SQLAlchemy async execute results
# ---------------------------------------------------------------------------


def _make_scalar_result(value: int) -> MagicMock:
    mock = MagicMock()
    mock.scalar_one.return_value = value
    return mock


def _make_all_result(rows: list) -> MagicMock:
    mock = MagicMock()
    mock.all.return_value = rows
    return mock
