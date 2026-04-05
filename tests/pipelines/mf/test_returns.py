"""Tests for MF return computation functions."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.pipelines.mf.returns import (
    RETURN_PERIODS,
    compute_cagr,
    compute_returns_for_date,
    compute_simple_return,
)


# ---------------------------------------------------------------------------
# compute_simple_return
# ---------------------------------------------------------------------------

def test_compute_simple_return_positive_gain() -> None:
    result = compute_simple_return(Decimal("110"), Decimal("100"))
    assert result == Decimal("10.0000")


def test_compute_simple_return_negative_loss() -> None:
    result = compute_simple_return(Decimal("90"), Decimal("100"))
    assert result == Decimal("-10.0000")


def test_compute_simple_return_zero_change() -> None:
    result = compute_simple_return(Decimal("100"), Decimal("100"))
    assert result == Decimal("0")


def test_compute_simple_return_zero_start_returns_none() -> None:
    result = compute_simple_return(Decimal("100"), Decimal("0"))
    assert result is None


def test_compute_simple_return_negative_start_returns_none() -> None:
    result = compute_simple_return(Decimal("100"), Decimal("-5"))
    assert result is None


def test_compute_simple_return_result_is_decimal_not_float() -> None:
    result = compute_simple_return(Decimal("150"), Decimal("100"))
    assert isinstance(result, Decimal)


def test_compute_simple_return_precise_decimal() -> None:
    """Verify Decimal precision: 200% gain."""
    result = compute_simple_return(Decimal("300"), Decimal("100"))
    assert result == Decimal("200.0000")


# ---------------------------------------------------------------------------
# compute_cagr
# ---------------------------------------------------------------------------

def test_compute_cagr_1_year_equals_simple_return() -> None:
    """For 1 year, CAGR should match simple return."""
    result = compute_cagr(Decimal("110"), Decimal("100"), 1.0)
    assert result is not None
    # ((110/100)^1 - 1) * 100 = 10%
    assert abs(result - Decimal("10")) < Decimal("0.01")


def test_compute_cagr_3_year_known_value() -> None:
    """Triple over 3 years: CAGR ≈ 44.22%."""
    result = compute_cagr(Decimal("200"), Decimal("100"), 3.0)
    assert result is not None
    # (2^(1/3) - 1) * 100 ≈ 25.992%
    assert abs(result - Decimal("25.992")) < Decimal("0.01")


def test_compute_cagr_zero_start_returns_none() -> None:
    result = compute_cagr(Decimal("100"), Decimal("0"), 5.0)
    assert result is None


def test_compute_cagr_zero_years_returns_none() -> None:
    result = compute_cagr(Decimal("100"), Decimal("100"), 0.0)
    assert result is None


def test_compute_cagr_result_is_decimal() -> None:
    result = compute_cagr(Decimal("150"), Decimal("100"), 5.0)
    assert isinstance(result, Decimal)


def test_compute_cagr_negative_years_returns_none() -> None:
    result = compute_cagr(Decimal("150"), Decimal("100"), -1.0)
    assert result is None


def test_compute_cagr_negative_ratio_returns_none() -> None:
    result = compute_cagr(Decimal("100"), Decimal("200"), 5.0)
    assert result is not None  # (0.5)^0.2 - 1 is valid


# ---------------------------------------------------------------------------
# RETURN_PERIODS configuration
# ---------------------------------------------------------------------------

def test_return_periods_has_expected_columns() -> None:
    col_names = [p[0] for p in RETURN_PERIODS]
    expected = [
        "return_1d", "return_1w", "return_1m", "return_3m",
        "return_6m", "return_1y", "return_3y", "return_5y", "return_10y",
    ]
    assert col_names == expected


def test_return_periods_3y_5y_10y_use_cagr() -> None:
    cagr_periods = {p[0]: p[2] for p in RETURN_PERIODS if p[2] is not None}
    assert cagr_periods == {"return_3y": 3.0, "return_5y": 5.0, "return_10y": 10.0}


def test_return_periods_shorter_use_simple_return() -> None:
    simple_periods = {p[0]: p[2] for p in RETURN_PERIODS if p[2] is None}
    assert "return_1d" in simple_periods
    assert "return_1w" in simple_periods
    assert "return_1m" in simple_periods


# ---------------------------------------------------------------------------
# compute_returns_for_date
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_compute_returns_for_date_returns_zero_when_no_funds() -> None:
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([]))
    session.execute = AsyncMock(return_value=mock_result)

    updated, failed = await compute_returns_for_date(session, date(2026, 4, 5))
    assert updated == 0
    assert failed == 0


@pytest.mark.asyncio
async def test_compute_returns_for_date_processes_funds() -> None:
    """Ensure the function calls session.execute for historical data and updates."""
    session = AsyncMock()

    # First call: get mstar_ids for the date
    mstar_result = MagicMock()
    mstar_row = MagicMock()
    mstar_row.mstar_id = "MSTAR001"
    mstar_result.__iter__ = MagicMock(return_value=iter([mstar_row]))

    # Second call: get today's NAVs
    today_result = MagicMock()
    today_row = MagicMock()
    today_row.mstar_id = "MSTAR001"
    today_row.nav = Decimal("100.0")
    today_result.__iter__ = MagicMock(return_value=iter([today_row]))

    # Third call: get historical NAVs
    from datetime import timedelta
    base_date = date(2026, 4, 5)
    hist_result = MagicMock()
    hist_rows = [
        MagicMock(
            mstar_id="MSTAR001",
            nav_date=base_date - timedelta(days=i),
            nav=Decimal(str(max(1, 100 - i))),
        )
        for i in range(300)
    ]
    hist_result.__iter__ = MagicMock(return_value=iter(hist_rows))

    # Fourth call onwards: update statements
    update_result = MagicMock()

    session.execute = AsyncMock(side_effect=[
        mstar_result,
        today_result,
        hist_result,
        update_result,
    ])

    updated, failed = await compute_returns_for_date(
        session, date(2026, 4, 5), mstar_ids=["MSTAR001"]
    )
    assert updated == 1
    assert failed == 0


@pytest.mark.asyncio
async def test_compute_returns_for_date_handles_missing_today_nav() -> None:
    """Fund in target list but missing from today_navs → counted as failed."""
    session = AsyncMock()

    mstar_result = MagicMock()
    mstar_row = MagicMock()
    mstar_row.mstar_id = "MSTAR001"
    mstar_result.__iter__ = MagicMock(return_value=iter([mstar_row]))

    # today_navs is empty — MSTAR001 has no NAV entry
    today_result = MagicMock()
    today_result.__iter__ = MagicMock(return_value=iter([]))

    hist_result = MagicMock()
    hist_result.__iter__ = MagicMock(return_value=iter([]))

    session.execute = AsyncMock(side_effect=[mstar_result, today_result, hist_result])

    updated, failed = await compute_returns_for_date(
        session, date(2026, 4, 5), mstar_ids=["MSTAR001"]
    )
    assert failed == 1
    assert updated == 0
