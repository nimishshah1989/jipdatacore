"""Unit tests for sector-level derived metrics.

Tests verify formula correctness, edge cases, and Decimal output for:
  - compute_weighted_sector_rs
  - compute_sector_breadth
"""

from __future__ import annotations

from decimal import Decimal


from app.computation.sectors import (
    compute_sector_breadth,
    compute_weighted_sector_rs,
)


# ---------------------------------------------------------------------------
# compute_weighted_sector_rs
# ---------------------------------------------------------------------------


def test_compute_weighted_sector_rs_basic() -> None:
    """Weighted average: (2*100 + 3*200) / (100+200) = 800/300 ≈ 2.6667."""
    rs_scores = [2.0, 3.0]
    market_caps = [100.0, 200.0]
    result = compute_weighted_sector_rs(rs_scores, market_caps)
    assert result is not None
    expected = Decimal(str(round((2.0 * 100.0 + 3.0 * 200.0) / 300.0, 4)))
    assert result == expected


def test_compute_weighted_sector_rs_equal_weights() -> None:
    """Equal caps → simple average."""
    rs_scores = [1.0, 2.0, 3.0]
    market_caps = [1.0, 1.0, 1.0]
    result = compute_weighted_sector_rs(rs_scores, market_caps)
    assert result is not None
    assert result == Decimal("2.0")


def test_compute_weighted_sector_rs_negative_scores() -> None:
    """Negative RS scores should be handled correctly."""
    rs_scores = [-1.5, 0.5]
    market_caps = [100.0, 100.0]
    result = compute_weighted_sector_rs(rs_scores, market_caps)
    assert result is not None
    expected = Decimal(str(round((-1.5 * 100.0 + 0.5 * 100.0) / 200.0, 4)))
    assert result == expected


def test_compute_weighted_sector_rs_mismatched_lengths_returns_none() -> None:
    """Mismatched input lengths must return None."""
    result = compute_weighted_sector_rs([1.0, 2.0], [100.0])
    assert result is None


def test_compute_weighted_sector_rs_empty_inputs_returns_none() -> None:
    """Empty inputs must return None."""
    result = compute_weighted_sector_rs([], [])
    assert result is None


def test_compute_weighted_sector_rs_zero_caps_returns_none() -> None:
    """If all market caps are zero, return None (division-by-zero guard)."""
    result = compute_weighted_sector_rs([1.0, 2.0], [0.0, 0.0])
    assert result is None


def test_compute_weighted_sector_rs_ignores_zero_cap_constituent() -> None:
    """Zero-cap constituent should be excluded from weighted average."""
    # Only second constituent counts: rs=5.0 with cap=50
    result = compute_weighted_sector_rs([1.0, 5.0], [0.0, 50.0])
    assert result is not None
    assert result == Decimal("5.0")


def test_compute_weighted_sector_rs_returns_decimal() -> None:
    """Output must be Decimal, not float."""
    result = compute_weighted_sector_rs([1.5, 2.5], [200.0, 300.0])
    assert isinstance(result, Decimal)


def test_compute_weighted_sector_rs_single_constituent() -> None:
    """Single constituent: result equals that constituent's RS."""
    result = compute_weighted_sector_rs([3.1415], [1000.0])
    assert result is not None
    assert result == Decimal(str(round(3.1415, 4)))


def test_compute_weighted_sector_rs_precision_4dp() -> None:
    """Result must be rounded to 4 decimal places."""
    rs_scores = [1.123456789]
    market_caps = [1.0]
    result = compute_weighted_sector_rs(rs_scores, market_caps)
    assert result is not None
    # 4dp rounding
    assert result == Decimal("1.1235")


# ---------------------------------------------------------------------------
# compute_sector_breadth
# ---------------------------------------------------------------------------


def test_compute_sector_breadth_all_above_50dma() -> None:
    """100% above 50DMA."""
    pct_50, pct_200 = compute_sector_breadth([True, True, True], [False, False, True])
    assert pct_50 == Decimal("100.0")
    assert pct_200 is not None
    assert pct_200 == Decimal(str(round(1 / 3 * 100.0, 4)))


def test_compute_sector_breadth_none_above_200dma() -> None:
    """0% above 200DMA."""
    pct_50, pct_200 = compute_sector_breadth([True, False], [False, False])
    assert pct_50 == Decimal("50.0")
    assert pct_200 == Decimal("0.0")


def test_compute_sector_breadth_half_above() -> None:
    """50% above each."""
    flags = [True, False, True, False]
    pct_50, pct_200 = compute_sector_breadth(flags, flags)
    assert pct_50 == Decimal("50.0")
    assert pct_200 == Decimal("50.0")


def test_compute_sector_breadth_empty_returns_none() -> None:
    """Empty lists must return (None, None)."""
    pct_50, pct_200 = compute_sector_breadth([], [])
    assert pct_50 is None
    assert pct_200 is None


def test_compute_sector_breadth_mismatched_lengths_returns_none() -> None:
    """Mismatched lengths must return (None, None)."""
    pct_50, pct_200 = compute_sector_breadth([True, False], [True])
    assert pct_50 is None
    assert pct_200 is None


def test_compute_sector_breadth_returns_decimal() -> None:
    """Output values must be Decimal type."""
    pct_50, pct_200 = compute_sector_breadth([True, False, True], [False, False, False])
    assert isinstance(pct_50, Decimal)
    assert isinstance(pct_200, Decimal)


def test_compute_sector_breadth_all_above_both() -> None:
    """All above both: 100% for each."""
    n = 10
    pct_50, pct_200 = compute_sector_breadth([True] * n, [True] * n)
    assert pct_50 == Decimal("100.0")
    assert pct_200 == Decimal("100.0")


def test_compute_sector_breadth_single_constituent_above() -> None:
    """Single constituent above 50DMA: 100% for 50DMA, 0% for 200DMA."""
    pct_50, pct_200 = compute_sector_breadth([True], [False])
    assert pct_50 == Decimal("100.0")
    assert pct_200 == Decimal("0.0")


def test_compute_sector_breadth_precision_4dp() -> None:
    """Result must have 4 decimal precision."""
    # 1/3 = 33.3333...
    pct_50, _ = compute_sector_breadth([True, False, False], [False, False, False])
    assert pct_50 is not None
    assert pct_50 == Decimal(str(round(100 / 3, 4)))
