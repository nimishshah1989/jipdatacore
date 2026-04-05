"""Unit tests for fund derived metrics.

Tests verify formula correctness, edge cases, and Decimal output for:
  - compute_holdings_weighted_rs
  - compute_coverage
  - compute_manager_alpha
  - compute_fund_risk_metrics
"""

from __future__ import annotations

from decimal import Decimal


from app.computation.fund_derived import (
    compute_coverage,
    compute_fund_risk_metrics,
    compute_holdings_weighted_rs,
    compute_manager_alpha,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_nav_prices(n: int, start: float = 100.0, step: float = 0.1) -> list[float]:
    """Generate a monotonically increasing price series."""
    return [start + i * step for i in range(n)]


def _make_random_nav(seed: int = 42, n: int = 300, start: float = 100.0) -> list[float]:
    """Deterministic pseudo-random price series."""
    prices = [start]
    a, c, m = 1664525, 1013904223, 2**32
    x = seed
    for _ in range(n - 1):
        x = (a * x + c) % m
        ret = (x / m) * 0.02 - 0.01
        prices.append(prices[-1] * (1 + ret))
    return prices


# ---------------------------------------------------------------------------
# compute_holdings_weighted_rs
# ---------------------------------------------------------------------------


def test_compute_holdings_weighted_rs_basic() -> None:
    """(rs=2*w=30 + rs=4*w=70) / 100 = 3.2."""
    rs_scores = [2.0, 4.0]
    weights = [30.0, 70.0]
    result = compute_holdings_weighted_rs(rs_scores, weights)
    assert result is not None
    expected = Decimal(str(round((2.0 * 30.0 + 4.0 * 70.0) / 100.0, 4)))
    assert result == expected


def test_compute_holdings_weighted_rs_equal_weights() -> None:
    """Equal weights → simple average."""
    rs_scores = [1.0, 2.0, 3.0]
    weights = [1.0, 1.0, 1.0]
    result = compute_holdings_weighted_rs(rs_scores, weights)
    assert result is not None
    assert result == Decimal("2.0")


def test_compute_holdings_weighted_rs_single_holding() -> None:
    """Single holding: result equals that holding's RS."""
    result = compute_holdings_weighted_rs([5.5], [25.0])
    assert result is not None
    assert result == Decimal(str(round(5.5, 4)))


def test_compute_holdings_weighted_rs_mismatched_lengths_returns_none() -> None:
    """Mismatched lengths must return None."""
    result = compute_holdings_weighted_rs([1.0, 2.0], [10.0])
    assert result is None


def test_compute_holdings_weighted_rs_empty_returns_none() -> None:
    """Empty inputs must return None."""
    result = compute_holdings_weighted_rs([], [])
    assert result is None


def test_compute_holdings_weighted_rs_zero_weights_returns_none() -> None:
    """All-zero weights must return None."""
    result = compute_holdings_weighted_rs([1.0, 2.0], [0.0, 0.0])
    assert result is None


def test_compute_holdings_weighted_rs_ignores_zero_weight_holding() -> None:
    """Zero-weight holding must be excluded."""
    # Only second holding counts
    result = compute_holdings_weighted_rs([1.0, 5.0], [0.0, 10.0])
    assert result is not None
    assert result == Decimal("5.0")


def test_compute_holdings_weighted_rs_negative_rs() -> None:
    """Negative RS values must be handled correctly."""
    rs_scores = [-2.0, 2.0]
    weights = [50.0, 50.0]
    result = compute_holdings_weighted_rs(rs_scores, weights)
    assert result is not None
    assert result == Decimal("0.0")


def test_compute_holdings_weighted_rs_returns_decimal() -> None:
    """Output must be Decimal type."""
    result = compute_holdings_weighted_rs([1.5], [100.0])
    assert isinstance(result, Decimal)


def test_compute_holdings_weighted_rs_precision_4dp() -> None:
    """Result must be rounded to 4 decimal places."""
    # 1/3 RS with equal weight
    rs_scores = [1.0 / 3.0]
    weights = [1.0]
    result = compute_holdings_weighted_rs(rs_scores, weights)
    assert result is not None
    assert result == Decimal(str(round(1.0 / 3.0, 4)))


# ---------------------------------------------------------------------------
# compute_coverage
# ---------------------------------------------------------------------------


def test_compute_coverage_full_coverage() -> None:
    """100% weight mapped → 100% coverage."""
    result = compute_coverage(100.0)
    assert result == Decimal("100.0")


def test_compute_coverage_half() -> None:
    """50% weight mapped → 50% coverage."""
    result = compute_coverage(50.0)
    assert result == Decimal("50.0")


def test_compute_coverage_zero() -> None:
    """No mapped holdings → 0% coverage."""
    result = compute_coverage(0.0)
    assert result == Decimal("0.0")


def test_compute_coverage_returns_decimal() -> None:
    """Output must be Decimal type."""
    result = compute_coverage(75.0)
    assert isinstance(result, Decimal)


def test_compute_coverage_capped_at_100() -> None:
    """Coverage cannot exceed 100% (guard against floating-point overshoot)."""
    result = compute_coverage(110.0)
    assert result <= Decimal("100.0")


def test_compute_coverage_precision_2dp() -> None:
    """Coverage is rounded to 2 decimal places."""
    result = compute_coverage(33.333)
    assert result == Decimal(str(round(33.333, 2)))


# ---------------------------------------------------------------------------
# compute_manager_alpha
# ---------------------------------------------------------------------------


def test_compute_manager_alpha_positive() -> None:
    """NAV RS > derived RS → positive alpha."""
    result = compute_manager_alpha(Decimal("3.5"), Decimal("2.0"))
    assert result is not None
    assert result == Decimal("1.5")


def test_compute_manager_alpha_negative() -> None:
    """NAV RS < derived RS → negative alpha."""
    result = compute_manager_alpha(Decimal("1.0"), Decimal("3.0"))
    assert result is not None
    assert result == Decimal("-2.0")


def test_compute_manager_alpha_zero() -> None:
    """Equal RS scores → alpha of 0."""
    result = compute_manager_alpha(Decimal("2.5"), Decimal("2.5"))
    assert result is not None
    assert result == Decimal("0.0")


def test_compute_manager_alpha_none_nav_rs() -> None:
    """None nav_rs_composite must return None."""
    result = compute_manager_alpha(None, Decimal("2.0"))
    assert result is None


def test_compute_manager_alpha_none_derived_rs() -> None:
    """None derived_rs_composite must return None."""
    result = compute_manager_alpha(Decimal("2.0"), None)
    assert result is None


def test_compute_manager_alpha_both_none() -> None:
    """Both None must return None."""
    result = compute_manager_alpha(None, None)
    assert result is None


def test_compute_manager_alpha_returns_decimal() -> None:
    """Output must be Decimal type."""
    result = compute_manager_alpha(Decimal("1.0"), Decimal("0.5"))
    assert isinstance(result, Decimal)


def test_compute_manager_alpha_precision_4dp() -> None:
    """Result must be rounded to 4 decimal places."""
    result = compute_manager_alpha(Decimal("1.12345678"), Decimal("0.0"))
    assert result is not None
    assert result == Decimal(str(round(1.12345678, 4)))


# ---------------------------------------------------------------------------
# compute_fund_risk_metrics
# ---------------------------------------------------------------------------


def test_compute_fund_risk_metrics_insufficient_data_returns_none() -> None:
    """Fewer than 2 NAV points → all metrics None."""
    metrics = compute_fund_risk_metrics([100.0], [3000.0])
    for key, val in metrics.items():
        assert val is None, f"Expected None for {key}, got {val}"


def test_compute_fund_risk_metrics_keys_present() -> None:
    """All expected metric keys must be present in the result."""
    prices = _make_nav_prices(300, step=0.5)
    bench = _make_nav_prices(300, start=18000.0, step=5.0)
    metrics = compute_fund_risk_metrics(prices, bench)
    expected_keys = {
        "sharpe_1y", "sharpe_3y", "sortino_1y",
        "max_drawdown_1y", "max_drawdown_3y",
        "volatility_1y", "volatility_3y",
        "beta_vs_nifty",
    }
    assert set(metrics.keys()) == expected_keys


def test_compute_fund_risk_metrics_1y_window_computed() -> None:
    """With 252+ NAV points, 1-year metrics should not all be None."""
    prices = _make_random_nav(seed=10, n=280)
    bench = _make_random_nav(seed=20, n=280, start=18000.0)
    metrics = compute_fund_risk_metrics(prices, bench)
    # At least sharpe_1y should be computed
    assert metrics["sharpe_1y"] is not None


def test_compute_fund_risk_metrics_3y_requires_sufficient_nav() -> None:
    """With fewer than MIN_NAV_OBSERVATIONS_3Y data points, 3y metrics remain None."""
    prices = _make_random_nav(n=252)
    bench = _make_random_nav(n=252, start=18000.0)
    metrics = compute_fund_risk_metrics(prices, bench)
    assert metrics["sharpe_3y"] is None
    assert metrics["volatility_3y"] is None
    assert metrics["max_drawdown_3y"] is None


def test_compute_fund_risk_metrics_returns_decimal_types() -> None:
    """Non-None metric values must be Decimal type."""
    prices = _make_random_nav(seed=7, n=280)
    bench = _make_random_nav(seed=8, n=280, start=18000.0)
    metrics = compute_fund_risk_metrics(prices, bench)
    for key, val in metrics.items():
        if val is not None:
            assert isinstance(val, Decimal), f"{key} should be Decimal, got {type(val)}"


def test_compute_fund_risk_metrics_monotonic_prices_zero_sortino() -> None:
    """Monotonically increasing prices → no downside returns → sortino None."""
    prices = _make_nav_prices(260, step=0.1)
    bench = _make_nav_prices(260, start=18000.0, step=1.0)
    metrics = compute_fund_risk_metrics(prices, bench)
    # Sortino with no downside returns should be None
    assert metrics["sortino_1y"] is None


def test_compute_fund_risk_metrics_max_drawdown_non_positive() -> None:
    """Max drawdown must be non-positive (losses are negative)."""
    prices = _make_random_nav(seed=99, n=280)
    bench = _make_random_nav(seed=88, n=280, start=18000.0)
    metrics = compute_fund_risk_metrics(prices, bench)
    if metrics["max_drawdown_1y"] is not None:
        assert metrics["max_drawdown_1y"] <= Decimal("0.0")


def test_compute_fund_risk_metrics_volatility_positive() -> None:
    """Annualised volatility must be positive for non-constant prices."""
    prices = _make_random_nav(seed=5, n=280)
    bench = _make_random_nav(seed=6, n=280, start=18000.0)
    metrics = compute_fund_risk_metrics(prices, bench)
    if metrics["volatility_1y"] is not None:
        assert metrics["volatility_1y"] > Decimal("0.0")


def test_compute_fund_risk_metrics_empty_benchmark_no_beta() -> None:
    """Empty benchmark price series → beta must be None."""
    prices = _make_random_nav(n=280)
    metrics = compute_fund_risk_metrics(prices, [])
    assert metrics["beta_vs_nifty"] is None
