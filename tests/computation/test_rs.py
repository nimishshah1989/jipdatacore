"""Unit tests for RS score computation.

Tests verify formula correctness, Decimal output, composite weights,
and edge cases (insufficient data, zero std).
"""

from __future__ import annotations

from typing import Optional


from app.computation.rs import (
    LOOKBACKS,
    RS_WEIGHTS,
    compute_rs_composite,
    compute_rs_score,
    _cumreturn,
    _rolling_std,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_prices(n: int, start: float = 100.0, step: float = 0.5) -> list[float]:
    return [start + i * step for i in range(n)]


def _make_random_prices(seed: int = 42, n: int = 300) -> list[float]:
    """Deterministic pseudo-random prices."""
    prices = [100.0]
    a, c, m = 1664525, 1013904223, 2**32
    x = seed
    for _ in range(n - 1):
        x = (a * x + c) % m
        ret = (x / m) * 0.04 - 0.02
        prices.append(prices[-1] * (1 + ret))
    return prices


# ---------------------------------------------------------------------------
# _cumreturn tests
# ---------------------------------------------------------------------------


def test_cumreturn_basic() -> None:
    """(close_today / close_N_days_ago) - 1."""
    prices = [100.0, 105.0, 110.0, 115.0, 120.0, 125.0]
    # lookback=5: (125 / 100) - 1 = 0.25
    result = _cumreturn(prices, 5)
    assert result is not None
    assert abs(result - 0.25) < 1e-10


def test_cumreturn_insufficient_data_returns_none() -> None:
    prices = [100.0, 110.0]
    result = _cumreturn(prices, 5)
    assert result is None


def test_cumreturn_zero_base_returns_none() -> None:
    prices = [0.0] * 6
    result = _cumreturn(prices, 5)
    assert result is None


def test_cumreturn_single_day_lookback() -> None:
    prices = [100.0, 102.0]
    result = _cumreturn(prices, 1)
    assert result is not None
    assert abs(result - 0.02) < 1e-10


# ---------------------------------------------------------------------------
# _rolling_std tests
# ---------------------------------------------------------------------------


def test_rolling_std_basic_positive() -> None:
    prices = _make_random_prices()
    result = _rolling_std(prices, 21)
    assert result is not None
    assert result > 0


def test_rolling_std_insufficient_data_returns_none() -> None:
    prices = [100.0, 101.0]
    result = _rolling_std(prices, 21)
    assert result is None


def test_rolling_std_constant_prices_returns_none() -> None:
    """Zero std in constant series → None (to prevent division by zero)."""
    prices = [100.0] * 30
    result = _rolling_std(prices, 21)
    assert result is None


# ---------------------------------------------------------------------------
# compute_rs_score tests
# ---------------------------------------------------------------------------


def test_compute_rs_score_returns_float() -> None:
    entity = _make_random_prices(seed=10)
    bench = _make_random_prices(seed=20)
    result = compute_rs_score(entity, bench, LOOKBACKS["rs_1m"])
    assert result is not None
    assert isinstance(result, float)


def test_compute_rs_score_insufficient_entity_data_returns_none() -> None:
    entity = [100.0, 101.0]
    bench = _make_random_prices()
    result = compute_rs_score(entity, bench, LOOKBACKS["rs_1m"])
    assert result is None


def test_compute_rs_score_insufficient_benchmark_data_returns_none() -> None:
    entity = _make_random_prices()
    bench = [100.0, 101.0]
    result = compute_rs_score(entity, bench, LOOKBACKS["rs_1m"])
    assert result is None


def test_compute_rs_score_formula_correct() -> None:
    """Verify: (entity_cum - bench_cum) / bench_std."""
    # entity goes from 100 to 105 in 5 steps (5% gain)
    # bench goes from 100 to 102 in 5 steps (2% gain)
    # bench std should be small (near-linear)
    entity = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    bench = [100.0, 100.4, 100.8, 101.2, 101.6, 102.0]

    entity_cum = (105.0 / 100.0) - 1.0  # 0.05
    bench_cum = (102.0 / 100.0) - 1.0  # 0.02

    # bench daily returns
    bench_rets = [(bench[i] / bench[i - 1]) - 1 for i in range(1, 6)]
    import numpy as np
    bench_std = float(np.std(bench_rets, ddof=1))

    expected = (entity_cum - bench_cum) / bench_std

    result = compute_rs_score(entity, bench, 5)
    assert result is not None
    assert abs(result - expected) < 1e-6


def test_compute_rs_score_zero_benchmark_std_returns_none() -> None:
    """Zero benchmark std → cannot compute RS → None."""
    entity = _make_random_prices()
    bench = [100.0] * 300  # constant prices → zero std
    result = compute_rs_score(entity, bench, LOOKBACKS["rs_1m"])
    assert result is None


# ---------------------------------------------------------------------------
# compute_rs_composite tests
# ---------------------------------------------------------------------------


def test_compute_rs_composite_all_present_correct_weights() -> None:
    """rs_1w*0.10 + rs_1m*0.20 + rs_3m*0.30 + rs_6m*0.25 + rs_12m*0.15 = 1.0."""
    # All scores = 1.0 → composite should = 1.0
    scores = {
        "rs_1w": 1.0,
        "rs_1m": 1.0,
        "rs_3m": 1.0,
        "rs_6m": 1.0,
        "rs_12m": 1.0,
    }
    result = compute_rs_composite(scores)
    assert result is not None
    # 1.0 * (0.10 + 0.20 + 0.30 + 0.25 + 0.15) = 1.0
    assert abs(result - 1.0) < 1e-10


def test_compute_rs_composite_known_value() -> None:
    """Verify with hand-computed weighted sum."""
    scores = {
        "rs_1w": 0.5,
        "rs_1m": 1.0,
        "rs_3m": 2.0,
        "rs_6m": 1.5,
        "rs_12m": 0.8,
    }
    expected = (
        0.5 * 0.10
        + 1.0 * 0.20
        + 2.0 * 0.30
        + 1.5 * 0.25
        + 0.8 * 0.15
    )
    result = compute_rs_composite(scores)
    assert result is not None
    assert abs(result - expected) < 1e-10


def test_compute_rs_composite_all_none_returns_none() -> None:
    scores: dict[str, Optional[float]] = {
        "rs_1w": None,
        "rs_1m": None,
        "rs_3m": None,
        "rs_6m": None,
        "rs_12m": None,
    }
    result = compute_rs_composite(scores)
    assert result is None


def test_compute_rs_composite_partial_missing_normalised() -> None:
    """With some lookbacks missing, composite normalises by available weight."""
    scores: dict[str, Optional[float]] = {
        "rs_1w": None,
        "rs_1m": None,
        "rs_3m": 1.0,
        "rs_6m": 1.0,
        "rs_12m": 1.0,
    }
    # Available weight = 0.30 + 0.25 + 0.15 = 0.70
    # weighted_sum = 0.30 + 0.25 + 0.15 = 0.70
    # Result = (0.70 / 0.70) * 1.0 = 1.0
    result = compute_rs_composite(scores)
    assert result is not None
    assert abs(result - 1.0) < 1e-10


def test_rs_weights_sum_to_one() -> None:
    """Verify that RS_WEIGHTS sum to exactly 1.0."""
    total = sum(RS_WEIGHTS.values())
    assert abs(total - 1.0) < 1e-10


def test_lookbacks_values_correct() -> None:
    """Verify lookback periods match spec Section 5.8."""
    assert LOOKBACKS["rs_1w"] == 5
    assert LOOKBACKS["rs_1m"] == 21
    assert LOOKBACKS["rs_3m"] == 63
    assert LOOKBACKS["rs_6m"] == 126
    assert LOOKBACKS["rs_12m"] == 252
