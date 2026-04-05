"""Unit tests for technical indicator computations.

Tests verify:
- Decimal output for all financial computations
- Formula correctness to 4 decimal places
- Edge cases: empty input, insufficient data, zero division
"""

from __future__ import annotations

from decimal import Decimal


from app.computation.technicals import (
    compute_adx,
    compute_beta,
    compute_bollinger,
    compute_ema,
    compute_macd,
    compute_max_drawdown,
    compute_mfi,
    compute_obv,
    compute_relative_volume,
    compute_roc,
    compute_rsi_wilder,
    compute_sharpe,
    compute_sma,
    compute_sortino,
    compute_volatility,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_prices(n: int, start: float = 100.0, step: float = 1.0) -> list[float]:
    """Generate a simple ascending price series."""
    return [start + i * step for i in range(n)]


def _make_random_prices(seed: int = 42) -> list[float]:
    """Generate deterministic pseudo-random prices (no external deps)."""
    prices = [100.0]
    # LCG for reproducibility
    a, c, m = 1664525, 1013904223, 2**32
    x = seed
    for _ in range(299):
        x = (a * x + c) % m
        # Returns in [-0.02, 0.02]
        ret = (x / m) * 0.04 - 0.02
        prices.append(prices[-1] * (1 + ret))
    return prices


# ---------------------------------------------------------------------------
# EMA tests
# ---------------------------------------------------------------------------


def test_compute_ema_empty_input_returns_empty() -> None:
    result = compute_ema([], 10)
    assert result == []


def test_compute_ema_insufficient_data_returns_all_none() -> None:
    prices = _make_prices(5)
    result = compute_ema(prices, 10)
    assert all(v is None for v in result)


def test_compute_ema_returns_decimal_values() -> None:
    prices = _make_prices(30)
    result = compute_ema(prices, 10)
    non_none = [v for v in result if v is not None]
    assert len(non_none) > 0
    for val in non_none:
        assert isinstance(val, Decimal), f"Expected Decimal, got {type(val)}"


def test_compute_ema_first_value_equals_sma_seed() -> None:
    """EMA seeds at index period-1 with the SMA of first `period` values."""
    prices = _make_prices(20, start=100.0, step=1.0)
    period = 10
    result = compute_ema(prices, period)
    # SMA of first 10 prices: 100..109 → mean=104.5
    expected_seed = Decimal("104.5000")
    assert result[period - 1] == expected_seed


def test_compute_ema_length_matches_input() -> None:
    prices = _make_prices(50)
    result = compute_ema(prices, 10)
    assert len(result) == 50


def test_compute_ema_formula_correctness() -> None:
    """Verify EMA update formula: EMA[i] = price[i]*k + EMA[i-1]*(1-k)."""
    prices = [10.0, 11.0, 12.0, 13.0, 14.0]
    period = 3
    # k = 2/(period+1) = 0.5

    result = compute_ema(prices, period)

    # Seed: SMA of first 3 = (10+11+12)/3 = 11.0
    assert result[2] == Decimal("11.0000")

    # EMA[3] = 13 * 0.5 + 11 * 0.5 = 12.0
    assert result[3] == Decimal("12.0000")

    # EMA[4] = 14 * 0.5 + 12 * 0.5 = 13.0
    assert result[4] == Decimal("13.0000")


# ---------------------------------------------------------------------------
# SMA tests
# ---------------------------------------------------------------------------


def test_compute_sma_empty_input_returns_empty() -> None:
    result = compute_sma([], 10)
    assert result == []


def test_compute_sma_insufficient_data_returns_all_none() -> None:
    prices = _make_prices(5)
    result = compute_sma(prices, 10)
    assert all(v is None for v in result)


def test_compute_sma_returns_decimal_values() -> None:
    prices = _make_prices(30)
    result = compute_sma(prices, 10)
    non_none = [v for v in result if v is not None]
    assert len(non_none) > 0
    for val in non_none:
        assert isinstance(val, Decimal)


def test_compute_sma_first_value_correct() -> None:
    prices = [1.0, 2.0, 3.0, 4.0, 5.0]
    result = compute_sma(prices, 3)
    # SMA(3) at index 2 = (1+2+3)/3 = 2.0
    assert result[2] == Decimal("2.0000")


def test_compute_sma_incremental_update_correct() -> None:
    prices = [1.0, 2.0, 3.0, 4.0, 5.0]
    result = compute_sma(prices, 3)
    # SMA at index 3 = (2+3+4)/3 = 3.0
    assert result[3] == Decimal("3.0000")
    # SMA at index 4 = (3+4+5)/3 = 4.0
    assert result[4] == Decimal("4.0000")


# ---------------------------------------------------------------------------
# RSI tests
# ---------------------------------------------------------------------------


def test_compute_rsi_wilder_empty_returns_empty() -> None:
    result = compute_rsi_wilder([], 14)
    assert result == []


def test_compute_rsi_wilder_insufficient_data_all_none() -> None:
    prices = _make_prices(10)
    result = compute_rsi_wilder(prices, 14)
    assert all(v is None for v in result)


def test_compute_rsi_wilder_returns_decimal() -> None:
    prices = _make_random_prices()
    result = compute_rsi_wilder(prices, 14)
    non_none = [v for v in result if v is not None]
    assert len(non_none) > 0
    for val in non_none:
        assert isinstance(val, Decimal)


def test_compute_rsi_wilder_range_0_to_100() -> None:
    prices = _make_random_prices()
    result = compute_rsi_wilder(prices, 14)
    non_none = [v for v in result if v is not None]
    for val in non_none:
        assert Decimal("0") <= val <= Decimal("100"), f"RSI out of range: {val}"


def test_compute_rsi_wilder_all_gains_returns_100() -> None:
    """If all moves are gains, RSI should be 100."""
    prices = [float(i) for i in range(1, 20)]  # strictly increasing
    result = compute_rsi_wilder(prices, 14)
    non_none = [v for v in result if v is not None]
    # All RSI values should be 100.0
    for val in non_none:
        assert val == Decimal("100.0000"), f"Expected 100, got {val}"


def test_compute_rsi_wilder_all_losses_returns_0() -> None:
    """If all moves are losses, RSI should be 0."""
    prices = [float(20 - i) for i in range(20)]  # strictly decreasing
    result = compute_rsi_wilder(prices, 14)
    non_none = [v for v in result if v is not None]
    for val in non_none:
        assert val == Decimal("0.0000"), f"Expected 0, got {val}"


# ---------------------------------------------------------------------------
# MACD tests
# ---------------------------------------------------------------------------


def test_compute_macd_returns_three_lists() -> None:
    prices = _make_random_prices()
    macd_line, signal_line, histogram = compute_macd(prices)
    assert len(macd_line) == len(prices)
    assert len(signal_line) == len(prices)
    assert len(histogram) == len(prices)


def test_compute_macd_returns_decimal_values() -> None:
    prices = _make_random_prices()
    macd_line, signal_line, histogram = compute_macd(prices)
    for lst in [macd_line, signal_line, histogram]:
        for val in lst:
            if val is not None:
                assert isinstance(val, Decimal)


def test_compute_macd_histogram_equals_macd_minus_signal() -> None:
    prices = _make_random_prices()
    macd_line, signal_line, histogram = compute_macd(prices)
    for i in range(len(prices)):
        if macd_line[i] is not None and signal_line[i] is not None and histogram[i] is not None:
            expected = Decimal(str(round(float(macd_line[i]) - float(signal_line[i]), 4)))
            assert histogram[i] == expected, f"Histogram mismatch at {i}"


def test_compute_macd_insufficient_data_all_none() -> None:
    prices = _make_prices(10)
    macd_line, signal_line, histogram = compute_macd(prices, fast=12, slow=26, signal=9)
    assert all(v is None for v in macd_line)
    assert all(v is None for v in signal_line)


# ---------------------------------------------------------------------------
# ADX tests
# ---------------------------------------------------------------------------


def test_compute_adx_returns_three_lists() -> None:
    prices = _make_random_prices()
    highs = [p * 1.01 for p in prices]
    lows = [p * 0.99 for p in prices]
    adx, plus_di, minus_di = compute_adx(highs, lows, prices, period=14)
    assert len(adx) == len(prices)
    assert len(plus_di) == len(prices)
    assert len(minus_di) == len(prices)


def test_compute_adx_insufficient_data_all_none() -> None:
    prices = _make_prices(10)
    highs = [p * 1.01 for p in prices]
    lows = [p * 0.99 for p in prices]
    adx, plus_di, minus_di = compute_adx(highs, lows, prices, period=14)
    assert all(v is None for v in adx)


def test_compute_adx_returns_decimal() -> None:
    prices = _make_random_prices()
    highs = [p * 1.01 for p in prices]
    lows = [p * 0.99 for p in prices]
    adx, plus_di, minus_di = compute_adx(highs, lows, prices, period=14)
    for lst in [adx, plus_di, minus_di]:
        for val in lst:
            if val is not None:
                assert isinstance(val, Decimal)


def test_compute_adx_range_0_to_100() -> None:
    prices = _make_random_prices()
    highs = [p * 1.02 for p in prices]
    lows = [p * 0.98 for p in prices]
    adx, plus_di, minus_di = compute_adx(highs, lows, prices, period=14)
    for lst in [adx, plus_di, minus_di]:
        for val in lst:
            if val is not None:
                assert Decimal("0") <= val <= Decimal("100"), f"ADX/DI out of range: {val}"


# ---------------------------------------------------------------------------
# MFI tests
# ---------------------------------------------------------------------------


def test_compute_mfi_returns_decimal() -> None:
    prices = _make_random_prices()
    highs = [p * 1.01 for p in prices]
    lows = [p * 0.99 for p in prices]
    volumes = [1_000_000.0] * len(prices)
    result = compute_mfi(highs, lows, prices, volumes, period=14)
    non_none = [v for v in result if v is not None]
    assert len(non_none) > 0
    for val in non_none:
        assert isinstance(val, Decimal)


def test_compute_mfi_range_0_to_100() -> None:
    prices = _make_random_prices()
    highs = [p * 1.02 for p in prices]
    lows = [p * 0.98 for p in prices]
    volumes = [float(i + 1) * 10000 for i in range(len(prices))]
    result = compute_mfi(highs, lows, prices, volumes, period=14)
    for val in result:
        if val is not None:
            assert Decimal("0") <= val <= Decimal("100"), f"MFI out of range: {val}"


def test_compute_mfi_mismatched_lengths_returns_none_list() -> None:
    result = compute_mfi([1.0, 2.0], [0.9, 1.9], [1.0], [100.0, 200.0], period=14)
    assert all(v is None for v in result)


# ---------------------------------------------------------------------------
# Bollinger tests
# ---------------------------------------------------------------------------


def test_compute_bollinger_returns_three_lists() -> None:
    prices = _make_random_prices()
    upper, middle, lower = compute_bollinger(prices, period=20)
    assert len(upper) == len(prices)
    assert len(middle) == len(prices)
    assert len(lower) == len(prices)


def test_compute_bollinger_upper_gt_middle_gt_lower() -> None:
    prices = _make_random_prices()
    upper, middle, lower = compute_bollinger(prices, period=20)
    for i in range(len(prices)):
        if upper[i] is not None:
            assert upper[i] > middle[i] > lower[i], f"Band ordering violated at {i}"


def test_compute_bollinger_returns_decimal() -> None:
    prices = _make_random_prices()
    upper, middle, lower = compute_bollinger(prices, period=20)
    for lst in [upper, middle, lower]:
        for val in lst:
            if val is not None:
                assert isinstance(val, Decimal)


def test_compute_bollinger_middle_equals_sma() -> None:
    prices = _make_prices(30, start=100.0, step=0.5)
    upper, middle, lower = compute_bollinger(prices, period=20)
    sma_vals = compute_sma(prices, 20)
    for i in range(len(prices)):
        if middle[i] is not None:
            assert middle[i] == sma_vals[i], f"Middle != SMA at index {i}"


# ---------------------------------------------------------------------------
# ROC tests
# ---------------------------------------------------------------------------


def test_compute_roc_returns_decimal() -> None:
    prices = _make_random_prices()
    result = compute_roc(prices, period=10)
    non_none = [v for v in result if v is not None]
    assert len(non_none) > 0
    for val in non_none:
        assert isinstance(val, Decimal)


def test_compute_roc_formula_correct() -> None:
    prices = [100.0, 110.0, 121.0]
    result = compute_roc(prices, period=1)
    # ROC at index 1 = (110 - 100) / 100 * 100 = 10.0
    assert result[1] == Decimal("10.0000")
    # ROC at index 2 = (121 - 110) / 110 * 100 = 10.0
    assert result[2] == Decimal("10.0000")


def test_compute_roc_insufficient_all_none() -> None:
    prices = [100.0]
    result = compute_roc(prices, period=5)
    assert all(v is None for v in result)


# ---------------------------------------------------------------------------
# Volatility tests
# ---------------------------------------------------------------------------


def test_compute_volatility_returns_decimal() -> None:
    prices = _make_random_prices()
    result = compute_volatility(prices)
    assert isinstance(result, Decimal)


def test_compute_volatility_insufficient_data_returns_none() -> None:
    result = compute_volatility([100.0])
    assert result is None


def test_compute_volatility_zero_returns_zero() -> None:
    """Constant prices → zero returns → zero volatility."""
    prices = [100.0] * 20
    result = compute_volatility(prices)
    assert result == Decimal("0.0000")


def test_compute_volatility_positive() -> None:
    prices = _make_random_prices()
    result = compute_volatility(prices)
    assert result is not None
    assert result >= Decimal("0")


# ---------------------------------------------------------------------------
# Beta tests
# ---------------------------------------------------------------------------


def test_compute_beta_returns_decimal() -> None:
    prices = _make_random_prices(seed=10)
    bench = _make_random_prices(seed=20)
    asset_rets = [(prices[i] / prices[i - 1]) - 1 for i in range(1, len(prices))]
    bench_rets = [(bench[i] / bench[i - 1]) - 1 for i in range(1, len(bench))]
    result = compute_beta(asset_rets, bench_rets)
    assert isinstance(result, Decimal)


def test_compute_beta_same_series_returns_one() -> None:
    """Beta of asset vs itself should be ~1.0."""
    prices = _make_random_prices()
    rets = [(prices[i] / prices[i - 1]) - 1 for i in range(1, len(prices))]
    result = compute_beta(rets, rets)
    assert result is not None
    assert abs(float(result) - 1.0) < 1e-3


def test_compute_beta_mismatched_lengths_returns_none() -> None:
    result = compute_beta([0.01, 0.02], [0.01])
    assert result is None


def test_compute_beta_zero_variance_benchmark_returns_none() -> None:
    """Zero variance in benchmark → beta undefined → None."""
    asset_rets = [0.01, -0.01, 0.02]
    bench_rets = [0.0, 0.0, 0.0]
    result = compute_beta(asset_rets, bench_rets)
    assert result is None


# ---------------------------------------------------------------------------
# Sharpe tests
# ---------------------------------------------------------------------------


def test_compute_sharpe_returns_decimal() -> None:
    prices = _make_random_prices()
    rets = [(prices[i] / prices[i - 1]) - 1 for i in range(1, len(prices))]
    result = compute_sharpe(rets)
    assert isinstance(result, Decimal)


def test_compute_sharpe_insufficient_data_returns_none() -> None:
    result = compute_sharpe([0.01])
    assert result is None


def test_compute_sharpe_zero_std_returns_none() -> None:
    result = compute_sharpe([0.01, 0.01, 0.01])
    assert result is None


# ---------------------------------------------------------------------------
# Sortino tests
# ---------------------------------------------------------------------------


def test_compute_sortino_returns_decimal() -> None:
    prices = _make_random_prices()
    rets = [(prices[i] / prices[i - 1]) - 1 for i in range(1, len(prices))]
    result = compute_sortino(rets)
    assert isinstance(result, Decimal)


def test_compute_sortino_no_downside_returns_none() -> None:
    """No downside returns → downside std = 0 → None."""
    rets = [0.01] * 20  # all positive
    result = compute_sortino(rets, target_return=0.0)
    assert result is None


# ---------------------------------------------------------------------------
# Max Drawdown tests
# ---------------------------------------------------------------------------


def test_compute_max_drawdown_returns_decimal() -> None:
    prices = _make_random_prices()
    result = compute_max_drawdown(prices)
    assert isinstance(result, Decimal)


def test_compute_max_drawdown_no_drawdown_returns_zero() -> None:
    """Strictly increasing prices → max drawdown = 0."""
    prices = _make_prices(50, start=100.0, step=1.0)
    result = compute_max_drawdown(prices)
    assert result == Decimal("0.0000")


def test_compute_max_drawdown_known_value() -> None:
    """Peak 100, trough 50 → drawdown = -50%."""
    prices = [100.0, 90.0, 80.0, 70.0, 60.0, 50.0, 60.0]
    result = compute_max_drawdown(prices)
    assert result is not None
    assert result == Decimal("-50.0000")


def test_compute_max_drawdown_insufficient_data_returns_none() -> None:
    result = compute_max_drawdown([100.0])
    assert result is None


# ---------------------------------------------------------------------------
# OBV tests
# ---------------------------------------------------------------------------


def test_compute_obv_returns_decimal() -> None:
    closes = [100.0, 101.0, 100.5, 102.0]
    volumes = [1000.0, 1500.0, 800.0, 2000.0]
    result = compute_obv(closes, volumes)
    for val in result:
        if val is not None:
            assert isinstance(val, Decimal)


def test_compute_obv_formula_correct() -> None:
    closes = [100.0, 101.0, 100.5, 102.0]
    volumes = [1000.0, 1500.0, 800.0, 2000.0]
    result = compute_obv(closes, volumes)
    # OBV[0] = 1000
    assert result[0] == Decimal("1000.0000")
    # OBV[1] = 1000 + 1500 = 2500 (up)
    assert result[1] == Decimal("2500.0000")
    # OBV[2] = 2500 - 800 = 1700 (down)
    assert result[2] == Decimal("1700.0000")
    # OBV[3] = 1700 + 2000 = 3700 (up)
    assert result[3] == Decimal("3700.0000")


def test_compute_obv_mismatched_lengths_returns_empty() -> None:
    result = compute_obv([100.0, 101.0], [1000.0])
    assert result == []


# ---------------------------------------------------------------------------
# Relative Volume tests
# ---------------------------------------------------------------------------


def test_compute_relative_volume_returns_decimal() -> None:
    volumes = [float(1000 + i * 50) for i in range(30)]
    result = compute_relative_volume(volumes, period=20)
    non_none = [v for v in result if v is not None]
    assert len(non_none) > 0
    for val in non_none:
        assert isinstance(val, Decimal)


def test_compute_relative_volume_constant_volume_returns_one() -> None:
    """Constant volume → relative volume = 1.0 for all valid positions."""
    volumes = [1000.0] * 30
    result = compute_relative_volume(volumes, period=20)
    non_none = [v for v in result if v is not None]
    for val in non_none:
        assert val == Decimal("1.0000"), f"Expected 1.0000, got {val}"


def test_compute_relative_volume_insufficient_data_all_none() -> None:
    volumes = [1000.0] * 5
    result = compute_relative_volume(volumes, period=20)
    assert all(v is None for v in result)
