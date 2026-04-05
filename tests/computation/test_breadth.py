"""Unit tests for market breadth computation.

Tests verify formula correctness, Decimal output, and edge cases.
"""

from __future__ import annotations

from decimal import Decimal


from app.computation.breadth import (
    _safe_pct,
    compute_breadth_indicators,
    compute_mcclellan_oscillator,
)


# ---------------------------------------------------------------------------
# _safe_pct tests
# ---------------------------------------------------------------------------


def test_safe_pct_basic() -> None:
    result = _safe_pct(60, 100)
    assert result == Decimal("60.00")


def test_safe_pct_zero_denominator_returns_none() -> None:
    result = _safe_pct(10, 0)
    assert result is None


def test_safe_pct_returns_decimal() -> None:
    result = _safe_pct(25, 100)
    assert isinstance(result, Decimal)


def test_safe_pct_100_percent() -> None:
    result = _safe_pct(100, 100)
    assert result == Decimal("100.00")


def test_safe_pct_zero_numerator() -> None:
    result = _safe_pct(0, 100)
    assert result == Decimal("0.00")


# ---------------------------------------------------------------------------
# McClellan Oscillator tests
# ---------------------------------------------------------------------------


def test_mcclellan_oscillator_seeded_from_none() -> None:
    """When prev EMAs are None, seeds are set to net_advances."""
    advance, decline = 300, 200
    net = float(advance - decline)  # 100
    osc, ema19, ema39 = compute_mcclellan_oscillator(advance, decline, None, None)
    # Both EMAs seed to net (100), oscillator = 0
    assert osc == 0.0
    assert ema19 == net
    assert ema39 == net


def test_mcclellan_oscillator_updates_correctly() -> None:
    """Verify EMA update formula for 19 and 39 period."""
    advance, decline = 400, 100
    net = 300.0
    prev_ema19, prev_ema39 = 200.0, 180.0

    k19 = 2.0 / 20
    k39 = 2.0 / 40
    expected_ema19 = net * k19 + prev_ema19 * (1 - k19)
    expected_ema39 = net * k39 + prev_ema39 * (1 - k39)
    expected_osc = expected_ema19 - expected_ema39

    osc, ema19, ema39 = compute_mcclellan_oscillator(advance, decline, prev_ema19, prev_ema39)

    assert abs(osc - expected_osc) < 1e-10
    assert abs(ema19 - expected_ema19) < 1e-10
    assert abs(ema39 - expected_ema39) < 1e-10


def test_mcclellan_oscillator_bull_market_positive() -> None:
    """After a shift from bearish to bullish conditions, oscillator becomes positive.

    When net advances changes from low to high, EMA19 (faster) rises faster
    than EMA39 (slower), producing a positive oscillator.
    """
    # Start with bearish conditions (net=-400)
    advance_bear, decline_bear = 100, 500
    prev_ema19, prev_ema39 = None, None

    for _ in range(30):
        _, prev_ema19, prev_ema39 = compute_mcclellan_oscillator(
            advance_bear, decline_bear, prev_ema19, prev_ema39
        )

    # Switch to strongly bullish (net=+600)
    advance_bull, decline_bull = 800, 200
    for _ in range(10):
        osc, prev_ema19, prev_ema39 = compute_mcclellan_oscillator(
            advance_bull, decline_bull, prev_ema19, prev_ema39
        )

    # After a bull shift, EMA19 (faster) should be rising faster → osc > 0
    assert osc > 0, f"Expected positive oscillator after bull shift, got {osc}"


def test_mcclellan_oscillator_bear_market_negative() -> None:
    """After a shift from bullish to bearish conditions, oscillator becomes negative.

    When net advances drops from high to low, EMA19 (faster) falls faster
    than EMA39 (slower), producing a negative oscillator.
    """
    # Start with bullish conditions (net=+600)
    advance_bull, decline_bull = 800, 200
    prev_ema19, prev_ema39 = None, None

    for _ in range(30):
        _, prev_ema19, prev_ema39 = compute_mcclellan_oscillator(
            advance_bull, decline_bull, prev_ema19, prev_ema39
        )

    # Switch to strongly bearish (net=-400)
    advance_bear, decline_bear = 100, 500
    for _ in range(10):
        osc, prev_ema19, prev_ema39 = compute_mcclellan_oscillator(
            advance_bear, decline_bear, prev_ema19, prev_ema39
        )

    assert osc < 0, f"Expected negative oscillator after bear shift, got {osc}"


# ---------------------------------------------------------------------------
# compute_breadth_indicators tests
# ---------------------------------------------------------------------------


def test_breadth_indicators_returns_dict() -> None:
    result = compute_breadth_indicators(
        total=500,
        advance=300,
        decline=150,
        unchanged=50,
        above_50dma=250,
        above_200dma=200,
        above_20dma=280,
        new_52w_highs=30,
        new_52w_lows=5,
    )
    assert isinstance(result, dict)


def test_breadth_indicators_ad_ratio_correct() -> None:
    result = compute_breadth_indicators(
        total=500,
        advance=300,
        decline=150,
        unchanged=50,
        above_50dma=250,
        above_200dma=200,
        above_20dma=280,
        new_52w_highs=30,
        new_52w_lows=5,
    )
    # ad_ratio = 300 / 150 = 2.0
    assert result["ad_ratio"] == Decimal("2.0000")


def test_breadth_indicators_pct_above_200dma_correct() -> None:
    result = compute_breadth_indicators(
        total=500,
        advance=300,
        decline=150,
        unchanged=50,
        above_50dma=250,
        above_200dma=400,
        above_20dma=280,
        new_52w_highs=30,
        new_52w_lows=5,
    )
    # pct_above_200dma = 400/500 * 100 = 80.0
    assert result["pct_above_200dma"] == Decimal("80.00")


def test_breadth_indicators_pct_above_50dma_correct() -> None:
    result = compute_breadth_indicators(
        total=400,
        advance=200,
        decline=150,
        unchanged=50,
        above_50dma=200,
        above_200dma=150,
        above_20dma=220,
        new_52w_highs=10,
        new_52w_lows=2,
    )
    # pct_above_50dma = 200/400 * 100 = 50.0
    assert result["pct_above_50dma"] == Decimal("50.00")


def test_breadth_indicators_zero_decline_ad_ratio_none() -> None:
    result = compute_breadth_indicators(
        total=500,
        advance=500,
        decline=0,
        unchanged=0,
        above_50dma=500,
        above_200dma=500,
        above_20dma=500,
        new_52w_highs=50,
        new_52w_lows=0,
    )
    assert result["ad_ratio"] is None


def test_breadth_indicators_mcclellan_oscillator_decimal() -> None:
    result = compute_breadth_indicators(
        total=500,
        advance=300,
        decline=150,
        unchanged=50,
        above_50dma=250,
        above_200dma=200,
        above_20dma=280,
        new_52w_highs=30,
        new_52w_lows=5,
    )
    if result["mcclellan_oscillator"] is not None:
        assert isinstance(result["mcclellan_oscillator"], Decimal)


def test_breadth_indicators_summation_cumulates() -> None:
    """Summation index should accumulate across days."""
    result1 = compute_breadth_indicators(
        total=500, advance=300, decline=150, unchanged=50,
        above_50dma=250, above_200dma=200, above_20dma=280,
        new_52w_highs=30, new_52w_lows=5,
        prev_summation=0.0,
    )
    prev_sum = result1["_summation"]

    result2 = compute_breadth_indicators(
        total=500, advance=300, decline=150, unchanged=50,
        above_50dma=250, above_200dma=200, above_20dma=280,
        new_52w_highs=30, new_52w_lows=5,
        prev_ema19=result1["_ema19"],
        prev_ema39=result1["_ema39"],
        prev_summation=prev_sum,
    )
    # Second day summation should be first + second oscillator
    assert result2["_summation"] == prev_sum + result2["_summation"] - prev_sum
    # Or more directly:
    expected_sum = prev_sum + float(result2["mcclellan_oscillator"])
    assert abs(result2["_summation"] - expected_sum) < 1e-6


def test_breadth_indicators_pct_values_are_decimal() -> None:
    result = compute_breadth_indicators(
        total=500, advance=300, decline=150, unchanged=50,
        above_50dma=250, above_200dma=200, above_20dma=280,
        new_52w_highs=30, new_52w_lows=5,
    )
    for key in ["pct_above_200dma", "pct_above_50dma", "ad_ratio", "pct_new_highs", "pct_new_lows"]:
        val = result.get(key)
        if val is not None:
            assert isinstance(val, Decimal), f"{key} should be Decimal, got {type(val)}"


def test_breadth_indicators_counts_match_input() -> None:
    result = compute_breadth_indicators(
        total=500, advance=300, decline=150, unchanged=50,
        above_50dma=250, above_200dma=200, above_20dma=280,
        new_52w_highs=30, new_52w_lows=5,
    )
    assert result["advance"] == 300
    assert result["decline"] == 150
    assert result["unchanged"] == 50
    assert result["total_stocks"] == 500
    assert result["new_52w_highs"] == 30
    assert result["new_52w_lows"] == 5
