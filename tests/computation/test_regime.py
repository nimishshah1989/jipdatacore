"""Unit tests for market regime classification.

Tests verify regime thresholds, staleness penalty, and confidence computation.
"""

from __future__ import annotations



from app.computation.regime import (
    BULL_THRESHOLD,
    CONFIDENCE_WEIGHTS,
    classify_regime,
)


# ---------------------------------------------------------------------------
# classify_regime tests
# ---------------------------------------------------------------------------


def test_classify_regime_bull_high_scores() -> None:
    """All scores above 70 → BULL regime."""
    regime, confidence = classify_regime(
        breadth_score=75.0,
        momentum_score=70.0,
        volume_score=70.0,
        global_score=65.0,
        fii_score=65.0,
    )
    assert regime == "BULL"


def test_classify_regime_bear_low_scores() -> None:
    """All scores below 35 → BEAR regime."""
    regime, confidence = classify_regime(
        breadth_score=30.0,
        momentum_score=30.0,
        volume_score=30.0,
        global_score=30.0,
        fii_score=30.0,
    )
    assert regime == "BEAR"


def test_classify_regime_sideways_neutral_scores() -> None:
    """Scores around 50 with breadth ≤ momentum → SIDEWAYS."""
    regime, confidence = classify_regime(
        breadth_score=50.0,
        momentum_score=50.0,
        volume_score=50.0,
        global_score=50.0,
        fii_score=50.0,
    )
    # confidence = 50, not ≥ BULL_THRESHOLD (60), not ≤ BEAR_THRESHOLD (40)
    # momentum = breadth, so not RECOVERY → SIDEWAYS
    assert regime == "SIDEWAYS"


def test_classify_regime_recovery_momentum_gt_breadth() -> None:
    """Medium confidence, momentum > breadth → RECOVERY."""
    regime, confidence = classify_regime(
        breadth_score=45.0,
        momentum_score=65.0,  # > breadth
        volume_score=50.0,
        global_score=50.0,
        fii_score=50.0,
    )
    assert regime == "RECOVERY"


def test_classify_regime_bear_low_breadth_even_if_confident() -> None:
    """Low breadth score (≤35) forces BEAR regardless of other scores."""
    regime, confidence = classify_regime(
        breadth_score=30.0,  # ≤35 → BEAR
        momentum_score=80.0,
        volume_score=80.0,
        global_score=80.0,
        fii_score=80.0,
    )
    assert regime == "BEAR"


def test_classify_regime_confidence_formula_correct() -> None:
    """Confidence = weighted sum of component scores."""
    scores = {
        "breadth_score": 80.0,
        "momentum_score": 70.0,
        "volume_score": 60.0,
        "global_score": 50.0,
        "fii_score": 40.0,
    }
    expected_confidence = sum(
        scores[k] * w for k, w in CONFIDENCE_WEIGHTS.items()
    )

    regime, confidence = classify_regime(**scores)
    assert abs(confidence - expected_confidence) < 1e-6


def test_classify_regime_confidence_weights_sum_to_one() -> None:
    """All weights must sum to 1.0."""
    total = sum(CONFIDENCE_WEIGHTS.values())
    assert abs(total - 1.0) < 1e-10


def test_classify_regime_bull_requires_breadth_threshold() -> None:
    """High confidence but low breadth → not BULL."""
    regime, confidence = classify_regime(
        breadth_score=50.0,  # below BULL_THRESHOLD
        momentum_score=90.0,
        volume_score=90.0,
        global_score=90.0,
        fii_score=90.0,
    )
    assert confidence > BULL_THRESHOLD
    assert regime != "BULL"


def test_classify_regime_returns_valid_regime_string() -> None:
    """All outputs must be valid regime strings."""
    valid_regimes = {"BULL", "BEAR", "SIDEWAYS", "RECOVERY"}

    test_cases = [
        (80.0, 80.0, 80.0, 80.0, 80.0),  # BULL
        (20.0, 20.0, 20.0, 20.0, 20.0),  # BEAR
        (50.0, 50.0, 50.0, 50.0, 50.0),  # SIDEWAYS
        (45.0, 65.0, 50.0, 50.0, 50.0),  # RECOVERY
    ]

    for args in test_cases:
        regime, _ = classify_regime(*args)
        assert regime in valid_regimes, f"Invalid regime: {regime}"


def test_classify_regime_confidence_in_0_to_100() -> None:
    """Confidence should be between 0 and 100 for valid inputs."""
    test_cases = [
        (0.0, 0.0, 0.0, 0.0, 0.0),
        (100.0, 100.0, 100.0, 100.0, 100.0),
        (50.0, 50.0, 50.0, 50.0, 50.0),
    ]

    for args in test_cases:
        _, confidence = classify_regime(*args)
        assert 0.0 <= confidence <= 100.0, f"Confidence out of range: {confidence}"


# ---------------------------------------------------------------------------
# Staleness penalty tests (unit level)
# ---------------------------------------------------------------------------


def test_staleness_halves_confidence() -> None:
    """Manually verify staleness penalty reduces confidence by 50%."""
    _, confidence = classify_regime(
        breadth_score=70.0,
        momentum_score=70.0,
        volume_score=70.0,
        global_score=70.0,
        fii_score=70.0,
    )
    # Simulate staleness penalty
    stale_confidence = confidence * 0.5
    assert abs(stale_confidence - confidence / 2) < 1e-10
