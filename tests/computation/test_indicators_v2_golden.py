"""Golden-snapshot tests for indicators v2 -- drift detection on library bumps.

These tests load parquet fixtures that were generated with the currently
pinned ``pandas-ta-classic==0.4.47`` and ``empyrical-reloaded==0.5.12``.
A bump of either library that changes any indicator value by more than
the tolerance below will fail these tests, forcing a human review of
the diff before the fixture is regenerated.

To intentionally refresh the fixtures after approving a library bump:
    docker run --rm -v "$(pwd):/app" jip-data-engine:<tag> \\
        python scripts/regenerate_indicators_v2_golden.py

Pattern rationale (not TA-Lib oracle): the real goal is catching SILENT
formula drift, not proving pandas-ta-classic is mathematically "correct"
against some other library. The version pin + golden snapshot gives us
exactly that. TA-Lib (C library) takes ~3 minutes of Docker build time to
install and compile from source; that cost is not justified when the pin
itself is the canonical source of truth.
"""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd
import pytest

from app.computation.indicators_v2.risk_metrics import (
    compute_hv_series,
    compute_risk_series,
)
from app.computation.indicators_v2.strategy_loader import (
    get_rename_map,
    get_schema_columns,
    load_strategy_for_asset,
)

FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures" / "golden"

# Tolerance -- fail loud on any material drift; tiny float noise is OK
ABS_TOLERANCE = 1e-6


@pytest.fixture(scope="module")
def synthetic_ohlcv() -> pd.DataFrame:
    path = FIXTURES_DIR / "synthetic_ohlcv.parquet"
    if not path.exists():
        pytest.skip(
            f"golden fixture missing: {path} "
            "(run scripts/regenerate_indicators_v2_golden.py)"
        )
    return pd.read_parquet(path)


@pytest.fixture(scope="module")
def golden_equity() -> pd.DataFrame:
    path = FIXTURES_DIR / "synthetic_indicators_equity.parquet"
    if not path.exists():
        pytest.skip(f"golden fixture missing: {path}")
    return pd.read_parquet(path)


@pytest.fixture(scope="module")
def golden_mf() -> pd.DataFrame:
    path = FIXTURES_DIR / "synthetic_indicators_mf.parquet"
    if not path.exists():
        pytest.skip(f"golden fixture missing: {path}")
    return pd.read_parquet(path)


def _run_indicators(df: pd.DataFrame, asset: str, has_volume: bool) -> pd.DataFrame:
    """Mirror of scripts/regenerate_indicators_v2_golden.py::run_indicators."""
    d = df.copy()
    if not has_volume and "volume" in d.columns:
        d = d.drop(columns=["volume"])
    strat = load_strategy_for_asset(asset, has_volume)
    d.ta.strategy(strat)
    d = d.rename(columns=get_rename_map(asset, has_volume))
    hv = compute_hv_series(d["close"])
    risk = compute_risk_series(d["close"], benchmark_close=None)
    out = pd.concat([d, hv, risk], axis=1)
    schema = get_schema_columns(asset, has_volume)
    cols = sorted(c for c in out.columns if c in schema)
    return out[cols]


def test_golden_equity_matches_fixture(synthetic_ohlcv, golden_equity):
    """Equity indicators must match the committed fixture within ABS_TOLERANCE."""
    current = _run_indicators(synthetic_ohlcv, "equity", has_volume=True)
    # Same columns, same order
    assert list(current.columns) == list(golden_equity.columns), (
        f"column set drift: current has {set(current.columns) - set(golden_equity.columns)!r}, "
        f"golden has {set(golden_equity.columns) - set(current.columns)!r}"
    )
    # Same number of rows
    assert len(current) == len(golden_equity)
    # Per-column value parity
    drifted = []
    for col in current.columns:
        cur = current[col].to_numpy(dtype=float)
        gold = golden_equity[col].to_numpy(dtype=float)
        # Both NaN at same position is fine; skip those
        mask = ~(np.isnan(cur) & np.isnan(gold))
        if not mask.any():
            continue
        diff = np.abs(cur[mask] - gold[mask])
        nan_mismatch = np.isnan(cur[mask]) != np.isnan(gold[mask])
        if nan_mismatch.any() or np.nanmax(diff) > ABS_TOLERANCE:
            drifted.append(
                f"{col}: max_abs_diff={np.nanmax(diff):.2e}, "
                f"nan_mismatches={int(nan_mismatch.sum())}"
            )
    assert not drifted, "Golden fixture drift detected:\n  " + "\n  ".join(drifted)


def test_golden_mf_matches_fixture(synthetic_ohlcv, golden_mf):
    """MF indicators (strict single-price subset) must match the fixture."""
    current = _run_indicators(synthetic_ohlcv, "mf", has_volume=False)
    assert list(current.columns) == list(golden_mf.columns), (
        f"column set drift: current={set(current.columns) - set(golden_mf.columns)!r}, "
        f"golden={set(golden_mf.columns) - set(current.columns)!r}"
    )
    assert len(current) == len(golden_mf)
    drifted = []
    for col in current.columns:
        cur = current[col].to_numpy(dtype=float)
        gold = golden_mf[col].to_numpy(dtype=float)
        mask = ~(np.isnan(cur) & np.isnan(gold))
        if not mask.any():
            continue
        diff = np.abs(cur[mask] - gold[mask])
        nan_mismatch = np.isnan(cur[mask]) != np.isnan(gold[mask])
        if nan_mismatch.any() or np.nanmax(diff) > ABS_TOLERANCE:
            drifted.append(f"{col}: max_abs_diff={np.nanmax(diff):.2e}")
    assert not drifted, "MF golden fixture drift:\n  " + "\n  ".join(drifted)


def test_golden_equity_has_expected_families(golden_equity):
    """Sanity: the equity fixture has at least one column from each indicator family."""
    cols = set(golden_equity.columns)
    required = {
        "sma_50", "ema_20",                     # overlap
        "rsi_14", "macd_line",                  # momentum
        "bollinger_upper", "atr_14",            # volatility
        "obv", "cmf_20",                        # volume
        "adx_14", "plus_di",                    # trend
        "zscore_20", "linreg_slope_20",         # statistics
        "volatility_20d", "sharpe_1y",          # C3c (HV + empyrical, renamed to v1)
    }
    missing = required - cols
    assert not missing, f"equity fixture missing expected families: {missing}"


def test_golden_mf_excludes_ohlc_width_indicators(golden_mf):
    """Sanity: the MF fixture must not contain any OHLC-width or volume indicators."""
    forbidden = {
        "atr_14", "atr_7", "atr_21", "natr_14", "true_range",
        "keltner_upper", "keltner_middle", "keltner_lower",
        "donchian_upper", "donchian_middle", "donchian_lower",
        "supertrend_10_3", "psar", "cci_20", "williams_r_14", "ultosc",
        "aroon_up", "aroon_down", "aroon_osc",
        "adx_14", "plus_di", "minus_di",
        "stochastic_k", "stochastic_d",
        "obv", "ad", "adosc_3_10", "cmf_20", "efi_13", "eom_14", "kvo", "pvt",
        "vwap", "mfi_14",
    }
    present = set(golden_mf.columns) & forbidden
    assert not present, f"MF fixture has forbidden columns: {present}"
