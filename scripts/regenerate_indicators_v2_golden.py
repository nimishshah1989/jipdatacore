"""Regenerate golden-snapshot fixtures for indicators v2 engine.

Run from inside the docker image:
    docker run --rm -v "$(pwd):/app" jip-data-engine:<tag> \\
        python scripts/regenerate_indicators_v2_golden.py

Fixtures are committed to the repo; a bump of ``pandas-ta-classic``
or ``empyrical-reloaded`` that changes *any* output value by more
than the test tolerance will fail ``test_indicators_v2_golden.py``
until the human reviews the diff and regenerates the fixtures.

Inputs: a deterministic 500-row OHLCV DataFrame generated from
``numpy.random.default_rng(seed=42)`` with a DatetimeIndex starting
2022-01-03. Realistic prices around 100, bounded volatility.

Outputs:
- ``tests/computation/fixtures/golden/synthetic_ohlcv.parquet``
  (the input, so the test loads the identical frame)
- ``tests/computation/fixtures/golden/synthetic_indicators_equity.parquet``
  (equity + has_volume output — largest column set)
- ``tests/computation/fixtures/golden/synthetic_indicators_mf.parquet``
  (mf + no volume — strict single-price subset)
"""

from __future__ import annotations

import pathlib
import sys

import numpy as np
import pandas as pd

from app.computation.indicators_v2.risk_metrics import (
    compute_hv_series,
    compute_risk_series,
)
from app.computation.indicators_v2.strategy_loader import (
    get_rename_map,
    get_schema_columns,
    load_strategy_for_asset,
)

FIXTURES_DIR = pathlib.Path("tests/computation/fixtures/golden")
N_ROWS = 500
SEED = 42


def build_synthetic_ohlcv() -> pd.DataFrame:
    rng = np.random.default_rng(SEED)
    # Random walk close around 100
    close = (100 + rng.normal(0, 1, N_ROWS).cumsum()).clip(min=10)
    high = close + np.abs(rng.normal(0.5, 0.3, N_ROWS))
    low = close - np.abs(rng.normal(0.5, 0.3, N_ROWS))
    open_ = close + rng.normal(0, 0.3, N_ROWS)
    volume = rng.integers(1000, 100000, N_ROWS).astype("int64")
    idx = pd.date_range("2022-01-03", periods=N_ROWS, freq="B")
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": volume},
        index=idx,
    )


def run_indicators(df: pd.DataFrame, asset: str, has_volume: bool) -> pd.DataFrame:
    """Run the strategy + rename + risk/HV merge exactly as the engine does."""
    d = df.copy()
    if not has_volume and "volume" in d.columns:
        d = d.drop(columns=["volume"])
    strat = load_strategy_for_asset(asset, has_volume)
    d.ta.strategy(strat)
    d = d.rename(columns=get_rename_map(asset, has_volume))
    hv = compute_hv_series(d["close"])
    risk = compute_risk_series(d["close"], benchmark_close=None)
    out = pd.concat([d, hv, risk], axis=1)
    # Restrict to schema columns so the fixture is stable against future
    # pandas-ta emissions that aren't (yet) in the schema
    schema = get_schema_columns(asset, has_volume)
    cols = sorted(c for c in out.columns if c in schema)
    return out[cols]


def main() -> int:
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    ohlcv = build_synthetic_ohlcv()
    ohlcv.to_parquet(FIXTURES_DIR / "synthetic_ohlcv.parquet")
    print(f"wrote {FIXTURES_DIR / 'synthetic_ohlcv.parquet'} ({len(ohlcv)} rows)")

    equity_ind = run_indicators(ohlcv, "equity", has_volume=True)
    equity_ind.to_parquet(FIXTURES_DIR / "synthetic_indicators_equity.parquet")
    print(
        f"wrote {FIXTURES_DIR / 'synthetic_indicators_equity.parquet'} "
        f"({len(equity_ind)} rows x {len(equity_ind.columns)} cols)"
    )

    mf_ind = run_indicators(ohlcv, "mf", has_volume=False)
    mf_ind.to_parquet(FIXTURES_DIR / "synthetic_indicators_mf.parquet")
    print(
        f"wrote {FIXTURES_DIR / 'synthetic_indicators_mf.parquet'} "
        f"({len(mf_ind)} rows x {len(mf_ind.columns)} cols)"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
