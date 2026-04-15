"""Unit tests for IND-C3a: engine skeleton with SMA and EMA only.

All tests use synthetic data and AsyncMock for the DB session — no real DB calls.
Financial value tests use exact Decimal comparisons per project conventions.
"""

from __future__ import annotations

import math
import uuid
from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from app.computation.indicators_v2.engine import (
    CompResult,
    _build_column_limits,
    _to_decimal_row,
    compute_indicators,
)
from app.computation.indicators_v2.spec import AssetSpec
from app.computation.indicators_v2.strategy_loader import (
    get_rename_map,
    get_schema_columns,
    load_strategy_for_asset,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clear_strategy_cache() -> None:
    """Clear lru_cache before each test to avoid cross-test strategy pollution."""
    load_strategy_for_asset.cache_clear()


def _build_ohlcv_df(n: int, shuffled: bool = False) -> pd.DataFrame:
    """Return a DataFrame with (date index, close, open, high, low, volume) columns."""
    base_date = date(2023, 1, 2)
    dates = pd.bdate_range(start=base_date, periods=n)
    closes = [float(100 + i * 0.1) for i in range(n)]
    df = pd.DataFrame(
        {
            "close": closes,
            "open": [c - 0.5 for c in closes],
            "high": [c + 0.5 for c in closes],
            "low": [c - 1.0 for c in closes],
            "volume": [1000 + i for i in range(n)],
        },
        index=dates,
    )
    df.index.name = "date"
    if shuffled:
        import random

        idx = list(range(n))
        random.Random(42).shuffle(idx)
        if idx == list(range(n)):
            idx = list(range(n - 1, -1, -1))
        df = df.iloc[idx]
    return df


def _make_spec(min_history_days: int = 10) -> AssetSpec:
    """Return a minimal AssetSpec backed by a lightweight mock SQLAlchemy model."""
    # Build a mock model that has the attributes engine.py inspects
    model = MagicMock()
    model.__table__ = MagicMock()
    # Two columns: date (PK) and instrument_id (PK/FK) — no generated cols, no created_at
    col_date = MagicMock()
    col_date.name = "date"
    col_date.computed = None
    col_iid = MagicMock()
    col_iid.name = "instrument_id"
    col_iid.computed = None
    col_close = MagicMock()
    col_close.name = "close_adj"
    col_close.computed = None
    model.__table__.columns = [col_date, col_iid, col_close]
    # Column attribute access on the model (used in SELECT)
    model.date = MagicMock()
    model.instrument_id = MagicMock()
    model.close_adj = MagicMock()
    model.open = MagicMock()
    model.high = MagicMock()
    model.low = MagicMock()
    model.volume = MagicMock()

    return AssetSpec(
        asset_class_name="equity",
        source_model=model,
        output_model=model,
        id_column="instrument_id",
        date_column="date",
        close_col="close_adj",
        open_col="open",
        high_col="high",
        low_col="low",
        volume_col="volume",
        min_history_days=min_history_days,
    )



# ---------------------------------------------------------------------------
# Test 1: strategy_loader caching
# ---------------------------------------------------------------------------

def test_strategy_loader_caches() -> None:
    """lru_cache must return the exact same Strategy object on repeated calls."""
    # cache_clear() is called by autouse fixture before this test
    s1 = load_strategy_for_asset("equity", True)
    s2 = load_strategy_for_asset("equity", True)
    assert s1 is s2, "load_strategy_for_asset must return the cached object"


# ---------------------------------------------------------------------------
# Test 2: rename map contains expected keys
# ---------------------------------------------------------------------------

def test_rename_map_equity() -> None:
    """Equity rename map must include SMA_50 -> sma_50 and EMA_20 -> ema_20."""
    rename = get_rename_map("equity", True)
    assert rename.get("SMA_50") == "sma_50", f"SMA_50 missing: {rename}"
    assert rename.get("EMA_20") == "ema_20", f"EMA_20 missing: {rename}"


# ---------------------------------------------------------------------------
# Test 3: rename map is non-empty for mf without volume
# ---------------------------------------------------------------------------

def test_rename_map_filters_by_asset() -> None:
    """MF without volume still gets SMA/EMA — rename map must be non-empty."""
    rename = get_rename_map("mf", False)
    assert len(rename) > 0, "MF rename map should not be empty"
    # In 3a (SMA/EMA only) every indicator is non-volume so all 12 must appear
    assert "SMA_50" in rename
    assert "EMA_20" in rename


# ---------------------------------------------------------------------------
# Test 4: _to_decimal_row converts NaN to None
# ---------------------------------------------------------------------------

def test_to_decimal_row_nan_becomes_null() -> None:
    """NaN float in row must become None in the output dict (Fix 4)."""
    schema_cols = {"sma_50"}
    result = _to_decimal_row(
        {"sma_50": float("nan")},
        schema_cols,
        id_col="instrument_id",
        date_col="date",
        id_value=uuid.uuid4(),
        date_value=date(2024, 1, 1),
    )
    assert result["sma_50"] is None, "NaN must map to None"


# ---------------------------------------------------------------------------
# Test 5: _to_decimal_row quantizes float to 4 decimal places
# ---------------------------------------------------------------------------

def test_to_decimal_row_float_becomes_decimal() -> None:
    """Float 123.456789 must be quantized to Decimal('123.4568') (Fix 5)."""
    schema_cols = {"sma_50"}
    result = _to_decimal_row(
        {"sma_50": 123.456789},
        schema_cols,
        id_col="instrument_id",
        date_col="date",
        id_value=uuid.uuid4(),
        date_value=date(2024, 1, 1),
    )
    assert result["sma_50"] == Decimal("123.4568"), (
        f"Expected Decimal('123.4568'), got {result['sma_50']!r}"
    )


# ---------------------------------------------------------------------------
# Test 6: _to_decimal_row never leaks floats
# ---------------------------------------------------------------------------

def test_to_decimal_row_no_floats_leak() -> None:
    """Every value in the output dict must be Decimal, None, int, bool, date, or datetime."""
    import numpy as np

    schema_cols = {"sma_5", "ema_10", "sma_200"}
    row = {
        "sma_5": 55.12345,
        "ema_10": np.float64(60.9999),
        "sma_200": float("nan"),
    }
    iid = uuid.uuid4()
    d = date(2024, 6, 1)
    result = _to_decimal_row(
        row,
        schema_cols,
        id_col="instrument_id",
        date_col="date",
        id_value=iid,
        date_value=d,
    )
    allowed = (Decimal, type(None), int, bool, date, uuid.UUID)
    for key, val in result.items():
        assert isinstance(val, allowed), (
            f"Column {key!r} has disallowed type {type(val).__name__!r}: {val!r}"
        )
        assert not isinstance(val, float), f"Column {key!r} leaked a float: {val!r}"


# ---------------------------------------------------------------------------
# Test 7: engine skips instruments with insufficient history
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_engine_skips_short_history() -> None:
    """Instruments with fewer rows than min_history_days must be skipped, not errored."""
    spec = _make_spec(min_history_days=100)
    # Only 50 rows — below the 100-row threshold
    short_df = _build_ohlcv_df(50)
    session = AsyncMock()
    session.flush = AsyncMock()
    iid = uuid.uuid4()

    with (
        patch(
            "app.computation.indicators_v2.engine._load_ohlcv_bulk",
            new=AsyncMock(return_value={iid: short_df}),
        ),
        patch(
            "app.computation.indicators_v2.engine._upsert_batch", new_callable=AsyncMock
        ),
    ):
        result = await compute_indicators(
            spec,
            session,
            instrument_ids=[iid],
        )

    assert result.instruments_skipped_insufficient_history == 1
    assert result.instruments_processed == 0
    assert result.instruments_errored == 0
    assert result.rows_written == 0


# ---------------------------------------------------------------------------
# Test 8: per-instrument error isolation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_engine_per_instrument_error_isolation() -> None:
    """First instrument errors; second succeeds. errors list has 1 entry, processed=1."""
    spec = _make_spec(min_history_days=10)
    iid_bad = uuid.uuid4()
    iid_good = uuid.uuid4()
    # 350 rows: enough for ROC_252 (needs 252 rows) + warmup buffer
    good_df = _build_ohlcv_df(350)

    # bad_df has shuffled dates → monotonic assertion fires inside the
    # per-instrument try/except, recording an error without loading again.
    bad_df = _build_ohlcv_df(350, shuffled=True)

    session = AsyncMock()
    session.flush = AsyncMock()

    with (
        patch(
            "app.computation.indicators_v2.engine._load_ohlcv_bulk",
            new=AsyncMock(return_value={iid_bad: bad_df, iid_good: good_df}),
        ),
        patch(
            "app.computation.indicators_v2.engine._upsert_batch", new_callable=AsyncMock
        ),
    ):
        result = await compute_indicators(
            spec,
            session,
            instrument_ids=[iid_bad, iid_good],
        )

    assert len(result.errors) == 1
    assert result.errors[0]["instrument_id"] == str(iid_bad)
    assert result.instruments_processed == 1
    assert result.instruments_errored == 1


# ---------------------------------------------------------------------------
# Test 9: monotonic date assertion
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_engine_monotonic_assertion() -> None:
    """Shuffled dates must be caught by the engine and recorded as an error."""
    spec = _make_spec(min_history_days=5)
    shuffled_df = _build_ohlcv_df(20, shuffled=True)

    session = AsyncMock()
    session.flush = AsyncMock()
    iid = uuid.uuid4()

    with (
        patch(
            "app.computation.indicators_v2.engine._load_ohlcv_bulk",
            new=AsyncMock(return_value={iid: shuffled_df}),
        ),
        patch(
            "app.computation.indicators_v2.engine._upsert_batch", new_callable=AsyncMock
        ),
    ):
        result = await compute_indicators(
            spec,
            session,
            instrument_ids=[iid],
        )

    assert result.instruments_errored == 1
    assert len(result.errors) == 1
    assert result.instruments_processed == 0


# ---------------------------------------------------------------------------
# Test 10: end-to-end synthetic run emits sma_50 and ema_20 columns
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_engine_emits_sma_ema_columns() -> None:
    """300-row instrument: upserted rows must contain sma_50 and ema_20 keys."""
    spec = _make_spec(min_history_days=50)
    ohlcv_df = _build_ohlcv_df(300)

    session = AsyncMock()
    session.flush = AsyncMock()
    iid = uuid.uuid4()

    captured_batches: list[list[dict]] = []

    async def capture_upsert(
        sess: Any, sp: Any, batch: list[dict]
    ) -> None:
        captured_batches.append(batch)

    with (
        patch(
            "app.computation.indicators_v2.engine._load_ohlcv_bulk",
            new=AsyncMock(return_value={iid: ohlcv_df}),
        ),
        patch(
            "app.computation.indicators_v2.engine._upsert_batch",
            side_effect=capture_upsert,
        ),
    ):
        result = await compute_indicators(
            spec,
            session,
            instrument_ids=[iid],
        )

    assert result.instruments_processed == 1
    assert result.rows_written == 300
    # Flatten all batches
    all_rows = [r for batch in captured_batches for r in batch]
    assert len(all_rows) == 300

    # Every row must have the schema columns
    for row in all_rows:
        assert "sma_50" in row, "sma_50 missing from upserted row"
        assert "ema_20" in row, "ema_20 missing from upserted row"

    # SMA_50 needs 50 rows to be non-NaN (0-indexed: rows 0-48 are NaN = 49 NaN rows)
    warmup_rows = [r for r in all_rows if r.get("sma_50") is None]
    non_null_rows = [r for r in all_rows if r.get("sma_50") is not None]
    assert len(warmup_rows) == 49, (
        f"Expected 49 warmup NaN rows for SMA_50, got {len(warmup_rows)}"
    )
    assert len(non_null_rows) == 251

    # No float values in any row (Fix 5)
    allowed = (Decimal, type(None), int, bool, date, uuid.UUID)
    for row in all_rows:
        for k, v in row.items():
            if k in ("date", "instrument_id"):
                continue
            assert not isinstance(v, float), (
                f"Float leaked in upserted row column {k!r}: {v!r}"
            )


# ---------------------------------------------------------------------------
# IND-C3b Tests
# ---------------------------------------------------------------------------


def _build_synthetic_ohlcv(n: int = 400) -> pd.DataFrame:
    """Return a 400-row OHLCV DataFrame with DatetimeIndex suitable for full catalog."""
    import numpy as np

    rng = np.random.default_rng(7)
    close = (100 + rng.normal(0, 1, n).cumsum()).clip(min=10)
    df = pd.DataFrame(
        {
            "open": close + rng.normal(0, 0.3, n),
            "high": close + np.abs(rng.normal(0.5, 0.3, n)),
            "low": close - np.abs(rng.normal(0.5, 0.3, n)),
            "close": close,
            "volume": rng.integers(1000, 100000, n).astype(float),
        },
        index=pd.date_range("2020-01-01", periods=n, freq="B"),
    )
    return df


# ---------------------------------------------------------------------------
# Test 11: full catalog emits expected schema columns for equity/has_volume=True
# ---------------------------------------------------------------------------


def test_full_catalog_emits_expected_columns() -> None:
    """400-row equity run: every schema column must be present after rename + risk merge (IND-C3c)."""
    import pandas_ta_classic as ta  # noqa: F401 — side-effect import
    from app.computation.indicators_v2.risk_metrics import compute_hv_series, compute_risk_series

    df = _build_synthetic_ohlcv(400)
    strategy = load_strategy_for_asset("equity", True)
    df.ta.strategy(strategy)
    rename = get_rename_map("equity", True)

    # Keys in rename map that pandas-ta did NOT emit
    missing_keys = [k for k in rename if k not in df.columns]
    assert not missing_keys, (
        f"pandas-ta-classic did not emit these rename-map keys: {missing_keys}"
    )

    df = df.rename(columns=rename)

    # Mirror the engine's close→close_adj alias step (added after the
    # production smoke test showed close_adj landing NULL).
    if "close_adj" not in df.columns:
        df["close_adj"] = df["close"]

    # IND-C3c: merge risk + HV columns (not in rename_map — computed separately)
    df_hv = compute_hv_series(df["close"], extra_windows=[("3y", 756), ("5y", 1260)])
    df_risk = compute_risk_series(df["close"], benchmark_close=None)
    df = pd.concat([df, df_hv, df_risk], axis=1)

    schema_cols = get_schema_columns("equity", True)
    missing_schema = schema_cols - set(df.columns)
    assert not missing_schema, (
        f"Schema columns missing after rename+risk merge: {sorted(missing_schema)}"
    )


# ---------------------------------------------------------------------------
# Test 12: MF catalog excludes OHLC and volume indicators
# ---------------------------------------------------------------------------


def test_mf_catalog_excludes_ohlc_and_volume_indicators() -> None:
    """MF strategy (has_volume=False) must not include any OHLC/volume-dependent columns."""
    excluded = {
        "atr_14", "keltner_upper", "donchian_lower", "supertrend_10_3",
        "psar", "cci_20", "williams_r_14", "ultosc", "aroon_up",
        "adx_14", "plus_di", "minus_di", "stochastic_k", "stochastic_d",
        "obv", "ad", "cmf_20", "efi_13", "vwap", "mfi_14",
    }
    schema_cols = get_schema_columns("mf", False)
    present = excluded & schema_cols
    assert not present, (
        f"MF strategy must not include OHLC/volume columns, but found: {sorted(present)}"
    )
    # Sanity: MF must still have SMA/EMA/RSI/MACD
    assert "sma_50" in schema_cols
    assert "rsi_14" in schema_cols
    assert "macd_line" in schema_cols


# ---------------------------------------------------------------------------
# Test 13: index catalog excludes volume-dependent columns
# ---------------------------------------------------------------------------


def test_index_catalog_excludes_volume_indicators() -> None:
    """Index strategy (has_volume=False, applies_to=index) must exclude volume columns."""
    volume_cols = {
        "obv", "ad", "adosc_3_10", "cmf_20", "efi_13",
        "eom_14", "kvo", "pvt", "vwap", "mfi_14",
    }
    schema_cols = get_schema_columns("index", False)
    present = volume_cols & schema_cols
    assert not present, (
        f"Index strategy must not include volume columns, but found: {sorted(present)}"
    )
    # Index still has OHLC indicators
    assert "atr_14" in schema_cols
    assert "adx_14" in schema_cols
    assert "cci_20" in schema_cols


# ---------------------------------------------------------------------------
# Test 14: strategy column set is a subset of the corresponding table schema
# ---------------------------------------------------------------------------


def test_strategy_column_set_is_subset_of_table_schema() -> None:
    """get_schema_columns must only produce columns that exist in the ORM model (Fix 3)."""
    from app.models.indicators_v2 import (
        DeEquityTechnicalDailyV2,
        DeEtfTechnicalDailyV2,
        DeGlobalTechnicalDailyV2,
        DeIndexTechnicalDaily,
        DeMfTechnicalDaily,
    )

    _AUDIT = {"created_at", "updated_at"}

    def _model_columns(model: type, pk_cols: set[str]) -> set[str]:
        return {
            c.name
            for c in model.__table__.columns
            if c.computed is None and c.name not in _AUDIT and c.name not in pk_cols
        }

    cases = [
        ("equity", True, DeEquityTechnicalDailyV2, {"date", "instrument_id"}),
        ("etf", True, DeEtfTechnicalDailyV2, {"date", "ticker"}),
        ("global", True, DeGlobalTechnicalDailyV2, {"date", "ticker"}),
        ("index", False, DeIndexTechnicalDaily, {"date", "index_code"}),
        ("mf", False, DeMfTechnicalDaily, {"nav_date", "mstar_id"}),
    ]

    for asset_class, has_vol, model, pk_cols in cases:
        schema_cols = get_schema_columns(asset_class, has_vol)
        model_cols = _model_columns(model, pk_cols)
        extras = schema_cols - model_cols
        assert not extras, (
            f"[{asset_class}] get_schema_columns returned columns not in {model.__tablename__}: "
            f"{sorted(extras)}"
        )


# ---------------------------------------------------------------------------
# Test 15: rename map has no duplicate target schema columns
# ---------------------------------------------------------------------------


def test_rename_map_has_no_duplicate_targets() -> None:
    """Two pandas-ta keys must not map to the same schema column — silent overwrite risk.

    PSAR is the known exception: PSARl -> psar, PSARs dropped.
    This test documents that the current map has NO duplicate targets.
    """
    for asset_class, has_vol in [
        ("equity", True), ("etf", True), ("global", True),
        ("index", False), ("mf", False),
    ]:
        rename = get_rename_map(asset_class, has_vol)
        targets = list(rename.values())
        unique_targets = set(targets)
        assert len(targets) == len(unique_targets), (
            f"[{asset_class}] rename map has duplicate targets: "
            f"{[t for t in targets if targets.count(t) > 1]}"
        )


# ---------------------------------------------------------------------------
# IND-C3c Tests — risk_metrics.py
# ---------------------------------------------------------------------------


def _build_close_series(n: int, seed: int = 42) -> pd.Series:
    """Synthetic close price series with DatetimeIndex."""
    import numpy as np

    rng = np.random.default_rng(seed)
    prices = (100 + rng.normal(0, 1, n).cumsum()).clip(min=10)
    return pd.Series(prices, index=pd.date_range("2020-01-01", periods=n, freq="B"), name="close")


# ---------------------------------------------------------------------------
# Test 16: HV annualization math
# ---------------------------------------------------------------------------


def test_hv_series_annualized_percent() -> None:
    """volatility_20d last row must equal daily_stdev * sqrt(252) * 100 within 1e-6."""
    import numpy as np
    from app.computation.indicators_v2.risk_metrics import compute_hv_series

    close = _build_close_series(300)
    hv_df = compute_hv_series(close)

    # Recompute expected value manually
    log_ret = np.log(close / close.shift(1))
    expected_hv20 = log_ret.iloc[-20:].std() * np.sqrt(252) * 100

    assert "volatility_20d" in hv_df.columns
    assert "volatility_60d" in hv_df.columns
    assert "hv_252" in hv_df.columns
    actual = float(hv_df["volatility_20d"].iloc[-1])
    assert abs(actual - float(expected_hv20)) < 1e-6, (
        f"volatility_20d mismatch: got {actual}, expected {float(expected_hv20)}"
    )


# ---------------------------------------------------------------------------
# Test 17: HV insufficient history returns all NaN
# ---------------------------------------------------------------------------


def test_hv_series_insufficient_history_is_nan() -> None:
    """Close series with 10 rows must produce all-NaN volatility_20d."""
    from app.computation.indicators_v2.risk_metrics import compute_hv_series

    close = _build_close_series(10)
    hv_df = compute_hv_series(close)
    assert hv_df["volatility_20d"].isna().all(), "volatility_20d must be all NaN when fewer than 20 rows"


# ---------------------------------------------------------------------------
# Test 18: risk_sharpe matches direct empyrical computation
# ---------------------------------------------------------------------------


def test_risk_series_sharpe_matches_direct_empyrical() -> None:
    """sharpe_1y last row must match empyrical.sharpe_ratio on the same window."""
    import empyrical
    from app.computation.indicators_v2.risk_metrics import compute_risk_series, TRADING_DAYS_PER_YEAR

    close = _build_close_series(500)
    risk_df = compute_risk_series(close)

    returns = close.pct_change().astype(float)
    last_window = returns.iloc[-TRADING_DAYS_PER_YEAR:].dropna()
    expected = float(empyrical.sharpe_ratio(last_window, annualization=TRADING_DAYS_PER_YEAR))
    actual = float(risk_df["sharpe_1y"].iloc[-1])

    assert abs(actual - expected) < 1e-6, (
        f"sharpe_1y mismatch: got {actual}, expected {expected}"
    )


# ---------------------------------------------------------------------------
# Test 19: without benchmark, beta column is all NaN but sharpe is not
# ---------------------------------------------------------------------------


def test_risk_series_without_benchmark_fills_beta_nan() -> None:
    """With benchmark_close=None, beta/alpha/info_ratio are NaN; sharpe is not."""
    from app.computation.indicators_v2.risk_metrics import compute_risk_series

    close = _build_close_series(500)
    risk_df = compute_risk_series(close, benchmark_close=None)

    assert risk_df["beta_nifty"].isna().all(), "beta_nifty must be all NaN without benchmark"
    assert risk_df["risk_alpha_nifty"].isna().all(), "risk_alpha_nifty must be all NaN without benchmark"
    assert risk_df["risk_information_ratio"].isna().all(), "info_ratio must be all NaN without benchmark"
    # Sharpe should have non-NaN values for the last rows
    assert risk_df["sharpe_1y"].notna().any(), "sharpe_1y should have values for 500-row series"


# ---------------------------------------------------------------------------
# Test 20: engine end-to-end with risk columns
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_engine_full_pipeline_with_risk() -> None:
    """400-row instrument: upserted rows must contain sma_50, sharpe_1y, volatility_20d."""
    spec = _make_spec(min_history_days=50)
    ohlcv_df = _build_synthetic_ohlcv(400)

    session = AsyncMock()
    session.flush = AsyncMock()

    captured_batches: list[list[dict]] = []

    async def capture_upsert(sess: Any, sp: Any, batch: list[dict]) -> None:
        captured_batches.append(batch)

    iid = uuid.uuid4()
    with (
        patch(
            "app.computation.indicators_v2.engine._load_ohlcv_bulk",
            new=AsyncMock(return_value={iid: ohlcv_df}),
        ),
        patch(
            "app.computation.indicators_v2.engine._upsert_batch",
            side_effect=capture_upsert,
        ),
    ):
        result = await compute_indicators(
            spec,
            session,
            instrument_ids=[iid],
        )

    assert result.instruments_processed == 1
    all_rows = [r for batch in captured_batches for r in batch]
    assert len(all_rows) == 400

    # Every row must carry sma_50, sharpe_1y, volatility_20d keys
    for row in all_rows:
        assert "sma_50" in row, "sma_50 missing from upserted row"
        assert "sharpe_1y" in row, "sharpe_1y missing from upserted row"
        assert "volatility_20d" in row, "volatility_20d missing from upserted row"

    # sharpe_1y must have non-None values for later rows (>252 warmup)
    non_null_sharpe = [r for r in all_rows if r.get("sharpe_1y") is not None]
    assert len(non_null_sharpe) > 0, "Expected at least some non-NULL sharpe_1y in 400-row run"

    # No floats must leak (Fix 5)
    allowed = (Decimal, type(None), int, bool, date, uuid.UUID)
    for row in all_rows:
        for k, v in row.items():
            if k in ("date", "instrument_id"):
                continue
            assert not isinstance(v, float), f"Float leaked in column {k!r}: {v!r}"


# ---------------------------------------------------------------------------
# Test 21: get_schema_columns includes all risk + HV columns
# ---------------------------------------------------------------------------


def test_get_schema_columns_includes_risk_and_hv() -> None:
    """All risk+volatility+close_adj columns must be in get_schema_columns for equity and mf."""
    from app.computation.indicators_v2.strategy_loader import _RISK_COLUMNS

    # close_adj is aliased from df["close"] by the engine; it's in _RISK_COLUMNS
    # because it lives outside strategy.yaml, not because it's a risk metric.
    # The v1-compatible names (sharpe_1y etc.) were renamed via migration 010.
    expected = {
        "close_adj",
        # 1y risk
        "sharpe_1y", "sortino_1y", "calmar_ratio",
        "max_drawdown_1y", "beta_nifty",
        "risk_alpha_nifty", "risk_omega", "risk_information_ratio",
        "treynor_1y", "downside_risk_1y",
        # 3y risk
        "sharpe_3y", "sortino_3y", "calmar_3y",
        "max_drawdown_3y", "beta_3y",
        "information_ratio_3y", "treynor_3y", "downside_risk_3y",
        # 5y risk
        "sharpe_5y", "sortino_5y", "calmar_5y",
        "max_drawdown_5y", "beta_5y",
        "information_ratio_5y", "treynor_5y", "downside_risk_5y",
        # Volatility
        "volatility_20d", "volatility_60d", "hv_252",
        "volatility_3y", "volatility_5y",
    }
    assert expected == set(_RISK_COLUMNS), f"_RISK_COLUMNS mismatch: {_RISK_COLUMNS}"

    equity_cols = get_schema_columns("equity", True)
    for col in expected:
        assert col in equity_cols, f"{col!r} missing from equity schema cols"

    mf_cols = get_schema_columns("mf", False)
    for col in expected:
        assert col in mf_cols, f"{col!r} missing from mf schema cols"


# ---------------------------------------------------------------------------
# Test 22: vectorized calmar/omega/information_ratio match empyrical scalar
# ---------------------------------------------------------------------------


def test_vectorized_risk_metrics_match_empyrical_scalar() -> None:
    """Vectorized calmar/omega/information_ratio must match empyrical's scalar
    per-row output within 1e-4 on the last row of a 500-row series.

    This is the parity gate for the 50x speedup rewrite — if the vectorized
    implementation drifts, this test fails and we can eyeball the diff.
    """
    import empyrical
    import numpy as np
    from app.computation.indicators_v2.risk_metrics import (
        compute_risk_series,
        TRADING_DAYS_PER_YEAR,
    )

    # Build a deterministic synthetic close series with some drawdowns
    rng = np.random.default_rng(123)
    n = 500
    daily_returns = rng.normal(0.0008, 0.015, n)
    close_vals = 100.0 * np.cumprod(1 + daily_returns)
    close = pd.Series(
        close_vals,
        index=pd.date_range("2022-01-03", periods=n, freq="B"),
        name="close",
    )
    bench_vals = 100.0 * np.cumprod(1 + rng.normal(0.0005, 0.01, n))
    bench = pd.Series(bench_vals, index=close.index, name="close")

    risk_df = compute_risk_series(close, benchmark_close=bench)

    returns = close.pct_change().astype(float)
    bench_returns = bench.pct_change().astype(float)
    last_window = returns.iloc[-TRADING_DAYS_PER_YEAR:].dropna()
    last_bench_window = bench_returns.iloc[-TRADING_DAYS_PER_YEAR:].dropna()
    common = last_window.index.intersection(last_bench_window.index)
    wr = last_window.loc[common]
    br = last_bench_window.loc[common]

    # Expected via empyrical scalar (the old code path)
    expected_calmar = float(
        empyrical.calmar_ratio(last_window, annualization=TRADING_DAYS_PER_YEAR)
    )
    expected_omega = float(empyrical.omega_ratio(last_window))
    expected_ir = float(empyrical.excess_sharpe(wr, br))

    actual_calmar = float(risk_df["calmar_ratio"].iloc[-1])
    actual_omega = float(risk_df["risk_omega"].iloc[-1])
    actual_ir = float(risk_df["risk_information_ratio"].iloc[-1])

    assert abs(actual_calmar - expected_calmar) < 1e-4, (
        f"calmar drift: vectorized={actual_calmar} empyrical={expected_calmar}"
    )
    assert abs(actual_omega - expected_omega) < 1e-4, (
        f"omega drift: vectorized={actual_omega} empyrical={expected_omega}"
    )
    assert abs(actual_ir - expected_ir) < 1e-3, (
        f"information_ratio drift: vectorized={actual_ir} empyrical={expected_ir}"
    )


# ---------------------------------------------------------------------------
# Test 23: vectorized risk metrics are fast (regression guard on speedup)
# ---------------------------------------------------------------------------


def test_vectorized_risk_metrics_finish_under_5_seconds() -> None:
    """A 4800-row instrument must compute_risk_series in under 5 seconds.

    The pre-vectorization code took ~20 seconds per 4800-row instrument;
    this test asserts the per-instrument budget is at most 25% of that.
    Replaces the "trust me it'll be fast" claim with an actual timer.
    """
    import time
    import numpy as np
    from app.computation.indicators_v2.risk_metrics import compute_risk_series

    rng = np.random.default_rng(42)
    n = 4800
    daily = rng.normal(0.0005, 0.012, n)
    close = pd.Series(
        100.0 * np.cumprod(1 + daily),
        index=pd.date_range("2007-01-03", periods=n, freq="B"),
    )
    bench = pd.Series(
        100.0 * np.cumprod(1 + rng.normal(0.0004, 0.009, n)),
        index=close.index,
    )

    t0 = time.perf_counter()
    risk_df = compute_risk_series(close, benchmark_close=bench)
    elapsed = time.perf_counter() - t0

    assert elapsed < 5.0, (
        f"compute_risk_series took {elapsed:.2f}s for 4800 rows — regression"
    )
    # Sanity: some risk columns must be populated
    assert risk_df["sharpe_1y"].notna().any()
    assert risk_df["calmar_ratio"].notna().any()
    assert risk_df["risk_omega"].notna().any()


# ---------------------------------------------------------------------------
# Test 24: 3y risk parity against empyrical scalar (GAP-06)
# ---------------------------------------------------------------------------


def test_risk_series_3y_matches_empyrical_scalar() -> None:
    """3y risk columns must match empyrical scalar calls within 1e-4 on a 1500-row series."""
    import empyrical
    import numpy as np
    from app.computation.indicators_v2.risk_metrics import (
        compute_risk_series,
        TRADING_DAYS_PER_YEAR,
    )

    rng = np.random.default_rng(99)
    n = 1500
    daily = rng.normal(0.0006, 0.013, n)
    close = pd.Series(
        100.0 * np.cumprod(1 + daily),
        index=pd.date_range("2018-01-03", periods=n, freq="B"),
        name="close",
    )
    bench_vals = 100.0 * np.cumprod(1 + rng.normal(0.0004, 0.01, n))
    bench = pd.Series(bench_vals, index=close.index, name="close")

    risk_df = compute_risk_series(close, benchmark_close=bench)

    returns = close.pct_change().astype(float)
    bench_returns = bench.pct_change().astype(float)
    window_3y = 756
    last_3y = returns.iloc[-window_3y:].dropna()
    last_bench_3y = bench_returns.iloc[-window_3y:].dropna()
    common = last_3y.index.intersection(last_bench_3y.index)
    wr = last_3y.loc[common]
    br = last_bench_3y.loc[common]

    # Sharpe
    expected_sharpe = float(empyrical.sharpe_ratio(last_3y, annualization=TRADING_DAYS_PER_YEAR))
    actual_sharpe = float(risk_df["sharpe_3y"].iloc[-1])
    assert abs(actual_sharpe - expected_sharpe) < 1e-4, (
        f"sharpe_3y: got {actual_sharpe}, expected {expected_sharpe}"
    )

    # Sortino
    expected_sortino = float(empyrical.sortino_ratio(last_3y, annualization=TRADING_DAYS_PER_YEAR))
    actual_sortino = float(risk_df["sortino_3y"].iloc[-1])
    assert abs(actual_sortino - expected_sortino) < 1e-4, (
        f"sortino_3y: got {actual_sortino}, expected {expected_sortino}"
    )

    # Max drawdown
    expected_mdd = float(empyrical.max_drawdown(last_3y))
    actual_mdd = float(risk_df["max_drawdown_3y"].iloc[-1])
    assert abs(actual_mdd - expected_mdd) < 1e-4, (
        f"max_drawdown_3y: got {actual_mdd}, expected {expected_mdd}"
    )

    # Calmar
    expected_calmar = float(empyrical.calmar_ratio(last_3y, annualization=TRADING_DAYS_PER_YEAR))
    actual_calmar = float(risk_df["calmar_3y"].iloc[-1])
    assert abs(actual_calmar - expected_calmar) < 1e-4, (
        f"calmar_3y: got {actual_calmar}, expected {expected_calmar}"
    )

    # Information ratio
    expected_ir = float(empyrical.excess_sharpe(wr, br))
    actual_ir = float(risk_df["information_ratio_3y"].iloc[-1])
    assert abs(actual_ir - expected_ir) < 1e-3, (
        f"information_ratio_3y: got {actual_ir}, expected {expected_ir}"
    )


# ---------------------------------------------------------------------------
# Test 25: 5y risk parity against empyrical scalar (GAP-06)
# ---------------------------------------------------------------------------


def test_risk_series_5y_matches_empyrical_scalar() -> None:
    """5y risk columns must match empyrical scalar calls within 1e-4 on a 1500-row series."""
    import empyrical
    import numpy as np
    from app.computation.indicators_v2.risk_metrics import (
        compute_risk_series,
        TRADING_DAYS_PER_YEAR,
    )

    rng = np.random.default_rng(77)
    n = 1500
    daily = rng.normal(0.0005, 0.014, n)
    close = pd.Series(
        100.0 * np.cumprod(1 + daily),
        index=pd.date_range("2018-01-03", periods=n, freq="B"),
        name="close",
    )
    bench_vals = 100.0 * np.cumprod(1 + rng.normal(0.0003, 0.011, n))
    bench = pd.Series(bench_vals, index=close.index, name="close")

    risk_df = compute_risk_series(close, benchmark_close=bench)

    returns = close.pct_change().astype(float)
    bench_returns = bench.pct_change().astype(float)
    window_5y = 1260
    last_5y = returns.iloc[-window_5y:].dropna()
    last_bench_5y = bench_returns.iloc[-window_5y:].dropna()
    common = last_5y.index.intersection(last_bench_5y.index)
    wr = last_5y.loc[common]
    br = last_bench_5y.loc[common]

    # Sharpe
    expected_sharpe = float(empyrical.sharpe_ratio(last_5y, annualization=TRADING_DAYS_PER_YEAR))
    actual_sharpe = float(risk_df["sharpe_5y"].iloc[-1])
    assert abs(actual_sharpe - expected_sharpe) < 1e-4, (
        f"sharpe_5y: got {actual_sharpe}, expected {expected_sharpe}"
    )

    # Max drawdown
    expected_mdd = float(empyrical.max_drawdown(last_5y))
    actual_mdd = float(risk_df["max_drawdown_5y"].iloc[-1])
    assert abs(actual_mdd - expected_mdd) < 1e-4, (
        f"max_drawdown_5y: got {actual_mdd}, expected {expected_mdd}"
    )

    # Calmar
    expected_calmar = float(empyrical.calmar_ratio(last_5y, annualization=TRADING_DAYS_PER_YEAR))
    actual_calmar = float(risk_df["calmar_5y"].iloc[-1])
    assert abs(actual_calmar - expected_calmar) < 1e-4, (
        f"calmar_5y: got {actual_calmar}, expected {expected_calmar}"
    )

    # Information ratio
    expected_ir = float(empyrical.excess_sharpe(wr, br))
    actual_ir = float(risk_df["information_ratio_5y"].iloc[-1])
    assert abs(actual_ir - expected_ir) < 1e-3, (
        f"information_ratio_5y: got {actual_ir}, expected {expected_ir}"
    )


# ---------------------------------------------------------------------------
# Test 26: get_schema_columns includes multi-year risk columns (GAP-06)
# ---------------------------------------------------------------------------


def test_get_schema_columns_includes_multi_year_risk() -> None:
    """Schema columns must include all 3y/5y risk + treynor + downside_risk columns."""
    multi_year_cols = {
        "sharpe_3y", "sharpe_5y",
        "sortino_3y", "sortino_5y",
        "calmar_3y", "calmar_5y",
        "max_drawdown_3y", "max_drawdown_5y",
        "beta_3y", "beta_5y",
        "information_ratio_3y", "information_ratio_5y",
        "treynor_1y", "treynor_3y", "treynor_5y",
        "downside_risk_1y", "downside_risk_3y", "downside_risk_5y",
        "volatility_3y", "volatility_5y",
    }
    equity_cols = get_schema_columns("equity", True)
    missing = multi_year_cols - equity_cols
    assert not missing, f"Multi-year risk columns missing from equity schema: {sorted(missing)}"

    mf_cols = get_schema_columns("mf", False)
    missing_mf = multi_year_cols - mf_cols
    assert not missing_mf, f"Multi-year risk columns missing from mf schema: {sorted(missing_mf)}"


# ---------------------------------------------------------------------------
# Test 27: compute_risk_series returns >= 20 columns (GAP-06)
# ---------------------------------------------------------------------------


def test_risk_series_returns_at_least_20_columns() -> None:
    """compute_risk_series with 3 windows must return >= 20 risk columns."""
    import numpy as np
    from app.computation.indicators_v2.risk_metrics import compute_risk_series

    rng = np.random.default_rng(55)
    n = 1500
    close = pd.Series(
        100.0 * np.cumprod(1 + rng.normal(0.0005, 0.012, n)),
        index=pd.date_range("2018-01-03", periods=n, freq="B"),
    )
    bench = pd.Series(
        100.0 * np.cumprod(1 + rng.normal(0.0004, 0.01, n)),
        index=close.index,
    )
    risk_df = compute_risk_series(close, benchmark_close=bench)
    assert len(risk_df.columns) >= 20, (
        f"Expected >= 20 risk columns, got {len(risk_df.columns)}: {sorted(risk_df.columns)}"
    )


# ---------------------------------------------------------------------------
# Test 28: per-column precision clamp (GAP-04)
# ---------------------------------------------------------------------------


def test_to_decimal_row_per_column_precision_clamp() -> None:
    """Values exceeding Numeric(8,4) max (9999.9999) must be NULLed even if
    they fit within the old global clamp of 999999.9999.
    """
    column_limits = {
        "rsi_14": Decimal("9999.9999"),       # Numeric(8,4)
        "cci_20": Decimal("999999.9999"),      # Numeric(10,4)
        "sma_50": Decimal("99999999999999.9999"),  # Numeric(18,4)
    }
    schema_cols = {"rsi_14", "cci_20", "sma_50"}

    row = {
        "rsi_14": 50000.0,   # exceeds Numeric(8,4) max of 9999.9999
        "cci_20": 50000.0,   # fits Numeric(10,4)
        "sma_50": 50000.0,   # fits Numeric(18,4)
    }
    result = _to_decimal_row(
        row,
        schema_cols,
        id_col="instrument_id",
        date_col="date",
        id_value="TEST",
        date_value=date(2024, 1, 1),
        column_limits=column_limits,
    )
    assert result["rsi_14"] is None, (
        f"rsi_14=50000 should overflow Numeric(8,4), got {result['rsi_14']}"
    )
    assert result["cci_20"] == Decimal("50000.0000"), (
        f"cci_20=50000 fits Numeric(10,4), got {result['cci_20']}"
    )
    assert result["sma_50"] == Decimal("50000.0000"), (
        f"sma_50=50000 fits Numeric(18,4), got {result['sma_50']}"
    )


def test_to_decimal_row_numpy_scalar_per_column_clamp() -> None:
    """numpy float64 values must also respect per-column limits."""
    import numpy as np

    column_limits = {
        "bollinger_width": Decimal("9999.9999"),  # Numeric(8,4)
    }
    schema_cols = {"bollinger_width"}

    row = {"bollinger_width": np.float64(15000.0)}
    result = _to_decimal_row(
        row,
        schema_cols,
        id_col="instrument_id",
        date_col="date",
        id_value="TEST",
        date_value=date(2024, 1, 1),
        column_limits=column_limits,
    )
    assert result["bollinger_width"] is None, (
        f"bollinger_width=15000 should overflow Numeric(8,4), got {result['bollinger_width']}"
    )


def test_build_column_limits_from_model() -> None:
    """_build_column_limits must extract correct max values from SQLAlchemy model."""
    from app.models.indicators_v2 import DeIndexTechnicalDaily

    limits = _build_column_limits(DeIndexTechnicalDaily)

    assert limits["rsi_14"] == Decimal("9999.9999"), f"Numeric(8,4) → 9999.9999, got {limits['rsi_14']}"
    assert limits["cci_20"] == Decimal("999999.9999"), f"Numeric(10,4) → 999999.9999, got {limits['cci_20']}"
    assert limits["close_adj"] == Decimal("99999999999999.9999"), (
        f"Numeric(18,4) → 99999999999999.9999, got {limits['close_adj']}"
    )
