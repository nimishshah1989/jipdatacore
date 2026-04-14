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

    with (
        patch(
            "app.computation.indicators_v2.engine._load_ohlcv",
            new=AsyncMock(return_value=short_df),
        ),
        patch(
            "app.computation.indicators_v2.engine._upsert_batch", new_callable=AsyncMock
        ),
    ):
        result = await compute_indicators(
            spec,
            session,
            instrument_ids=[uuid.uuid4()],
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
    # 250 rows so pandas-ta emits SMA_200 / EMA_200 columns without error
    good_df = _build_ohlcv_df(250)

    call_count = 0

    async def load_ohlcv_side_effect(
        session: Any, sp: Any, instrument_id: Any
    ) -> pd.DataFrame:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("synthetic DB error for instrument 1")
        return good_df

    session = AsyncMock()
    session.flush = AsyncMock()

    with (
        patch(
            "app.computation.indicators_v2.engine._load_ohlcv",
            side_effect=load_ohlcv_side_effect,
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

    with (
        patch(
            "app.computation.indicators_v2.engine._load_ohlcv",
            new=AsyncMock(return_value=shuffled_df),
        ),
        patch(
            "app.computation.indicators_v2.engine._upsert_batch", new_callable=AsyncMock
        ),
    ):
        result = await compute_indicators(
            spec,
            session,
            instrument_ids=[uuid.uuid4()],
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

    captured_batches: list[list[dict]] = []

    async def capture_upsert(
        sess: Any, sp: Any, batch: list[dict]
    ) -> None:
        captured_batches.append(batch)

    with (
        patch(
            "app.computation.indicators_v2.engine._load_ohlcv",
            new=AsyncMock(return_value=ohlcv_df),
        ),
        patch(
            "app.computation.indicators_v2.engine._upsert_batch",
            side_effect=capture_upsert,
        ),
    ):
        result = await compute_indicators(
            spec,
            session,
            instrument_ids=[uuid.uuid4()],
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
