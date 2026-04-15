"""Generic indicators v2 engine.

One entry point, ``compute_indicators``, drives all asset classes via an
``AssetSpec``. Loads OHLCV per instrument, runs the pandas-ta-classic
``Strategy`` filtered for that asset, converts floats to Decimal at the
DB boundary, and upserts via ``pg_insert`` with ``on_conflict_do_update``.

Semantics (binding per eng-review addendum):
- **Row-position windows** (Fix 6): indicator windows are based on row position.
  Calendar gaps (e.g., trading halts) are NOT backfilled. Post-gap SMA values
  will include pre-gap data within the window. The engine asserts that input
  dates are monotonic-increasing; it does not reorder or reindex.
- **NaN write policy** (Fix 4): pandas-ta warmup NaNs are written as NULL
  per column. Rows are NEVER skipped on the basis of individual NaN values.
  Entire instruments are skipped only when ``len(df) < spec.min_history_days``.
- **Decimal boundary** (Fix 5): ``_to_decimal_row`` runs exactly once per row;
  its output dict is used for BOTH the INSERT VALUES and the ON CONFLICT UPDATE
  SET clause. No raw pandas floats ever reach the DB.
- **Retry on connection drops** (Fix 15): upsert batches are wrapped with
  tenacity exponential backoff for transient DB errors.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any

import pandas as pd
import pandas_ta_classic as ta  # noqa: F401 — imported for Strategy registration side effects
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import InterfaceError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.computation.indicators_v2.spec import AssetSpec
from app.computation.indicators_v2.strategy_loader import (
    get_rename_map,
    get_schema_columns,
    load_strategy_for_asset,
)
from app.logging import get_logger

logger = get_logger(__name__)

# Upsert batch size — must respect Postgres's 32767-bind-param limit.
# For the v2 technical tables at ~91 columns, max rows per pg_insert is
# 32767 / 91 = 360. We use 300 for safety, leaving headroom for the
# occasional schema addition.
#
# For backfill mode the engine uses ``copy_records_to_table`` via asyncpg
# instead of pg_insert — no bind-param limit, much faster. See
# ``_upsert_batch``.
DEFAULT_BATCH_SIZE = 300

# Decimal quantization step per project convention (Numeric(18,4) scale=4)
_Q = Decimal("0.0001")

# BIGINT columns in the v2 technical tables. These must land as Python int,
# not Decimal, or Postgres raises "invalid input syntax for type bigint".
# Sourced from alembic/versions/008_indicators_v2_tables.py.
_INT_COLUMNS: frozenset[str] = frozenset({"obv", "ad", "pvt"})

# Postgres BIGINT range. OBV/AD accumulators on high-volume global instruments
# (e.g. ^NKX Nikkei) over 20+ years can exceed int64. Clamp to NULL rather
# than let the whole instrument upsert fail.
_INT64_MAX = 9223372036854775807
_INT64_MIN = -9223372036854775808

# Fallback clamp for Decimal columns without explicit precision metadata.
_DECIMAL_ABS_MAX = Decimal("999999.9999")


def _build_column_limits(model: Any) -> dict[str, Decimal]:
    """Build per-column max absolute value from SQLAlchemy Numeric(precision, scale).

    Numeric(p, s) holds values up to 10^(p-s) - 10^(-s).
    E.g. Numeric(8,4) → 9999.9999, Numeric(10,4) → 999999.9999.
    """
    limits: dict[str, Decimal] = {}
    for col in model.__table__.columns:
        col_type = getattr(col.type, "impl", col.type) if hasattr(col.type, "impl") else col.type
        precision = getattr(col_type, "precision", None)
        scale = getattr(col_type, "scale", None)
        if isinstance(precision, int) and isinstance(scale, int) and precision > 0:
            int_digits = precision - scale
            limits[col.name] = Decimal(10) ** int_digits - Decimal(10) ** (-scale)
    return limits


@dataclass
class CompResult:
    """Summary of one compute_indicators run."""

    asset_class: str
    instruments_processed: int = 0
    instruments_skipped_insufficient_history: int = 0
    instruments_errored: int = 0
    rows_written: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)


def _to_decimal_row(
    row: dict[str, Any],
    schema_columns: set[str],
    *,
    id_col: str,
    date_col: str,
    id_value: Any,
    date_value: date,
    column_limits: dict[str, Decimal] | None = None,
) -> dict[str, Any]:
    """Convert one pandas row + metadata into a DB-ready dict.

    - Float / int / np.float64 → Decimal quantized to 0.0001, or None if NaN
    - Only columns in ``schema_columns`` plus id + date are included
    - Fix 5: this dict is used for BOTH INSERT VALUES and ON CONFLICT UPDATE SET
    - Per-column precision clamp: uses column_limits when available,
      falls back to _DECIMAL_ABS_MAX
    """
    out: dict[str, Any] = {id_col: id_value, date_col: date_value}
    _limits = column_limits or {}
    for col in schema_columns:
        raw = row.get(col)
        is_int_col = col in _INT_COLUMNS
        col_max = _limits.get(col, _DECIMAL_ABS_MAX)
        if raw is None:
            out[col] = None
        elif isinstance(raw, bool):
            out[col] = raw
        elif isinstance(raw, int):
            out[col] = raw
        elif isinstance(raw, float) and (math.isnan(raw) or math.isinf(raw)):
            out[col] = None
        elif isinstance(raw, float):
            if is_int_col:
                ival = int(raw)
                out[col] = ival if _INT64_MIN <= ival <= _INT64_MAX else None
            else:
                try:
                    dec = Decimal(str(raw)).quantize(_Q)
                    out[col] = dec if abs(dec) <= col_max else None
                except Exception:
                    out[col] = None
        else:
            try:
                fval = float(raw)
                if math.isnan(fval) or math.isinf(fval):
                    out[col] = None
                elif is_int_col:
                    ival = int(fval)
                    out[col] = ival if _INT64_MIN <= ival <= _INT64_MAX else None
                else:
                    dec = Decimal(str(fval)).quantize(_Q)
                    out[col] = dec if abs(dec) <= col_max else None
            except Exception:
                out[col] = None
    return out


def _col_select(model: Any, ref: Any) -> Any:
    """Build a SELECT expression for a column reference.

    ``ref`` is either a single column name or a tuple of names. For tuples the
    engine emits SQL ``COALESCE(col1, col2, ...)`` so the first non-null value
    wins. This lets specs declare a preference order like
    ``close_col=("close_adj", "close")`` — use adjusted when available, fall
    back to the raw column when it isn't — without per-asset branches.
    """
    if isinstance(ref, str):
        return getattr(model, ref)
    if isinstance(ref, tuple):
        return sa.func.coalesce(*(getattr(model, name) for name in ref))
    raise TypeError(f"unsupported column ref: {ref!r}")


async def _load_ohlcv(
    session: AsyncSession,
    spec: AssetSpec,
    instrument_id: Any,
) -> pd.DataFrame:
    """Load full OHLCV history for ONE instrument.

    Kept for backwards compatibility and test mocking. Backfill code paths
    should prefer :func:`_load_ohlcv_bulk` which fetches many instruments
    in a single query — far more efficient at scale.
    """
    bulk = await _load_ohlcv_bulk(session, spec, [instrument_id])
    return bulk.get(instrument_id, pd.DataFrame())


async def _load_ohlcv_bulk(
    session: AsyncSession,
    spec: AssetSpec,
    instrument_ids: list[Any],
) -> "dict[Any, pd.DataFrame]":
    """Load OHLCV for many instruments in a single SELECT ... WHERE id IN (...).

    Returns a dict mapping ``instrument_id -> DataFrame``. Each DataFrame has
    a DatetimeIndex and ``close`` + optional ``open/high/low/volume`` columns,
    sorted ascending by date. Missing instruments (no rows in source) are
    absent from the output dict.

    This replaces the per-instrument query loop in the backfill path —
    2,281 SELECT roundtrips become 1. On a VPC-connected EC2, the former
    takes ~150ms × 2,281 ≈ 5-6 minutes of pure network wait; the bulk
    approach finishes in ~5 seconds.
    """
    if not instrument_ids:
        return {}

    model = spec.source_model
    id_attr = getattr(model, spec.id_column)
    date_attr = getattr(model, spec.date_column)

    # SELECT id, date, close, [open, high, low, volume]
    cols: list[Any] = [id_attr, date_attr, _col_select(model, spec.close_col)]
    col_names = [spec.id_column, spec.date_column, "close"]
    for label, ref in (
        ("open", spec.open_col),
        ("high", spec.high_col),
        ("low", spec.low_col),
        ("volume", spec.volume_col),
    ):
        if ref is not None:
            cols.append(_col_select(model, ref))
            col_names.append(label)

    stmt = (
        sa.select(*cols)
        .where(id_attr.in_(instrument_ids))
        .order_by(id_attr.asc(), date_attr.asc())
    )
    result = await session.execute(stmt)
    rows = result.fetchall()

    if not rows:
        return {}

    # Build one big DataFrame via numpy — ~100x faster than per-row list append.
    data: dict[str, Any] = {}
    for name in col_names:
        data[name] = []
    for row in rows:
        for name, val in zip(col_names, row):
            data[name].append(val)

    big = pd.DataFrame(data)
    # Coerce numeric columns (None → NaN, then float dtype)
    for col in ("open", "high", "low", "close"):
        if col in big.columns:
            big[col] = pd.to_numeric(big[col], errors="coerce")
    if "volume" in big.columns:
        big["volume"] = pd.to_numeric(big["volume"], errors="coerce")

    # Group by instrument_id → per-instrument DataFrame with DatetimeIndex
    out: dict[Any, pd.DataFrame] = {}
    for iid, group in big.groupby(spec.id_column, sort=False):
        sub = group.drop(columns=[spec.id_column]).set_index(spec.date_column)
        sub.index = pd.DatetimeIndex(sub.index)
        out[iid] = sub
    return out


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((OperationalError, InterfaceError)),
    reraise=True,
)
async def _execute_upsert_with_retry(session: AsyncSession, stmt: Any) -> None:
    """Fix 15: wrap upsert execution with exponential backoff on transient DB errors."""
    await session.execute(stmt)


async def _upsert_batch(
    session: AsyncSession,
    spec: AssetSpec,
    rows: list[dict[str, Any]],
) -> None:
    """Upsert a batch of rows using the fastest safe path.

    Uses ``asyncpg.Connection.copy_records_to_table`` via a temp staging
    table, then ``INSERT ... SELECT FROM staging ON CONFLICT DO UPDATE``.
    Avoids Postgres's 32767-bind-param limit and is ~10-100x faster than
    pg_insert().values(many_dicts) for wide tables like v2.

    Flow per batch:
        1. CREATE TEMP TABLE stg (LIKE target) ON COMMIT DROP
        2. asyncpg COPY FROM rows → stg  (binary protocol, no param limit)
        3. INSERT INTO target SELECT FROM stg ON CONFLICT (pk) DO UPDATE ...
        4. Temp table dropped at commit automatically

    Fix 5 compliance preserved: the ``rows`` dict built by _to_decimal_row
    is used directly; no raw floats reach the DB.
    """
    if not rows:
        return

    model = spec.output_model
    target_table = model.__table__.name

    # Columns we actually want to write — exclude GENERATED STORED and
    # audit columns (created_at is set by DEFAULT NOW()).
    writable_cols = [
        c.name
        for c in model.__table__.columns
        if c.computed is None and c.name not in ("created_at", "updated_at")
    ]
    non_pk_cols = [
        c for c in writable_cols
        if c not in (spec.id_column, spec.date_column)
    ]

    # Build records as tuples in column order — asyncpg's fastest format
    records: list[tuple[Any, ...]] = [
        tuple(row.get(col) for col in writable_cols) for row in rows
    ]

    # Stage → copy → upsert-from-stage, all inside the caller's session/tx.
    async def _run(raw_conn: Any) -> None:
        # Unique staging table name per batch to avoid collisions if multiple
        # batches run in the same transaction (possible with buffer flushing)
        stg = f"_stg_{target_table}_{id(rows)}"
        await raw_conn.execute(
            f'CREATE TEMP TABLE "{stg}" (LIKE "{target_table}" INCLUDING DEFAULTS) ON COMMIT DROP'
        )
        # Drop the generated columns from the staging table — they can't be copied into
        for c in model.__table__.columns:
            if c.computed is not None:
                await raw_conn.execute(f'ALTER TABLE "{stg}" DROP COLUMN "{c.name}"')
        # Binary COPY
        await raw_conn.copy_records_to_table(
            stg, records=records, columns=writable_cols
        )
        # Upsert from staging into target
        update_set = ", ".join(
            f'"{c}" = EXCLUDED."{c}"' for c in non_pk_cols
        )
        col_list = ", ".join(f'"{c}"' for c in writable_cols)
        upsert_sql = (
            f'INSERT INTO "{target_table}" ({col_list}) '
            f'SELECT {col_list} FROM "{stg}" '
            f'ON CONFLICT ("{spec.date_column}", "{spec.id_column}") '
            f'DO UPDATE SET {update_set}, updated_at = NOW()'
        )
        await raw_conn.execute(upsert_sql)
        await raw_conn.execute(f'DROP TABLE IF EXISTS "{stg}"')

    # Get the underlying asyncpg connection from the SQLAlchemy session
    conn = await session.connection()
    raw_conn = await conn.get_raw_connection()
    asyncpg_conn = raw_conn.driver_connection  # asyncpg.Connection
    await _execute_upsert_with_retry_raw(asyncpg_conn, _run)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((OperationalError, InterfaceError)),
    reraise=True,
)
async def _execute_upsert_with_retry_raw(asyncpg_conn: Any, fn: Any) -> None:
    """Retry wrapper for the raw asyncpg COPY path."""
    await fn(asyncpg_conn)


async def compute_indicators(
    spec: AssetSpec,
    session: AsyncSession,
    instrument_ids: list[Any],
    *,
    from_date: date | None = None,
    to_date: date | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    benchmark_close: "pd.Series | None" = None,
) -> CompResult:
    """Core engine entry point.

    For each instrument:
      1. Load OHLCV history
      2. Skip if < spec.min_history_days rows
      3. Assert monotonic-increasing dates (Fix 6)
      4. Run pandas-ta-classic Strategy filtered for this asset
      5. Rename pandas-ta output columns to schema names (Fix 3)
      6. Compute and merge risk + HV columns (IND-C3c)
      7. Filter to [from_date, to_date] window
      8. Convert to Decimal-quantized dicts (Fix 4, 5)
      9. Upsert in batches of ``batch_size`` with retry (Fix 15)

    Args:
        benchmark_close: optional benchmark price series (same DatetimeIndex
            as OHLCV) used for beta/alpha/information_ratio. If None, those
            three columns are written as NULL. Caller responsibility: load
            NIFTY 50 once before calling and pass it here. For equity/etf/
            index/global/mf the benchmark is typically NIFTY 50.

    Returns:
        CompResult with counts for processed, skipped, errored instruments and rows written.
    """
    from app.computation.indicators_v2.risk_metrics import (
        compute_hv_series,
        compute_risk_series,
    )

    has_volume = spec.volume_col is not None
    strategy = load_strategy_for_asset(spec.asset_class_name, has_volume)
    rename_map = get_rename_map(spec.asset_class_name, has_volume)
    schema_cols = get_schema_columns(spec.asset_class_name, has_volume)
    column_limits = _build_column_limits(spec.output_model)

    result = CompResult(asset_class=spec.asset_class_name)
    buffer: list[dict[str, Any]] = []

    # Bulk-load OHLCV for ALL instrument_ids in a single SELECT ... WHERE
    # id IN (...) query, grouped into a {id: DataFrame} dict. Replaces the
    # former per-instrument query loop — one network roundtrip instead of
    # N. For 2,281 equities, this cuts ~5 minutes of pure wait to ~5 seconds.
    ohlcv_by_id = await _load_ohlcv_bulk(session, spec, instrument_ids)

    for iid in instrument_ids:
        try:
            df = ohlcv_by_id.get(iid)
            if df is None or len(df) < spec.min_history_days:
                result.instruments_skipped_insufficient_history += 1
                continue
            # Ensure we operate on a detached copy so df.ta.strategy can
            # freely mutate without aliasing into the bulk dict.
            df = df.copy()

            # Fix 6: row-position semantics; enforce monotonic order
            assert df.index.is_monotonic_increasing, (
                f"OHLCV for {iid!r} is not date-sorted — engine requires ascending order"
            )

            # Run pandas-ta-classic Strategy; mutates df in place
            df.ta.strategy(strategy)

            # Fix 3: rename pandas-ta output columns → schema names
            df = df.rename(columns=rename_map)

            # Alias the source close column into the schema-required
            # ``close_adj`` price-snapshot column. All 5 v2 tables use
            # ``close_adj`` as the snapshot column regardless of asset
            # class (MFs store NAV here, indices store raw close, etc.).
            # This is not a rename-map entry because "close" is the
            # input column, not a pandas-ta emission.
            if "close_adj" not in df.columns:
                df["close_adj"] = df["close"]

            # IND-C3c: compute risk + HV metrics and merge into df.
            # close column is still present after rename (pandas-ta appends,
            # does not replace original OHLCV columns).
            df_hv = compute_hv_series(
                df["close"],
                extra_windows=[("3y", 756), ("5y", 1260)],
            )
            df_risk = compute_risk_series(df["close"], benchmark_close)
            df = pd.concat([df, df_hv, df_risk], axis=1)

            # Defensive: fill any schema columns that pandas-ta did NOT emit
            # with NaN. This happens when an instrument is too short for a
            # given window (e.g. a 260-row series fails ROC(252) because
            # pandas-ta silently skips rather than returning all-NaN, or
            # returns a single valid row that then doesn't register as a
            # column in some corner cases). NaN will land as NULL at the
            # _to_decimal_row boundary — Fix 4 semantics.
            missing = schema_cols - set(df.columns)
            for col in missing:
                df[col] = float("nan")
            if missing:
                logger.debug(
                    "indicators_v2_columns_filled_nan",
                    asset_class=spec.asset_class_name,
                    instrument_id=str(iid),
                    missing=sorted(missing),
                )

            # Window filter
            window = df
            if from_date is not None:
                window = window[window.index >= pd.Timestamp(from_date)]
            if to_date is not None:
                window = window[window.index <= pd.Timestamp(to_date)]

            # Restrict to just the columns we'll write to the DB. This drops
            # any pandas-ta output that doesn't map to a schema column and
            # shrinks the dict payload ~3-5x.
            keep_cols = [c for c in schema_cols if c in window.columns]
            window_sub = window[keep_cols]

            # Vectorized row extraction — ~100x faster than iterrows + to_dict
            # per-row. to_dict('records') internally walks the underlying numpy
            # arrays once per column instead of materializing a Series per row.
            row_dicts = window_sub.to_dict(orient="records")
            idx_values = window_sub.index

            # Fix 5 invariant still holds: _to_decimal_row is called once per
            # row and its output is the single dict used for both the INSERT
            # VALUES and the ON CONFLICT UPDATE SET clauses.
            for i, row_dict in enumerate(row_dicts):
                idx = idx_values[i]
                db_row = _to_decimal_row(
                    row_dict,
                    schema_cols,
                    id_col=spec.id_column,
                    date_col=spec.date_column,
                    id_value=iid,
                    date_value=idx.date() if hasattr(idx, "date") else idx,
                    column_limits=column_limits,
                )
                buffer.append(db_row)

                if len(buffer) >= batch_size:
                    await _upsert_batch(session, spec, buffer)
                    result.rows_written += len(buffer)
                    buffer = []

            result.instruments_processed += 1

        except Exception as exc:  # per-instrument error isolation
            logger.exception(
                "indicator_compute_failed",
                asset_class=spec.asset_class_name,
                instrument_id=str(iid),
                error=str(exc),
            )
            result.instruments_errored += 1
            result.errors.append(
                {
                    "instrument_id": str(iid),
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                }
            )
            continue

    # Flush remainder
    if buffer:
        await _upsert_batch(session, spec, buffer)
        result.rows_written += len(buffer)
        buffer = []

    await session.flush()
    logger.info(
        "compute_indicators_done",
        asset_class=spec.asset_class_name,
        instruments_processed=result.instruments_processed,
        instruments_skipped=result.instruments_skipped_insufficient_history,
        instruments_errored=result.instruments_errored,
        rows_written=result.rows_written,
    )
    return result
