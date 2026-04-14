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

# Matches existing runner.py convention
DEFAULT_BATCH_SIZE = 200

# Decimal quantization step per project convention (Numeric(18,4) scale=4)
_Q = Decimal("0.0001")


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
) -> dict[str, Any]:
    """Convert one pandas row + metadata into a DB-ready dict.

    - Float / int / np.float64 → Decimal quantized to 0.0001, or None if NaN
    - Only columns in ``schema_columns`` plus id + date are included
    - Fix 5: this dict is used for BOTH INSERT VALUES and ON CONFLICT UPDATE SET
    """
    out: dict[str, Any] = {id_col: id_value, date_col: date_value}
    for col in schema_columns:
        raw = row.get(col)
        if raw is None:
            out[col] = None
        elif isinstance(raw, bool):
            # bool must come before int check (bool is subclass of int)
            out[col] = raw
        elif isinstance(raw, int):
            out[col] = raw
        elif isinstance(raw, float) and (math.isnan(raw) or math.isinf(raw)):
            out[col] = None
        elif isinstance(raw, float):
            try:
                out[col] = Decimal(str(raw)).quantize(_Q)
            except Exception:
                out[col] = None
        else:
            # numpy scalar or other numeric type — go through str(float()) path
            try:
                fval = float(raw)
                if math.isnan(fval) or math.isinf(fval):
                    out[col] = None
                else:
                    out[col] = Decimal(str(fval)).quantize(_Q)
            except Exception:
                out[col] = None
    return out


async def _load_ohlcv(
    session: AsyncSession,
    spec: AssetSpec,
    instrument_id: Any,
) -> pd.DataFrame:
    """Load full OHLCV history for one instrument as a pandas DataFrame."""
    model = spec.source_model
    cols = [getattr(model, spec.date_column), getattr(model, spec.close_col)]
    for extra in (spec.open_col, spec.high_col, spec.low_col, spec.volume_col):
        if extra is not None:
            cols.append(getattr(model, extra))

    id_attr = getattr(model, spec.id_column)
    stmt = (
        sa.select(*cols)
        .where(id_attr == instrument_id)
        .order_by(getattr(model, spec.date_column).asc())
    )
    result = await session.execute(stmt)
    rows = result.fetchall()
    if not rows:
        return pd.DataFrame()

    data: dict[str, list] = {spec.date_column: [], "close": []}
    if spec.open_col:
        data["open"] = []
    if spec.high_col:
        data["high"] = []
    if spec.low_col:
        data["low"] = []
    if spec.volume_col:
        data["volume"] = []

    for row in rows:
        vals = list(row)
        data[spec.date_column].append(vals[0])
        data["close"].append(float(vals[1]) if vals[1] is not None else None)
        idx = 2
        if spec.open_col:
            data["open"].append(float(vals[idx]) if vals[idx] is not None else None)
            idx += 1
        if spec.high_col:
            data["high"].append(float(vals[idx]) if vals[idx] is not None else None)
            idx += 1
        if spec.low_col:
            data["low"].append(float(vals[idx]) if vals[idx] is not None else None)
            idx += 1
        if spec.volume_col:
            data["volume"].append(int(vals[idx]) if vals[idx] is not None else None)
            idx += 1

    df = pd.DataFrame(data)
    df = df.set_index(spec.date_column)
    return df


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
    """Upsert a batch of rows via ON CONFLICT DO UPDATE. Fix 5 compliance."""
    if not rows:
        return
    stmt = pg_insert(spec.output_model).values(rows)
    non_pk = [
        c.name
        for c in spec.output_model.__table__.columns
        if c.name not in (spec.id_column, spec.date_column)
        and c.computed is None  # GENERATED columns must not appear in UPDATE SET
        and c.name not in ("created_at",)
    ]
    stmt = stmt.on_conflict_do_update(
        index_elements=[spec.date_column, spec.id_column],
        set_={col: stmt.excluded[col] for col in non_pk} | {"updated_at": sa.func.now()},
    )
    await _execute_upsert_with_retry(session, stmt)


async def compute_indicators(
    spec: AssetSpec,
    session: AsyncSession,
    instrument_ids: list[Any],
    *,
    from_date: date | None = None,
    to_date: date | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> CompResult:
    """Core engine entry point.

    For each instrument:
      1. Load OHLCV history
      2. Skip if < spec.min_history_days rows
      3. Assert monotonic-increasing dates (Fix 6)
      4. Run pandas-ta-classic Strategy filtered for this asset
      5. Rename pandas-ta output columns to schema names (Fix 3)
      6. Filter to [from_date, to_date] window
      7. Convert to Decimal-quantized dicts (Fix 4, 5)
      8. Upsert in batches of ``batch_size`` with retry (Fix 15)

    Returns:
        CompResult with counts for processed, skipped, errored instruments and rows written.
    """
    has_volume = spec.volume_col is not None
    strategy = load_strategy_for_asset(spec.asset_class_name, has_volume)
    rename_map = get_rename_map(spec.asset_class_name, has_volume)
    schema_cols = get_schema_columns(spec.asset_class_name, has_volume)

    result = CompResult(asset_class=spec.asset_class_name)
    buffer: list[dict[str, Any]] = []

    for iid in instrument_ids:
        try:
            df = await _load_ohlcv(session, spec, iid)
            if len(df) < spec.min_history_days:
                result.instruments_skipped_insufficient_history += 1
                continue

            # Fix 6: row-position semantics; enforce monotonic order
            assert df.index.is_monotonic_increasing, (
                f"OHLCV for {iid!r} is not date-sorted — engine requires ascending order"
            )

            # Run pandas-ta-classic Strategy; mutates df in place
            df.ta.strategy(strategy)

            # Fix 3: rename pandas-ta output columns → schema names
            df = df.rename(columns=rename_map)

            # Defensive: assert every schema column is present
            missing = schema_cols - set(df.columns)
            if missing:
                raise RuntimeError(
                    f"pandas-ta did not emit expected columns after rename: {sorted(missing)}"
                )

            # Window filter
            window = df
            if from_date is not None:
                window = window[window.index >= pd.Timestamp(from_date)]
            if to_date is not None:
                window = window[window.index <= pd.Timestamp(to_date)]

            # Fix 5: build DB rows via _to_decimal_row once each
            for idx, row in window.iterrows():
                row_dict = row.to_dict()
                db_row = _to_decimal_row(
                    row_dict,
                    schema_cols,
                    id_col=spec.id_column,
                    date_col=spec.date_column,
                    id_value=iid,
                    date_value=idx.date() if hasattr(idx, "date") else idx,
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
