"""Indian indices asset wrapper for the indicators v2 engine.

Runs the same pandas-ta-classic strategy as equities/ETFs but with
volume_col=None so the requires_volume-filter drops OBV/CMF/MFI/VWAP/etc.
Fix 12 from the eng-review: sectoral index 'volume' isn't meaningful,
so volume indicators are excluded at the spec level, not patched per-row.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.computation.indicators_v2.engine import CompResult, compute_indicators
from app.computation.indicators_v2.spec import AssetSpec
from app.models.indicators_v2 import DeIndexTechnicalDaily
from app.models.instruments import DeIndexMaster
from app.models.prices import DeIndexPrices

INDEX_SPEC = AssetSpec(
    asset_class_name="index",
    source_model=DeIndexPrices,
    output_model=DeIndexTechnicalDaily,
    id_column="index_code",
    date_column="date",
    close_col="close",  # de_index_prices has only raw OHLC, no _adj columns
    open_col="open",
    high_col="high",
    low_col="low",
    volume_col=None,  # Fix 12: indices have no meaningful volume; drops OBV/CMF/MFI/VWAP
    min_history_days=250,
)


async def load_index_codes(
    session: AsyncSession,
    categories: Optional[list[str]] = None,
) -> list[str]:
    """Return index_codes from de_index_master.

    ``categories`` filters by the 'category' column (one of broad, sectoral,
    thematic, strategy). None returns all 135.
    """
    stmt = sa.select(DeIndexMaster.index_code)
    if categories:
        stmt = stmt.where(DeIndexMaster.category.in_(categories))
    stmt = stmt.order_by(DeIndexMaster.index_code.asc())
    result = await session.execute(stmt)
    return [row[0] for row in result.fetchall()]


async def load_nifty50_benchmark(session: AsyncSession) -> pd.Series:
    """Load NIFTY 50 close prices as the benchmark for sector beta/alpha.

    Returns an empty Series (dtype float) if the table has no NIFTY 50 data;
    the engine will then write NULL for beta/alpha/information_ratio columns.
    """
    stmt = (
        sa.select(DeIndexPrices.date, DeIndexPrices.close)
        .where(DeIndexPrices.index_code == "NIFTY 50")
        .order_by(DeIndexPrices.date.asc())
    )
    result = await session.execute(stmt)
    rows = result.fetchall()
    if not rows:
        return pd.Series(dtype=float, name="close")
    dates = [r[0] for r in rows]
    closes = [float(r[1]) if r[1] is not None else None for r in rows]
    return pd.Series(closes, index=pd.DatetimeIndex(dates), name="close")


async def compute_index_indicators(
    session: AsyncSession,
    index_codes: list[str] | None = None,
    categories: list[str] | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> CompResult:
    """Compute technical + risk indicators for Indian indices.

    Uses NIFTY 50 as the benchmark for sector beta/alpha — meaningful for
    sectoral indices (measures sector beta to the market). NIFTY 50 itself
    will compute beta=1.0 against itself, which is acceptable.

    ``index_codes`` takes precedence over ``categories``. Pass neither to
    process all 135 indices.
    """
    if index_codes is None:
        index_codes = await load_index_codes(session, categories=categories)
    benchmark = await load_nifty50_benchmark(session)
    return await compute_indicators(
        INDEX_SPEC,
        session,
        index_codes,
        from_date=from_date,
        to_date=to_date,
        benchmark_close=benchmark,
    )
