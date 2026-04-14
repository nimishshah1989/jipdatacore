"""Equity asset wrapper for the indicators v2 engine."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.computation.indicators_v2.engine import CompResult, compute_indicators
from app.computation.indicators_v2.spec import AssetSpec
from app.models.indicators_v2 import DeEquityTechnicalDailyV2
from app.models.instruments import DeInstrument
from app.models.prices import DeEquityOhlcv, DeIndexPrices

EQUITY_SPEC = AssetSpec(
    asset_class_name="equity",
    source_model=DeEquityOhlcv,
    output_model=DeEquityTechnicalDailyV2,
    id_column="instrument_id",
    date_column="date",
    # COALESCE adjusted → raw: the engine emits SQL ``COALESCE(close_adj, close)``
    # etc., so the spec survives today's data-quality state (volume_adj is 0%
    # populated in production; close/open/high/low _adj are ~99.77% populated)
    # AND it will automatically pick up the adjusted values when future
    # ingestion backfills arrive.
    close_col=("close_adj", "close"),
    open_col=("open_adj", "open"),
    high_col=("high_adj", "high"),
    low_col=("low_adj", "low"),
    volume_col=("volume_adj", "volume"),
    min_history_days=250,
)


async def load_active_equity_ids(session: AsyncSession) -> list[Any]:
    """Return UUIDs of all active, tradeable, non-suspended equity instruments.

    Ordered by id ASC so the backfill cursor (Fix 8) can resume correctly.
    """
    stmt = (
        sa.select(DeInstrument.id)
        .where(
            DeInstrument.is_active.is_(True),
            DeInstrument.is_tradeable.is_(True),
            DeInstrument.is_suspended.is_(False),
        )
        .order_by(DeInstrument.id.asc())
    )
    result = await session.execute(stmt)
    return [row[0] for row in result.fetchall()]


async def load_nifty50_benchmark(session: AsyncSession) -> pd.Series:
    """Load NIFTY 50 close prices as a benchmark series for beta/alpha/info ratio.

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


async def compute_equity_indicators(
    session: AsyncSession,
    instrument_ids: list[Any] | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> CompResult:
    """Compute equity technical + risk indicators for the given instrument set.

    Loads NIFTY 50 once as the benchmark for beta/alpha/info ratio.
    If ``instrument_ids`` is None, defaults to all active tradeable equities
    queried from de_instrument (ordered by id ASC for cursor compatibility).
    """
    if instrument_ids is None:
        instrument_ids = await load_active_equity_ids(session)
    benchmark = await load_nifty50_benchmark(session)
    return await compute_indicators(
        EQUITY_SPEC,
        session,
        instrument_ids,
        from_date=from_date,
        to_date=to_date,
        benchmark_close=benchmark,
    )
