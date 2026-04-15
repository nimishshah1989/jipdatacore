"""Mutual fund asset wrapper for the indicators v2 engine.

Single-price asset: open/high/low/volume are all None, so the strategy
loader drops every indicator that requires OHLC width or volume (Fix 13).
The NAV column is used as the close price.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.computation.indicators_v2.engine import CompResult, compute_indicators
from app.computation.indicators_v2.spec import AssetSpec
from app.models.indicators_v2 import DeMfTechnicalDaily
from app.models.instruments import DeMfMaster
from app.models.prices import DeMfNavDaily, DeIndexPrices

MF_SPEC = AssetSpec(
    asset_class_name="mf",
    source_model=DeMfNavDaily,
    output_model=DeMfTechnicalDaily,
    id_column="mstar_id",
    date_column="nav_date",
    close_col="nav",
    open_col=None,
    high_col=None,
    low_col=None,
    volume_col=None,
    min_history_days=250,
)


async def load_eligible_mf_ids(session: AsyncSession) -> list[str]:
    """Return mstar_ids for equity-regular-growth funds eligible for technicals.

    Filters: active, not ETF, not index fund, purchase_mode=1 (Regular plan),
    broad_category='Equity', fund name excludes IDCW/Dividend, and fund must
    have NAV data in de_mf_nav_daily.

    Ordered by mstar_id ASC for cursor resumability.
    """
    has_nav = (
        sa.select(DeMfNavDaily.mstar_id)
        .distinct()
        .correlate(None)
        .scalar_subquery()
    )
    stmt = (
        sa.select(DeMfMaster.mstar_id)
        .where(
            DeMfMaster.is_active.is_(True),
            DeMfMaster.is_etf.is_(False),
            DeMfMaster.is_index_fund.is_(False),
            DeMfMaster.purchase_mode == 1,
            DeMfMaster.broad_category == "Equity",
            ~DeMfMaster.fund_name.ilike("%IDCW%"),
            ~DeMfMaster.fund_name.ilike("%Dividend%"),
            DeMfMaster.mstar_id.in_(has_nav),
        )
        .order_by(DeMfMaster.mstar_id.asc())
    )
    result = await session.execute(stmt)
    return [row[0] for row in result.fetchall()]


async def load_nifty50_benchmark(session: AsyncSession) -> pd.Series:
    """Load NIFTY 50 close prices as benchmark for beta/alpha/info ratio.

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


async def compute_mf_indicators(
    session: AsyncSession,
    mstar_ids: list[str] | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> CompResult:
    """Compute MF technical + risk indicators for the given fund set.

    Loads NIFTY 50 once as the benchmark for beta/alpha/info ratio.
    If ``mstar_ids`` is None, defaults to all eligible equity-regular-growth
    funds queried from de_mf_master (ordered by mstar_id ASC).
    """
    if mstar_ids is None:
        mstar_ids = await load_eligible_mf_ids(session)
    benchmark = await load_nifty50_benchmark(session)
    return await compute_indicators(
        MF_SPEC,
        session,
        mstar_ids,
        from_date=from_date,
        to_date=to_date,
        benchmark_close=benchmark,
    )
