"""ETF asset wrapper for the indicators v2 engine."""

from __future__ import annotations

from datetime import date

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.computation.indicators_v2.engine import CompResult, compute_indicators
from app.computation.indicators_v2.spec import AssetSpec
from app.models.indicators_v2 import DeEtfTechnicalDailyV2
from app.models.etf import DeEtfMaster, DeEtfOhlcv
from app.models.prices import DeIndexPrices

# DeEtfOhlcv has no adjusted-price columns (splits are rare for ETFs and are
# already reflected in market prices by the exchange). Use raw column names
# directly — no COALESCE tuples needed.
ETF_SPEC = AssetSpec(
    asset_class_name="etf",
    source_model=DeEtfOhlcv,
    output_model=DeEtfTechnicalDailyV2,
    id_column="ticker",
    date_column="date",
    close_col="close",
    open_col="open",
    high_col="high",
    low_col="low",
    volume_col="volume",
    min_history_days=100,  # ETFs often have shorter history than equities
)


async def load_active_etf_tickers(session: AsyncSession) -> list[str]:
    """Return tickers of all active ETFs from de_etf_master, ordered ASC for cursor resumability."""
    stmt = (
        sa.select(DeEtfMaster.ticker)
        .where(DeEtfMaster.is_active.is_(True))
        .order_by(DeEtfMaster.ticker.asc())
    )
    result = await session.execute(stmt)
    return [row[0] for row in result.fetchall()]


async def load_nifty50_benchmark(session: AsyncSession) -> pd.Series:
    """Load NIFTY 50 close prices as benchmark series.

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


async def compute_etf_indicators(
    session: AsyncSession,
    tickers: list[str] | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> CompResult:
    """Compute ETF technical + risk indicators for the given ticker set.

    Loads NIFTY 50 once as the benchmark for beta/alpha/info ratio.
    If ``tickers`` is None, defaults to all active ETFs from de_etf_master
    (ordered by ticker ASC for cursor compatibility).
    """
    if tickers is None:
        tickers = await load_active_etf_tickers(session)
    benchmark = await load_nifty50_benchmark(session)
    return await compute_indicators(
        ETF_SPEC,
        session,
        tickers,
        from_date=from_date,
        to_date=to_date,
        benchmark_close=benchmark,
    )
