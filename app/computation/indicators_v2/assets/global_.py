"""Global instruments asset wrapper for the indicators v2 engine.

Covers every row in de_global_instrument_master: global ETFs, broad indices
(S&P 500, Nasdaq, etc.), commodities (gold, oil), bonds, forex pairs,
cryptocurrencies. The engine uses the same pandas-ta-classic strategy as
equities/ETFs with has_volume=True — the requires_volume filter is handled
per-strategy, not per-instrument, so instruments without volume (e.g. forex)
still produce OBV/CMF/etc. as NaN which is fine.

No benchmark is passed for beta/alpha/info ratio — NIFTY 50 is not a
meaningful benchmark for S&P 500 or gold. Those columns will be NaN.
A later enhancement can pick a per-type benchmark (S&P 500 for global
equities, etc.).
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.computation.indicators_v2.engine import CompResult, compute_indicators
from app.computation.indicators_v2.spec import AssetSpec
from app.models.indicators_v2 import DeGlobalTechnicalDailyV2
from app.models.instruments import DeGlobalInstrumentMaster
from app.models.prices import DeGlobalPrices

GLOBAL_SPEC = AssetSpec(
    asset_class_name="global",
    source_model=DeGlobalPrices,
    output_model=DeGlobalTechnicalDailyV2,
    id_column="ticker",
    date_column="date",
    # de_global_prices has no _adj variants — plain column names.
    close_col="close",
    open_col="open",
    high_col="high",
    low_col="low",
    # volume is nullable (BigInteger); forex/index rows without volume produce NaN
    # for volume-based indicators, which is acceptable per spec.
    volume_col="volume",
    # 100 rows: enough for SMA_100; crypto/forex histories are shorter than
    # Indian equities so we use a lower threshold than the equity spec (250).
    min_history_days=100,
)


async def load_active_global_tickers(
    session: AsyncSession,
    instrument_types: Optional[list[str]] = None,
) -> list[str]:
    """Return tickers from de_global_instrument_master.

    de_global_instrument_master has no is_active field — all rows are
    treated as active. If ``instrument_types`` is provided, filter to those
    types (e.g. ["etf", "index"]). If None, return all instruments ordered
    by ticker ascending.
    """
    stmt = sa.select(DeGlobalInstrumentMaster.ticker)
    if instrument_types:
        stmt = stmt.where(
            DeGlobalInstrumentMaster.instrument_type.in_(instrument_types)
        )
    stmt = stmt.order_by(DeGlobalInstrumentMaster.ticker.asc())
    result = await session.execute(stmt)
    return [row[0] for row in result.fetchall()]


async def compute_global_indicators(
    session: AsyncSession,
    tickers: Optional[list[str]] = None,
    instrument_types: Optional[list[str]] = None,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
) -> CompResult:
    """Compute global technical + risk indicators.

    Args:
        session: Async SQLAlchemy session.
        tickers: Explicit list of tickers to process. If None, all tickers
            from de_global_instrument_master are loaded (optionally filtered
            by instrument_types).
        instrument_types: Optional filter — e.g. ["etf", "index"]. Ignored
            when ``tickers`` is provided explicitly.
        from_date: Start of the date window (inclusive). None = all history.
        to_date: End of the date window (inclusive). None = latest available.

    Returns:
        CompResult with rows_written, instruments_processed, etc.
    """
    if tickers is None:
        tickers = await load_active_global_tickers(
            session, instrument_types=instrument_types
        )
    return await compute_indicators(
        GLOBAL_SPEC,
        session,
        tickers,
        from_date=from_date,
        to_date=to_date,
        benchmark_close=None,  # intentional — see module docstring
    )
