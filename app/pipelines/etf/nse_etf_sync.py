"""NSE ETF OHLCV sync pipeline.

Copies daily OHLCV from de_equity_ohlcv into de_etf_ohlcv for all NSE-listed
ETFs that are active in de_etf_master.

Source: de_equity_ohlcv (populated by BHAV copy pipeline)
Target: de_etf_ohlcv (PK: date, ticker)

Pre-requisite: scripts/ingest/nse_etf_master.py must have been run to seed
de_etf_master with NSE ETF definitions before this pipeline executes.

Trigger: daily after BHAV copy pipeline completes.
"""

from __future__ import annotations

from datetime import date

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

_SYNC_SQL = sa.text("""
    INSERT INTO de_etf_ohlcv (date, ticker, open, high, low, close, volume)
    SELECT
        eo.date,
        i.current_symbol,
        eo.open,
        eo.high,
        eo.low,
        eo.close,
        eo.volume
    FROM de_equity_ohlcv eo
    JOIN de_instrument i ON i.id = eo.instrument_id
    WHERE i.current_symbol = ANY(:nse_tickers)
      AND eo.date = :business_date
    ON CONFLICT (date, ticker) DO UPDATE SET
        open       = EXCLUDED.open,
        high       = EXCLUDED.high,
        low        = EXCLUDED.low,
        close      = EXCLUDED.close,
        volume     = EXCLUDED.volume,
        updated_at = NOW()
""")


class NseEtfSyncPipeline(BasePipeline):
    """Syncs daily OHLCV for NSE ETFs from de_equity_ohlcv into de_etf_ohlcv."""

    pipeline_name = "nse_etf_sync"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        # Step 1: fetch active NSE tickers from master
        master_result = await session.execute(
            sa.text(
                "SELECT ticker FROM de_etf_master WHERE exchange = 'NSE' AND is_active = TRUE"
            )
        )
        nse_tickers: list[str] = [row[0] for row in master_result.fetchall()]

        if not nse_tickers:
            logger.warning(
                "nse_etf_sync_no_tickers",
                business_date=business_date.isoformat(),
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        logger.info(
            "nse_etf_sync_start",
            ticker_count=len(nse_tickers),
            business_date=business_date.isoformat(),
        )

        # Step 2: copy OHLCV via SQL — no Python memory pressure
        result = await session.execute(
            _SYNC_SQL,
            {"nse_tickers": nse_tickers, "business_date": business_date},
        )
        rowcount: int = result.rowcount if result.rowcount is not None else 0

        logger.info(
            "nse_etf_sync_done",
            rows_upserted=rowcount,
            ticker_count=len(nse_tickers),
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(rows_processed=rowcount, rows_failed=0)
