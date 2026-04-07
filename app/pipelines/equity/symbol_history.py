"""Symbol history pipeline — detects historical symbol changes via OHLCV mismatch.

The de_corporate_actions table has no name_change records. Symbol changes are
detected by comparing the symbol stored in de_equity_ohlcv rows (the symbol at
the time of the trade) against the current de_instrument.current_symbol.

When an instrument traded under a different symbol in the past, the last date
it traded under the old symbol is treated as the effective_date of the change.
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.instruments import DeSymbolHistory
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.validation import AnomalyRecord

logger = get_logger(__name__)

# SQL that detects every (instrument, old_symbol) pair where the OHLCV symbol
# differs from the current symbol, and finds the last trading date under the
# old symbol (used as the effective_date of the change).
_SYMBOL_CHANGES_SQL = text(
    """
    WITH symbol_changes AS (
        SELECT
            e.instrument_id,
            e.symbol AS old_symbol,
            i.current_symbol AS new_symbol,
            MAX(e.date) AS last_date_old_symbol
        FROM de_equity_ohlcv e
        JOIN de_instrument i ON e.instrument_id = i.id
        WHERE e.symbol IS NOT NULL
          AND e.symbol != i.current_symbol
        GROUP BY e.instrument_id, e.symbol, i.current_symbol
    )
    SELECT instrument_id, old_symbol, new_symbol, last_date_old_symbol
    FROM symbol_changes
    ORDER BY last_date_old_symbol
    """
)

_CHANGE_REASON = "Historical OHLCV symbol mismatch"


async def detect_ohlcv_symbol_changes(
    session: AsyncSession,
) -> list[dict[str, Any]]:
    """Query de_equity_ohlcv for instruments whose historical symbol differs from current.

    Returns:
        List of dicts with keys: instrument_id, old_symbol, new_symbol,
        last_date_old_symbol (the effective_date of the change).
    """
    result = await session.execute(_SYMBOL_CHANGES_SQL)
    rows = result.fetchall()
    return [
        {
            "instrument_id": row.instrument_id,
            "old_symbol": row.old_symbol,
            "new_symbol": row.new_symbol,
            "last_date_old_symbol": row.last_date_old_symbol,
        }
        for row in rows
    ]


class SymbolHistoryPipeline(BasePipeline):
    """Populate de_symbol_history from historical OHLCV symbol mismatches.

    No external HTTP fetch — purely an internal transformation pipeline.
    Runs regardless of trading day since it backfills historical data.

    Strategy:
      1. Find every (instrument_id, old_symbol) combination in de_equity_ohlcv
         where old_symbol != de_instrument.current_symbol.
      2. Use the last date the stock traded under the old symbol as effective_date.
      3. Upsert into de_symbol_history with ON CONFLICT DO NOTHING so that
         records written by MasterRefreshPipeline are not overwritten.
    """

    pipeline_name = "symbol_history"
    requires_trading_day = False

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Detect OHLCV symbol mismatches and upsert into de_symbol_history."""
        logger.info(
            "symbol_history_execute_start",
            business_date=business_date.isoformat(),
        )

        changes = await detect_ohlcv_symbol_changes(session)

        logger.info(
            "symbol_history_changes_found",
            count=len(changes),
        )

        if not changes:
            logger.info(
                "symbol_history_no_changes",
                business_date=business_date.isoformat(),
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        insert_rows: list[dict[str, Any]] = []
        for change in changes:
            instrument_id: uuid.UUID = change["instrument_id"]
            old_symbol: str = change["old_symbol"]
            new_symbol: str = change["new_symbol"]
            effective_date: date = change["last_date_old_symbol"]

            # Sanity guard: skip self-referential rows (should not occur but be safe)
            if old_symbol == new_symbol:
                logger.warning(
                    "symbol_history_self_referential_skipped",
                    instrument_id=str(instrument_id),
                    symbol=old_symbol,
                )
                continue

            insert_rows.append(
                {
                    "instrument_id": instrument_id,
                    "effective_date": effective_date,
                    "old_symbol": old_symbol,
                    "new_symbol": new_symbol,
                    "reason": _CHANGE_REASON,
                }
            )

        rows_processed = 0
        if insert_rows:
            stmt = pg_insert(DeSymbolHistory).values(insert_rows)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["instrument_id", "effective_date"]
            )
            await session.execute(stmt)
            rows_processed = len(insert_rows)

        logger.info(
            "symbol_history_execute_complete",
            rows_processed=rows_processed,
            rows_failed=0,
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(rows_processed=rows_processed, rows_failed=0)

    async def validate(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> list[AnomalyRecord]:
        """No anomaly detection for historical backfill — structural data only."""
        return []


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.config import get_settings

    async def main() -> None:
        settings = get_settings()
        engine = create_async_engine(settings.database_url, pool_size=5, pool_pre_ping=True)
        sf = async_sessionmaker(engine, expire_on_commit=False)
        pipeline = SymbolHistoryPipeline()

        print("Detecting historical symbol changes from OHLCV mismatch...")
        async with sf() as session:
            async with session.begin():
                result = await pipeline.run(date.today(), session)
                print(f"  Status:          {result.status}")
                print(f"  Rows processed:  {result.rows_processed}")
                print(f"  Rows failed:     {result.rows_failed}")
                print(f"  Anomalies:       {result.anomalies_detected}")

        await engine.dispose()
        print("Done.")

    asyncio.run(main())
