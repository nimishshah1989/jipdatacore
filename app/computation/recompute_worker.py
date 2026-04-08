"""Recompute worker — processes de_recompute_queue items.

Constraints:
- Max 2 concurrent workers
- Max 50,000 rows per batch
- heartbeat_at updated every 60 seconds
- Processes status='pending' items ordered by priority DESC, enqueued_at ASC
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.logging import get_logger
from app.models.prices import DeRecomputeQueue

logger = get_logger(__name__)

MAX_CONCURRENT_WORKERS = 2
MAX_BATCH_ROWS = 50_000
HEARTBEAT_INTERVAL_SECONDS = 60
WORKER_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_WORKERS)


async def _fetch_pending_items(
    session: AsyncSession,
    batch_size: int = MAX_BATCH_ROWS,
) -> list[DeRecomputeQueue]:
    """Fetch pending recompute queue items, ordered by priority and enqueue time.

    Args:
        session: Async DB session.
        batch_size: Maximum number of items to fetch.

    Returns:
        List of DeRecomputeQueue items with status='pending'.
    """
    result = await session.execute(
        sa.select(DeRecomputeQueue)
        .where(DeRecomputeQueue.status == "pending")
        .order_by(DeRecomputeQueue.priority.desc(), DeRecomputeQueue.enqueued_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    return list(result.scalars().all())


async def _mark_processing(
    session: AsyncSession,
    item: DeRecomputeQueue,
) -> None:
    """Mark a queue item as processing and set started_at."""
    now = datetime.now(tz=timezone.utc)
    item.status = "processing"
    item.started_at = now
    item.heartbeat_at = now
    session.add(item)
    await session.flush()


async def _update_heartbeat(
    session: AsyncSession,
    item_id: str,
) -> None:
    """Update heartbeat_at for an in-progress queue item."""
    await session.execute(
        sa.update(DeRecomputeQueue)
        .where(DeRecomputeQueue.id == item_id)
        .values(heartbeat_at=datetime.now(tz=timezone.utc))
    )
    await session.flush()


async def _mark_complete(
    session: AsyncSession,
    item: DeRecomputeQueue,
) -> None:
    """Mark a queue item as complete."""
    item.status = "complete"
    item.completed_at = datetime.now(tz=timezone.utc)
    session.add(item)
    await session.flush()


async def _mark_failed(
    session: AsyncSession,
    item: DeRecomputeQueue,
    error: str,
) -> None:
    """Mark a queue item as failed with error detail."""
    item.status = "failed"
    item.completed_at = datetime.now(tz=timezone.utc)
    item.error_detail = error[:2000]  # truncate to DB field limit
    session.add(item)
    await session.flush()


async def _process_single_item(
    item: DeRecomputeQueue,
    session_factory: async_sessionmaker,
) -> bool:
    """Process a single recompute queue item.

    Runs technicals recomputation for the instrument from item.from_date.
    Uses a separate session per item for isolation.

    Args:
        item: The queue item to process.
        session_factory: Async session factory.

    Returns:
        True if successful, False if failed.
    """
    item_id = str(item.id)
    instrument_id = str(item.instrument_id)
    from_date = item.from_date

    logger.info(
        "recompute_item_start",
        item_id=item_id,
        instrument_id=instrument_id,
        from_date=from_date.isoformat(),
    )

    # Set up heartbeat task
    heartbeat_task: Optional[asyncio.Task] = None

    try:
        async with session_factory() as session:
            async with session.begin():
                await _mark_processing(session, item)

        async def _heartbeat_loop() -> None:
            while True:
                await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
                try:
                    async with session_factory() as hb_session:
                        async with hb_session.begin():
                            await _update_heartbeat(hb_session, item_id)
                except Exception as hb_exc:
                    logger.warning(
                        "recompute_heartbeat_failed",
                        item_id=item_id,
                        error=str(hb_exc),
                    )

        heartbeat_task = asyncio.create_task(_heartbeat_loop())

        # Perform the actual recomputation
        # Import here to avoid circular imports at module load time
        from app.computation.technicals import (  # noqa: PLC0415
            compute_ema,
            compute_sma,
        )

        async with session_factory() as session:
            # Fetch price history for this instrument from from_date
            price_query = sa.text("""
                SELECT date, CAST(close_adj AS FLOAT) AS close_adj
                FROM de_equity_ohlcv
                WHERE instrument_id = :iid
                  AND date >= :from_date
                  AND close_adj IS NOT NULL
                  AND data_status = 'validated'
                ORDER BY date ASC
            """)
            result = await session.execute(
                price_query,
                {"iid": instrument_id, "from_date": from_date},
            )
            rows = result.fetchall()

        if not rows:
            logger.warning(
                "recompute_no_price_data",
                item_id=item_id,
                instrument_id=instrument_id,
            )
            async with session_factory() as session:
                async with session.begin():
                    # Re-fetch item within this session
                    item_result = await session.get(DeRecomputeQueue, item.id)
                    if item_result:
                        await _mark_complete(session, item_result)
            return True

        prices = [r.close_adj for r in rows]

        # Compute key indicators
        sma_50 = compute_sma(prices, 50)
        sma_200 = compute_sma(prices, 200)
        ema_20 = compute_ema(prices, 20)

        # Upsert back to de_equity_technical_daily
        from sqlalchemy.dialects.postgresql import insert as pg_insert  # noqa: PLC0415

        from app.models.computed import DeEquityTechnicalDaily  # noqa: PLC0415
        from decimal import Decimal  # noqa: PLC0415

        upsert_rows = []
        for idx, row in enumerate(rows):
            upsert_rows.append({
                "date": row.date,
                "instrument_id": instrument_id,
                "sma_50": sma_50[idx],
                "sma_200": sma_200[idx],
                "ema_20": ema_20[idx],
                "close_adj": Decimal(str(round(row.close_adj, 4))),
            })

        if upsert_rows:
            async with session_factory() as session:
                async with session.begin():
                    for offset in range(0, len(upsert_rows), 1000):
                        batch = upsert_rows[offset : offset + 1000]
                        stmt = pg_insert(DeEquityTechnicalDaily).values(batch)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=["date", "instrument_id"],
                            set_={
                                "sma_50": stmt.excluded.sma_50,
                                "sma_200": stmt.excluded.sma_200,
                                "ema_20": stmt.excluded.ema_20,
                                "close_adj": stmt.excluded.close_adj,
                                "updated_at": sa.func.now(),
                            },
                        )
                        await session.execute(stmt)

        # Mark complete
        async with session_factory() as session:
            async with session.begin():
                item_result = await session.get(DeRecomputeQueue, item.id)
                if item_result:
                    await _mark_complete(session, item_result)

        logger.info(
            "recompute_item_complete",
            item_id=item_id,
            rows_processed=len(rows),
        )
        return True

    except Exception as exc:
        error_msg = str(exc)
        logger.error(
            "recompute_item_failed",
            item_id=item_id,
            error=error_msg,
        )
        try:
            async with session_factory() as session:
                async with session.begin():
                    item_result = await session.get(DeRecomputeQueue, item.id)
                    if item_result:
                        await _mark_failed(session, item_result, error_msg)
        except Exception as mark_exc:
            logger.error(
                "recompute_mark_failed_error",
                item_id=item_id,
                error=str(mark_exc),
            )
        return False

    finally:
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass


async def run_recompute_worker(
    session_factory: async_sessionmaker,
    batch_size: int = MAX_BATCH_ROWS,
) -> dict[str, int]:
    """Run one batch of recompute worker — respects MAX_CONCURRENT_WORKERS semaphore.

    Fetches pending items, processes them concurrently (max 2 at a time).

    Args:
        session_factory: Async session factory for creating DB sessions.
        batch_size: Maximum number of items to process in this run.

    Returns:
        Dict with counts: processed, succeeded, failed.
    """
    logger.info("recompute_worker_start", batch_size=batch_size)

    # Fetch pending items
    async with session_factory() as session:
        items = await _fetch_pending_items(session, batch_size)

    if not items:
        logger.info("recompute_worker_no_pending_items")
        return {"processed": 0, "succeeded": 0, "failed": 0}

    logger.info("recompute_worker_items_fetched", count=len(items))

    results = {"processed": 0, "succeeded": 0, "failed": 0}

    async def _bounded_process(item: DeRecomputeQueue) -> None:
        async with WORKER_SEMAPHORE:
            success = await _process_single_item(item, session_factory)
            results["processed"] += 1
            if success:
                results["succeeded"] += 1
            else:
                results["failed"] += 1

    tasks = [asyncio.create_task(_bounded_process(item)) for item in items]
    await asyncio.gather(*tasks, return_exceptions=True)

    logger.info(
        "recompute_worker_complete",
        **results,
    )

    return results
