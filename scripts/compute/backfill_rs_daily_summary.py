#!/usr/bin/env python3
"""Backfill de_rs_daily_summary from de_rs_scores JOIN de_instrument.

Populates historical rows that were never written because Step 13 was missing.
Processes one date at a time to keep memory bounded.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.compute.db import get_async_url

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker


BATCH_SIZE = 2000


async def backfill() -> None:
    url = get_async_url()
    engine = create_async_engine(url, pool_size=5, max_overflow=0)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        dates_result = await session.execute(sa.text(
            "SELECT DISTINCT date FROM de_rs_scores "
            "WHERE entity_type = 'equity' ORDER BY date"
        ))
        all_dates = [r.date for r in dates_result.fetchall()]

    print(f"Found {len(all_dates)} distinct dates to backfill")
    total = 0

    for i, bdate in enumerate(all_dates):
        async with async_session() as session:
            upsert_sql = sa.text("""
                INSERT INTO de_rs_daily_summary
                    (date, instrument_id, symbol, sector, vs_benchmark,
                     rs_composite, rs_1m, rs_3m)
                SELECT
                    s.date,
                    s.entity_id::uuid,
                    i.current_symbol,
                    i.sector,
                    s.vs_benchmark,
                    s.rs_composite,
                    s.rs_1m,
                    s.rs_3m
                FROM de_rs_scores s
                JOIN de_instrument i ON i.id = s.entity_id::uuid
                WHERE s.date = :bdate AND s.entity_type = 'equity'
                ON CONFLICT (date, instrument_id, vs_benchmark) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    sector = EXCLUDED.sector,
                    rs_composite = EXCLUDED.rs_composite,
                    rs_1m = EXCLUDED.rs_1m,
                    rs_3m = EXCLUDED.rs_3m,
                    updated_at = NOW()
            """)
            result = await session.execute(upsert_sql, {"bdate": bdate})
            await session.commit()
            rows = result.rowcount
            total += rows

        if (i + 1) % 50 == 0 or i == len(all_dates) - 1:
            print(f"  [{i+1}/{len(all_dates)}] date={bdate} rows={rows} cumulative={total}")

    print(f"\nBackfill complete: {total} total rows upserted across {len(all_dates)} dates")

    async with async_session() as session:
        count = (await session.execute(sa.text(
            "SELECT COUNT(*) FROM de_rs_daily_summary"
        ))).scalar()
        date_count = (await session.execute(sa.text(
            "SELECT COUNT(DISTINCT date) FROM de_rs_daily_summary"
        ))).scalar()
        print(f"Verification: {count} rows, {date_count} distinct dates")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(backfill())
