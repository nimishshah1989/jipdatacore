"""Step 1: Compute SMA50/200 + close_adj via SQL window functions.

Zero Python memory — runs entirely inside PostgreSQL.
Processes all OHLCV rows in a single INSERT...SELECT.

Usage:
    python -m scripts.compute.technicals_sql
    python -m scripts.compute.technicals_sql --start-date 2025-01-01
"""

import argparse
import asyncio
import time

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from scripts.compute.db import get_async_url


TECHNICALS_SQL = """
INSERT INTO de_equity_technical_daily (date, instrument_id, sma_50, sma_200, close_adj)
SELECT
    date,
    instrument_id,
    CASE WHEN COUNT(*) OVER w50 >= 50 THEN
        ROUND(AVG(COALESCE(close_adj, close)) OVER w50, 4)
    END AS sma_50,
    CASE WHEN COUNT(*) OVER w200 >= 200 THEN
        ROUND(AVG(COALESCE(close_adj, close)) OVER w200, 4)
    END AS sma_200,
    ROUND(COALESCE(close_adj, close)::numeric, 4) AS close_adj
FROM de_equity_ohlcv
WHERE COALESCE(close_adj, close) IS NOT NULL
  AND date >= :start_date
WINDOW
    w50 AS (PARTITION BY instrument_id ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),
    w200 AS (PARTITION BY instrument_id ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
ON CONFLICT (date, instrument_id) DO UPDATE SET
    sma_50 = EXCLUDED.sma_50,
    sma_200 = EXCLUDED.sma_200,
    close_adj = EXCLUDED.close_adj,
    updated_at = NOW()
"""


async def run(start_date: str = "2007-01-01") -> None:
    t0 = time.time()
    print(f"Computing SMA50/200 from {start_date}...", flush=True)

    engine = create_async_engine(get_async_url(), pool_size=1)
    async with engine.begin() as conn:
        await conn.execute(sa.text("SET LOCAL work_mem = '512MB'"))
        await conn.execute(sa.text("SET LOCAL statement_timeout = '1800s'"))
        await conn.execute(sa.text(TECHNICALS_SQL), {"start_date": start_date})
    await engine.dispose()

    # Verify
    engine = create_async_engine(get_async_url(), pool_size=1)
    async with engine.connect() as conn:
        r = await conn.execute(sa.text("SELECT COUNT(*) FROM de_equity_technical_daily"))
        print(f"Done: {r.scalar_one():,} rows ({time.time()-t0:.0f}s)")
    await engine.dispose()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="2007-01-01")
    args = parser.parse_args()
    asyncio.run(run(args.start_date))


if __name__ == "__main__":
    main()
