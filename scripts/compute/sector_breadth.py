"""Compute per-sector breadth rollups from de_equity_technical_daily.

Aggregates above_50dma / above_200dma / above_20ema / RSI / MACD booleans
into daily per-sector counts and percentages.

Usage:
    python -m scripts.compute.sector_breadth
    python -m scripts.compute.sector_breadth --backfill --from 2020-01-01
"""

from __future__ import annotations

import argparse
import asyncio
import time

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from scripts.compute.db import get_async_url

SECTOR_BREADTH_SQL = """
INSERT INTO de_sector_breadth_daily
    (date, sector, stocks_total,
     stocks_above_50dma, stocks_above_200dma, stocks_above_20ema,
     pct_above_50dma, pct_above_200dma, pct_above_20ema,
     stocks_rsi_overbought, stocks_rsi_oversold, stocks_macd_bullish,
     breadth_regime, updated_at)
SELECT
    t.date,
    COALESCE(m.sector, 'Unclassified') AS sector,
    count(*)::int AS stocks_total,
    count(*) FILTER (WHERE t.above_50dma)::int AS stocks_above_50dma,
    count(*) FILTER (WHERE t.above_200dma)::int AS stocks_above_200dma,
    count(*) FILTER (WHERE t.above_20ema)::int AS stocks_above_20ema,
    ROUND(count(*) FILTER (WHERE t.above_50dma)::numeric
          / NULLIF(count(*), 0) * 100, 2) AS pct_above_50dma,
    ROUND(count(*) FILTER (WHERE t.above_200dma)::numeric
          / NULLIF(count(*), 0) * 100, 2) AS pct_above_200dma,
    ROUND(count(*) FILTER (WHERE t.above_20ema)::numeric
          / NULLIF(count(*), 0) * 100, 2) AS pct_above_20ema,
    count(*) FILTER (WHERE t.rsi_overbought)::int AS stocks_rsi_overbought,
    count(*) FILTER (WHERE t.rsi_oversold)::int AS stocks_rsi_oversold,
    count(*) FILTER (WHERE t.macd_bullish)::int AS stocks_macd_bullish,
    CASE
        WHEN ROUND(count(*) FILTER (WHERE t.above_50dma)::numeric
              / NULLIF(count(*), 0) * 100, 2) > 70 THEN 'bullish'
        WHEN ROUND(count(*) FILTER (WHERE t.above_50dma)::numeric
              / NULLIF(count(*), 0) * 100, 2) < 30 THEN 'bearish'
        ELSE 'neutral'
    END AS breadth_regime,
    NOW() AS updated_at
FROM de_equity_technical_daily t
JOIN de_instrument m ON m.id = t.instrument_id
WHERE t.close_adj IS NOT NULL
GROUP BY t.date, m.sector
ON CONFLICT (date, sector) DO UPDATE SET
    stocks_total = EXCLUDED.stocks_total,
    stocks_above_50dma = EXCLUDED.stocks_above_50dma,
    stocks_above_200dma = EXCLUDED.stocks_above_200dma,
    stocks_above_20ema = EXCLUDED.stocks_above_20ema,
    pct_above_50dma = EXCLUDED.pct_above_50dma,
    pct_above_200dma = EXCLUDED.pct_above_200dma,
    pct_above_20ema = EXCLUDED.pct_above_20ema,
    stocks_rsi_overbought = EXCLUDED.stocks_rsi_overbought,
    stocks_rsi_oversold = EXCLUDED.stocks_rsi_oversold,
    stocks_macd_bullish = EXCLUDED.stocks_macd_bullish,
    breadth_regime = EXCLUDED.breadth_regime,
    updated_at = EXCLUDED.updated_at
"""

SECTOR_BREADTH_INCREMENTAL_SQL = """
INSERT INTO de_sector_breadth_daily
    (date, sector, stocks_total,
     stocks_above_50dma, stocks_above_200dma, stocks_above_20ema,
     pct_above_50dma, pct_above_200dma, pct_above_20ema,
     stocks_rsi_overbought, stocks_rsi_oversold, stocks_macd_bullish,
     breadth_regime, updated_at)
SELECT
    t.date,
    COALESCE(m.sector, 'Unclassified') AS sector,
    count(*)::int AS stocks_total,
    count(*) FILTER (WHERE t.above_50dma)::int AS stocks_above_50dma,
    count(*) FILTER (WHERE t.above_200dma)::int AS stocks_above_200dma,
    count(*) FILTER (WHERE t.above_20ema)::int AS stocks_above_20ema,
    ROUND(count(*) FILTER (WHERE t.above_50dma)::numeric
          / NULLIF(count(*), 0) * 100, 2) AS pct_above_50dma,
    ROUND(count(*) FILTER (WHERE t.above_200dma)::numeric
          / NULLIF(count(*), 0) * 100, 2) AS pct_above_200dma,
    ROUND(count(*) FILTER (WHERE t.above_20ema)::numeric
          / NULLIF(count(*), 0) * 100, 2) AS pct_above_20ema,
    count(*) FILTER (WHERE t.rsi_overbought)::int AS stocks_rsi_overbought,
    count(*) FILTER (WHERE t.rsi_oversold)::int AS stocks_rsi_oversold,
    count(*) FILTER (WHERE t.macd_bullish)::int AS stocks_macd_bullish,
    CASE
        WHEN ROUND(count(*) FILTER (WHERE t.above_50dma)::numeric
              / NULLIF(count(*), 0) * 100, 2) > 70 THEN 'bullish'
        WHEN ROUND(count(*) FILTER (WHERE t.above_50dma)::numeric
              / NULLIF(count(*), 0) * 100, 2) < 30 THEN 'bearish'
        ELSE 'neutral'
    END AS breadth_regime,
    NOW() AS updated_at
FROM de_equity_technical_daily t
JOIN de_instrument m ON m.id = t.instrument_id
WHERE t.close_adj IS NOT NULL
  AND t.date >= :start_date
GROUP BY t.date, m.sector
ON CONFLICT (date, sector) DO UPDATE SET
    stocks_total = EXCLUDED.stocks_total,
    stocks_above_50dma = EXCLUDED.stocks_above_50dma,
    stocks_above_200dma = EXCLUDED.stocks_above_200dma,
    stocks_above_20ema = EXCLUDED.stocks_above_20ema,
    pct_above_50dma = EXCLUDED.pct_above_50dma,
    pct_above_200dma = EXCLUDED.pct_above_200dma,
    pct_above_20ema = EXCLUDED.pct_above_20ema,
    stocks_rsi_overbought = EXCLUDED.stocks_rsi_overbought,
    stocks_rsi_oversold = EXCLUDED.stocks_rsi_oversold,
    stocks_macd_bullish = EXCLUDED.stocks_macd_bullish,
    breadth_regime = EXCLUDED.breadth_regime,
    updated_at = EXCLUDED.updated_at
"""


async def run(start_date: str | None = None):
    from datetime import date as _date

    t0 = time.time()
    engine = create_async_engine(get_async_url(), pool_size=1)

    params: dict = {}
    if start_date:
        params["start_date"] = _date.fromisoformat(start_date)
        print(f"Incremental from {start_date}", flush=True)
        sql = SECTOR_BREADTH_INCREMENTAL_SQL
    else:
        sql = SECTOR_BREADTH_SQL

    print("Sector breadth...", flush=True)
    async with engine.begin() as conn:
        await conn.execute(sa.text("SET LOCAL statement_timeout = '600s'"))
        await conn.execute(sa.text(sql), params)
    print(f"  Done ({time.time() - t0:.0f}s)", flush=True)

    async with engine.connect() as conn:
        r = await conn.execute(
            sa.text("SELECT count(*) FROM de_sector_breadth_daily")
        )
        total = r.scalar_one()
        r = await conn.execute(
            sa.text("SELECT count(DISTINCT sector) FROM de_sector_breadth_daily")
        )
        sectors = r.scalar_one()
        print(f"Sector breadth: {total:,} rows, {sectors} sectors", flush=True)

    await engine.dispose()
    print(f"Total: {time.time() - t0:.0f}s", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Sector breadth rollups")
    parser.add_argument(
        "--backfill", action="store_true",
        help="Run full historical backfill",
    )
    parser.add_argument(
        "--from", dest="from_date", type=str, default=None,
        help="Start date for backfill (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    start_date = args.from_date if args.backfill else None
    asyncio.run(run(start_date=start_date))


if __name__ == "__main__":
    main()
