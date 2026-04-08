"""Compute RS scores for global instruments (indices, FX, commodities, etc.).

RS formula identical to equity/ETF RS:
    rs_Nt = (global_cumreturn_N - bench_cumreturn_N) / bench_rolling_std_N
    composite = rs_1w*0.10 + rs_1m*0.20 + rs_3m*0.30 + rs_6m*0.25 + rs_12m*0.15

Source table: de_global_prices (ticker column, close column)
Benchmark: ^SPX from de_global_prices
Stored in de_rs_scores with entity_type = 'global', entity_id = ticker.

Usage:
    python -m scripts.compute.global_rs
    python -m scripts.compute.global_rs --lookback-start 2015-01-01 --compute-start 2016-04-01
"""

from __future__ import annotations

import argparse
import asyncio
import time

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from scripts.compute.db import get_async_url

# ---------------------------------------------------------------------------
# RS SQL — adapted from ETF_RS_SPX_SQL in etf_rs.py
# Differences from ETF variant:
#   - Source: de_global_prices instead of de_etf_ohlcv
#   - entity_type = 'global'
#   - Benchmark is ^SPX from de_global_prices (same table as entity data)
#   - ^SPX is excluded from entity rows (WHERE ticker != '^SPX')
# ---------------------------------------------------------------------------

GLOBAL_RS_SQL = """
INSERT INTO de_rs_scores
    (date, entity_type, entity_id, vs_benchmark, rs_1w, rs_1m, rs_3m, rs_6m, rs_12m, rs_composite, computation_version)
WITH global_inst AS (
    SELECT ticker, date, close AS c,
        close / NULLIF(LAG(close, 5)   OVER w, 0) - 1 AS c5,
        close / NULLIF(LAG(close, 21)  OVER w, 0) - 1 AS c21,
        close / NULLIF(LAG(close, 63)  OVER w, 0) - 1 AS c63,
        close / NULLIF(LAG(close, 126) OVER w, 0) - 1 AS c126,
        close / NULLIF(LAG(close, 252) OVER w, 0) - 1 AS c252
    FROM de_global_prices
    WHERE close IS NOT NULL AND date >= :lookback_start
    WINDOW w AS (PARTITION BY ticker ORDER BY date)
),
bd AS (
    SELECT date,
        close / NULLIF(LAG(close, 1)   OVER (ORDER BY date), 0) - 1 AS dr,
        close / NULLIF(LAG(close, 5)   OVER (ORDER BY date), 0) - 1 AS c5,
        close / NULLIF(LAG(close, 21)  OVER (ORDER BY date), 0) - 1 AS c21,
        close / NULLIF(LAG(close, 63)  OVER (ORDER BY date), 0) - 1 AS c63,
        close / NULLIF(LAG(close, 126) OVER (ORDER BY date), 0) - 1 AS c126,
        close / NULLIF(LAG(close, 252) OVER (ORDER BY date), 0) - 1 AS c252
    FROM de_global_prices
    WHERE ticker = '^SPX' AND close IS NOT NULL
),
bs AS (
    SELECT date, c5, c21, c63, c126, c252,
        STDDEV(dr) OVER (ORDER BY date ROWS BETWEEN 4   PRECEDING AND CURRENT ROW) AS s5,
        STDDEV(dr) OVER (ORDER BY date ROWS BETWEEN 20  PRECEDING AND CURRENT ROW) AS s21,
        STDDEV(dr) OVER (ORDER BY date ROWS BETWEEN 62  PRECEDING AND CURRENT ROW) AS s63,
        STDDEV(dr) OVER (ORDER BY date ROWS BETWEEN 125 PRECEDING AND CURRENT ROW) AS s126,
        STDDEV(dr) OVER (ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS s252
    FROM bd
),
rs AS (
    SELECT e.date, e.ticker AS eid,
        CASE WHEN b.s5   > 0 THEN ROUND(((e.c5   - b.c5)   / b.s5)::numeric,   4) END AS r1w,
        CASE WHEN b.s21  > 0 THEN ROUND(((e.c21  - b.c21)  / b.s21)::numeric,  4) END AS r1m,
        CASE WHEN b.s63  > 0 THEN ROUND(((e.c63  - b.c63)  / b.s63)::numeric,  4) END AS r3m,
        CASE WHEN b.s126 > 0 THEN ROUND(((e.c126 - b.c126) / b.s126)::numeric, 4) END AS r6m,
        CASE WHEN b.s252 > 0 THEN ROUND(((e.c252 - b.c252) / b.s252)::numeric, 4) END AS r12m
    FROM global_inst e
    JOIN bs b ON b.date = e.date
    WHERE e.ticker != '^SPX'
      AND e.date >= :compute_start
)
SELECT
    date,
    'global',
    eid,
    '^SPX',
    r1w, r1m, r3m, r6m, r12m,
    ROUND(
        (COALESCE(r1w * 0.10, 0) + COALESCE(r1m * 0.20, 0) + COALESCE(r3m * 0.30, 0)
         + COALESCE(r6m * 0.25, 0) + COALESCE(r12m * 0.15, 0))
        / NULLIF(
            (CASE WHEN r1w  IS NOT NULL THEN 0.10 ELSE 0 END
           + CASE WHEN r1m  IS NOT NULL THEN 0.20 ELSE 0 END
           + CASE WHEN r3m  IS NOT NULL THEN 0.30 ELSE 0 END
           + CASE WHEN r6m  IS NOT NULL THEN 0.25 ELSE 0 END
           + CASE WHEN r12m IS NOT NULL THEN 0.15 ELSE 0 END),
            0
        ),
        4
    ),
    1
FROM rs
WHERE r1w IS NOT NULL OR r1m IS NOT NULL
ON CONFLICT (date, entity_type, entity_id, vs_benchmark) DO UPDATE SET
    rs_1w               = EXCLUDED.rs_1w,
    rs_1m               = EXCLUDED.rs_1m,
    rs_3m               = EXCLUDED.rs_3m,
    rs_6m               = EXCLUDED.rs_6m,
    rs_12m              = EXCLUDED.rs_12m,
    rs_composite        = EXCLUDED.rs_composite,
    computation_version = EXCLUDED.computation_version,
    updated_at          = NOW()
"""


async def compute_global_rs(lookback_start: str = "2015-01-01", compute_start: str = "2016-04-01") -> None:
    """Compute RS scores for global instruments benchmarked against ^SPX."""
    from datetime import date as date_type

    engine = create_async_engine(get_async_url(), pool_size=1)
    t0 = time.time()
    print("  Global RS vs ^SPX...", flush=True)
    async with engine.begin() as conn:
        await conn.execute(sa.text("SET LOCAL work_mem = '512MB'"))
        await conn.execute(sa.text("SET LOCAL statement_timeout = '1200s'"))
        await conn.execute(
            sa.text(GLOBAL_RS_SQL),
            {
                "lookback_start": date_type.fromisoformat(lookback_start),
                "compute_start": date_type.fromisoformat(compute_start),
            },
        )
    await engine.dispose()
    print(f"    Done in {time.time() - t0:.0f}s", flush=True)


async def run(lookback_start: str, compute_start: str) -> None:
    t0 = time.time()
    print("Global RS computation:", flush=True)
    await compute_global_rs(lookback_start, compute_start)
    print(f"\nGlobal RS done in {time.time() - t0:.0f}s", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute RS scores for global instruments vs ^SPX")
    parser.add_argument(
        "--lookback-start",
        default="2015-01-01",
        help="Load prices from this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--compute-start",
        default="2016-04-01",
        help="Write RS scores from this date (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    asyncio.run(run(args.lookback_start, args.compute_start))


if __name__ == "__main__":
    main()
