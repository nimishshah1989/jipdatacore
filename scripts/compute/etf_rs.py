"""Compute RS scores for ETFs.

RS formula identical to equity RS:
    rs_Nt = (etf_cumreturn_N - bench_cumreturn_N) / bench_rolling_std_N
    composite = rs_1w*0.10 + rs_1m*0.20 + rs_3m*0.30 + rs_6m*0.25 + rs_12m*0.15

Benchmark: SPY from de_etf_ohlcv (primary) or ^SPX from de_global_prices (fallback).
Stored in de_rs_scores with entity_type = 'etf', entity_id = ticker.

Usage:
    python -m scripts.compute.etf_rs
    python -m scripts.compute.etf_rs --benchmark spx
    python -m scripts.compute.etf_rs --lookback-start 2015-01-01 --compute-start 2016-04-01
"""

from __future__ import annotations

import argparse
import asyncio
import time

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from scripts.compute.db import get_async_url

# ---------------------------------------------------------------------------
# RS SQL — adapted from EQUITY_RS_SQL in rs_scores.py
# Differences:
#   - Source: de_etf_ohlcv instead of de_equity_ohlcv
#   - Key: ticker (text) instead of instrument_id (UUID)
#   - No close_adj (ETFs use unadjusted close from stooq)
#   - entity_type = 'etf'
#   - Benchmark drawn from de_etf_ohlcv WHERE ticker = 'SPY'
# ---------------------------------------------------------------------------

ETF_RS_SPY_SQL = """
INSERT INTO de_rs_scores
    (date, entity_type, entity_id, vs_benchmark, rs_1w, rs_1m, rs_3m, rs_6m, rs_12m, rs_composite, computation_version)
WITH etf AS (
    SELECT ticker, date, close AS c,
        close / NULLIF(LAG(close, 5)  OVER w, 0) - 1 AS c5,
        close / NULLIF(LAG(close, 21) OVER w, 0) - 1 AS c21,
        close / NULLIF(LAG(close, 63) OVER w, 0) - 1 AS c63,
        close / NULLIF(LAG(close, 126) OVER w, 0) - 1 AS c126,
        close / NULLIF(LAG(close, 252) OVER w, 0) - 1 AS c252
    FROM de_etf_ohlcv
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
    FROM de_etf_ohlcv
    WHERE ticker = 'SPY' AND close IS NOT NULL
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
    FROM etf e
    JOIN bs b ON b.date = e.date
    WHERE e.ticker != 'SPY'
      AND e.date >= :compute_start
)
SELECT
    date,
    'etf',
    eid,
    'SPY',
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
    rs_1w             = EXCLUDED.rs_1w,
    rs_1m             = EXCLUDED.rs_1m,
    rs_3m             = EXCLUDED.rs_3m,
    rs_6m             = EXCLUDED.rs_6m,
    rs_12m            = EXCLUDED.rs_12m,
    rs_composite      = EXCLUDED.rs_composite,
    computation_version = EXCLUDED.computation_version,
    updated_at        = NOW()
"""

# Variant using ^SPX from de_global_prices as benchmark
ETF_RS_SPX_SQL = """
INSERT INTO de_rs_scores
    (date, entity_type, entity_id, vs_benchmark, rs_1w, rs_1m, rs_3m, rs_6m, rs_12m, rs_composite, computation_version)
WITH etf AS (
    SELECT ticker, date, close AS c,
        close / NULLIF(LAG(close, 5)  OVER w, 0) - 1 AS c5,
        close / NULLIF(LAG(close, 21) OVER w, 0) - 1 AS c21,
        close / NULLIF(LAG(close, 63) OVER w, 0) - 1 AS c63,
        close / NULLIF(LAG(close, 126) OVER w, 0) - 1 AS c126,
        close / NULLIF(LAG(close, 252) OVER w, 0) - 1 AS c252
    FROM de_etf_ohlcv
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
    FROM etf e
    JOIN bs b ON b.date = e.date
    WHERE e.date >= :compute_start
)
SELECT
    date,
    'etf',
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
    rs_1w             = EXCLUDED.rs_1w,
    rs_1m             = EXCLUDED.rs_1m,
    rs_3m             = EXCLUDED.rs_3m,
    rs_6m             = EXCLUDED.rs_6m,
    rs_12m            = EXCLUDED.rs_12m,
    rs_composite      = EXCLUDED.rs_composite,
    computation_version = EXCLUDED.computation_version,
    updated_at        = NOW()
"""


async def compute_etf_rs_spy(lookback_start: str = "2015-01-01", compute_start: str = "2016-04-01") -> None:
    """Compute ETF RS scores benchmarked against SPY (from de_etf_ohlcv)."""
    from datetime import date as date_type
    engine = create_async_engine(get_async_url(), pool_size=1)
    t0 = time.time()
    print("  ETF RS vs SPY...", flush=True)
    async with engine.begin() as conn:
        await conn.execute(sa.text("SET LOCAL work_mem = '512MB'"))
        await conn.execute(sa.text("SET LOCAL statement_timeout = '1200s'"))
        await conn.execute(
            sa.text(ETF_RS_SPY_SQL),
            {"lookback_start": date_type.fromisoformat(lookback_start),
             "compute_start": date_type.fromisoformat(compute_start)},
        )
    await engine.dispose()
    print(f"    Done in {time.time()-t0:.0f}s", flush=True)


async def compute_etf_rs_spx(lookback_start: str = "2015-01-01", compute_start: str = "2016-04-01") -> None:
    """Compute ETF RS scores benchmarked against ^SPX (from de_global_prices)."""
    from datetime import date as date_type
    engine = create_async_engine(get_async_url(), pool_size=1)
    t0 = time.time()
    print("  ETF RS vs ^SPX...", flush=True)
    async with engine.begin() as conn:
        await conn.execute(sa.text("SET LOCAL work_mem = '512MB'"))
        await conn.execute(sa.text("SET LOCAL statement_timeout = '1200s'"))
        await conn.execute(
            sa.text(ETF_RS_SPX_SQL),
            {"lookback_start": date_type.fromisoformat(lookback_start),
             "compute_start": date_type.fromisoformat(compute_start)},
        )
    await engine.dispose()
    print(f"    Done in {time.time()-t0:.0f}s", flush=True)


async def run(benchmark: str, lookback_start: str, compute_start: str) -> None:
    t0 = time.time()
    print("ETF RS computation:", flush=True)
    if benchmark in ("spy", "both"):
        await compute_etf_rs_spy(lookback_start, compute_start)
    if benchmark in ("spx", "both"):
        await compute_etf_rs_spx(lookback_start, compute_start)
    print(f"\nETF RS done in {time.time()-t0:.0f}s", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute ETF RS scores")
    parser.add_argument(
        "--benchmark",
        choices=["spy", "spx", "both"],
        default="both",
        help="Benchmark source: 'spy' (de_etf_ohlcv), 'spx' (de_global_prices), or 'both'",
    )
    parser.add_argument("--lookback-start", default="2015-01-01", help="Load prices from this date")
    parser.add_argument("--compute-start", default="2016-04-01", help="Write RS scores from this date")
    args = parser.parse_args()

    asyncio.run(run(args.benchmark, args.lookback_start, args.compute_start))


if __name__ == "__main__":
    main()
