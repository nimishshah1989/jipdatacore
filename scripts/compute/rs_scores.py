"""Step 3: Compute RS scores for equity, mutual funds, and sectors via SQL.

RS formula: rs_Nt = (entity_cumreturn_N - bench_cumreturn_N) / bench_rolling_std_N
Composite: rs_1w*0.10 + rs_1m*0.20 + rs_3m*0.30 + rs_6m*0.25 + rs_12m*0.15

Usage:
    python -m scripts.compute.rs_scores
    python -m scripts.compute.rs_scores --entity-type equity --benchmark "NIFTY 50"
    python -m scripts.compute.rs_scores --entity-type mf
"""

import argparse
import asyncio
import time

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from scripts.compute.db import get_async_url

BENCHMARKS = ["NIFTY 50", "NIFTY 500", "NIFTY MIDCAP 100"]

EQUITY_RS_SQL = """
INSERT INTO de_rs_scores (date, entity_type, entity_id, vs_benchmark, rs_1w, rs_1m, rs_3m, rs_6m, rs_12m, rs_composite, computation_version)
WITH eq AS (
    SELECT instrument_id, date, COALESCE(close_adj,close) AS c,
        COALESCE(close_adj,close)/NULLIF(LAG(COALESCE(close_adj,close),5) OVER w,0)-1 AS c5,
        COALESCE(close_adj,close)/NULLIF(LAG(COALESCE(close_adj,close),21) OVER w,0)-1 AS c21,
        COALESCE(close_adj,close)/NULLIF(LAG(COALESCE(close_adj,close),63) OVER w,0)-1 AS c63,
        COALESCE(close_adj,close)/NULLIF(LAG(COALESCE(close_adj,close),126) OVER w,0)-1 AS c126,
        COALESCE(close_adj,close)/NULLIF(LAG(COALESCE(close_adj,close),252) OVER w,0)-1 AS c252
    FROM de_equity_ohlcv WHERE COALESCE(close_adj,close) IS NOT NULL AND date >= :lookback_start
    WINDOW w AS (PARTITION BY instrument_id ORDER BY date)
),
bd AS (
    SELECT date, close/NULLIF(LAG(close,1) OVER(ORDER BY date),0)-1 AS dr,
        close/NULLIF(LAG(close,5) OVER(ORDER BY date),0)-1 AS c5,
        close/NULLIF(LAG(close,21) OVER(ORDER BY date),0)-1 AS c21,
        close/NULLIF(LAG(close,63) OVER(ORDER BY date),0)-1 AS c63,
        close/NULLIF(LAG(close,126) OVER(ORDER BY date),0)-1 AS c126,
        close/NULLIF(LAG(close,252) OVER(ORDER BY date),0)-1 AS c252
    FROM de_index_prices WHERE index_code=:bm AND close IS NOT NULL
),
bs AS (
    SELECT date, c5,c21,c63,c126,c252,
        STDDEV(dr) OVER(ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s5,
        STDDEV(dr) OVER(ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS s21,
        STDDEV(dr) OVER(ORDER BY date ROWS BETWEEN 62 PRECEDING AND CURRENT ROW) AS s63,
        STDDEV(dr) OVER(ORDER BY date ROWS BETWEEN 125 PRECEDING AND CURRENT ROW) AS s126,
        STDDEV(dr) OVER(ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS s252
    FROM bd
),
rs AS (
    SELECT e.date, e.instrument_id::text AS eid,
        CASE WHEN b.s5>0 THEN ROUND(((e.c5-b.c5)/b.s5)::numeric,4) END AS r1w,
        CASE WHEN b.s21>0 THEN ROUND(((e.c21-b.c21)/b.s21)::numeric,4) END AS r1m,
        CASE WHEN b.s63>0 THEN ROUND(((e.c63-b.c63)/b.s63)::numeric,4) END AS r3m,
        CASE WHEN b.s126>0 THEN ROUND(((e.c126-b.c126)/b.s126)::numeric,4) END AS r6m,
        CASE WHEN b.s252>0 THEN ROUND(((e.c252-b.c252)/b.s252)::numeric,4) END AS r12m
    FROM eq e JOIN bs b ON b.date=e.date WHERE e.date >= :compute_start
)
SELECT date,'equity',eid,:bm,r1w,r1m,r3m,r6m,r12m,
    ROUND((COALESCE(r1w*0.10,0)+COALESCE(r1m*0.20,0)+COALESCE(r3m*0.30,0)+COALESCE(r6m*0.25,0)+COALESCE(r12m*0.15,0))
    /NULLIF((CASE WHEN r1w IS NOT NULL THEN 0.10 ELSE 0 END+CASE WHEN r1m IS NOT NULL THEN 0.20 ELSE 0 END+CASE WHEN r3m IS NOT NULL THEN 0.30 ELSE 0 END+CASE WHEN r6m IS NOT NULL THEN 0.25 ELSE 0 END+CASE WHEN r12m IS NOT NULL THEN 0.15 ELSE 0 END),0),4),1
FROM rs WHERE r1w IS NOT NULL OR r1m IS NOT NULL
ON CONFLICT (date, entity_type, entity_id, vs_benchmark) DO UPDATE SET
    rs_1w=EXCLUDED.rs_1w,rs_1m=EXCLUDED.rs_1m,rs_3m=EXCLUDED.rs_3m,rs_6m=EXCLUDED.rs_6m,rs_12m=EXCLUDED.rs_12m,
    rs_composite=EXCLUDED.rs_composite,computation_version=EXCLUDED.computation_version,updated_at=NOW()
"""

# MF RS uses same formula but with NAV instead of close
MF_RS_SQL = EQUITY_RS_SQL.replace("de_equity_ohlcv", "de_mf_nav_daily").replace(
    "COALESCE(close_adj,close)", "nav"
).replace("instrument_id", "mstar_id").replace("'equity'", "'mf'").replace(
    "de_mf_nav_daily WHERE", "de_mf_nav_daily WHERE"
).replace(" date,", " nav_date AS date,").replace(" date ", " nav_date ").replace(
    " date>=", " nav_date>="
)
# Simpler approach: just use a wrapper CTE
MF_RS_SQL = """
INSERT INTO de_rs_scores (date, entity_type, entity_id, vs_benchmark, rs_1w, rs_1m, rs_3m, rs_6m, rs_12m, rs_composite, computation_version)
WITH mfp AS (
    SELECT mstar_id, nav_date AS date, nav AS c,
        nav/NULLIF(LAG(nav,5) OVER w,0)-1 AS c5,
        nav/NULLIF(LAG(nav,21) OVER w,0)-1 AS c21,
        nav/NULLIF(LAG(nav,63) OVER w,0)-1 AS c63,
        nav/NULLIF(LAG(nav,126) OVER w,0)-1 AS c126,
        nav/NULLIF(LAG(nav,252) OVER w,0)-1 AS c252
    FROM de_mf_nav_daily WHERE nav IS NOT NULL AND nav_date >= :lookback_start
    WINDOW w AS (PARTITION BY mstar_id ORDER BY nav_date)
),
bd AS (
    SELECT date, close/NULLIF(LAG(close,1) OVER(ORDER BY date),0)-1 AS dr,
        close/NULLIF(LAG(close,5) OVER(ORDER BY date),0)-1 AS c5,
        close/NULLIF(LAG(close,21) OVER(ORDER BY date),0)-1 AS c21,
        close/NULLIF(LAG(close,63) OVER(ORDER BY date),0)-1 AS c63,
        close/NULLIF(LAG(close,126) OVER(ORDER BY date),0)-1 AS c126,
        close/NULLIF(LAG(close,252) OVER(ORDER BY date),0)-1 AS c252
    FROM de_index_prices WHERE index_code=:bm AND close IS NOT NULL
),
bs AS (
    SELECT date, c5,c21,c63,c126,c252,
        STDDEV(dr) OVER(ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s5,
        STDDEV(dr) OVER(ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS s21,
        STDDEV(dr) OVER(ORDER BY date ROWS BETWEEN 62 PRECEDING AND CURRENT ROW) AS s63,
        STDDEV(dr) OVER(ORDER BY date ROWS BETWEEN 125 PRECEDING AND CURRENT ROW) AS s126,
        STDDEV(dr) OVER(ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS s252
    FROM bd
),
rs AS (
    SELECT e.date, e.mstar_id AS eid,
        CASE WHEN b.s5>0 THEN ROUND(((e.c5-b.c5)/b.s5)::numeric,4) END AS r1w,
        CASE WHEN b.s21>0 THEN ROUND(((e.c21-b.c21)/b.s21)::numeric,4) END AS r1m,
        CASE WHEN b.s63>0 THEN ROUND(((e.c63-b.c63)/b.s63)::numeric,4) END AS r3m,
        CASE WHEN b.s126>0 THEN ROUND(((e.c126-b.c126)/b.s126)::numeric,4) END AS r6m,
        CASE WHEN b.s252>0 THEN ROUND(((e.c252-b.c252)/b.s252)::numeric,4) END AS r12m
    FROM mfp e JOIN bs b ON b.date=e.date WHERE e.date >= :compute_start
)
SELECT date,'mf',eid,:bm,r1w,r1m,r3m,r6m,r12m,
    ROUND((COALESCE(r1w*0.10,0)+COALESCE(r1m*0.20,0)+COALESCE(r3m*0.30,0)+COALESCE(r6m*0.25,0)+COALESCE(r12m*0.15,0))
    /NULLIF((CASE WHEN r1w IS NOT NULL THEN 0.10 ELSE 0 END+CASE WHEN r1m IS NOT NULL THEN 0.20 ELSE 0 END+CASE WHEN r3m IS NOT NULL THEN 0.30 ELSE 0 END+CASE WHEN r6m IS NOT NULL THEN 0.25 ELSE 0 END+CASE WHEN r12m IS NOT NULL THEN 0.15 ELSE 0 END),0),4),1
FROM rs WHERE r1w IS NOT NULL OR r1m IS NOT NULL
ON CONFLICT (date, entity_type, entity_id, vs_benchmark) DO UPDATE SET
    rs_1w=EXCLUDED.rs_1w,rs_1m=EXCLUDED.rs_1m,rs_3m=EXCLUDED.rs_3m,rs_6m=EXCLUDED.rs_6m,rs_12m=EXCLUDED.rs_12m,
    rs_composite=EXCLUDED.rs_composite,computation_version=EXCLUDED.computation_version,updated_at=NOW()
"""

SECTOR_RS_SQL = """
INSERT INTO de_rs_scores (date, entity_type, entity_id, vs_benchmark, rs_composite, computation_version)
SELECT rs.date, 'sector', i.sector, rs.vs_benchmark,
    ROUND(AVG(rs.rs_composite::float)::numeric, 4), 1
FROM de_rs_scores rs
JOIN de_instrument i ON i.id::text = rs.entity_id
WHERE rs.entity_type = 'equity' AND i.sector IS NOT NULL AND rs.rs_composite IS NOT NULL
AND rs.date = (SELECT MAX(date) FROM de_rs_scores WHERE entity_type = 'equity')
GROUP BY rs.date, i.sector, rs.vs_benchmark
ON CONFLICT (date, entity_type, entity_id, vs_benchmark) DO UPDATE SET
    rs_composite = EXCLUDED.rs_composite, updated_at = NOW()
"""


async def compute_equity_rs(benchmarks: list[str] = None, lookback_start: str = "2015-01-01", compute_start: str = "2016-04-01"):
    from datetime import date as date_type
    benchmarks = benchmarks or BENCHMARKS
    engine = create_async_engine(get_async_url(), pool_size=1)
    for bm in benchmarks:
        t0 = time.time()
        print(f"  Equity RS vs {bm}...", flush=True)
        async with engine.begin() as conn:
            await conn.execute(sa.text("SET LOCAL work_mem = '512MB'"))
            await conn.execute(sa.text("SET LOCAL statement_timeout = '1200s'"))
            await conn.execute(
                sa.text(EQUITY_RS_SQL),
                {"bm": bm,
                 "lookback_start": date_type.fromisoformat(lookback_start),
                 "compute_start": date_type.fromisoformat(compute_start)},
            )
        print(f"    Done in {time.time()-t0:.0f}s", flush=True)
    await engine.dispose()


async def compute_mf_rs(benchmarks: list[str] = None, lookback_start: str = "2015-01-01", compute_start: str = "2016-04-01"):
    from datetime import date as date_type
    benchmarks = benchmarks or BENCHMARKS
    engine = create_async_engine(get_async_url(), pool_size=1)
    for bm in benchmarks:
        t0 = time.time()
        print(f"  MF RS vs {bm}...", flush=True)
        async with engine.begin() as conn:
            await conn.execute(sa.text("SET LOCAL work_mem = '512MB'"))
            await conn.execute(sa.text("SET LOCAL statement_timeout = '1200s'"))
            await conn.execute(
                sa.text(MF_RS_SQL),
                {"bm": bm,
                 "lookback_start": date_type.fromisoformat(lookback_start),
                 "compute_start": date_type.fromisoformat(compute_start)},
            )
        print(f"    Done in {time.time()-t0:.0f}s", flush=True)
    await engine.dispose()


async def compute_sector_rs():
    engine = create_async_engine(get_async_url(), pool_size=1)
    t0 = time.time()
    print("  Sector RS...", flush=True)
    async with engine.begin() as conn:
        await conn.execute(sa.text(SECTOR_RS_SQL))
    await engine.dispose()
    print(f"    Done in {time.time()-t0:.0f}s", flush=True)


async def run(entity_type: str = "all", start_date: str | None = None):
    from datetime import date as date_type, timedelta

    t0 = time.time()

    # Incremental mode: only compute from start_date, with 18-month lookback for window functions
    if start_date:
        compute_start = start_date
        lookback_start = (date_type.fromisoformat(start_date) - timedelta(days=550)).isoformat()
        print(f"Incremental RS from {compute_start} (lookback: {lookback_start})", flush=True)
    else:
        compute_start = "2016-04-01"
        lookback_start = "2015-01-01"

    if entity_type in ("all", "equity"):
        print("Equity RS:", flush=True)
        await compute_equity_rs(lookback_start=lookback_start, compute_start=compute_start)
    if entity_type in ("all", "mf"):
        print("MF RS:", flush=True)
        await compute_mf_rs(lookback_start=lookback_start, compute_start=compute_start)
    if entity_type in ("all", "sector"):
        print("Sector RS:", flush=True)
        await compute_sector_rs()
    print(f"\nAll RS done in {time.time()-t0:.0f}s", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity-type", choices=["all", "equity", "mf", "sector"], default="all")
    parser.add_argument(
        "--start-date", default=None,
        help="Compute from this date (incremental). Omit for full rebuild.",
    )
    args = parser.parse_args()
    asyncio.run(run(args.entity_type, args.start_date))


if __name__ == "__main__":
    main()
