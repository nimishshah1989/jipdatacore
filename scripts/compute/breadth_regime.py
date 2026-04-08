"""Step 4: Compute breadth indicators and market regime via SQL.

Breadth: advance/decline/unchanged, A/D ratio, % above DMA
Regime: BULL/BEAR/SIDEWAYS from breadth score

Usage:
    python -m scripts.compute.breadth_regime
"""

import asyncio
import time

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from scripts.compute.db import get_async_url

BREADTH_SQL = """
INSERT INTO de_breadth_daily
    (date, advance, decline, unchanged, total_stocks, ad_ratio,
     pct_above_200dma, pct_above_50dma, new_52w_highs, new_52w_lows)
WITH pc AS (
    SELECT date, COALESCE(close_adj,close) AS c,
        LAG(COALESCE(close_adj,close)) OVER(PARTITION BY instrument_id ORDER BY date) AS pc
    FROM de_equity_ohlcv WHERE COALESCE(close_adj,close) IS NOT NULL
),
dc AS (
    SELECT date, COUNT(*) AS total,
        SUM(CASE WHEN c>pc THEN 1 ELSE 0 END) AS adv,
        SUM(CASE WHEN c<pc THEN 1 ELSE 0 END) AS dec,
        SUM(CASE WHEN c=pc THEN 1 ELSE 0 END) AS unch
    FROM pc WHERE pc IS NOT NULL GROUP BY date
),
dma AS (
    SELECT date,
        ROUND(SUM(CASE WHEN above_200dma THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS p200,
        ROUND(SUM(CASE WHEN above_50dma THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(*),0)*100,2) AS p50
    FROM de_equity_technical_daily WHERE close_adj IS NOT NULL GROUP BY date
)
SELECT d.date, d.adv, d.dec, d.unch, d.total,
    ROUND(d.adv::numeric/NULLIF(d.dec,0),4), dm.p200, dm.p50, 0, 0
FROM dc d LEFT JOIN dma dm ON dm.date=d.date
ON CONFLICT (date) DO UPDATE SET
    advance=EXCLUDED.advance, decline=EXCLUDED.decline, unchanged=EXCLUDED.unchanged,
    total_stocks=EXCLUDED.total_stocks, ad_ratio=EXCLUDED.ad_ratio,
    pct_above_200dma=EXCLUDED.pct_above_200dma, pct_above_50dma=EXCLUDED.pct_above_50dma,
    updated_at=NOW()
"""

REGIME_SQL = """
INSERT INTO de_market_regime
    (computed_at, date, regime, confidence, breadth_score, momentum_score,
     volume_score, global_score, fii_score, indicator_detail, computation_version)
WITH breadth AS (
    SELECT date,
        (advance::float / NULLIF(total_stocks, 0) * 100 * 0.5 +
        LEAST(100, GREATEST(0, 50 + (COALESCE(CAST(ad_ratio AS FLOAT), 1) - 1) * 25)) * 0.5) AS bs
    FROM de_breadth_daily WHERE total_stocks > 0
),
-- Momentum: median RS composite of NIFTY 50 constituents → 0-100
momentum AS (
    SELECT r.date,
        LEAST(100, GREATEST(0,
            50 + COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r.rs_composite), 0) * 5
        )) AS ms
    FROM de_rs_scores r
    JOIN de_index_constituents ic ON ic.instrument_id::text = r.entity_id
        AND ic.index_code = 'NIFTY 50'
    WHERE r.entity_type = 'equity' AND r.vs_benchmark = 'NIFTY 50'
    GROUP BY r.date
),
-- Volume: average relative volume of top stocks → 0-100
volume AS (
    SELECT date,
        LEAST(100, GREATEST(0,
            AVG(CASE WHEN relative_volume IS NOT NULL THEN relative_volume ELSE NULL END) * 50
        )) AS vs
    FROM de_equity_technical_daily
    WHERE close_adj IS NOT NULL
    GROUP BY date
),
-- Global: 5-day avg return of SPY → 0-100
global_sig AS (
    SELECT date,
        LEAST(100, GREATEST(0,
            50 + COALESCE(
                AVG(close / NULLIF(LAG(close, 1) OVER (ORDER BY date), 0) - 1)
                OVER (ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
            , 0) * 1000
        )) AS gs
    FROM de_global_prices
    WHERE ticker = '^SPX' AND close IS NOT NULL
),
-- FII: use global_score as proxy (FII flows table is empty)
combined AS (
    SELECT b.date, b.bs,
        COALESCE(m.ms, 50) AS ms,
        COALESCE(v.vs, 50) AS vs,
        COALESCE(g.gs, 50) AS gs,
        COALESCE(g.gs, 50) AS fs  -- FII proxy = global
    FROM breadth b
    LEFT JOIN momentum m ON m.date = b.date
    LEFT JOIN volume v ON v.date = b.date
    LEFT JOIN global_sig g ON g.date = b.date
)
SELECT
    (date::text || ' 23:30:00')::timestamptz, date,
    CASE
        WHEN (bs*0.30 + ms*0.25 + vs*0.10 + gs*0.20 + fs*0.15) >= 60 THEN 'BULL'
        WHEN (bs*0.30 + ms*0.25 + vs*0.10 + gs*0.20 + fs*0.15) <= 40 THEN 'BEAR'
        ELSE 'SIDEWAYS'
    END,
    ROUND((bs*0.30 + ms*0.25 + vs*0.10 + gs*0.20 + fs*0.15)::numeric, 2),
    ROUND(bs::numeric, 2),
    ROUND(ms::numeric, 2),
    ROUND(vs::numeric, 2),
    ROUND(gs::numeric, 2),
    ROUND(fs::numeric, 2),
    '{}'::jsonb, 2
FROM combined
ON CONFLICT (computed_at) DO UPDATE SET
    regime=EXCLUDED.regime, confidence=EXCLUDED.confidence,
    breadth_score=EXCLUDED.breadth_score,
    momentum_score=EXCLUDED.momentum_score,
    volume_score=EXCLUDED.volume_score,
    global_score=EXCLUDED.global_score,
    fii_score=EXCLUDED.fii_score,
    computation_version=EXCLUDED.computation_version
"""


async def run():
    t0 = time.time()
    engine = create_async_engine(get_async_url(), pool_size=1)

    print("Breadth...", flush=True)
    async with engine.begin() as conn:
        await conn.execute(sa.text("SET LOCAL statement_timeout = '600s'"))
        await conn.execute(sa.text(BREADTH_SQL))
    print(f"  Done ({time.time()-t0:.0f}s)", flush=True)

    print("Regime...", flush=True)
    t1 = time.time()
    # Truncate first (separate statement)
    async with engine.begin() as conn:
        await conn.execute(sa.text("TRUNCATE de_market_regime"))
    async with engine.begin() as conn:
        await conn.execute(sa.text(REGIME_SQL))
    print(f"  Done ({time.time()-t1:.0f}s)", flush=True)

    # Verify
    async with engine.connect() as conn:
        r = await conn.execute(sa.text("SELECT COUNT(*) FROM de_breadth_daily"))
        print(f"Breadth: {r.scalar_one():,}")
        r = await conn.execute(sa.text("SELECT COUNT(*) FROM de_market_regime"))
        print(f"Regime: {r.scalar_one():,}")
    await engine.dispose()

    print(f"Total: {time.time()-t0:.0f}s", flush=True)


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
