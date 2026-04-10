"""Step 5: Compute fund risk metrics, derived RS, manager alpha, sector exposure.

All via SQL — Sharpe/Sortino/StdDev (1Y/3Y/5Y), MaxDD (1Y/3Y/5Y), Beta, Treynor,
holdings-weighted RS, NAV RS, manager alpha, sector exposure.

Usage:
    python -m scripts.compute.fund_metrics
    python -m scripts.compute.fund_metrics --start-date 2026-04-01
"""

import argparse
import time
from datetime import date

import psycopg2

from scripts.compute.db import get_sync_url


def run_sync(cur, label, sql, params=None):
    t0 = time.time()
    print(f"  {label}...", flush=True)
    cur.execute(sql, params)
    print(f"    {cur.rowcount:,} rows ({time.time()-t0:.0f}s)", flush=True)
    return cur.rowcount


def main():
    parser = argparse.ArgumentParser(description="Fund risk metrics computation")
    parser.add_argument("--start-date", type=date.fromisoformat, default=None,
                        help="Compute only for nav_date >= this date (YYYY-MM-DD). Omit for full rebuild.")
    args = parser.parse_args()
    start_date = args.start_date

    t_start = time.time()
    conn = psycopg2.connect(get_sync_url())
    conn.autocommit = True
    cur = conn.cursor()

    # Date filter for incremental runs — parameterized to prevent SQL injection
    date_clause = ""
    params: tuple = ()
    if start_date:
        date_clause = " AND d.nav_date >= %s"
        params = (start_date,)
        print(f"Incremental from {start_date}", flush=True)

    # 1. Sharpe 1Y/3Y/5Y
    print("\n=== SHARPE ===", flush=True)
    run_sync(cur, "Sharpe 1Y/3Y/5Y", """
        UPDATE de_mf_derived_daily d SET sharpe_1y=sub.s1, sharpe_3y=sub.s3, sharpe_5y=sub.s5
        FROM (
            SELECT mstar_id, nav_date,
                CASE WHEN COUNT(*) OVER w252 >= 200 AND STDDEV(dr) OVER w252 > 0 THEN
                    ROUND(((AVG(dr) OVER w252 - 0.07/252) / STDDEV(dr) OVER w252 * SQRT(252.0))::numeric, 4) END AS s1,
                CASE WHEN COUNT(*) OVER w756 >= 600 AND STDDEV(dr) OVER w756 > 0 THEN
                    ROUND(((AVG(dr) OVER w756 - 0.07/252) / STDDEV(dr) OVER w756 * SQRT(252.0))::numeric, 4) END AS s3,
                CASE WHEN COUNT(*) OVER w1260 >= 1000 AND STDDEV(dr) OVER w1260 > 0 THEN
                    ROUND(((AVG(dr) OVER w1260 - 0.07/252) / STDDEV(dr) OVER w1260 * SQRT(252.0))::numeric, 4) END AS s5
            FROM (SELECT mstar_id, nav_date, nav/NULLIF(LAG(nav) OVER(PARTITION BY mstar_id ORDER BY nav_date),0)-1 AS dr
                  FROM de_mf_nav_daily WHERE nav IS NOT NULL AND data_status='validated' AND nav_date >= '2015-01-01') rets
            WINDOW w252 AS (PARTITION BY mstar_id ORDER BY nav_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW),
                   w756 AS (PARTITION BY mstar_id ORDER BY nav_date ROWS BETWEEN 755 PRECEDING AND CURRENT ROW),
                   w1260 AS (PARTITION BY mstar_id ORDER BY nav_date ROWS BETWEEN 1259 PRECEDING AND CURRENT ROW)
        ) sub WHERE d.mstar_id=sub.mstar_id AND d.nav_date=sub.nav_date
    """ + date_clause, params or None)

    # 2. Sortino 1Y/3Y/5Y
    print("\n=== SORTINO ===", flush=True)
    run_sync(cur, "Sortino 1Y/3Y/5Y", """
        UPDATE de_mf_derived_daily d SET sortino_1y=sub.so1, sortino_3y=sub.so3, sortino_5y=sub.so5
        FROM (
            SELECT mstar_id, nav_date,
                CASE WHEN COUNT(*) OVER w252 >= 200 AND STDDEV(CASE WHEN dr<0 THEN dr END) OVER w252 > 0 THEN
                    ROUND(((AVG(dr) OVER w252 - 0.07/252) / STDDEV(CASE WHEN dr<0 THEN dr END) OVER w252 * SQRT(252.0))::numeric, 4) END AS so1,
                CASE WHEN COUNT(*) OVER w756 >= 600 AND STDDEV(CASE WHEN dr<0 THEN dr END) OVER w756 > 0 THEN
                    ROUND(((AVG(dr) OVER w756 - 0.07/252) / STDDEV(CASE WHEN dr<0 THEN dr END) OVER w756 * SQRT(252.0))::numeric, 4) END AS so3,
                CASE WHEN COUNT(*) OVER w1260 >= 1000 AND STDDEV(CASE WHEN dr<0 THEN dr END) OVER w1260 > 0 THEN
                    ROUND(((AVG(dr) OVER w1260 - 0.07/252) / STDDEV(CASE WHEN dr<0 THEN dr END) OVER w1260 * SQRT(252.0))::numeric, 4) END AS so5
            FROM (SELECT mstar_id, nav_date, nav/NULLIF(LAG(nav) OVER(PARTITION BY mstar_id ORDER BY nav_date),0)-1 AS dr
                  FROM de_mf_nav_daily WHERE nav IS NOT NULL AND data_status='validated' AND nav_date >= '2015-01-01') rets
            WINDOW w252 AS (PARTITION BY mstar_id ORDER BY nav_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW),
                   w756 AS (PARTITION BY mstar_id ORDER BY nav_date ROWS BETWEEN 755 PRECEDING AND CURRENT ROW),
                   w1260 AS (PARTITION BY mstar_id ORDER BY nav_date ROWS BETWEEN 1259 PRECEDING AND CURRENT ROW)
        ) sub WHERE d.mstar_id=sub.mstar_id AND d.nav_date=sub.nav_date
    """ + date_clause, params or None)

    # 3. StdDev 1Y/3Y/5Y
    print("\n=== STDDEV ===", flush=True)
    run_sync(cur, "StdDev 1Y/3Y/5Y", """
        UPDATE de_mf_derived_daily d SET stddev_1y=sub.sd1, stddev_3y=sub.sd3, stddev_5y=sub.sd5
        FROM (
            SELECT mstar_id, nav_date,
                CASE WHEN COUNT(*) OVER w252 >= 200 THEN ROUND((STDDEV(dr) OVER w252 * SQRT(252.0) * 100)::numeric, 4) END AS sd1,
                CASE WHEN COUNT(*) OVER w756 >= 600 THEN ROUND((STDDEV(dr) OVER w756 * SQRT(252.0) * 100)::numeric, 4) END AS sd3,
                CASE WHEN COUNT(*) OVER w1260 >= 1000 THEN ROUND((STDDEV(dr) OVER w1260 * SQRT(252.0) * 100)::numeric, 4) END AS sd5
            FROM (SELECT mstar_id, nav_date, nav/NULLIF(LAG(nav) OVER(PARTITION BY mstar_id ORDER BY nav_date),0)-1 AS dr
                  FROM de_mf_nav_daily WHERE nav IS NOT NULL AND data_status='validated' AND nav_date >= '2015-01-01') rets
            WINDOW w252 AS (PARTITION BY mstar_id ORDER BY nav_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW),
                   w756 AS (PARTITION BY mstar_id ORDER BY nav_date ROWS BETWEEN 755 PRECEDING AND CURRENT ROW),
                   w1260 AS (PARTITION BY mstar_id ORDER BY nav_date ROWS BETWEEN 1259 PRECEDING AND CURRENT ROW)
        ) sub WHERE d.mstar_id=sub.mstar_id AND d.nav_date=sub.nav_date
    """ + date_clause, params or None)

    # 4. Max Drawdown 1Y/3Y/5Y
    print("\n=== MAX DRAWDOWN ===", flush=True)
    run_sync(cur, "MaxDD 1Y/3Y/5Y", """
        UPDATE de_mf_derived_daily d SET max_drawdown_1y=sub.dd1, max_drawdown_3y=sub.dd3, max_drawdown_5y=sub.dd5
        FROM (
            WITH peaks AS (
                SELECT mstar_id, nav_date, nav,
                    MAX(nav) OVER(PARTITION BY mstar_id ORDER BY nav_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS p1,
                    MAX(nav) OVER(PARTITION BY mstar_id ORDER BY nav_date ROWS BETWEEN 755 PRECEDING AND CURRENT ROW) AS p3,
                    MAX(nav) OVER(PARTITION BY mstar_id ORDER BY nav_date ROWS BETWEEN 1259 PRECEDING AND CURRENT ROW) AS p5
                FROM de_mf_nav_daily WHERE nav IS NOT NULL AND nav_date >= '2016-04-01'
            )
            SELECT mstar_id, nav_date,
                ROUND((MIN((nav-p1)/NULLIF(p1,0)) OVER(PARTITION BY mstar_id ORDER BY nav_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)*100)::numeric,4) AS dd1,
                ROUND((MIN((nav-p3)/NULLIF(p3,0)) OVER(PARTITION BY mstar_id ORDER BY nav_date ROWS BETWEEN 755 PRECEDING AND CURRENT ROW)*100)::numeric,4) AS dd3,
                ROUND((MIN((nav-p5)/NULLIF(p5,0)) OVER(PARTITION BY mstar_id ORDER BY nav_date ROWS BETWEEN 1259 PRECEDING AND CURRENT ROW)*100)::numeric,4) AS dd5
            FROM peaks
        ) sub WHERE d.mstar_id=sub.mstar_id AND d.nav_date=sub.nav_date
    """ + date_clause, params or None)

    # 5. Treynor — also check volatility_1y IS NOT NULL
    print("\n=== TREYNOR ===", flush=True)
    treynor_clause = " AND nav_date >= %s" if start_date else ""
    treynor_params = (start_date,) if start_date else None
    run_sync(cur, "Treynor", """
        UPDATE de_mf_derived_daily SET
            treynor_ratio = CASE WHEN beta_vs_nifty IS NOT NULL AND beta_vs_nifty::float != 0
                AND sharpe_1y IS NOT NULL AND volatility_1y IS NOT NULL THEN
                ROUND((sharpe_1y::float * volatility_1y::float / 100 / beta_vs_nifty::float)::numeric, 4) END
        WHERE nav_date >= '2016-04-01' AND beta_vs_nifty IS NOT NULL
    """ + treynor_clause, treynor_params)

    # 6. Holdings-weighted RS
    print("\n=== DERIVED RS ===", flush=True)
    rs_clause = " AND rs.date >= %s" if start_date else ""
    # Build combined params: rs_date_filter param + date_clause param
    rs_params = tuple(p for p in [start_date if start_date else None, start_date if start_date else None] if p is not None) or None
    run_sync(cur, "Holdings-weighted RS", """
        UPDATE de_mf_derived_daily d SET derived_rs_composite=sub.drs, coverage_pct=sub.cov
        FROM (
            SELECT h.mstar_id, rs.date,
                ROUND((SUM(h.weight_pct::float * rs.rs_composite::float)/NULLIF(SUM(h.weight_pct::float),0))::numeric,4) AS drs,
                ROUND((SUM(CASE WHEN rs.rs_composite IS NOT NULL THEN h.weight_pct::float ELSE 0 END)/NULLIF(SUM(h.weight_pct::float),0)*100)::numeric,2) AS cov
            FROM de_mf_holdings h
            JOIN de_rs_scores rs ON rs.entity_id=h.instrument_id::text AND rs.entity_type='equity' AND rs.vs_benchmark='NIFTY 50'
            WHERE h.is_mapped=TRUE AND h.weight_pct>0
    """ + rs_clause + """
            GROUP BY h.mstar_id, rs.date
        ) sub WHERE d.mstar_id=sub.mstar_id AND d.nav_date=sub.date
    """ + date_clause, rs_params)

    # 7. NAV RS + Manager Alpha
    print("\n=== NAV RS + ALPHA ===", flush=True)
    run_sync(cur, "NAV RS + Alpha", """
        UPDATE de_mf_derived_daily d SET
            nav_rs_composite=rs.rs_composite::float,
            manager_alpha=ROUND((rs.rs_composite::float - COALESCE(d.derived_rs_composite::float,0))::numeric,4)
        FROM de_rs_scores rs
        WHERE rs.entity_id=d.mstar_id AND rs.entity_type='mf' AND rs.vs_benchmark='NIFTY 50' AND rs.date=d.nav_date
    """ + date_clause, params or None)

    # 8. Fund sector exposure — use ON CONFLICT upsert (no TRUNCATE)
    print("\n=== SECTOR EXPOSURE ===", flush=True)
    run_sync(cur, "Sector exposure", """
        INSERT INTO de_mf_sector_exposure (mstar_id, sector, weight_pct, stock_count, as_of_date)
        SELECT h.mstar_id, i.sector, ROUND(SUM(h.weight_pct::float)::numeric,4),
            COUNT(DISTINCT h.instrument_id), MAX(h.as_of_date)
        FROM de_mf_holdings h JOIN de_instrument i ON i.id=h.instrument_id
        WHERE h.is_mapped=TRUE AND i.sector IS NOT NULL AND h.weight_pct>0
        GROUP BY h.mstar_id, i.sector
        ON CONFLICT (mstar_id, sector) DO UPDATE SET
            weight_pct=EXCLUDED.weight_pct,
            stock_count=EXCLUDED.stock_count,
            as_of_date=EXCLUDED.as_of_date
    """)

    cur.close()
    conn.close()
    print(f"\n=== ALL FUND METRICS DONE in {time.time()-t_start:.0f}s ({(time.time()-t_start)/60:.1f} min) ===", flush=True)


if __name__ == "__main__":
    main()
