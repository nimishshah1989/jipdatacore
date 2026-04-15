"""Final acceptance audit for the GAP-01..14 slice.

Runs the validation SQL for every chunk and prints PASS/FAIL. Run inside
the data-engine docker container:

    docker exec jip-data-engine-data-engine-1 python /app/scripts/gap_final_audit.py
"""

import asyncio
import os

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

CHECKS = [
    # (chunk_id, description, sql, expected_min)
    (
        "GAP-01",
        "de_mf_master.purchase_mode populated",
        "SELECT count(*) FROM de_mf_master WHERE purchase_mode IS NOT NULL",
        10_000,
    ),
    (
        "GAP-02",
        "de_mf_technical_daily funds covered",
        "SELECT count(DISTINCT mstar_id) FROM de_mf_technical_daily",
        680,
    ),
    (
        "GAP-03",
        "de_index_prices coverage (# indices with >=250 days)",
        "SELECT count(*) FROM (SELECT index_code, count(*) n FROM de_index_prices "
        "GROUP BY index_code HAVING count(*) >= 250) x",
        130,
    ),
    (
        "GAP-04",
        "de_index_technical_daily populated",
        "SELECT count(DISTINCT index_code) FROM de_index_technical_daily",
        130,
    ),
    (
        "GAP-05",
        "de_equity_technical_daily has sharpe_3y column",
        "SELECT count(*) FROM information_schema.columns WHERE "
        "table_name='de_equity_technical_daily' AND column_name='sharpe_3y'",
        1,
    ),
    (
        "GAP-06",
        "de_equity_technical_daily sharpe_3y populated rows",
        "SELECT count(*) FROM de_equity_technical_daily WHERE sharpe_3y IS NOT NULL",
        2_000_000,
    ),
    (
        "GAP-07",
        "de_equity_technical_daily downside_risk_3y populated rows",
        "SELECT count(*) FROM de_equity_technical_daily WHERE downside_risk_3y IS NOT NULL",
        2_000_000,
    ),
    (
        "GAP-07b",
        "de_etf_technical_daily sharpe_3y populated",
        "SELECT count(*) FROM de_etf_technical_daily WHERE sharpe_3y IS NOT NULL",
        100_000,
    ),
    (
        "GAP-07c",
        "de_global_technical_daily sharpe_3y populated",
        "SELECT count(*) FROM de_global_technical_daily WHERE sharpe_3y IS NOT NULL",
        100_000,
    ),
    (
        "GAP-07d",
        "de_index_technical_daily sharpe_3y populated",
        "SELECT count(*) FROM de_index_technical_daily WHERE sharpe_3y IS NOT NULL",
        100_000,
    ),
    (
        "GAP-08",
        "de_equity_fundamentals distinct instruments",
        "SELECT count(DISTINCT instrument_id) FROM de_equity_fundamentals",
        2_000,
    ),
    (
        "GAP-08b",
        "de_equity_fundamentals with full core metrics",
        "SELECT count(*) FROM de_equity_fundamentals WHERE "
        "pe_ratio IS NOT NULL AND pb_ratio IS NOT NULL "
        "AND roe_pct IS NOT NULL AND market_cap_cr IS NOT NULL",
        1_500,
    ),
    (
        "GAP-09",
        "sector mapping table entries",
        "SELECT count(*) FROM de_sector_mapping",
        25,
    ),
    (
        "GAP-10",
        "de_mf_derived_daily sharpe_3y populated",
        "SELECT count(*) FROM de_mf_derived_daily WHERE sharpe_3y IS NOT NULL",
        1_000_000,
    ),
    (
        "GAP-11",
        "de_rs_daily_summary populated",
        "SELECT count(*) FROM de_rs_daily_summary",
        5_000_000,
    ),
    (
        "GAP-12",
        "de_global_instrument_master ETF count",
        "SELECT count(*) FROM de_global_instrument_master WHERE instrument_type='etf'",
        100,
    ),
    (
        "GAP-13",
        "de_mf_nav_daily funds with NAV",
        "SELECT count(DISTINCT mstar_id) FROM de_mf_nav_daily",
        1_200,
    ),
    (
        "GAP-14",
        "de_mf_technical_daily covers eligible universe (fresh rerun)",
        "SELECT count(DISTINCT mstar_id) FROM de_mf_technical_daily",
        680,
    ),
    (
        "GAP-14b",
        "Young-fund partial rows (sma_20 ok, sharpe_1y null)",
        "SELECT count(*) FROM de_mf_technical_daily "
        "WHERE sma_20 IS NOT NULL AND sharpe_1y IS NULL",
        50_000,
    ),
]


async def main():
    e = create_async_engine(os.environ["DATABASE_URL"])
    results = []
    async with e.connect() as c:
        for chunk, desc, sql, expected in CHECKS:
            try:
                r = await c.execute(text(sql))
                val = r.scalar() or 0
                status = "PASS" if val >= expected else "FAIL"
                results.append((chunk, desc, val, expected, status))
            except Exception as exc:
                results.append((chunk, desc, "ERR", expected, f"ERR: {exc}"))

    print(f"{'chunk':<8} {'status':<8} {'actual':>12} {'min':>12}  desc")
    print("-" * 100)
    fail = 0
    for chunk, desc, val, expected, status in results:
        print(f"{chunk:<8} {status:<8} {val:>12}  {expected:>12}  {desc}")
        if status != "PASS":
            fail += 1
    print()
    print(f"RESULTS: {len(results) - fail}/{len(results)} PASS")
    if fail:
        print(f"         {fail} FAIL — see above")


if __name__ == "__main__":
    asyncio.run(main())
