"""GAP-14 — Deep MF NAV backfill from mfpulse_reimagined for eligible funds
with insufficient history in JIP.

Target universe: equity/regular/growth funds in de_mf_master that have
<252 NAV rows in de_mf_nav_daily. Pulls full history from mfpulse's
nav_daily table for each mstar_id and UPSERTs into de_mf_nav_daily.

Runs from the host (not inside the data-engine container) so it can
docker-exec into both the data-engine container (JIP DB) and the
mf-pulse container (mfpulse DB) in one script.

Usage (from the EC2 host shell):
    python3 /home/ubuntu/jip-data-engine/scripts/backfill_mf_nav_deep.py
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from typing import List, Tuple


def _run_in(container: str, py_expr: str) -> str:
    """Execute a python one-liner inside a docker container and return stdout."""
    result = subprocess.run(
        ["docker", "exec", container, "python", "-c", py_expr],
        capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


def list_missing_mstar_ids() -> List[str]:
    """Ask JIP DB which eligible funds have <252 NAV rows."""
    py = r"""
import os, asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
async def m():
    e = create_async_engine(os.environ['DATABASE_URL'])
    async with e.connect() as c:
        r = await c.execute(text('''
            WITH elig AS (
              SELECT mstar_id, inception_date FROM de_mf_master
              WHERE purchase_mode=1 AND broad_category='Equity' AND is_active
                AND NOT is_etf AND NOT is_index_fund
                AND fund_name !~* '\y(IDCW|Dividend|Segregated|Direct)\y'
            ),
            ns AS (SELECT mstar_id, count(*) n FROM de_mf_nav_daily GROUP BY mstar_id)
            SELECT e.mstar_id FROM elig e LEFT JOIN ns USING (mstar_id)
            WHERE COALESCE(ns.n,0) < 252
              AND (e.inception_date IS NULL OR e.inception_date <= CURRENT_DATE - INTERVAL '365 days')
        '''))
        for row in r.fetchall():
            print(row[0])
asyncio.run(m())
"""
    out = _run_in("jip-data-engine-data-engine-1", py)
    return [line.strip() for line in out.splitlines() if line.strip()]


def fetch_nav_from_mfpulse(mstar_ids: List[str]) -> List[Tuple[str, str, float]]:
    """Fetch all NAV rows for the given mstar_ids from mfpulse_reimagined.
    Returns list of (mstar_id, nav_date_iso, nav)."""
    ids_repr = repr(mstar_ids)
    py = f"""
import os, psycopg2, json
c = psycopg2.connect(os.environ['DATABASE_URL'])
cur = c.cursor()
cur.execute('SELECT mstar_id, nav_date, nav FROM nav_daily WHERE mstar_id = ANY(%s) ORDER BY mstar_id, nav_date', ({ids_repr},))
out = []
for mid, d, nav in cur.fetchall():
    if nav is None or nav <= 0: continue
    out.append((mid, d.isoformat(), float(nav)))
print(json.dumps(out))
"""
    raw = _run_in("mf-pulse", py)
    import json
    return [tuple(r) for r in json.loads(raw)]


def upsert_nav_into_jip(rows: List[Tuple[str, str, float]]) -> int:
    """COPY + upsert into de_mf_nav_daily. Returns row count."""
    if not rows:
        return 0
    import json
    payload = json.dumps(rows)
    # Write via a helper python script inside the jip container
    helper = r"""
import os, sys, json, asyncio
import asyncpg
async def m():
    rows = json.loads(sys.stdin.read())
    conn = await asyncpg.connect(os.environ['DATABASE_URL'].replace('postgresql+asyncpg://','postgresql://'))
    try:
        await conn.execute('DROP TABLE IF EXISTS _nav_stg')
        await conn.execute('CREATE TEMP TABLE _nav_stg (mstar_id text, nav_date date, nav numeric)')
        # asyncpg wants date objects, convert from iso
        from datetime import date
        recs = [(r[0], date.fromisoformat(r[1]), r[2]) for r in rows]
        await conn.copy_records_to_table('_nav_stg', records=recs, columns=['mstar_id','nav_date','nav'])
        r = await conn.execute('''
            INSERT INTO de_mf_nav_daily (mstar_id, nav_date, nav, data_status, created_at, updated_at)
            SELECT s.mstar_id, s.nav_date, s.nav, 'validated', now(), now() FROM _nav_stg s
            ON CONFLICT (mstar_id, nav_date) DO NOTHING
        ''')
        print(r)
    finally:
        await conn.close()
asyncio.run(m())
"""
    result = subprocess.run(
        ["docker", "exec", "-i", "jip-data-engine-data-engine-1", "python", "-c", helper],
        input=payload, capture_output=True, text=True, check=True,
    )
    # asyncpg returns "INSERT 0 N"
    status = result.stdout.strip()
    try:
        return int(status.split()[-1])
    except Exception:
        return len(rows)


def main():
    t0 = time.time()
    print("[gap14] listing missing eligible mstar_ids…", flush=True)
    missing = list_missing_mstar_ids()
    print(f"[gap14] {len(missing)} older eligible funds with <252 NAV rows",
          flush=True)
    if not missing:
        print("[gap14] nothing to do")
        return

    # Process in chunks to keep mfpulse payload manageable
    chunk = 50
    total_fetched = 0
    total_inserted = 0
    for i in range(0, len(missing), chunk):
        batch = missing[i:i + chunk]
        print(f"[gap14] batch {i // chunk + 1}: fetching NAV for {len(batch)} funds…",
              flush=True)
        rows = fetch_nav_from_mfpulse(batch)
        total_fetched += len(rows)
        print(f"[gap14]   mfpulse returned {len(rows):,} rows", flush=True)
        n = upsert_nav_into_jip(rows)
        total_inserted += n
        print(f"[gap14]   upserted (pg: {n}) cumulative inserted={total_inserted:,}",
              flush=True)

    print(f"[gap14] done. fetched={total_fetched:,} inserted={total_inserted:,} "
          f"in {time.time() - t0:.1f}s", flush=True)


if __name__ == "__main__":
    main()
