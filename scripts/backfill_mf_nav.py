"""Backfill de_mf_nav_daily for equity regular growth funds missing NAV data.

Source priority:
  1. mfpulse_reimagined nav_daily (mstar_id keyed, richest coverage)
  2. AMFI historical NAV files (amfi_code required, semicolon-delimited)

Idempotent: uses ON CONFLICT (nav_date, mstar_id) DO NOTHING.

Env vars required:
  MFPULSE_DATABASE_URL_SYNC  — e.g. postgresql://fie:fie_dev_password@172.17.0.1:5432/mf_pulse
  DATABASE_URL_SYNC           — JIP RDS (psycopg2 sync DSN)

Usage:
    python scripts/backfill_mf_nav.py [--dry-run] [--limit N]
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation

import psycopg2
import psycopg2.extras

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))


TIGHT_FILTER_SQL = """
    SELECT m.mstar_id, m.amfi_code
    FROM de_mf_master m
    LEFT JOIN (SELECT DISTINCT mstar_id FROM de_mf_nav_daily) n
        ON m.mstar_id = n.mstar_id
    WHERE m.is_active
      AND m.broad_category = 'Equity'
      AND NOT m.is_etf
      AND NOT m.is_index_fund
      AND m.purchase_mode = 1
      AND m.fund_name NOT ILIKE '%%IDCW%%'
      AND m.fund_name NOT ILIKE '%%Dividend%%'
      AND m.fund_name NOT ILIKE '%%Segregated%%'
      AND m.fund_name NOT ILIKE '%%Direct%%'
      AND n.mstar_id IS NULL
"""

BATCH_SIZE = 5000


def _jip_url() -> str:
    return os.environ["DATABASE_URL_SYNC"].replace("+psycopg2", "")


def _mfpulse_url() -> str:
    return os.environ.get(
        "MFPULSE_DATABASE_URL_SYNC",
        "postgresql://fie:fie_dev_password@172.17.0.1:5432/mf_pulse",
    )


def get_missing_funds(jip_conn) -> list[tuple[str, str | None]]:
    """Return (mstar_id, amfi_code) for tight-filter funds missing NAV."""
    with jip_conn.cursor() as cur:
        cur.execute(TIGHT_FILTER_SQL)
        return cur.fetchall()


def backfill_from_mfpulse(
    jip_conn,
    mstar_ids: list[str],
    *,
    dry_run: bool = False,
    limit: int | None = None,
) -> tuple[int, int]:
    """Pull NAV rows from mfpulse nav_daily and upsert into JIP.

    Returns (funds_covered, rows_inserted).
    """
    if not mstar_ids:
        return 0, 0

    mfpulse_conn = psycopg2.connect(_mfpulse_url())
    try:
        with mfpulse_conn.cursor("mfpulse_nav_cursor") as src_cur:
            src_cur.itersize = BATCH_SIZE
            placeholders = ",".join(["%s"] * len(mstar_ids))
            query = f"""
                SELECT mstar_id, nav_date, nav
                FROM nav_daily
                WHERE mstar_id IN ({placeholders})
                ORDER BY mstar_id, nav_date
            """
            src_cur.execute(query, mstar_ids)

            total_rows = 0
            funds_seen: set[str] = set()
            batch: list[tuple] = []

            for row in src_cur:
                mstar_id, nav_date, nav = row
                if nav is None or nav <= 0:
                    continue

                funds_seen.add(mstar_id)
                batch.append((nav_date, mstar_id, str(nav)))

                if len(batch) >= BATCH_SIZE:
                    if not dry_run:
                        total_rows += _upsert_batch(jip_conn, batch)
                    else:
                        total_rows += len(batch)
                    batch.clear()

                if limit and total_rows >= limit:
                    break

            if batch:
                if not dry_run:
                    total_rows += _upsert_batch(jip_conn, batch)
                else:
                    total_rows += len(batch)

    finally:
        mfpulse_conn.close()

    return len(funds_seen), total_rows


def backfill_from_amfi(
    jip_conn,
    funds: list[tuple[str, str]],
    *,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Pull historical NAV from AMFI for funds with amfi_code.

    Returns (funds_covered, rows_inserted).
    """
    if not funds:
        return 0, 0

    try:
        import httpx
    except ImportError:
        print("[amfi] httpx not installed, skipping AMFI source", flush=True)
        return 0, 0

    total_rows = 0
    funds_covered = 0

    for mstar_id, amfi_code in funds:
        url = (
            "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
            f"?frmdt=01-Jan-2006&todt={datetime.now().strftime('%d-%b-%Y')}"
            f"&MFCode={amfi_code}"
        )
        try:
            resp = httpx.get(url, timeout=60, follow_redirects=True)
            resp.raise_for_status()
        except Exception as e:
            print(f"[amfi] {mstar_id} amfi={amfi_code}: fetch failed: {e}", flush=True)
            continue

        rows = _parse_amfi_response(resp.text, mstar_id)
        if not rows:
            continue

        funds_covered += 1
        if not dry_run:
            total_rows += _upsert_batch(jip_conn, rows)
        else:
            total_rows += len(rows)

        time.sleep(0.5)

    return funds_covered, total_rows


def _parse_amfi_response(text: str, mstar_id: str) -> list[tuple]:
    """Parse AMFI semicolon-delimited NAV history into (nav_date, mstar_id, nav) tuples."""
    rows = []
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line or ";" not in line:
            continue
        parts = line.split(";")
        if len(parts) < 5:
            continue
        date_str = parts[4].strip() if len(parts) > 4 else parts[-1].strip()
        nav_str = parts[1].strip() if len(parts) > 1 else ""

        try:
            nav_val = Decimal(nav_str)
        except (InvalidOperation, ValueError):
            continue
        if nav_val <= 0:
            continue

        nav_date = _parse_amfi_date(date_str)
        if nav_date is None:
            continue

        rows.append((nav_date, mstar_id, str(nav_val)))

    return rows


def _parse_amfi_date(s: str):
    """Parse dd-Mon-YYYY or dd-MM-YYYY date string."""
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _upsert_batch(conn, batch: list[tuple]) -> int:
    """INSERT rows with ON CONFLICT DO NOTHING. Returns rows actually inserted."""
    if not batch:
        return 0

    sql = """
        INSERT INTO de_mf_nav_daily (nav_date, mstar_id, nav, data_status)
        VALUES %s
        ON CONFLICT (nav_date, mstar_id) DO NOTHING
    """
    template = "(%s, %s, %s, 'raw')"

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur, sql, batch, template=template, page_size=BATCH_SIZE
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


def run(*, dry_run: bool = False, limit: int | None = None) -> int:
    jip_conn = psycopg2.connect(_jip_url())

    try:
        missing = get_missing_funds(jip_conn)
        print(f"[init] {len(missing)} funds missing NAV (tight filter)", flush=True)

        if not missing:
            print("[done] No missing funds. Nothing to do.", flush=True)
            return 0

        missing_ids = [r[0] for r in missing]

        # --- Source 1: mfpulse ---
        print(f"[mfpulse] Checking {len(missing_ids)} funds...", flush=True)
        mf_funds, mf_rows = backfill_from_mfpulse(
            jip_conn, missing_ids, dry_run=dry_run, limit=limit
        )
        print(
            f"[mfpulse] {'Would insert' if dry_run else 'Inserted'} "
            f"{mf_rows:,} rows for {mf_funds} funds",
            flush=True,
        )

        # --- Source 2: AMFI (for funds mfpulse didn't cover) ---
        remaining = get_missing_funds(jip_conn) if not dry_run else missing
        remaining_with_amfi = [(r[0], r[1]) for r in remaining if r[1]]
        if remaining_with_amfi:
            print(
                f"[amfi] {len(remaining_with_amfi)} funds with AMFI code still missing...",
                flush=True,
            )
            amfi_funds_n, amfi_rows = backfill_from_amfi(
                jip_conn, remaining_with_amfi, dry_run=dry_run
            )
            print(
                f"[amfi] {'Would insert' if dry_run else 'Inserted'} "
                f"{amfi_rows:,} rows for {amfi_funds_n} funds",
                flush=True,
            )
        else:
            print("[amfi] No remaining funds with AMFI code.", flush=True)

        # --- Verification ---
        with jip_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(DISTINCT m.mstar_id) as eligible,
                       COUNT(DISTINCT CASE WHEN n.mstar_id IS NOT NULL
                             THEN m.mstar_id END) as with_nav
                FROM de_mf_master m
                LEFT JOIN (SELECT DISTINCT mstar_id FROM de_mf_nav_daily) n
                    ON m.mstar_id = n.mstar_id
                WHERE m.is_active
                  AND m.broad_category = 'Equity'
                  AND NOT m.is_etf AND NOT m.is_index_fund
                  AND m.purchase_mode = 1
                  AND m.fund_name NOT ILIKE '%%IDCW%%'
                  AND m.fund_name NOT ILIKE '%%Dividend%%'
                  AND m.fund_name NOT ILIKE '%%Segregated%%'
                  AND m.fund_name NOT ILIKE '%%Direct%%'
            """)
            row = cur.fetchone()
            print(
                f"\n--- Coverage ---\n"
                f"Eligible (tight filter): {row[0]}\n"
                f"With NAV: {row[1]}\n"
                f"Coverage: {row[1]*100/row[0]:.1f}%",
                flush=True,
            )

    finally:
        jip_conn.close()

    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true", help="Show what would be done")
    p.add_argument("--limit", type=int, default=None, help="Limit total rows inserted")
    args = p.parse_args()
    return run(dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    sys.exit(main())
