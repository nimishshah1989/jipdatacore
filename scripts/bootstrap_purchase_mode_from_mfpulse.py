"""Bootstrap de_mf_master.purchase_mode from mfpulse_reimagined's fund_master.

Reads mstar_id + purchase_mode from the mfpulse Postgres DB (same EC2 host),
then batch-UPDATEs JIP's de_mf_master. Idempotent — skips rows where
purchase_mode is already set to the same value.

Env vars required:
  MFPULSE_DATABASE_URL_SYNC  — e.g. postgresql://fie:...@172.17.0.1:5432/mf_pulse
  DATABASE_URL_SYNC           — JIP RDS (psycopg2 sync DSN)

Usage:
    python scripts/bootstrap_purchase_mode_from_mfpulse.py [--limit N] [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

import psycopg2
import psycopg2.extras


def run(*, limit: int | None = None, dry_run: bool = False) -> int:
    mfpulse_url = os.environ["MFPULSE_DATABASE_URL_SYNC"]
    jip_url = os.environ["DATABASE_URL_SYNC"].replace("+psycopg2", "")

    # --- Step 1: read purchase_mode from mfpulse ---
    with psycopg2.connect(mfpulse_url) as src:
        with src.cursor() as cur:
            sql = (
                "SELECT mstar_id, purchase_mode FROM fund_master "
                "WHERE purchase_mode IS NOT NULL"
            )
            if limit:
                sql += f" LIMIT {int(limit)}"
            cur.execute(sql)
            rows = cur.fetchall()

    print(f"[mfpulse] fetched {len(rows)} rows with purchase_mode", flush=True)
    if not rows:
        print("Nothing to do.", flush=True)
        return 0

    lookup = {mstar_id: pm for mstar_id, pm in rows}

    # --- Step 2: read current state from JIP ---
    with psycopg2.connect(jip_url) as dst:
        with dst.cursor() as cur:
            cur.execute(
                "SELECT mstar_id, purchase_mode FROM de_mf_master "
                "WHERE mstar_id = ANY(%s)",
                (list(lookup.keys()),),
            )
            existing = {r[0]: r[1] for r in cur.fetchall()}

    # --- Step 3: compute diff ---
    updates = []
    for mstar_id, pm in lookup.items():
        if mstar_id not in existing:
            continue
        if existing[mstar_id] == pm:
            continue
        updates.append((pm, mstar_id))

    print(
        f"[diff] {len(existing)} matched in JIP, {len(updates)} need update",
        flush=True,
    )

    if dry_run:
        for pm, mid in updates[:20]:
            old = existing.get(mid)
            print(f"  {mid}: {old} -> {pm}")
        if len(updates) > 20:
            print(f"  ... and {len(updates) - 20} more")
        return 0

    # --- Step 4: batch update ---
    if updates:
        with psycopg2.connect(jip_url) as dst:
            with dst.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur,
                    "UPDATE de_mf_master SET purchase_mode = %s, "
                    "updated_at = NOW() WHERE mstar_id = %s",
                    updates,
                    page_size=500,
                )
            dst.commit()

    print(f"[done] updated {len(updates)} rows", flush=True)

    # --- Step 5: verify ---
    with psycopg2.connect(jip_url) as dst:
        with dst.cursor() as cur:
            cur.execute(
                "SELECT purchase_mode, COUNT(*) FROM de_mf_master "
                "GROUP BY purchase_mode ORDER BY purchase_mode"
            )
            counts = cur.fetchall()
            cur.execute("SELECT COUNT(*) FROM de_mf_master")
            total = cur.fetchone()[0]

    print(f"\n--- Verification ---")
    print(f"Total funds: {total}")
    populated = 0
    for pm, cnt in counts:
        label = {1: "Regular", 2: "Direct"}.get(pm, "NULL")
        print(f"  purchase_mode={pm} ({label}): {cnt}")
        if pm is not None:
            populated += cnt
    print(f"Populated: {populated}/{total} ({populated*100/total:.1f}%)")

    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--limit", type=int, default=None, help="Limit source rows")
    p.add_argument("--dry-run", action="store_true", help="Show changes without applying")
    args = p.parse_args()
    return run(limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
