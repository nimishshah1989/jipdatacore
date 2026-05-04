"""Atlas-M0 -- seed de_global_prices from Stooq files (path-based).

Generic loader that takes one or more (target_ticker, stooq_file_path) pairs
and ingests them into de_global_prices. Use for any Stooq-format daily file
regardless of which Stooq pack it came from.

Stooq daily file format:
  <TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>
  Dates: YYYYMMDD. Sentinel for missing values: -2.

Idempotent: ON CONFLICT (date, ticker) DO NOTHING. Re-running fills only
missing days.

Usage (inside data-engine container):
    python /app/scripts/atlas_m0_seed_global_from_stooq.py \\
        --pair "^GSPC=/tmp/spx.txt" \\
        --pair "URTH=/tmp/urth.txt"

The target ticker must already exist in de_global_instrument_master (this
script does NOT create master rows). To bulk-ingest a directory of files
under one prefix, repeat --pair as many times as needed.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


def _conn_url() -> str:
    url = os.environ.get("DATABASE_URL_SYNC") or os.environ["DATABASE_URL"]
    return url.replace("+asyncpg", "").replace("+psycopg2", "")


def load_stooq_file(path: Path) -> list[tuple]:
    """Parse a Stooq daily .txt file. Returns list of (date, o, h, l, c, vol)."""
    out: list[tuple] = []
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        first = fh.readline()
        if first and "DATE" not in first.upper():
            fh.seek(0)
        for line in fh:
            parts = line.strip().split(",")
            if len(parts) < 9:
                continue
            try:
                d = date(int(parts[2][0:4]), int(parts[2][4:6]), int(parts[2][6:8]))
            except (ValueError, IndexError):
                continue

            def num(s: str) -> Optional[Decimal]:
                s = s.strip()
                if not s or s == "-2":
                    return None
                try:
                    v = Decimal(s)
                    return None if v == -2 else v
                except Exception:
                    return None

            o, hi, lo, c = num(parts[4]), num(parts[5]), num(parts[6]), num(parts[7])
            try:
                v = int(float(parts[8])) if parts[8].strip() not in {"", "-2"} else 0
            except (ValueError, IndexError):
                v = 0
            if c is None:
                continue
            out.append((d, o, hi, lo, c, v))
    return out


def insert_prices(conn, ticker: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO de_global_prices (date, ticker, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (date, ticker) DO NOTHING
    """
    template = "(%s, %s, %s, %s, %s, %s, %s)"
    payload = [(d, ticker, o, h, lo, c, v) for (d, o, h, lo, c, v) in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, payload, template=template, page_size=2000)
        inserted = cur.rowcount
    conn.commit()
    return inserted


def ensure_master(conn, ticker: str) -> bool:
    """Return True if ticker already exists in de_global_instrument_master."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM de_global_instrument_master WHERE ticker = %s LIMIT 1",
            (ticker,),
        )
        return cur.fetchone() is not None


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--pair",
        action="append",
        required=True,
        help='ticker=path/to/stooq.txt (repeatable). e.g. --pair "^GSPC=/tmp/spx.txt"',
    )
    args = p.parse_args()

    pairs: list[tuple[str, Path]] = []
    for spec in args.pair:
        if "=" not in spec:
            print(f"[ERR] bad --pair: {spec}", flush=True)
            return 1
        ticker, path = spec.split("=", 1)
        pairs.append((ticker.strip(), Path(path.strip())))

    conn = psycopg2.connect(_conn_url())
    try:
        for ticker, path in pairs:
            print(f"\n[seed] {ticker}  <-  {path}", flush=True)
            if not path.exists():
                print("[seed] FAIL: file not found", flush=True)
                continue
            if not ensure_master(conn, ticker):
                print(
                    f"[seed] FAIL: {ticker} not in de_global_instrument_master "
                    "(add a master row first or use a different target ticker)",
                    flush=True,
                )
                continue
            rows = load_stooq_file(path)
            if not rows:
                print(f"[seed] FAIL: 0 rows parsed from {path}", flush=True)
                continue
            earliest = min(r[0] for r in rows)
            latest = max(r[0] for r in rows)
            inserted = insert_prices(conn, ticker, rows)
            print(
                f"[seed] {ticker}: parsed={len(rows)} inserted={inserted} "
                f"range={earliest}..{latest}",
                flush=True,
            )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
