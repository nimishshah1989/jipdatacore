"""Atlas-M0 Job 1 — gap audit.

Enumerates the per-instrument and per-scheme gaps that Atlas-M1 needs filled,
matching the GAP_MAP.md V3 specification:

  PARTIAL stocks  : 156 -- fetch_from = 2011-04-01,
                          fetch_to   = instrument.earliest_date - 1
                          source     = NSE BHAV
  PARTIAL MFs     : 100 -- fetch_from = scheme.latest_date + 1,
                          fetch_to   = T-1
                          source     = AMFI daily NAV
  MISSING intl    :   2 -- INTL_SPX, INTL_MSCIWORLD
                          fetch_from = 2011-04-01, fetch_to = T-1
                          source     = Stooq d_us_txt dump (preferred) -> yfinance fallback

The output is a JSON report at reports/atlas_m0_gap_report.json that the
gap-fill orchestrator (atlas_m0_gap_fill.py) consumes. Run this on the
deployment server where DATABASE_URL_SYNC points at the JIP RDS.

Acceptance thresholds (from spec Section 6, "Pass criteria"):
  Job 1 -- >=95 % of identified gaps filled (delisted/data-unavailable
  instruments allowed as small residual).

Usage:
    python scripts/atlas_m0_gap_audit.py [--out reports/atlas_m0_gap_report.json]

Environment:
    DATABASE_URL_SYNC -- psycopg2 sync DSN for the JIP RDS
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras

from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).parent.parent
load_dotenv(_REPO_ROOT / ".env")

# Atlas-M0 boundaries (from spec section 2.1 and DoD).
ATLAS_HISTORY_START = date(2011, 4, 1)
ATLAS_PARTIAL_CUTOFF = date(2014, 4, 1)         # >=252 trading days before this
PARTIAL_MIN_DAYS_BEFORE = 252                    # Section 2.4 DoD
INTL_TICKERS = ("INTL_SPX", "INTL_MSCIWORLD")

# SQL ----------------------------------------------------------------------

# Stocks that are active and tradeable but have <252 trading days before
# 2014-04-01. Two cases: no rows at all before cutoff, or some rows but
# fewer than 252 distinct trading dates.
PARTIAL_STOCKS_SQL = """
WITH active AS (
    SELECT id, current_symbol, isin, listing_date
    FROM de_instrument
    WHERE is_active AND is_tradeable
),
hist AS (
    SELECT instrument_id,
           MIN(date) AS earliest_date,
           MAX(date) AS latest_date,
           COUNT(*) FILTER (WHERE date < %(cutoff)s) AS days_before_cutoff
    FROM de_equity_ohlcv
    GROUP BY instrument_id
)
SELECT a.id::text       AS instrument_id,
       a.current_symbol AS symbol,
       a.isin,
       a.listing_date,
       h.earliest_date,
       h.latest_date,
       COALESCE(h.days_before_cutoff, 0) AS days_before_cutoff
FROM active a
LEFT JOIN hist h ON h.instrument_id = a.id
WHERE COALESCE(h.days_before_cutoff, 0) < %(min_days)s
ORDER BY a.current_symbol;
"""

# MFs that are active equity-growth-regular but whose latest NAV is older
# than T-2 (so we want to refill from latest_date + 1 onwards). Reuses the
# tight filter from scripts/backfill_mf_nav.py.
PARTIAL_MFS_SQL = """
WITH eligible AS (
    SELECT m.mstar_id, m.amfi_code, m.fund_name
    FROM de_mf_master m
    WHERE m.is_active
      AND m.broad_category = 'Equity'
      AND NOT m.is_etf
      AND NOT m.is_index_fund
      AND m.purchase_mode = 1
      AND m.fund_name NOT ILIKE '%%IDCW%%'
      AND m.fund_name NOT ILIKE '%%Dividend%%'
      AND m.fund_name NOT ILIKE '%%Segregated%%'
      AND m.fund_name NOT ILIKE '%%Direct%%'
),
hist AS (
    SELECT mstar_id,
           MAX(nav_date) AS latest_nav_date,
           COUNT(*)      AS row_count
    FROM de_mf_nav_daily
    GROUP BY mstar_id
)
SELECT e.mstar_id,
       e.amfi_code,
       e.fund_name,
       h.latest_nav_date,
       COALESCE(h.row_count, 0) AS row_count
FROM eligible e
LEFT JOIN hist h ON h.mstar_id = e.mstar_id
WHERE h.latest_nav_date IS NULL OR h.latest_nav_date < %(target_date)s
ORDER BY e.mstar_id;
"""

INTL_PRESENCE_SQL = """
SELECT g.ticker,
       COALESCE(MIN(p.date), NULL::date) AS earliest_date,
       COALESCE(MAX(p.date), NULL::date) AS latest_date,
       COUNT(p.date)                     AS row_count
FROM de_global_instrument_master g
LEFT JOIN de_global_prices p ON p.ticker = g.ticker
WHERE g.ticker = ANY(%(tickers)s)
GROUP BY g.ticker;
"""


def _conn_url() -> str:
    url = os.environ.get("DATABASE_URL_SYNC") or os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL_SYNC or DATABASE_URL must be set")
    return url.replace("+psycopg2", "").replace("+asyncpg", "")


def _row_to_jsonable(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, (date, datetime)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def audit_partial_stocks(conn) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            PARTIAL_STOCKS_SQL,
            {"cutoff": ATLAS_PARTIAL_CUTOFF, "min_days": PARTIAL_MIN_DAYS_BEFORE},
        )
        rows = cur.fetchall()

    items = []
    for r in rows:
        listing = r["listing_date"]
        # If the instrument listed after the partial cutoff, the spec's
        # "no historical BHAV before listing" exception applies -- we still
        # report it so the readiness report can document residual coverage,
        # but we mark it unfillable for downstream scripts.
        unfillable = listing is not None and listing >= ATLAS_PARTIAL_CUTOFF
        item = _row_to_jsonable(dict(r))
        item["fetch_from"] = ATLAS_HISTORY_START.isoformat()
        item["fetch_to"] = (
            (r["earliest_date"] - timedelta(days=1)).isoformat()
            if r["earliest_date"]
            else (ATLAS_PARTIAL_CUTOFF - timedelta(days=1)).isoformat()
        )
        item["source"] = "NSE BHAV"
        item["unfillable_pre_listing"] = unfillable
        items.append(item)
    return items


def audit_partial_mfs(conn, target_date: date) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(PARTIAL_MFS_SQL, {"target_date": target_date})
        rows = cur.fetchall()

    items = []
    for r in rows:
        latest = r["latest_nav_date"]
        item = _row_to_jsonable(dict(r))
        item["fetch_from"] = (
            (latest + timedelta(days=1)).isoformat() if latest else "2006-01-01"
        )
        item["fetch_to"] = target_date.isoformat()
        item["source"] = "AMFI"
        item["has_amfi_code"] = bool(r["amfi_code"])
        items.append(item)
    return items


def audit_intl(conn, target_date: date) -> list[dict[str, Any]]:
    """Return a 2-row report covering INTL_SPX and INTL_MSCIWORLD."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(INTL_PRESENCE_SQL, {"tickers": list(INTL_TICKERS)})
        existing = {r["ticker"]: r for r in cur.fetchall()}

    items = []
    for ticker in INTL_TICKERS:
        row = existing.get(ticker)
        if row is None:
            items.append(
                {
                    "ticker": ticker,
                    "earliest_date": None,
                    "latest_date": None,
                    "row_count": 0,
                    "fetch_from": ATLAS_HISTORY_START.isoformat(),
                    "fetch_to": target_date.isoformat(),
                    "source": "Stooq -> yfinance fallback",
                    "status": "MISSING",
                }
            )
        else:
            item = _row_to_jsonable(dict(row))
            item["fetch_from"] = ATLAS_HISTORY_START.isoformat()
            item["fetch_to"] = target_date.isoformat()
            item["source"] = "Stooq -> yfinance fallback"
            covered = (
                row["earliest_date"] is not None
                and row["earliest_date"] <= ATLAS_HISTORY_START
                and row["latest_date"] is not None
                and row["latest_date"] >= target_date - timedelta(days=3)
            )
            item["status"] = "OK" if covered else "PARTIAL"
            items.append(item)
    return items


def run(out_path: Path, target_date: date) -> int:
    conn = psycopg2.connect(_conn_url())
    try:
        partial_stocks = audit_partial_stocks(conn)
        partial_mfs = audit_partial_mfs(conn, target_date)
        intl = audit_intl(conn, target_date)
    finally:
        conn.close()

    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "target_date": target_date.isoformat(),
        "atlas_history_start": ATLAS_HISTORY_START.isoformat(),
        "partial_cutoff": ATLAS_PARTIAL_CUTOFF.isoformat(),
        "partial_min_days_before": PARTIAL_MIN_DAYS_BEFORE,
        "summary": {
            "partial_stocks_count": len(partial_stocks),
            "partial_stocks_unfillable_pre_listing": sum(
                1 for s in partial_stocks if s.get("unfillable_pre_listing")
            ),
            "partial_mfs_count": len(partial_mfs),
            "intl_missing_or_partial": sum(
                1 for i in intl if i["status"] != "OK"
            ),
        },
        "partial_stocks": partial_stocks,
        "partial_mfs": partial_mfs,
        "intl": intl,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2))

    print(
        f"[audit] partial_stocks={len(partial_stocks)} "
        f"(unfillable_pre_listing={report['summary']['partial_stocks_unfillable_pre_listing']}) "
        f"partial_mfs={len(partial_mfs)} "
        f"intl_to_fill={report['summary']['intl_missing_or_partial']}",
        flush=True,
    )
    print(f"[audit] wrote {out_path}", flush=True)
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--out",
        type=Path,
        default=_REPO_ROOT / "reports" / "atlas_m0_gap_report.json",
    )
    p.add_argument(
        "--target-date",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=1),
        help="T-1 cutoff date (default: yesterday)",
    )
    args = p.parse_args()
    return run(args.out, args.target_date)


if __name__ == "__main__":
    sys.exit(main())
