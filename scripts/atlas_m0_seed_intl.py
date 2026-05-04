"""Atlas-M0 Job 1.3 -- seed INTL_SPX and INTL_MSCIWORLD.

Per ATLAS_M0 spec section 2.3:
  S&P 500     -> de_global_prices ticker INTL_SPX
                 source: Stooq d_us_txt dump (preferred) -> yfinance ^GSPC
  MSCI World  -> de_global_prices ticker INTL_MSCIWORLD
                 source: Stooq URTH (iShares MSCI World ETF) -> yfinance URTH

Both target the date range 2011-04-01 .. T-1.

Usage:
    # Stooq path is optional; if provided, the Stooq dump is searched for
    # SPX (case-insensitive: spx.us.txt or _gspc) and URTH (urth.us.txt).
    python scripts/atlas_m0_seed_intl.py \\
        --stooq-root /opt/stooq/d_us_txt \\
        --start 2011-04-01

If --stooq-root is omitted or files aren't found, the script falls back to
yfinance for the missing tickers.

Idempotent: ON CONFLICT (date, ticker) DO NOTHING for de_global_prices and
ON CONFLICT (ticker) DO NOTHING for de_global_instrument_master.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).parent.parent
load_dotenv(_REPO_ROOT / ".env")

ATLAS_HISTORY_START = date(2011, 4, 1)

INTL_INSTRUMENTS: list[dict[str, str]] = [
    {
        "ticker": "INTL_SPX",
        "name": "S&P 500 (Standard & Poor's 500)",
        "instrument_type": "index",
        "exchange": "NYSE",
        "currency": "USD",
        "country": "US",
        "category": "Broad Equity",
        "stooq_candidates": ["spx.us.txt", "^spx.us.txt", "spx_us.txt"],
        "stooq_subdirs": ["data/daily/us/indices", "daily/us/indices", "us/indices"],
        "yfinance_ticker": "^GSPC",
    },
    {
        "ticker": "INTL_MSCIWORLD",
        "name": "MSCI World (URTH proxy)",
        "instrument_type": "etf",
        "exchange": "NYSEArca",
        "currency": "USD",
        "country": "US",
        "category": "Global Equity",
        "stooq_candidates": ["urth.us.txt"],
        "stooq_subdirs": [
            "data/daily/us/nyse etfs",
            "data/daily/us/nasdaq etfs",
            "daily/us/nyse etfs",
            "daily/us/nasdaq etfs",
        ],
        "yfinance_ticker": "URTH",
    },
]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def _conn_url() -> str:
    url = os.environ.get("DATABASE_URL_SYNC") or os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL_SYNC or DATABASE_URL must be set")
    return url.replace("+psycopg2", "").replace("+asyncpg", "")


def upsert_master(conn, instr: dict[str, str]) -> None:
    sql = """
        INSERT INTO de_global_instrument_master
            (ticker, name, instrument_type, exchange, currency, country, category, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker) DO UPDATE
            SET name = EXCLUDED.name,
                instrument_type = EXCLUDED.instrument_type,
                exchange = EXCLUDED.exchange,
                currency = EXCLUDED.currency,
                country = EXCLUDED.country,
                category = EXCLUDED.category,
                source = EXCLUDED.source,
                updated_at = now()
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                instr["ticker"],
                instr["name"],
                instr["instrument_type"],
                instr["exchange"],
                instr["currency"],
                instr["country"],
                instr["category"],
                "atlas_m0",
            ),
        )
    conn.commit()


def insert_prices(
    conn,
    ticker: str,
    rows: list[tuple[date, Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[int]]],
) -> int:
    """Bulk insert (date, ticker, open, high, low, close, volume) rows.

    Returns number of rows actually inserted (no overwrite).
    """
    if not rows:
        return 0
    sql = """
        INSERT INTO de_global_prices (date, ticker, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (date, ticker) DO NOTHING
    """
    template = "(%s, %s, %s, %s, %s, %s, %s)"
    payload = [(d, ticker, o, hi, lo, c, v) for (d, o, hi, lo, c, v) in rows]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, payload, template=template, page_size=1000)
        inserted = cur.rowcount
    conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# Stooq loader
# ---------------------------------------------------------------------------
def _resolve_stooq_path(root: Path, instr: dict[str, str]) -> Optional[Path]:
    """Locate the Stooq daily file for an instrument inside the dump root.

    The Stooq d_us_txt zip extracts to `data/daily/us/...`. We try the
    canonical subdirs first; if nothing matches, we fall back to a recursive
    glob (slower but resilient to dump layout changes).
    """
    candidates = [c.lower() for c in instr["stooq_candidates"]]
    for sub in instr["stooq_subdirs"]:
        d = root / sub
        if not d.exists():
            continue
        for fname in candidates:
            p = d / fname
            if p.exists():
                return p
    # Recursive fallback
    for cand in candidates:
        for p in root.rglob(cand):
            return p
    return None


def load_stooq_file(path: Path) -> list[tuple[date, Decimal, Decimal, Decimal, Decimal, int]]:
    """Parse a Stooq daily .txt file.

    Stooq columns: <TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>
    Sentinel: -2 means missing.
    """
    out: list[tuple[date, Decimal, Decimal, Decimal, Decimal, int]] = []
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        header = fh.readline()
        if not header or "DATE" not in header.upper():
            # Some Stooq dumps omit headers
            fh.seek(0)
        for line in fh:
            parts = line.strip().split(",")
            if len(parts) < 9:
                continue
            try:
                d = date(
                    int(parts[2][0:4]), int(parts[2][4:6]), int(parts[2][6:8])
                )
            except (ValueError, IndexError):
                continue

            def _num(s: str) -> Optional[Decimal]:
                s = s.strip()
                if not s or s == "-2":
                    return None
                try:
                    val = Decimal(s)
                    return None if val == -2 else val
                except Exception:
                    return None

            o, hi, lo, c = _num(parts[4]), _num(parts[5]), _num(parts[6]), _num(parts[7])
            try:
                v = int(float(parts[8])) if parts[8].strip() not in {"", "-2"} else 0
            except (ValueError, IndexError):
                v = 0
            if c is None:
                continue
            out.append((d, o, hi, lo, c, v))
    return out


# ---------------------------------------------------------------------------
# yfinance loader (fallback)
# ---------------------------------------------------------------------------
def load_yfinance(ticker: str, start: date, end: date) -> list[tuple[date, Decimal, Decimal, Decimal, Decimal, int]]:
    try:
        import yfinance as yf
    except ImportError:
        print("[yf] yfinance not installed -- pip install yfinance", flush=True)
        return []

    # yfinance >= 0.2.47 defaults multi_level_index=True even for single
    # tickers, returning columns like ('Open', 'INTL_SPX'). Force a flat
    # column index so plain row.get('Open') works.
    download_kwargs = {
        "tickers": ticker,
        "start": start.isoformat(),
        "end": (end + timedelta(days=1)).isoformat(),
        "progress": False,
        "auto_adjust": False,
        "group_by": "column",
    }
    try:
        df = yf.download(multi_level_index=False, **download_kwargs)
    except TypeError:
        # Older yfinance (<0.2.47) doesn't accept multi_level_index; flatten
        # manually below if columns came back as a MultiIndex.
        df = yf.download(**download_kwargs)
    if df is None or df.empty:
        print(f"[yf] no data for {ticker}", flush=True)
        return []
    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = df.columns.get_level_values(0)

    def _is_nan(x: Any) -> bool:
        return x is None or (isinstance(x, float) and x != x)

    out = []
    for ts, row in df.iterrows():
        try:
            d = ts.date()
        except AttributeError:
            d = ts
        try:
            raw_o, raw_h, raw_l = row.get("Open"), row.get("High"), row.get("Low")
            raw_c, raw_v = row.get("Close"), row.get("Volume")
            o = None if _is_nan(raw_o) else Decimal(str(raw_o))
            hi = None if _is_nan(raw_h) else Decimal(str(raw_h))
            lo = None if _is_nan(raw_l) else Decimal(str(raw_l))
            c = None if _is_nan(raw_c) else Decimal(str(raw_c))
            v = 0 if _is_nan(raw_v) else int(raw_v)
        except (TypeError, ValueError, InvalidOperation):
            continue
        if c is None:
            continue
        out.append((d, o, hi, lo, c, v))
    return out


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def seed_one(conn, instr: dict[str, str], stooq_root: Optional[Path], start: date, end: date) -> dict:
    print(f"\n[seed] {instr['ticker']} ({instr['name']})", flush=True)

    upsert_master(conn, instr)

    rows: list[tuple] = []
    source_used = None

    if stooq_root is not None:
        path = _resolve_stooq_path(stooq_root, instr)
        if path is not None:
            print(f"[seed] Stooq file: {path}", flush=True)
            all_rows = load_stooq_file(path)
            rows = [r for r in all_rows if start <= r[0] <= end]
            if rows:
                source_used = f"stooq:{path.name}"
        else:
            print(
                f"[seed] no Stooq file matched in {stooq_root} "
                f"(candidates={instr['stooq_candidates']})",
                flush=True,
            )

    if not rows:
        print(f"[seed] falling back to yfinance ticker {instr['yfinance_ticker']}", flush=True)
        rows = load_yfinance(instr["yfinance_ticker"], start, end)
        if rows:
            source_used = f"yfinance:{instr['yfinance_ticker']}"

    if not rows:
        print(f"[seed] FAIL: no data for {instr['ticker']}", flush=True)
        return {"ticker": instr["ticker"], "rows_inserted": 0, "source": None}

    inserted = insert_prices(conn, instr["ticker"], rows)
    earliest = min(r[0] for r in rows)
    latest = max(r[0] for r in rows)
    print(
        f"[seed] {instr['ticker']}: inserted={inserted} "
        f"range={earliest}..{latest} via {source_used}",
        flush=True,
    )
    return {
        "ticker": instr["ticker"],
        "rows_inserted": inserted,
        "earliest": earliest.isoformat(),
        "latest": latest.isoformat(),
        "source": source_used,
    }


def run(stooq_root: Optional[Path], start: date, end: date) -> int:
    conn = psycopg2.connect(_conn_url())
    try:
        results = []
        for instr in INTL_INSTRUMENTS:
            results.append(seed_one(conn, instr, stooq_root, start, end))
    finally:
        conn.close()

    print("\n[seed] summary:")
    for r in results:
        print(f"  {r['ticker']:<18} rows={r['rows_inserted']:>6} via {r['source']}", flush=True)
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--stooq-root",
        type=Path,
        default=None,
        help="Path to extracted Stooq d_us_txt dump root (e.g. /opt/stooq/d_us_txt)",
    )
    p.add_argument(
        "--start",
        type=date.fromisoformat,
        default=ATLAS_HISTORY_START,
        help=f"Start date (default {ATLAS_HISTORY_START.isoformat()})",
    )
    p.add_argument(
        "--end",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=1),
        help="End date (default yesterday)",
    )
    args = p.parse_args()
    return run(args.stooq_root, args.start, args.end)


if __name__ == "__main__":
    sys.exit(main())
