"""Backfill historical OHLCV for ETFs via yfinance.

Usage:
  python etf_backfill.py --new-only
  python etf_backfill.py --tickers AGG,BND,ARKK --start-date 2016-04-01
"""
from __future__ import annotations

import argparse
import io
import math
import os
import time
from datetime import datetime
from decimal import Decimal

import pandas as pd
import psycopg2
import yfinance as yf

_raw_db = os.environ.get(
    "DATABASE_URL_SYNC",
    "postgresql://jip_admin:JipDataEngine2026Secure@jip-data-engine.ctay2iewomaj.ap-south-1.rds.amazonaws.com:5432/data_engine",
)
DB = _raw_db.replace("postgresql+psycopg2://", "postgresql://").replace("postgresql+asyncpg://", "postgresql://")

BATCH_SIZE = 50


def _safe_decimal(value: object) -> Decimal | None:
    """Convert a float/numeric value to Decimal(4dp). Returns None for NaN/inf/None."""
    if value is None:
        return None
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return Decimal(str(round(f, 4)))
    except Exception:
        return None


def _get_new_only_tickers(cur: "psycopg2.extensions.cursor") -> list[str]:
    """Return tickers in de_etf_master that have no rows in de_etf_ohlcv."""
    cur.execute("""
        SELECT ticker
        FROM de_etf_master
        WHERE is_active = TRUE
          AND ticker NOT IN (SELECT DISTINCT ticker FROM de_etf_ohlcv)
        ORDER BY ticker
    """)
    return [row[0] for row in cur.fetchall()]


def _build_yf_mapping(cur: "psycopg2.extensions.cursor", db_tickers: list[str]) -> dict[str, str]:
    """Build yfinance_ticker → db_ticker mapping. NSE tickers get .NS suffix."""
    cur.execute(
        "SELECT ticker, exchange FROM de_etf_master WHERE ticker = ANY(%s)",
        (db_tickers,),
    )
    mapping: dict[str, str] = {}
    for ticker, exchange in cur.fetchall():
        yf_ticker = ticker + ".NS" if exchange == "NSE" else ticker
        mapping[yf_ticker] = ticker
    # Include any tickers not in DB (use as-is)
    for t in db_tickers:
        if t not in mapping.values():
            mapping[t] = t
    return mapping


def _download_batch(
    batch: list[str],
    start_date: str,
) -> list[dict]:
    """Download OHLCV for a batch of tickers. Returns list of row dicts."""
    tickers_str = " ".join(batch)
    try:
        df = yf.download(
            tickers_str,
            start=start_date,
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
        )
    except Exception as exc:
        print(f"  [ERROR] yfinance download failed for batch {batch}: {exc}", flush=True)
        return []

    if df is None or df.empty:
        print(f"  [WARN] Empty download for batch starting {batch[0]}", flush=True)
        return []

    rows: list[dict] = []

    # Detect MultiIndex: first level may be ticker names or field names
    is_multi = hasattr(df.columns, "levels") and len(df.columns.levels) > 1

    if is_multi:
        first_level = set(df.columns.get_level_values(0))
        ticker_first = bool(first_level & set(batch))

        for ticker in batch:
            try:
                if ticker_first:
                    sub = df[ticker].dropna(subset=["Close"])
                else:
                    # columns are (Field, Ticker) — transpose
                    sub = df.xs(ticker, axis=1, level=1).dropna(subset=["Close"])
            except KeyError:
                print(f"  [WARN] Ticker {ticker} not in download response", flush=True)
                continue

            if sub.empty:
                continue

            ticker_rows = _df_to_rows(sub, ticker)
            print(
                f"  {ticker}: {len(ticker_rows)} rows "
                f"({sub.index[0].date()} — {sub.index[-1].date()})",
                flush=True,
            )
            rows.extend(ticker_rows)

    else:
        # Single ticker — flat columns
        ticker = batch[0]
        sub = df.dropna(subset=["Close"])
        if not sub.empty:
            ticker_rows = _df_to_rows(sub, ticker)
            print(
                f"  {ticker}: {len(ticker_rows)} rows "
                f"({sub.index[0].date()} — {sub.index[-1].date()})",
                flush=True,
            )
            rows.extend(ticker_rows)

    return rows


def _df_to_rows(sub: pd.DataFrame, ticker: str) -> list[dict]:
    """Convert a single-ticker OHLCV DataFrame to a list of row dicts."""
    rows: list[dict] = []
    # Vectorized: iterate index (dates) via itertuples — OK at <10K rows per ticker
    for row in sub.itertuples():
        close_val = _safe_decimal(row.Close)
        if close_val is None:
            continue
        rows.append(
            {
                "ticker": ticker,
                "date": row.Index.date() if hasattr(row.Index, "date") else row.Index,
                "open": _safe_decimal(row.Open),
                "high": _safe_decimal(row.High),
                "low": _safe_decimal(row.Low),
                "close": close_val,
                "volume": int(row.Volume) if row.Volume and not math.isnan(float(row.Volume)) else None,
            }
        )
    return rows


def _dedup_rows(rows: list[dict]) -> list[dict]:
    """Deduplicate by (date, ticker) — last occurrence wins."""
    seen: dict[tuple, dict] = {}
    for row in rows:
        key = (row["date"], row["ticker"])
        seen[key] = row
    return list(seen.values())


def _upsert_rows(cur: "psycopg2.extensions.cursor", rows: list[dict]) -> int:
    """Bulk upsert rows into de_etf_ohlcv via staging TEMP TABLE + COPY."""
    if not rows:
        return 0

    rows = _dedup_rows(rows)

    # Build CSV buffer
    buf = io.StringIO()
    for r in rows:
        open_v = str(r["open"]) if r["open"] is not None else "\\N"
        high_v = str(r["high"]) if r["high"] is not None else "\\N"
        low_v = str(r["low"]) if r["low"] is not None else "\\N"
        close_v = str(r["close"]) if r["close"] is not None else "\\N"
        vol_v = str(r["volume"]) if r["volume"] is not None else "\\N"
        buf.write(f"{r['ticker']}\t{r['date']}\t{open_v}\t{high_v}\t{low_v}\t{close_v}\t{vol_v}\n")
    buf.seek(0)

    staging = "tmp_etf_backfill"
    cur.execute(f"DROP TABLE IF EXISTS {staging}")
    cur.execute(f"""
        CREATE TEMP TABLE {staging} (
            ticker  VARCHAR(30),
            date    DATE,
            open    NUMERIC(18,4),
            high    NUMERIC(18,4),
            low     NUMERIC(18,4),
            close   NUMERIC(18,4),
            volume  BIGINT
        )
    """)

    cur.copy_expert(
        f"COPY {staging} (ticker,date,open,high,low,close,volume) FROM STDIN WITH (FORMAT TEXT, NULL '\\N')",
        buf,
    )

    cur.execute(f"""
        INSERT INTO de_etf_ohlcv (ticker, date, open, high, low, close, volume)
        SELECT ticker, date, open, high, low, close, volume
        FROM {staging}
        ON CONFLICT (date, ticker) DO UPDATE
            SET open       = EXCLUDED.open,
                high       = EXCLUDED.high,
                low        = EXCLUDED.low,
                close      = EXCLUDED.close,
                volume     = EXCLUDED.volume,
                updated_at = NOW()
    """)

    cur.execute(f"DROP TABLE IF EXISTS {staging}")
    return len(rows)


def _ensure_master(cur: "psycopg2.extensions.cursor", tickers: list[str]) -> None:
    """Insert any tickers missing from de_etf_master (with placeholder metadata)."""
    cur.execute("SELECT ticker FROM de_etf_master")
    existing = {row[0] for row in cur.fetchall()}
    missing = [t for t in tickers if t not in existing]
    if missing:
        print(
            f"  [WARN] {len(missing)} ticker(s) not in de_etf_master — inserting with placeholder metadata: {missing}",
            flush=True,
        )
        for t in missing:
            cur.execute(
                """
                INSERT INTO de_etf_master (ticker, name, exchange, country, sector, currency)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker) DO NOTHING
                """,
                (t, t, "NYSE", "US", "Unknown", "USD"),
            )


def backfill(
    tickers: list[str],
    start_date: str,
) -> None:
    """Main backfill routine: download + upsert for given tickers."""
    t0 = time.time()
    conn = psycopg2.connect(DB)
    conn.autocommit = True
    cur = conn.cursor()

    print(f"Backfill starting: {len(tickers)} tickers, start_date={start_date}", flush=True)

    # Ensure all tickers exist in master (FK constraint)
    _ensure_master(cur, tickers)

    # Build yfinance→DB ticker mapping (NSE tickers get .NS suffix)
    yf_map = _build_yf_mapping(cur, tickers)
    yf_tickers = list(yf_map.keys())

    total_rows = 0
    failed_tickers: list[str] = []

    for i in range(0, len(yf_tickers), BATCH_SIZE):
        batch = yf_tickers[i: i + BATCH_SIZE]
        db_batch = [yf_map[t] for t in batch]
        print(f"\nBatch {i // BATCH_SIZE + 1}: {db_batch}", flush=True)

        batch_t0 = time.time()
        rows = _download_batch(batch, start_date)

        # Remap yfinance tickers back to DB tickers
        for row in rows:
            row["ticker"] = yf_map.get(row["ticker"], row["ticker"])

        if not rows:
            failed_tickers.extend(batch)
            continue

        rows_before = len(rows)
        inserted = _upsert_rows(cur, rows)
        elapsed = time.time() - batch_t0
        print(
            f"  Batch upserted: {rows_before} rows in -> {inserted} rows out ({elapsed:.1f}s)",
            flush=True,
        )
        total_rows += inserted

    # Verify final state
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT ticker) FROM de_etf_ohlcv")
    total_in_db, tickers_in_db = cur.fetchone()

    print("\n--- Backfill complete ---", flush=True)
    print(f"Tickers processed : {len(tickers) - len(failed_tickers)}", flush=True)
    print(f"Total rows upserted: {total_rows:,}", flush=True)
    print(f"Tickers failed     : {len(failed_tickers)} {failed_tickers or ''}", flush=True)
    print(f"DB totals          : {total_in_db:,} rows, {tickers_in_db} ETFs", flush=True)
    print(f"Elapsed            : {time.time() - t0:.1f}s", flush=True)

    cur.close()
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill ETF OHLCV via yfinance")
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Comma-separated tickers to backfill (e.g. AGG,BND,ARKK)",
    )
    parser.add_argument(
        "--new-only",
        action="store_true",
        help="Backfill only tickers with no existing OHLCV data",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2016-04-01",
        help="Start date for historical download (default: 2016-04-01)",
    )
    args = parser.parse_args()

    # Validate start-date
    try:
        datetime.strptime(args.start_date, "%Y-%m-%d")
    except ValueError:
        parser.error(f"--start-date must be YYYY-MM-DD, got: {args.start_date}")

    if args.new_only and args.tickers:
        parser.error("--new-only and --tickers are mutually exclusive")

    if not args.new_only and not args.tickers:
        parser.error("Provide --new-only or --tickers TICKER1,TICKER2,...")

    if args.new_only:
        conn = psycopg2.connect(DB)
        conn.autocommit = True
        cur = conn.cursor()
        tickers = _get_new_only_tickers(cur)
        cur.close()
        conn.close()
        if not tickers:
            print("No new tickers found — all ETFs already have OHLCV data.", flush=True)
            return
        print(f"New-only mode: {len(tickers)} tickers with no existing data", flush=True)
    else:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    backfill(tickers, args.start_date)


if __name__ == "__main__":
    main()
