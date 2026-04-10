"""Enrich de_etf_master with metadata from yfinance Ticker.info.

Usage:
    python scripts/ingest/etf_enrich.py
    python scripts/ingest/etf_enrich.py --tickers SPY,QQQ,NIFTYBEES
    python scripts/ingest/etf_enrich.py --force
    python scripts/ingest/etf_enrich.py --dry-run
    python scripts/ingest/etf_enrich.py --tickers NIFTYBEES --dry-run
"""
from __future__ import annotations

import argparse
import os
import time
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

import psycopg2
import yfinance as yf

_raw_db = os.environ.get(
    "DATABASE_URL_SYNC",
    "postgresql://jip_admin:JipDataEngine2026Secure@jip-data-engine.ctay2iewomaj.ap-south-1.rds.amazonaws.com:5432/data_engine",
)
DB = _raw_db.replace("postgresql+psycopg2://", "postgresql://").replace("postgresql+asyncpg://", "postgresql://")

# Fields tracked for per-field summary counts
TRACKED_FIELDS = ("category", "sector", "expense_ratio", "benchmark", "inception_date", "currency", "asset_class")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich de_etf_master with yfinance metadata.")
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Comma-separated ticker list. Default: all active with missing metadata.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Re-enrich even if category is already populated.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print what would be updated without writing to DB.",
    )
    return parser.parse_args()


def _safe_decimal(value: object) -> Optional[Decimal]:
    """Convert float/string to Decimal(str()) for Numeric(6,4) storage. Returns None on failure."""
    if value is None:
        return None
    try:
        # Explicitly check for float NaN/Inf before converting
        f = float(value)
        import math

        if math.isnan(f) or math.isinf(f):
            return None
        return Decimal(str(round(f, 4)))
    except (ValueError, TypeError, InvalidOperation):
        return None


def _safe_date_from_unix(value: object) -> Optional[date]:
    """Convert yfinance unix timestamp (int) to date. Returns None if not a valid int."""
    if value is None:
        return None
    try:
        ts = int(value)
        return datetime.fromtimestamp(ts).date()
    except (ValueError, TypeError, OSError, OverflowError):
        return None


def _derive_asset_class(quote_type: Optional[str], category: Optional[str]) -> Optional[str]:
    """Derive asset_class from yfinance quoteType + category string.

    Rules:
    - quoteType ETF → start with "Equity" as default
    - category contains Bond/Fixed/Debt → "Fixed Income"
    - category contains Gold/Silver/Commodity/Metal → "Commodity"
    - category contains Real Estate/REIT → "Real Estate"
    - Otherwise keep "Equity" for ETF quoteType, None for others
    """
    if quote_type is None:
        return None
    if quote_type.upper() != "ETF":
        return None

    if category:
        cat_upper = category.upper()
        if any(kw in cat_upper for kw in ("BOND", "FIXED INCOME", "DEBT", "TREASURY", "CREDIT")):
            return "Fixed Income"
        commodity_kws = ("GOLD", "SILVER", "COMMODITY", "COMMODIT", "METAL", "OIL", "ENERGY COMMODIT")
        if any(kw in cat_upper for kw in commodity_kws):
            return "Commodity"
        if any(kw in cat_upper for kw in ("REAL ESTATE", "REIT")):
            return "Real Estate"

    return "Equity"


def _fetch_yfinance_info(ticker: str, exchange: str) -> Optional[dict]:
    """Fetch yfinance Ticker.info for a given ticker/exchange pair.

    Returns the info dict or None on error.
    """
    yf_symbol = f"{ticker}.NS" if exchange and exchange.upper() == "NSE" else ticker
    try:
        info = yf.Ticker(yf_symbol).info
        if not info or not isinstance(info, dict):
            print(f"  [WARN] {ticker} ({yf_symbol}): empty or invalid info response", flush=True)
            return None
        # Minimal check: if only 1-2 keys returned, likely a dead symbol
        if len(info) <= 3:
            print(f"  [WARN] {ticker} ({yf_symbol}): sparse info ({len(info)} keys) — likely delisted", flush=True)
            return None
        return info
    except Exception as exc:
        print(f"  [ERROR] {ticker} ({yf_symbol}): yfinance fetch failed — {exc}", flush=True)
        return None


def _extract_fields(info: dict) -> dict:
    """Extract and normalize metadata fields from a yfinance info dict.

    Returns a dict with only non-None fields (dynamic SET).
    All values are DB-safe types (Decimal, date, str).
    """
    result: dict = {}

    # category
    category = info.get("category") or None
    if category:
        result["category"] = str(category)

    # sector — prefer sectorDisp, fall back to sector
    sector_raw = info.get("sectorDisp") or info.get("sector") or None
    if sector_raw:
        result["sector"] = str(sector_raw)

    # expense_ratio — yfinance returns float like 0.0004
    er_raw = info.get("annualReportExpenseRatio")
    if er_raw is not None:  # 0.0 is valid — do NOT use `or` here
        er_decimal = _safe_decimal(er_raw)
        if er_decimal is not None:
            result["expense_ratio"] = er_decimal

    # benchmark
    benchmark_raw = info.get("benchmark") or info.get("benchmarkSymbol") or None
    if benchmark_raw:
        result["benchmark"] = str(benchmark_raw)

    # inception_date — unix timestamp
    inception_raw = info.get("fundInceptionDate")
    if inception_raw is not None:
        inception = _safe_date_from_unix(inception_raw)
        if inception is not None:
            result["inception_date"] = inception

    # currency
    currency_raw = info.get("currency") or None
    if currency_raw:
        result["currency"] = str(currency_raw)

    # asset_class — derived
    asset_class = _derive_asset_class(
        info.get("quoteType"),
        result.get("category"),
    )
    if asset_class is not None:
        result["asset_class"] = asset_class

    return result


def _build_update_sql(fields: dict) -> "tuple[str, list]":
    """Build a dynamic UPDATE SQL + params list for the given fields dict.

    Returns (sql_string, params_list). The last param is always the ticker (WHERE clause).
    """
    set_clauses = []
    params: list = []
    for col, val in fields.items():
        set_clauses.append(f"{col} = %s")
        params.append(val)
    sql = f"UPDATE de_etf_master SET {', '.join(set_clauses)} WHERE ticker = %s"
    return sql, params


def _fetch_tickers_to_enrich(
    cur: "psycopg2.extensions.cursor",
    force: bool,
    ticker_filter: Optional[list],
) -> "list[tuple[str, str]]":
    """Query DB for (ticker, exchange) pairs to enrich."""
    if force:
        base_sql = "SELECT ticker, exchange FROM de_etf_master WHERE is_active = TRUE"
    else:
        base_sql = (
            "SELECT ticker, exchange FROM de_etf_master "
            "WHERE is_active = TRUE AND (category IS NULL OR expense_ratio IS NULL)"
        )

    if ticker_filter:
        placeholders = ", ".join(["%s"] * len(ticker_filter))
        sql = f"{base_sql} AND ticker IN ({placeholders}) ORDER BY ticker"
        cur.execute(sql, ticker_filter)
    else:
        cur.execute(f"{base_sql} ORDER BY ticker")

    return cur.fetchall()


def main() -> None:
    args = _parse_args()

    ticker_filter: Optional[list] = None
    if args.tickers:
        ticker_filter = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    conn = psycopg2.connect(DB)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            pairs = _fetch_tickers_to_enrich(cur, args.force, ticker_filter)

        total = len(pairs)
        print(f"[INFO] Tickers to enrich: {total}", flush=True)

        if total == 0:
            print("[INFO] Nothing to enrich. Use --force to re-enrich all active tickers.", flush=True)
            return

        enriched = 0
        failed = 0
        field_counts: dict[str, int] = {f: 0 for f in TRACKED_FIELDS}

        for i, (ticker, exchange) in enumerate(pairs, start=1):
            print(f"[{i}/{total}] {ticker} (exchange={exchange})", flush=True)

            info = _fetch_yfinance_info(ticker, exchange)
            if info is None:
                failed += 1
                if i < total:
                    time.sleep(1.0)
                continue

            fields = _extract_fields(info)

            if not fields:
                print(f"  [INFO] {ticker}: no fields extracted from yfinance info", flush=True)
                if i < total:
                    time.sleep(1.0)
                continue

            # Log what we found
            field_summary = ", ".join(f"{k}={v!r}" for k, v in fields.items())
            print(f"  [FOUND] {ticker}: {field_summary}", flush=True)

            if args.dry_run:
                print(f"  [DRY-RUN] Would UPDATE {ticker} with {len(fields)} field(s)", flush=True)
                enriched += 1
                for field in TRACKED_FIELDS:
                    if field in fields:
                        field_counts[field] += 1
            else:
                sql, params = _build_update_sql(fields)
                params.append(ticker)
                try:
                    with conn.cursor() as cur:
                        cur.execute(sql, params)
                    conn.commit()
                    enriched += 1
                    for field in TRACKED_FIELDS:
                        if field in fields:
                            field_counts[field] += 1
                    print(f"  [OK] {ticker}: updated {len(fields)} field(s)", flush=True)
                except Exception as exc:
                    conn.rollback()
                    failed += 1
                    print(f"  [ERROR] {ticker}: DB update failed — {exc}", flush=True)

            if i < total:
                time.sleep(1.0)

    finally:
        conn.close()

    # Summary
    print("\n" + "=" * 60, flush=True)
    print(f"SUMMARY — {'DRY-RUN ' if args.dry_run else ''}ETF Enrichment", flush=True)
    print(f"  Total attempted : {total}", flush=True)
    print(f"  Enriched        : {enriched}", flush=True)
    print(f"  Failed / skipped: {failed}", flush=True)
    print("  Per-field counts:", flush=True)
    for field, count in field_counts.items():
        print(f"    {field:<20} {count}", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    main()
