"""One-shot backfill script for de_equity_fundamentals from Screener.in."""

import asyncio
import os
import sys
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional

import httpx
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import importlib.util
import logging
import types

# Shim app.logging so the fetcher/enricher modules can import without pulling pydantic_settings
_app_logging = types.ModuleType("app.logging")
_app_logging.get_logger = logging.getLogger
sys.modules["app.logging"] = _app_logging
sys.modules.setdefault("app", types.ModuleType("app"))

def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

_base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
screener_enricher = _load_module(
    "screener_enricher",
    os.path.join(_base, "app", "pipelines", "fundamentals", "screener_enricher.py"),
)
screener_fetcher = _load_module(
    "screener_fetcher",
    os.path.join(_base, "app", "pipelines", "fundamentals", "screener_fetcher.py"),
)

parse_screener_html = screener_enricher.parse_screener_html
extract_balance_sheet_latest = screener_fetcher.extract_balance_sheet_latest
extract_pl_growth = screener_fetcher.extract_pl_growth
extract_shareholding = screener_fetcher.extract_shareholding
fetch_company_html = screener_fetcher.fetch_company_html

DELAY_SECONDS = 1.2
MAX_CONSECUTIVE_FAILURES = 5
AS_OF_DATE = date.today()

DB_DSN = os.environ.get(
    "DATABASE_URL_SYNC",
    "dbname=data_engine user=jip_admin password=JipDataEngine2026Secure "
    "host=jip-data-engine.ctay2iewomaj.ap-south-1.rds.amazonaws.com port=5432",
)


def _to_decimal(val, precision: int = 4) -> Optional[Decimal]:
    if val is None:
        return None
    try:
        return round(Decimal(str(val)), precision)
    except (InvalidOperation, ValueError, TypeError):
        return None


def get_universe(conn) -> list[dict]:
    cur = conn.cursor()
    cur.execute(
        "SELECT id, current_symbol FROM de_instrument "
        "WHERE is_active = true AND exchange = 'NSE' "
        "ORDER BY current_symbol"
    )
    rows = cur.fetchall()
    cur.close()
    return [{"id": str(r[0]), "symbol": r[1]} for r in rows]


def upsert_row(conn, row: dict) -> bool:
    cols = list(row.keys())
    vals = [row[c] for c in cols]
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in cols
        if c not in ("instrument_id", "as_of_date", "created_at")
    )
    sql = (
        f"INSERT INTO de_equity_fundamentals ({col_names}) VALUES ({placeholders}) "
        f"ON CONFLICT (instrument_id, as_of_date) DO UPDATE SET {update_set}"
    )
    cur = conn.cursor()
    cur.execute(sql, vals)
    cur.close()
    return True


def build_row(html: str, instrument_id: str, as_of_date: date) -> Optional[dict]:
    snapshot = parse_screener_html(html)
    if not snapshot:
        return None

    shareholding = extract_shareholding(html)
    pl_growth = extract_pl_growth(html)
    bs_ratios = extract_balance_sheet_latest(html)

    return {
        "instrument_id": instrument_id,
        "as_of_date": as_of_date,
        "market_cap_cr": _to_decimal(snapshot.get("market_cap_cr"), 2),
        "pe_ratio": _to_decimal(snapshot.get("pe_ratio")),
        "pb_ratio": _to_decimal(snapshot.get("pb_ratio")),
        "peg_ratio": _to_decimal(snapshot.get("peg_ratio")),
        "ev_ebitda": _to_decimal(snapshot.get("ev_ebitda")),
        "roe_pct": _to_decimal(snapshot.get("roe_pct")),
        "roce_pct": _to_decimal(snapshot.get("roce_pct")),
        "operating_margin_pct": _to_decimal(pl_growth.get("operating_margin_pct")),
        "net_margin_pct": _to_decimal(pl_growth.get("net_margin_pct")),
        "debt_to_equity": _to_decimal(bs_ratios.get("debt_to_equity")),
        "interest_coverage": None,
        "current_ratio": None,
        "eps_ttm": _to_decimal(pl_growth.get("eps_ttm")),
        "book_value": _to_decimal(snapshot.get("book_value")),
        "face_value": _to_decimal(snapshot.get("face_value"), 2),
        "dividend_per_share": None,
        "dividend_yield_pct": _to_decimal(snapshot.get("dividend_yield_pct")),
        "promoter_holding_pct": _to_decimal(shareholding.get("promoter_pct"), 2),
        "pledged_pct": None,
        "fii_holding_pct": _to_decimal(shareholding.get("fii_pct"), 2),
        "dii_holding_pct": _to_decimal(shareholding.get("dii_pct"), 2),
        "revenue_growth_yoy_pct": _to_decimal(pl_growth.get("revenue_growth_yoy_pct")),
        "profit_growth_yoy_pct": _to_decimal(pl_growth.get("profit_growth_yoy_pct")),
        "high_52w": _to_decimal(snapshot.get("high_52w")),
        "low_52w": _to_decimal(snapshot.get("low_52w")),
        "source": "screener",
    }


async def main():
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = True

    universe = get_universe(conn)
    print(f"Universe: {len(universe)} instruments")

    rows_ok = 0
    rows_fail = 0
    consecutive_failures = 0

    async with httpx.AsyncClient(
        timeout=20,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Referer": "https://www.screener.in/",
        },
        follow_redirects=True,
    ) as client:
        for i, inst in enumerate(universe):
            symbol = inst["symbol"]
            instrument_id = inst["id"]

            html = await fetch_company_html(client, symbol)

            if html is None:
                consecutive_failures += 1
                rows_fail += 1
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    print(f"ABORT: {MAX_CONSECUTIVE_FAILURES} consecutive failures at {symbol}")
                    break
                await asyncio.sleep(DELAY_SECONDS)
                continue

            consecutive_failures = 0

            try:
                row = build_row(html, instrument_id, AS_OF_DATE)
                if row:
                    upsert_row(conn, row)
                    rows_ok += 1
                else:
                    rows_fail += 1
            except Exception as e:
                print(f"  ERROR {symbol}: {e}")
                rows_fail += 1

            await asyncio.sleep(DELAY_SECONDS)

            if (i + 1) % 100 == 0:
                print(f"  Progress: {i+1}/{len(universe)} ok={rows_ok} fail={rows_fail}")

    conn.close()
    print(f"\nDone: ok={rows_ok} fail={rows_fail} total={rows_ok + rows_fail}/{len(universe)}")


if __name__ == "__main__":
    asyncio.run(main())
