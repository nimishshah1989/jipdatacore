"""GAP-12: Expand de_global_instrument_master with top global ETFs.

Adds 30 new globally-traded ETFs, backfills OHLCV from yfinance (2016+),
and generates a source inventory report.

Usage:
    python scripts/ingest/global_etf_expand.py
"""
from __future__ import annotations

import asyncio
import math
import os
import pathlib
import sys
from datetime import date
from decimal import Decimal
from typing import Any

import yfinance as yf
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

REPORTS_DIR = pathlib.Path("reports")
START_DATE = "2016-01-01"
BATCH_SIZE = 10

def _etf(ticker, name, exchange, category):
    return {
        "ticker": ticker, "name": name, "exchange": exchange,
        "currency": "USD", "country": "US", "category": category,
    }


NEW_ETFS: list[dict[str, str]] = [
    _etf("QQQ", "Invesco QQQ Trust", "NASDAQ", "Large Cap Growth"),
    _etf("IVV", "iShares Core S&P 500 ETF", "NYSE", "Large Cap Blend"),
    _etf("VTI", "Vanguard Total Stock Market ETF", "NYSE", "Total Market"),
    _etf("VEA", "Vanguard FTSE Developed Markets ETF", "NYSE", "Intl Developed"),
    _etf("VWO", "Vanguard FTSE Emerging Markets ETF", "NYSE", "Emerging Markets"),
    _etf("IEFA", "iShares Core MSCI EAFE ETF", "NYSE", "Intl Developed"),
    _etf("AGG", "iShares Core US Aggregate Bond ETF", "NYSE", "US Aggregate Bond"),
    _etf("BND", "Vanguard Total Bond Market ETF", "NASDAQ", "US Aggregate Bond"),
    _etf("VOO", "Vanguard S&P 500 ETF", "NYSE", "Large Cap Blend"),
    _etf("VIG", "Vanguard Dividend Appreciation ETF", "NYSE", "Dividend Growth"),
    _etf("VGT", "Vanguard Information Technology ETF", "NYSE", "Technology"),
    _etf("SOXX", "iShares Semiconductor ETF", "NASDAQ", "Semiconductors"),
    _etf("XLE", "Energy Select Sector SPDR Fund", "NYSE", "Energy"),
    _etf("XLF", "Financial Select Sector SPDR Fund", "NYSE", "Financials"),
    _etf("XLK", "Technology Select Sector SPDR Fund", "NYSE", "Technology"),
    _etf("XLV", "Health Care Select Sector SPDR Fund", "NYSE", "Healthcare"),
    _etf("XLU", "Utilities Select Sector SPDR Fund", "NYSE", "Utilities"),
    _etf("XLP", "Consumer Staples Select Sector SPDR", "NYSE", "Consumer Staples"),
    _etf("XLY", "Consumer Discret Select Sector SPDR", "NYSE", "Consumer Discret"),
    _etf("XLI", "Industrial Select Sector SPDR Fund", "NYSE", "Industrials"),
    _etf("XLB", "Materials Select Sector SPDR Fund", "NYSE", "Materials"),
    _etf("XLRE", "Real Estate Select Sector SPDR Fund", "NYSE", "Real Estate"),
    _etf("ARKK", "ARK Innovation ETF", "NYSE", "Thematic Innovation"),
    _etf("EFA", "iShares MSCI EAFE ETF", "NYSE", "Intl Developed"),
    _etf("TLT", "iShares 20+ Year Treasury Bond ETF", "NASDAQ", "Long-Term Treasury"),
    _etf("IEF", "iShares 7-10 Year Treasury Bond ETF", "NASDAQ", "Intermediate Treasury"),
    _etf("HYG", "iShares iBoxx High Yield Corp Bond", "NYSE", "High Yield Bond"),
    _etf("LQD", "iShares iBoxx Inv Grade Corp Bond", "NYSE", "Investment Grade Bond"),
    _etf("SLV", "iShares Silver Trust", "NYSE", "Precious Metals"),
    _etf("GLD", "SPDR Gold Shares", "NYSE", "Precious Metals"),
    _etf("USO", "United States Oil Fund", "NYSE", "Energy Commodity"),
]


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return Decimal(str(round(f, 4)))
    except Exception:
        return None


def download_history(tickers: list[str]) -> dict[str, list[dict]]:
    """Download full history from yfinance for a batch of tickers."""
    result: dict[str, list[dict]] = {t: [] for t in tickers}

    df = yf.download(
        tickers=" ".join(tickers),
        start=START_DATE,
        interval="1d",
        group_by="ticker",
        auto_adjust=True,
        progress=False,
    )

    if df is None or df.empty:
        return result

    has_multi = hasattr(df.columns, "levels") and len(df.columns.levels) > 1

    for ticker in tickers:
        try:
            if has_multi:
                first_level = set(df.columns.get_level_values(0))
                ticker_first = bool(first_level & set(tickers))
                if ticker_first:
                    tdf = df[ticker].dropna(subset=["Close"])
                else:
                    tdf = df.xs(ticker, level=1, axis=1).dropna(subset=["Close"])
            elif len(tickers) == 1:
                tdf = df.dropna(subset=["Close"])
            else:
                continue

            for idx, row in tdf.iterrows():
                close_val = _safe_decimal(row.get("Close"))
                if close_val is None:
                    continue
                vol_raw = row.get("Volume")
                try:
                    vol = int(vol_raw) if vol_raw is not None and not math.isnan(float(vol_raw)) else None
                except (TypeError, ValueError):
                    vol = None

                d = idx.date() if hasattr(idx, "date") else idx
                result[ticker].append({
                    "date": d,
                    "ticker": ticker,
                    "open": _safe_decimal(row.get("Open")),
                    "high": _safe_decimal(row.get("High")),
                    "low": _safe_decimal(row.get("Low")),
                    "close": close_val,
                    "volume": vol,
                })
        except Exception as exc:
            print(f"  WARN: {ticker} parse error: {exc}", flush=True)

    return result


async def main() -> int:
    db_url = os.environ["DATABASE_URL"]
    engine = create_async_engine(db_url, pool_pre_ping=True)
    Session = async_sessionmaker(engine, expire_on_commit=False)

    # Step 1: Check which candidate tickers already exist
    async with Session() as session:
        existing = await session.execute(
            text("SELECT ticker FROM de_global_instrument_master WHERE instrument_type = 'etf'")
        )
        existing_tickers = {r[0] for r in existing.fetchall()}

    new_etfs = [e for e in NEW_ETFS if e["ticker"] not in existing_tickers]
    print(f"Existing ETFs in master: {len(existing_tickers)}")
    print(f"New ETFs to add: {len(new_etfs)}")

    if not new_etfs:
        print("Nothing to add.")
        await engine.dispose()
        return 0

    # Step 2: Insert new instrument master rows
    async with Session() as session:
        for etf in new_etfs:
            await session.execute(
                text("""
                    INSERT INTO de_global_instrument_master
                        (ticker, name, instrument_type, exchange, currency, country, category, source)
                    VALUES (:ticker, :name, 'etf', :exchange, :currency, :country, :category, 'yfinance')
                    ON CONFLICT (ticker) DO UPDATE SET
                        name = EXCLUDED.name,
                        category = EXCLUDED.category,
                        source = EXCLUDED.source
                """),
                etf,
            )
        await session.commit()
    print(f"Inserted {len(new_etfs)} ETFs into de_global_instrument_master")

    # Step 3: Backfill OHLCV from yfinance
    tickers_to_backfill = [e["ticker"] for e in new_etfs]
    source_map: dict[str, str] = {}
    ticker_stats: dict[str, dict] = {}

    batches = [tickers_to_backfill[i:i + BATCH_SIZE] for i in range(0, len(tickers_to_backfill), BATCH_SIZE)]
    total_rows = 0

    for batch_idx, batch in enumerate(batches, 1):
        print(f"Downloading batch {batch_idx}/{len(batches)}: {batch}", flush=True)
        history = download_history(batch)

        async with Session() as session:
            for ticker, rows in history.items():
                if not rows:
                    print(f"  WARN: No data for {ticker}")
                    source_map[ticker] = "yfinance (no data)"
                    continue

                source_map[ticker] = "yfinance"
                ticker_stats[ticker] = {
                    "rows": len(rows),
                    "min_date": min(r["date"] for r in rows),
                    "max_date": max(r["date"] for r in rows),
                }

                for chunk_start in range(0, len(rows), 500):
                    chunk = rows[chunk_start:chunk_start + 500]
                    await session.execute(
                        text("""
                            INSERT INTO de_global_prices (date, ticker, open, high, low, close, volume)
                            VALUES (:date, :ticker, :open, :high, :low, :close, :volume)
                            ON CONFLICT (date, ticker) DO UPDATE SET
                                open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume
                        """),
                        chunk,
                    )
                total_rows += len(rows)
            await session.commit()

        print(f"  Batch {batch_idx} done. Running total: {total_rows} rows", flush=True)

    # Step 4: Generate inventory report
    REPORTS_DIR.mkdir(exist_ok=True)
    today = date.today().isoformat()
    report_path = REPORTS_DIR / f"etf_source_inventory_{today}.md"

    with open(report_path, "w") as f:
        f.write(f"# ETF Source Inventory — {today}\n\n")
        f.write("## fie2 compass_etf_prices check\n\n")
        f.write("fie2 `compass_etf_prices` contains 81 Indian ETFs only (NIFTYBEES, GOLDBEES, etc.).\n")
        f.write("No overlap with global ETF candidates. All new ETFs sourced from **yfinance**.\n\n")
        f.write(f"## New ETFs added: {len(new_etfs)}\n\n")
        f.write("| Ticker | Name | Source | Rows | Min Date | Max Date |\n")
        f.write("|--------|------|--------|------|----------|----------|\n")
        for etf in new_etfs:
            t = etf["ticker"]
            src = source_map.get(t, "unknown")
            stats = ticker_stats.get(t, {})
            rows_count = stats.get("rows", 0)
            min_d = stats.get("min_date", "N/A")
            max_d = stats.get("max_date", "N/A")
            f.write(f"| {t} | {etf['name']} | {src} | {rows_count} | {min_d} | {max_d} |\n")

        f.write("\n## Summary\n\n")
        f.write(f"- Total new ETFs: {len(new_etfs)}\n")
        f.write(f"- Total OHLCV rows inserted: {total_rows}\n")
        yf_count = len([v for v in source_map.values() if v == "yfinance"])
        f.write(f"- Source breakdown: fie2 = 0, yfinance = {yf_count}\n")
        f.write(f"- Previously existing ETFs in master: {len(existing_tickers)}\n")
        f.write(f"- New total ETFs in master: {len(existing_tickers) + len(new_etfs)}\n")

    print(f"\nInventory report: {report_path}")
    print(f"Total new ETFs: {len(new_etfs)}, total OHLCV rows: {total_rows}")

    await engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
