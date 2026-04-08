"""ETF ingestion pipeline.

Workflow:
1. CREATE TABLE IF NOT EXISTS for de_etf_master, de_etf_ohlcv, de_etf_technical_daily
2. Seed de_etf_master with curated ETF universe
3. Parse local OHLCV files, bulk-load into de_etf_ohlcv via psycopg2 COPY
4. Ingest world indices into de_global_instrument_master + de_global_prices

Usage:
    python -m scripts.ingest.etf_ingest
    python -m scripts.ingest.etf_ingest --etfs-only
    python -m scripts.ingest.etf_ingest --indices-only
"""

from __future__ import annotations

import argparse
import gc
import io
import os
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2

from scripts.compute.db import get_sync_url

# ---------------------------------------------------------------------------
# Data directory
# ---------------------------------------------------------------------------

DATA_ROOT = Path(os.environ.get("ETF_DATA_DIR", "/Users/nimishshah/projects/global-pulse/data"))
NYSE_ETF_DIRS = [
    DATA_ROOT / "us" / "nyse etfs" / "1",
    DATA_ROOT / "us" / "nyse etfs" / "2",
]
NASDAQ_ETF_DIR = DATA_ROOT / "us" / "nasdaq etfs"
WORLD_INDEX_DIR = DATA_ROOT / "world" / "daily" / "world" / "indices"

# Filter: only load data on/after this date (10-year window)
MIN_DATE = "2016-04-01"

# ---------------------------------------------------------------------------
# Curated ETF universe
# ---------------------------------------------------------------------------

TIER1_US: dict[str, dict] = {
    # Broad Market
    "SPY": {
        "name": "SPDR S&P 500 ETF", "sector": "Broad Market",
        "exchange": "NYSE", "category": "broad", "benchmark": "S&P 500",
    },
    "QQQ": {
        "name": "Invesco QQQ Trust", "sector": "IT",
        "exchange": "NASDAQ", "category": "broad", "benchmark": "NASDAQ 100",
    },
    # Sector SPDRs
    "XLK": {"name": "Technology Select Sector SPDR", "sector": "IT", "exchange": "NYSE", "category": "sectoral"},
    "XLF": {
        "name": "Financial Select Sector SPDR", "sector": "Financial Services",
        "exchange": "NYSE", "category": "sectoral",
    },
    "XLV": {
        "name": "Health Care Select Sector SPDR", "sector": "Healthcare",
        "exchange": "NYSE", "category": "sectoral",
    },
    "XLE": {"name": "Energy Select Sector SPDR", "sector": "Energy", "exchange": "NYSE", "category": "sectoral"},
    "XLI": {
        "name": "Industrial Select Sector SPDR", "sector": "Infrastructure",
        "exchange": "NYSE", "category": "sectoral",
    },
    "XLB": {"name": "Materials Select Sector SPDR", "sector": "Chemicals", "exchange": "NYSE", "category": "sectoral"},
    "XLY": {
        "name": "Consumer Discretionary Select SPDR", "sector": "Consumer Durables",
        "exchange": "NYSE", "category": "sectoral",
    },
    "XLP": {"name": "Consumer Staples Select SPDR", "sector": "FMCG", "exchange": "NYSE", "category": "sectoral"},
    "XLU": {"name": "Utilities Select Sector SPDR", "sector": "Energy", "exchange": "NYSE", "category": "sectoral"},
    "XLRE": {"name": "Real Estate Select Sector SPDR", "sector": "Realty", "exchange": "NYSE", "category": "sectoral"},
    # Broad
    "IWM": {
        "name": "iShares Russell 2000 ETF", "sector": "Broad Market",
        "exchange": "NYSE", "category": "broad", "benchmark": "Russell 2000",
    },
    "DIA": {
        "name": "SPDR Dow Jones Industrial ETF", "sector": "Broad Market",
        "exchange": "NYSE", "category": "broad", "benchmark": "Dow Jones",
    },
    "VTI": {
        "name": "Vanguard Total Stock Market ETF", "sector": "Broad Market",
        "exchange": "NYSE", "category": "broad",
    },
    "VOO": {
        "name": "Vanguard S&P 500 ETF", "sector": "Broad Market",
        "exchange": "NYSE", "category": "broad", "benchmark": "S&P 500",
    },
    # Thematic / International
    "EEM": {
        "name": "iShares MSCI Emerging Markets ETF", "sector": "Emerging Markets",
        "exchange": "NYSE", "category": "thematic",
    },
    "VWO": {
        "name": "Vanguard FTSE Emerging Markets ETF", "sector": "Emerging Markets",
        "exchange": "NYSE", "category": "thematic",
    },
    "GLD": {
        "name": "SPDR Gold Shares", "sector": "Commodities",
        "exchange": "NYSE", "category": "commodity", "asset_class": "commodity",
    },
    "SLV": {
        "name": "iShares Silver Trust", "sector": "Commodities",
        "exchange": "NYSE", "category": "commodity", "asset_class": "commodity",
    },
    "TLT": {
        "name": "iShares 20+ Year Treasury Bond ETF", "sector": "Debt",
        "exchange": "NASDAQ", "category": "bond", "asset_class": "bond",
    },
    "HYG": {
        "name": "iShares iBoxx High Yield Corporate Bond ETF", "sector": "Debt",
        "exchange": "NYSE", "category": "bond", "asset_class": "bond",
    },
    # Sector deep-dive
    "SOXX": {"name": "iShares Semiconductor ETF", "sector": "IT", "exchange": "NASDAQ", "category": "sectoral"},
    "IBB": {"name": "iShares Biotechnology ETF", "sector": "Healthcare", "exchange": "NASDAQ", "category": "sectoral"},
    "XOP": {
        "name": "SPDR S&P Oil & Gas Exploration ETF", "sector": "Oil & Gas",
        "exchange": "NYSE", "category": "sectoral",
    },
    "KRE": {"name": "SPDR S&P Regional Banking ETF", "sector": "Banking", "exchange": "NYSE", "category": "sectoral"},
    "XHB": {"name": "SPDR S&P Homebuilders ETF", "sector": "Realty", "exchange": "NYSE", "category": "sectoral"},
    # Country
    "EWJ": {"name": "iShares MSCI Japan ETF", "sector": "Japan", "exchange": "NYSE", "category": "country"},
    "EWZ": {"name": "iShares MSCI Brazil ETF", "sector": "Brazil", "exchange": "NYSE", "category": "country"},
    "EWG": {"name": "iShares MSCI Germany ETF", "sector": "Germany", "exchange": "NYSE", "category": "country"},
    "FXI": {"name": "iShares China Large-Cap ETF", "sector": "China", "exchange": "NYSE", "category": "country"},
    "EWU": {"name": "iShares MSCI United Kingdom ETF", "sector": "UK", "exchange": "NYSE", "category": "country"},
    "EWA": {"name": "iShares MSCI Australia ETF", "sector": "Australia", "exchange": "NYSE", "category": "country"},
}

WORLD_INDICES: dict[str, dict] = {
    "^SPX": {"name": "S&P 500", "country": "US"},
    "^NDQ": {"name": "NASDAQ 100", "country": "US"},
    "^DJI": {"name": "Dow Jones Industrial", "country": "US"},
    "^UKX": {"name": "FTSE 100", "country": "UK"},
    "^DAX": {"name": "DAX", "country": "DE"},
    "^CAC": {"name": "CAC 40", "country": "FR"},
    "^NKX": {"name": "Nikkei 225", "country": "JP"},
    "^HSI": {"name": "Hang Seng", "country": "HK"},
    "^SHBS": {"name": "Shanghai Composite", "country": "CN"},
    "^KOSPI": {"name": "KOSPI", "country": "KR"},
}

# ---------------------------------------------------------------------------
# DDL — CREATE TABLE IF NOT EXISTS (skip Alembic for speed)
# ---------------------------------------------------------------------------

CREATE_ETF_MASTER = """
CREATE TABLE IF NOT EXISTS de_etf_master (
    ticker      VARCHAR(30) PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    exchange    VARCHAR(20) NOT NULL,
    country     VARCHAR(5) NOT NULL DEFAULT 'US',
    currency    VARCHAR(5),
    sector      VARCHAR(100),
    asset_class VARCHAR(50) DEFAULT 'equity',
    category    VARCHAR(100),
    benchmark   VARCHAR(50),
    expense_ratio NUMERIC(6,4),
    inception_date DATE,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    source      VARCHAR(20) NOT NULL DEFAULT 'stooq',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

CREATE_ETF_OHLCV = """
CREATE TABLE IF NOT EXISTS de_etf_ohlcv (
    date        DATE NOT NULL,
    ticker      VARCHAR(30) NOT NULL REFERENCES de_etf_master(ticker) ON DELETE CASCADE,
    open        NUMERIC(18,4),
    high        NUMERIC(18,4),
    low         NUMERIC(18,4),
    close       NUMERIC(18,4),
    volume      BIGINT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date, ticker)
)
"""

CREATE_ETF_OHLCV_IDX = "CREATE INDEX IF NOT EXISTS idx_etf_ohlcv_ticker ON de_etf_ohlcv(ticker)"

CREATE_ETF_TECHNICAL = """
CREATE TABLE IF NOT EXISTS de_etf_technical_daily (
    date            DATE NOT NULL,
    ticker          VARCHAR(30) NOT NULL REFERENCES de_etf_master(ticker) ON DELETE CASCADE,
    close           NUMERIC(18,4),
    sma_50          NUMERIC(18,4),
    sma_200         NUMERIC(18,4),
    ema_10          NUMERIC(18,4),
    ema_20          NUMERIC(18,4),
    ema_50          NUMERIC(18,4),
    ema_200         NUMERIC(18,4),
    rsi_14          NUMERIC(8,4),
    rsi_7           NUMERIC(8,4),
    macd_line       NUMERIC(18,4),
    macd_signal     NUMERIC(18,4),
    macd_histogram  NUMERIC(18,4),
    roc_5           NUMERIC(10,4),
    roc_21          NUMERIC(10,4),
    volatility_20d  NUMERIC(10,4),
    volatility_60d  NUMERIC(10,4),
    bollinger_upper NUMERIC(18,4),
    bollinger_lower NUMERIC(18,4),
    relative_volume NUMERIC(10,4),
    adx_14          NUMERIC(8,4),
    above_50dma     BOOLEAN GENERATED ALWAYS AS (close > sma_50) STORED,
    above_200dma    BOOLEAN GENERATED ALWAYS AS (close > sma_200) STORED,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date, ticker)
)
"""

CREATE_ETF_TECH_IDX = "CREATE INDEX IF NOT EXISTS idx_etf_tech_ticker ON de_etf_technical_daily(ticker)"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def find_etf_file(ticker: str, exchange: str) -> Optional[Path]:
    """Locate the local OHLCV file for an ETF ticker."""
    filename = ticker.lower() + ".us.txt"

    if exchange == "NASDAQ":
        p = NASDAQ_ETF_DIR / filename
        if p.exists():
            return p

    # NYSE: search both subdirs (1 and 2)
    for d in NYSE_ETF_DIRS:
        p = d / filename
        if p.exists():
            return p

    # Fallback: also check NASDAQ for NYSE-listed tickers (some cross-list)
    p = NASDAQ_ETF_DIR / filename
    if p.exists():
        return p

    return None


def parse_ohlcv_file(path: Path, ticker: str, min_date: str = MIN_DATE) -> pd.DataFrame:
    """Parse a stooq-format OHLCV file, return clean DataFrame."""
    df = pd.read_csv(
        path,
        header=0,
        names=["ticker_raw", "per", "date", "time", "open", "high", "low", "close", "volume", "openint"],
    )
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["date"])
    df = df[df["date"] >= pd.Timestamp(min_date)].copy()
    df["ticker"] = ticker
    df = df[["ticker", "date", "open", "high", "low", "close", "volume"]].copy()
    df["date"] = df["date"].dt.date
    # Cast volume to int (stooq stores as float), replace 0 with NULL
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
    df.loc[df["volume"] == 0, "volume"] = None
    return df


def copy_df_to_table(cur, df: pd.DataFrame, table: str, columns: list[str]) -> int:
    """Bulk-load a DataFrame into a PostgreSQL table via COPY."""
    out = df[columns].copy()
    # Fix: volume as float with None → convert to nullable Int64 to avoid "123.0" in CSV
    if "volume" in out.columns:
        out["volume"] = out["volume"].astype("Int64")
    buf = io.StringIO()
    out.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    col_str = ",".join(columns)
    cur.copy_expert(f"COPY {table} ({col_str}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')", buf)
    return cur.rowcount


# ---------------------------------------------------------------------------
# Step 1: Create tables
# ---------------------------------------------------------------------------


def create_tables(conn) -> None:
    cur = conn.cursor()
    print("Creating ETF tables...", flush=True)
    cur.execute(CREATE_ETF_MASTER)
    cur.execute(CREATE_ETF_OHLCV)
    cur.execute(CREATE_ETF_OHLCV_IDX)
    cur.execute(CREATE_ETF_TECHNICAL)
    cur.execute(CREATE_ETF_TECH_IDX)
    conn.commit()
    print("  Tables ready.", flush=True)


# ---------------------------------------------------------------------------
# Step 2: Seed ETF master
# ---------------------------------------------------------------------------


def seed_etf_master(conn) -> None:
    cur = conn.cursor()
    print(f"Seeding de_etf_master with {len(TIER1_US)} ETFs...", flush=True)

    for ticker, meta in TIER1_US.items():
        cur.execute(
            """
            INSERT INTO de_etf_master
                (ticker, name, exchange, country, currency, sector, asset_class, category, benchmark, source)
            VALUES (%s, %s, %s, 'US', 'USD', %s, %s, %s, %s, 'stooq')
            ON CONFLICT (ticker) DO UPDATE SET
                name = EXCLUDED.name,
                exchange = EXCLUDED.exchange,
                sector = EXCLUDED.sector,
                asset_class = EXCLUDED.asset_class,
                category = EXCLUDED.category,
                benchmark = EXCLUDED.benchmark,
                updated_at = NOW()
            """,
            (
                ticker,
                meta["name"],
                meta["exchange"],
                meta.get("sector"),
                meta.get("asset_class", "equity"),
                meta.get("category"),
                meta.get("benchmark"),
            ),
        )

    conn.commit()
    print(f"  Seeded {len(TIER1_US)} ETFs.", flush=True)


# ---------------------------------------------------------------------------
# Step 3: Ingest ETF OHLCV
# ---------------------------------------------------------------------------


def ingest_etf_ohlcv(conn) -> None:
    cur = conn.cursor()
    t0 = time.time()
    total_rows = 0
    missing = []

    print(f"Ingesting OHLCV for {len(TIER1_US)} ETFs (since {MIN_DATE})...", flush=True)

    for ticker, meta in TIER1_US.items():
        path = find_etf_file(ticker, meta["exchange"])
        if path is None:
            missing.append(ticker)
            print(f"  WARN: file not found for {ticker}", flush=True)
            continue

        df = parse_ohlcv_file(path, ticker, MIN_DATE)
        if df.empty:
            print(f"  WARN: no data after {MIN_DATE} for {ticker}", flush=True)
            continue

        # Upsert via staging
        staging = f"tmp_etf_ohlcv_{ticker.lower()}"
        cur.execute(f"DROP TABLE IF EXISTS {staging}")
        cur.execute(
            f"CREATE TEMP TABLE {staging} "
            "(ticker VARCHAR(30), date DATE, open NUMERIC(18,4), high NUMERIC(18,4), "
            "low NUMERIC(18,4), close NUMERIC(18,4), volume BIGINT)"
        )

        copy_df_to_table(cur, df, staging, ["ticker", "date", "open", "high", "low", "close", "volume"])

        cur.execute(
            f"""
            INSERT INTO de_etf_ohlcv (ticker, date, open, high, low, close, volume)
            SELECT ticker, date, open, high, low, close, volume FROM {staging}
            ON CONFLICT (date, ticker) DO UPDATE SET
                open   = EXCLUDED.open,
                high   = EXCLUDED.high,
                low    = EXCLUDED.low,
                close  = EXCLUDED.close,
                volume = EXCLUDED.volume,
                updated_at = NOW()
            """
        )
        rows = cur.rowcount
        total_rows += rows
        cur.execute(f"DROP TABLE IF EXISTS {staging}")
        conn.commit()
        print(f"  {ticker}: {rows:,} rows from {path.name}", flush=True)

    gc.collect()
    print(f"\nETF OHLCV done: {total_rows:,} rows in {time.time()-t0:.1f}s", flush=True)
    if missing:
        print(f"Missing files: {', '.join(missing)}", flush=True)


# ---------------------------------------------------------------------------
# Step 4: Ingest world indices into de_global_instrument_master + de_global_prices
# ---------------------------------------------------------------------------


def find_index_file(ticker: str) -> Optional[Path]:
    """Locate the local file for a world index ticker like ^SPX."""
    filename = ticker.lower() + ".txt"
    p = WORLD_INDEX_DIR / filename
    return p if p.exists() else None


def ingest_world_indices(conn) -> None:
    cur = conn.cursor()
    t0 = time.time()
    total_rows = 0
    missing = []

    print(f"Ingesting {len(WORLD_INDICES)} world indices...", flush=True)

    for ticker, meta in WORLD_INDICES.items():
        # Upsert master record
        cur.execute(
            """
            INSERT INTO de_global_instrument_master (ticker, name, instrument_type, currency, country, source)
            VALUES (%s, %s, 'index', 'USD', %s, 'stooq')
            ON CONFLICT (ticker) DO UPDATE SET
                name = EXCLUDED.name,
                country = EXCLUDED.country,
                updated_at = NOW()
            """,
            (ticker, meta["name"], meta["country"]),
        )

        path = find_index_file(ticker)
        if path is None:
            missing.append(ticker)
            print(f"  WARN: file not found for {ticker}", flush=True)
            continue

        df = pd.read_csv(
            path,
            header=0,
            names=["ticker_raw", "per", "date", "time", "open", "high", "low", "close", "volume", "openint"],
        )
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["date"])
        df = df[df["date"] >= pd.Timestamp(MIN_DATE)].copy()
        df["ticker"] = ticker
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
        df.loc[df["volume"] == 0, "volume"] = None
        df["date"] = df["date"].dt.date

        if df.empty:
            print(f"  WARN: no data after {MIN_DATE} for {ticker}", flush=True)
            continue

        staging = "tmp_global_idx_staging"
        cur.execute(f"DROP TABLE IF EXISTS {staging}")
        cur.execute(
            f"CREATE TEMP TABLE {staging} "
            "(ticker VARCHAR(20), date DATE, open NUMERIC(18,4), high NUMERIC(18,4), "
            "low NUMERIC(18,4), close NUMERIC(18,4), volume BIGINT)"
        )

        copy_df_to_table(cur, df[["ticker", "date", "open", "high", "low", "close", "volume"]], staging,
                         ["ticker", "date", "open", "high", "low", "close", "volume"])

        cur.execute(
            f"""
            INSERT INTO de_global_prices (ticker, date, open, high, low, close, volume)
            SELECT ticker, date, open, high, low, close, volume FROM {staging}
            ON CONFLICT (date, ticker) DO UPDATE SET
                open   = EXCLUDED.open,
                high   = EXCLUDED.high,
                low    = EXCLUDED.low,
                close  = EXCLUDED.close,
                volume = EXCLUDED.volume,
                updated_at = NOW()
            """
        )
        rows = cur.rowcount
        total_rows += rows
        cur.execute(f"DROP TABLE IF EXISTS {staging}")
        conn.commit()
        print(f"  {ticker}: {rows:,} rows", flush=True)

    print(f"\nWorld indices done: {total_rows:,} rows in {time.time()-t0:.1f}s", flush=True)
    if missing:
        print(f"Missing files: {', '.join(missing)}", flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="ETF ingestion pipeline")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--etfs-only", action="store_true", help="Skip world indices")
    group.add_argument("--indices-only", action="store_true", help="Skip ETFs")
    args = parser.parse_args()

    t_start = time.time()
    conn = psycopg2.connect(get_sync_url())
    conn.autocommit = False

    create_tables(conn)

    if not args.indices_only:
        seed_etf_master(conn)
        ingest_etf_ohlcv(conn)

    if not args.etfs_only:
        ingest_world_indices(conn)

    conn.close()
    print(f"\nAll done in {time.time()-t_start:.0f}s ({(time.time()-t_start)/60:.1f} min)", flush=True)


if __name__ == "__main__":
    main()
