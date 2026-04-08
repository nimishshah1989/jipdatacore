"""Ingest downloaded stooq zip files into the database.

Supports four data categories routed to their respective tables:
  macro       → de_macro_master + de_macro_values   (same logic as macro_ingest.py)
  bonds       → de_global_instrument_master + de_global_prices  (*.b.txt files)
  commodities → de_global_instrument_master + de_global_prices  (*.f.txt files)
  etfs        → de_etf_master + de_etf_ohlcv                    (*.us.txt files)

Usage:
    python scripts/ingest/stooq_ingest.py \
        --download-dir /tmp/stooq_downloads \
        --categories macro,bonds,commodities

All category names are optional; omitting --categories processes all four.

File format (all categories):
    <TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>
    Dates: YYYYMMDD  |  Null sentinel: -2
"""

import argparse
import gc
import io
import logging
import os
import sys
import time
import zipfile
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2

# ---------------------------------------------------------------------------
# Bootstrap: load .env so DATABASE_URL_SYNC is available
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).parent.parent.parent


def _load_env() -> None:
    env_path = _REPO_ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key not in os.environ:
                os.environ[key] = val


_load_env()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_DOWNLOAD_DIR = os.environ.get("STOOQ_DOWNLOAD_DIR", "/tmp/stooq_downloads")
STOOQ_NULL = -2.0

ALL_CATEGORIES = ["macro", "bonds", "commodities", "etfs"]

# Bond ticker → (human name, country, instrument_type)
# Pattern: {tenor}Y{country_code}{type}.B where type=Y(yield)/P(price)
BOND_TICKER_MAP: dict[str, tuple[str, str, str]] = {
    "10YUSY.B": ("US 10-Year Treasury Yield", "US", "bond"),
    "10YINY.B": ("India 10-Year Government Bond Yield", "IN", "bond"),
    "10YDEY.B": ("Germany 10-Year Bund Yield", "DE", "bond"),
    "10YGBY.B": ("UK 10-Year Gilt Yield", "GB", "bond"),
    "10YJPY.B": ("Japan 10-Year JGB Yield", "JP", "bond"),
    "10YCNY.B": ("China 10-Year Bond Yield", "CN", "bond"),
    "10YAUY.B": ("Australia 10-Year Bond Yield", "AU", "bond"),
    "10YCAY.B": ("Canada 10-Year Bond Yield", "CA", "bond"),
    "10YFRY.B": ("France 10-Year OAT Yield", "FR", "bond"),
    "10YITY.B": ("Italy 10-Year BTP Yield", "IT", "bond"),
    "10YESY.B": ("Spain 10-Year Bonos Yield", "ES", "bond"),
    "10YBRY.B": ("Brazil 10-Year Bond Yield", "BR", "bond"),
    "2YUSY.B": ("US 2-Year Treasury Yield", "US", "bond"),
    "5YUSY.B": ("US 5-Year Treasury Yield", "US", "bond"),
    "30YUSY.B": ("US 30-Year Treasury Yield", "US", "bond"),
    "3MUSY.B": ("US 3-Month T-Bill Yield", "US", "bond"),
    "USYC2Y10.B": ("US 2s10s Yield Curve Spread", "US", "bond"),
}

# Commodity ticker → (human name, instrument_type)
COMMODITY_TICKER_MAP: dict[str, tuple[str, str]] = {
    "GC.F": ("Gold Futures", "commodity"),
    "SI.F": ("Silver Futures", "commodity"),
    "CL.F": ("Crude Oil WTI Futures", "commodity"),
    "BRN.F": ("Brent Crude Oil Futures", "commodity"),
    "NG.F": ("Natural Gas Futures", "commodity"),
    "HG.F": ("Copper Futures", "commodity"),
    "W.F": ("Wheat Futures", "commodity"),
    "C.F": ("Corn Futures", "commodity"),
    "S.F": ("Soybean Futures", "commodity"),
    "PL.F": ("Platinum Futures", "commodity"),
    "PA.F": ("Palladium Futures", "commodity"),
    "ZN.F": ("Zinc Futures", "commodity"),
    "AL.F": ("Aluminum Futures", "commodity"),
    "DX.F": ("US Dollar Index Futures", "commodity"),
}


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

def get_db_url() -> str:
    """Resolve sync psycopg2 connection URL from environment."""
    url = os.environ.get("DATABASE_URL_SYNC") or os.environ.get("DATABASE_URL", "")
    if url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
    elif url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql+psycopg2://", "postgresql://", 1)
    if not url:
        raise RuntimeError(
            "DATABASE_URL_SYNC or DATABASE_URL must be set. Check your .env file."
        )
    return url


# ---------------------------------------------------------------------------
# Zip extraction
# ---------------------------------------------------------------------------

def extract_zip(zip_path: Path, extract_to: Path) -> Path:
    """Extract a zip file and return the extraction directory."""
    extract_dir = extract_to / zip_path.stem
    extract_dir.mkdir(parents=True, exist_ok=True)

    log.info("Extracting %s -> %s ...", zip_path.name, extract_dir)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)

    txt_count = len(list(extract_dir.rglob("*.txt")))
    log.info("Extracted %d .txt files from %s", txt_count, zip_path.name)
    return extract_dir


def find_latest_zips(download_dir: Path, category_prefix: str) -> list[Path]:
    """Find zip files in download_dir matching the given stooq category prefix.

    Looks for files like: 2026-04-08_d_macro_txt.zip or d_macro_txt.zip
    Returns sorted list (newest first by filename).
    """
    pattern = f"*{category_prefix}*.zip"
    zips = sorted(download_dir.glob(pattern), reverse=True)
    return zips


# ---------------------------------------------------------------------------
# Stooq file parser (shared for all categories)
# ---------------------------------------------------------------------------

def parse_stooq_file(
    path: Path,
    ticker_override: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """Parse a single stooq-format .txt file.

    Returns DataFrame with columns depending on file type:
      - macro/bonds: [ticker, date, open, high, low, close, volume]
      - All values numeric; STOOQ_NULL rows preserved (caller filters).

    Returns None if file cannot be parsed or is empty.
    """
    try:
        df = pd.read_csv(
            path,
            header=0,
            names=["ticker", "per", "date", "time", "open", "high", "low", "close", "volume", "oi"],
        )
    except Exception as exc:
        log.error("Failed to read %s: %s", path, exc)
        return None

    if df.empty:
        return None

    # Parse date
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        return None

    # Numeric columns
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["date"] = df["date"].dt.date

    # Use ticker from first data row (stooq embeds it), or override
    if ticker_override:
        df["ticker"] = ticker_override
    else:
        # stooq stores ticker in column 0; normalise to uppercase
        df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()

    return df[["ticker", "date", "open", "high", "low", "close", "volume"]].copy()


# ---------------------------------------------------------------------------
# Macro ingestion (delegates to macro_ingest.py logic, self-contained here)
# ---------------------------------------------------------------------------

def ingest_macro(extract_dir: Path, conn: psycopg2.extensions.connection) -> dict:
    """Ingest macro .txt files from an extracted stooq directory."""
    # Import the proven helper functions directly from macro_ingest
    sys.path.insert(0, str(Path(__file__).parent))
    from macro_ingest import (  # type: ignore[import]
        scan_macro_files,
        build_master_records,
        upsert_macro_master,
        bulk_upsert_values,
        parse_macro_file,
    )

    # Find the macro sub-directory in extract_dir
    macro_dir = extract_dir
    # stooq extracts macro zips into a 'data/daily/macro' path
    candidates = list(extract_dir.rglob("macro"))
    if candidates:
        macro_dir = candidates[0]
        log.info("Found macro subdir: %s", macro_dir)
    else:
        log.info("No 'macro' subdir found — scanning %s directly", extract_dir)

    files = scan_macro_files(macro_dir)
    log.info("Macro files found: %d", len(files))
    if not files:
        return {"files_processed": 0, "rows_inserted": 0}

    master_records = build_master_records(files)
    log.info("Macro master candidates: %d", len(master_records))

    total_inserted = 0
    files_ok = 0
    files_skipped = 0

    with conn.cursor() as cur:
        upsert_macro_master(cur, master_records)
        conn.commit()
        log.info("Upserted %d macro master records", len(master_records))

    for path in files:
        df = parse_macro_file(path)
        if df is None:
            files_skipped += 1
            continue
        ticker = df["ticker"].iloc[0]
        with conn.cursor() as cur:
            cur.execute("SELECT ticker FROM de_macro_master WHERE ticker = %s", (ticker,))
            if cur.fetchone() is None:
                log.warning("Ticker %s not in de_macro_master — skipping", ticker)
                files_skipped += 1
                continue
            inserted = bulk_upsert_values(cur, df)
            conn.commit()
        total_inserted += inserted
        files_ok += 1
        del df
        gc.collect()

    log.info(
        "Macro ingest: %d files processed, %d skipped, %d rows inserted/updated",
        files_ok, files_skipped, total_inserted,
    )
    return {"files_processed": files_ok, "rows_inserted": total_inserted}


# ---------------------------------------------------------------------------
# Global instrument master helpers
# ---------------------------------------------------------------------------

def ensure_global_instrument_master_table(cur: psycopg2.extensions.cursor) -> None:
    """Create de_global_instrument_master if it does not exist."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS de_global_instrument_master (
            ticker          VARCHAR(30)     PRIMARY KEY,
            name            VARCHAR(200)    NOT NULL,
            instrument_type VARCHAR(30)     NOT NULL,
            country         VARCHAR(5),
            currency        VARCHAR(5),
            exchange        VARCHAR(30),
            source          VARCHAR(20)     DEFAULT 'stooq',
            is_active       BOOLEAN         DEFAULT TRUE,
            created_at      TIMESTAMPTZ     DEFAULT NOW(),
            updated_at      TIMESTAMPTZ     DEFAULT NOW()
        )
    """)
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_global_instr_type "
        "ON de_global_instrument_master(instrument_type)"
    )


def ensure_global_prices_table(cur: psycopg2.extensions.cursor) -> None:
    """Create de_global_prices if it does not exist."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS de_global_prices (
            date        DATE            NOT NULL,
            ticker      VARCHAR(30)     NOT NULL
                            REFERENCES de_global_instrument_master(ticker),
            open        NUMERIC(18,4),
            high        NUMERIC(18,4),
            low         NUMERIC(18,4),
            close       NUMERIC(18,4),
            volume      BIGINT,
            created_at  TIMESTAMPTZ     DEFAULT NOW(),
            updated_at  TIMESTAMPTZ     DEFAULT NOW(),
            PRIMARY KEY (date, ticker)
        )
    """)
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_global_prices_ticker "
        "ON de_global_prices(ticker)"
    )


def upsert_global_instruments(
    cur: psycopg2.extensions.cursor,
    records: list[dict],
) -> int:
    """Upsert records into de_global_instrument_master.

    Each record: {ticker, name, instrument_type, country, currency}
    """
    if not records:
        return 0
    sql = """
        INSERT INTO de_global_instrument_master
            (ticker, name, instrument_type, country, currency, source)
        VALUES
            (%(ticker)s, %(name)s, %(instrument_type)s, %(country)s, %(currency)s, %(source)s)
        ON CONFLICT (ticker) DO UPDATE
            SET name            = EXCLUDED.name,
                instrument_type = EXCLUDED.instrument_type,
                country         = EXCLUDED.country,
                updated_at      = NOW()
        WHERE de_global_instrument_master.name IS DISTINCT FROM EXCLUDED.name
           OR de_global_instrument_master.country IS DISTINCT FROM EXCLUDED.country
    """
    cur.executemany(sql, records)
    return len(records)


def bulk_upsert_global_prices(
    cur: psycopg2.extensions.cursor,
    df: pd.DataFrame,
    staging_table: str = "tmp_global_prices_stage",
) -> int:
    """COPY-stage df then upsert into de_global_prices.

    df must have columns: ticker, date, open, high, low, close, volume
    Returns row count inserted/updated.
    """
    if df.empty:
        return 0

    cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
    cur.execute(
        f"""CREATE TEMP TABLE {staging_table} (
            ticker  VARCHAR(30),
            date    DATE,
            open    NUMERIC(18,4),
            high    NUMERIC(18,4),
            low     NUMERIC(18,4),
            close   NUMERIC(18,4),
            volume  BIGINT
        ) ON COMMIT DROP"""
    )

    buf = io.StringIO()
    df[["ticker", "date", "open", "high", "low", "close", "volume"]].to_csv(
        buf, index=False, header=False, na_rep="\\N"
    )
    buf.seek(0)

    cur.copy_expert(
        f"COPY {staging_table} (ticker, date, open, high, low, close, volume) "
        f"FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
        buf,
    )

    cur.execute(
        f"""INSERT INTO de_global_prices (ticker, date, open, high, low, close, volume)
            SELECT ticker, date, open, high, low, close, volume FROM {staging_table}
            ON CONFLICT (date, ticker) DO UPDATE
                SET open       = EXCLUDED.open,
                    high       = EXCLUDED.high,
                    low        = EXCLUDED.low,
                    close      = EXCLUDED.close,
                    volume     = EXCLUDED.volume,
                    updated_at = NOW()
            WHERE de_global_prices.close IS DISTINCT FROM EXCLUDED.close
        """
    )
    inserted = cur.rowcount
    cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
    return inserted


# ---------------------------------------------------------------------------
# Bond ticker resolution
# ---------------------------------------------------------------------------

def resolve_bond_ticker(ticker: str) -> tuple[str, str, str]:
    """Return (name, country, instrument_type) for a bond ticker.

    Checks BOND_TICKER_MAP first; falls back to pattern parsing:
      {tenor}Y{country_code}Y.B  → "{tenor}-Year {country} Yield"
    """
    upper = ticker.upper()
    if upper in BOND_TICKER_MAP:
        return BOND_TICKER_MAP[upper]

    # Pattern parsing: e.g. "10YUSY.B" → tenor=10, country=US, type=Y(yield)
    stem = upper.replace(".B", "")
    # Find tenor prefix (digits)
    i = 0
    while i < len(stem) and (stem[i].isdigit() or stem[i] == "M"):
        i += 1
    tenor_raw = stem[:i]  # e.g. "10", "3M"
    remainder = stem[i:]   # e.g. "USY", "INY", "GBP"

    if len(remainder) >= 3:
        # Last char is type (Y=yield, P=price, R=rate); before it is country code
        bond_type_char = remainder[-1]
        country_code = remainder[-3:-1].upper()  # 2-char country code
        bond_type = "yield" if bond_type_char == "Y" else "price" if bond_type_char == "P" else "rate"
        tenor_label = f"{tenor_raw}-" if tenor_raw else ""
        name = f"{country_code} {tenor_label}Year Bond {bond_type.title()}"
        return name, country_code, "bond"

    # Last resort: use ticker as name
    return f"Bond {ticker}", "XX", "bond"


# ---------------------------------------------------------------------------
# Bond ingestion
# ---------------------------------------------------------------------------

def ingest_bonds(extract_dir: Path, conn: psycopg2.extensions.connection) -> dict:
    """Ingest bond yield .b.txt files from extracted world data directory."""
    # Find all .b.txt files (bonds have suffix .b.txt in stooq)
    bond_files = list(extract_dir.rglob("*.b.txt"))
    log.info("Bond files found: %d", len(bond_files))
    if not bond_files:
        log.info("No .b.txt files found under %s", extract_dir)
        return {"files_processed": 0, "rows_inserted": 0}

    with conn.cursor() as cur:
        ensure_global_instrument_master_table(cur)
        ensure_global_prices_table(cur)
        conn.commit()

    # Build master records from all files found
    master_records: list[dict] = []
    seen_tickers: set[str] = set()
    for path in bond_files:
        # Derive ticker from filename stem: e.g. "10yusy.b" → "10YUSY.B"
        stem = path.stem  # "10yusy.b" (without .txt)
        ticker = stem.upper()  # "10YUSY.B"
        if ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)
        name, country, instrument_type = resolve_bond_ticker(ticker)
        master_records.append({
            "ticker": ticker,
            "name": name,
            "instrument_type": instrument_type,
            "country": country,
            "currency": "USD" if country == "US" else None,
            "source": "stooq",
        })

    with conn.cursor() as cur:
        upsert_global_instruments(cur, master_records)
        conn.commit()
    log.info("Upserted %d bond master records", len(master_records))

    total_inserted = 0
    files_ok = 0
    files_skipped = 0

    for path in bond_files:
        stem = path.stem
        ticker = stem.upper()

        df = parse_stooq_file(path, ticker_override=ticker)
        if df is None:
            files_skipped += 1
            continue

        # Drop null sentinel rows; for bonds CLOSE is the yield value
        df = df.dropna(subset=["close"])
        df = df[df["close"] != STOOQ_NULL]
        if df.empty:
            files_skipped += 1
            continue

        row_count_before = len(df)
        log.debug("Bond %s: %d rows", ticker, row_count_before)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker FROM de_global_instrument_master WHERE ticker = %s",
                (ticker,),
            )
            if cur.fetchone() is None:
                log.warning("Ticker %s not in master — skipping %d rows", ticker, row_count_before)
                files_skipped += 1
                continue
            inserted = bulk_upsert_global_prices(cur, df)
            conn.commit()

        total_inserted += inserted
        files_ok += 1
        del df
        gc.collect()

    log.info(
        "Bond ingest: %d files processed, %d skipped, %d rows inserted/updated",
        files_ok, files_skipped, total_inserted,
    )
    return {"files_processed": files_ok, "rows_inserted": total_inserted}


# ---------------------------------------------------------------------------
# Commodity ingestion
# ---------------------------------------------------------------------------

def resolve_commodity_ticker(ticker: str) -> tuple[str, str]:
    """Return (name, instrument_type) for a commodity futures ticker."""
    upper = ticker.upper()
    if upper in COMMODITY_TICKER_MAP:
        return COMMODITY_TICKER_MAP[upper]
    # Unknown commodity: use ticker as name
    return f"Commodity {ticker}", "commodity"


def ingest_commodities(extract_dir: Path, conn: psycopg2.extensions.connection) -> dict:
    """Ingest commodity futures .f.txt files from extracted world data directory."""
    commodity_files = list(extract_dir.rglob("*.f.txt"))
    log.info("Commodity files found: %d", len(commodity_files))
    if not commodity_files:
        log.info("No .f.txt files found under %s", extract_dir)
        return {"files_processed": 0, "rows_inserted": 0}

    with conn.cursor() as cur:
        ensure_global_instrument_master_table(cur)
        ensure_global_prices_table(cur)
        conn.commit()

    master_records: list[dict] = []
    seen_tickers: set[str] = set()
    for path in commodity_files:
        stem = path.stem  # e.g. "gc.f"
        ticker = stem.upper()  # "GC.F"
        if ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)
        name, instrument_type = resolve_commodity_ticker(ticker)
        master_records.append({
            "ticker": ticker,
            "name": name,
            "instrument_type": instrument_type,
            "country": None,
            "currency": "USD",
            "source": "stooq",
        })

    with conn.cursor() as cur:
        upsert_global_instruments(cur, master_records)
        conn.commit()
    log.info("Upserted %d commodity master records", len(master_records))

    total_inserted = 0
    files_ok = 0
    files_skipped = 0

    for path in commodity_files:
        stem = path.stem
        ticker = stem.upper()

        df = parse_stooq_file(path, ticker_override=ticker)
        if df is None:
            files_skipped += 1
            continue

        # Drop null sentinel and rows with no close price
        df = df.dropna(subset=["close"])
        df = df[df["close"] != STOOQ_NULL]
        if df.empty:
            files_skipped += 1
            continue

        row_count_before = len(df)
        log.debug("Commodity %s: %d rows", ticker, row_count_before)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker FROM de_global_instrument_master WHERE ticker = %s",
                (ticker,),
            )
            if cur.fetchone() is None:
                log.warning("Ticker %s not in master — skipping %d rows", ticker, row_count_before)
                files_skipped += 1
                continue
            inserted = bulk_upsert_global_prices(cur, df)
            conn.commit()

        total_inserted += inserted
        files_ok += 1
        del df
        gc.collect()

    log.info(
        "Commodity ingest: %d files processed, %d skipped, %d rows inserted/updated",
        files_ok, files_skipped, total_inserted,
    )
    return {"files_processed": files_ok, "rows_inserted": total_inserted}


# ---------------------------------------------------------------------------
# ETF ingestion (reuses etf_ingest.py pattern)
# ---------------------------------------------------------------------------

def ingest_etfs(extract_dir: Path, conn: psycopg2.extensions.connection) -> dict:
    """Ingest ETF .us.txt files via de_etf_ohlcv."""
    sys.path.insert(0, str(Path(__file__).parent))
    from etf_ingest import ETFS, find_etf_file  # type: ignore[import]

    # Override ETF data dir to point at extracted stooq data
    import etf_ingest as etf_mod  # type: ignore[import]
    original_data_dir = etf_mod.DATA_DIR
    etf_mod.DATA_DIR = extract_dir

    # Ensure tables exist using etf_ingest DDL pattern
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS de_etf_master (
        ticker VARCHAR(30) PRIMARY KEY, name VARCHAR(200) NOT NULL, exchange VARCHAR(20) NOT NULL,
        country VARCHAR(5) NOT NULL, currency VARCHAR(5), sector VARCHAR(100), asset_class VARCHAR(50),
        category VARCHAR(100), benchmark VARCHAR(50), expense_ratio NUMERIC(6,4), inception_date DATE,
        is_active BOOLEAN DEFAULT TRUE, source VARCHAR(20) DEFAULT 'stooq',
        created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())""")
    cur.execute("""CREATE TABLE IF NOT EXISTS de_etf_ohlcv (
        date DATE NOT NULL, ticker VARCHAR(30) NOT NULL REFERENCES de_etf_master(ticker),
        open NUMERIC(18,4), high NUMERIC(18,4), low NUMERIC(18,4), close NUMERIC(18,4), volume BIGINT,
        created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (date, ticker))""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_etf_ohlcv_ticker ON de_etf_ohlcv(ticker)")
    conn.commit()

    # Seed master records for tickers we recognise
    for ticker, (country, sector, exchange, name) in ETFS.items():
        cur.execute(
            "INSERT INTO de_etf_master (ticker, name, exchange, country, sector, currency) "
            "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (ticker) DO UPDATE "
            "SET name=EXCLUDED.name, sector=EXCLUDED.sector, country=EXCLUDED.country, updated_at=NOW()",
            (ticker, name, exchange, country, sector, "USD"),
        )
    conn.commit()

    total_inserted = 0
    files_ok = 0
    missing = []

    for ticker, (_country, _sector, exchange, _name) in ETFS.items():
        path = find_etf_file(ticker, exchange)
        if path is None:
            missing.append(ticker)
            continue

        try:
            df = pd.read_csv(
                path, header=0,
                names=["t", "per", "date", "time", "open", "high", "low", "close", "volume", "oi"],
            )
        except Exception as exc:
            log.error("Failed to read %s: %s", path, exc)
            missing.append(ticker)
            continue

        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["date"])
        if df.empty:
            continue

        df["ticker"] = ticker
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
        df.loc[df["volume"] == 0, "volume"] = None
        df["date"] = df["date"].dt.date
        out = df[["ticker", "date", "open", "high", "low", "close", "volume"]].copy()
        out["volume"] = out["volume"].astype("Int64")

        staging = f"tmp_etf_stg_{ticker.lower().replace('-', '_')}"
        cur.execute(f"DROP TABLE IF EXISTS {staging}")
        cur.execute(
            f"CREATE TEMP TABLE {staging} "
            "(ticker VARCHAR(30), date DATE, open NUMERIC(18,4), high NUMERIC(18,4), "
            "low NUMERIC(18,4), close NUMERIC(18,4), volume BIGINT)"
        )
        buf = io.StringIO()
        out.to_csv(buf, index=False, header=False, na_rep="\\N")
        buf.seek(0)
        cur.copy_expert(
            f"COPY {staging} (ticker,date,open,high,low,close,volume) "
            f"FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
            buf,
        )
        cur.execute(
            f"INSERT INTO de_etf_ohlcv (ticker,date,open,high,low,close,volume) "
            f"SELECT * FROM {staging} "
            f"ON CONFLICT (date,ticker) DO UPDATE "
            f"SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, "
            f"    close=EXCLUDED.close, volume=EXCLUDED.volume, updated_at=NOW()"
        )
        cur.execute(f"DROP TABLE IF EXISTS {staging}")
        conn.commit()

        total_inserted += len(out)
        files_ok += 1
        del df, out
        gc.collect()

    cur.close()
    # Restore original data dir
    etf_mod.DATA_DIR = original_data_dir

    if missing:
        log.info("ETFs with missing data files (%d): %s", len(missing), ", ".join(missing[:20]))

    log.info(
        "ETF ingest: %d tickers processed, %d missing, %d rows inserted/updated",
        files_ok, len(missing), total_inserted,
    )
    return {"files_processed": files_ok, "rows_inserted": total_inserted}


# ---------------------------------------------------------------------------
# Zip dispatch
# ---------------------------------------------------------------------------

def find_and_extract_zip(download_dir: Path, stooq_category_id: str) -> Optional[Path]:
    """Find the most recent zip for a stooq category and extract it.

    Returns the extraction directory or None if no zip found.
    """
    pattern = f"*{stooq_category_id}*.zip"
    zips = sorted(download_dir.glob(pattern), reverse=True)  # newest filename first
    if not zips:
        log.warning("No zip files matching '%s' found in %s", pattern, download_dir)
        return None

    zip_path = zips[0]
    log.info("Using zip: %s", zip_path.name)
    return extract_zip(zip_path, download_dir / "extracted")


CATEGORY_TO_STOOQ_ZIP: dict[str, str] = {
    "macro": "d_macro_txt",
    "bonds": "d_world_txt",
    "commodities": "d_world_txt",
    "etfs": "d_world_txt",
}


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main() -> None:  # noqa: C901
    parser = argparse.ArgumentParser(
        description="Ingest downloaded stooq zip files into the database"
    )
    parser.add_argument(
        "--download-dir",
        default=DEFAULT_DOWNLOAD_DIR,
        help=f"Directory containing downloaded stooq zip files (default: {DEFAULT_DOWNLOAD_DIR})",
    )
    parser.add_argument(
        "--categories",
        default=",".join(ALL_CATEGORIES),
        help=(
            "Comma-separated list of categories to ingest: "
            f"{', '.join(ALL_CATEGORIES)}. Default: all."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract files and count rows without writing to DB",
    )
    args = parser.parse_args()

    download_dir = Path(args.download_dir)
    if not download_dir.exists():
        log.error("Download directory does not exist: %s", download_dir)
        sys.exit(1)

    requested_categories = [c.strip().lower() for c in args.categories.split(",") if c.strip()]
    invalid = [c for c in requested_categories if c not in ALL_CATEGORIES]
    if invalid:
        log.error("Unknown categories: %s. Valid: %s", invalid, ALL_CATEGORIES)
        sys.exit(1)

    log.info("Categories to ingest: %s", requested_categories)

    if args.dry_run:
        log.info("Dry run — extracting zips and counting rows, no DB writes")
        for category in requested_categories:
            stooq_id = CATEGORY_TO_STOOQ_ZIP[category]
            extract_dir = find_and_extract_zip(download_dir, stooq_id)
            if extract_dir is None:
                continue
            # Count files by category routing
            if category == "macro":
                files = list(extract_dir.rglob("*.m.txt"))
                log.info("Dry run [%s]: %d .m.txt files found", category, len(files))
            elif category == "bonds":
                files = list(extract_dir.rglob("*.b.txt"))
                log.info("Dry run [%s]: %d .b.txt files found", category, len(files))
            elif category == "commodities":
                files = list(extract_dir.rglob("*.f.txt"))
                log.info("Dry run [%s]: %d .f.txt files found", category, len(files))
            elif category == "etfs":
                files = list(extract_dir.rglob("*.us.txt"))
                log.info("Dry run [%s]: %d .us.txt files found", category, len(files))
        return

    t0 = time.time()
    db_url = get_db_url()
    conn = psycopg2.connect(db_url)
    conn.autocommit = False

    # Cache extracted directories to avoid double-extracting d_world_txt
    extracted_cache: dict[str, Path] = {}
    summary: dict[str, dict] = {}

    try:
        for category in requested_categories:
            stooq_id = CATEGORY_TO_STOOQ_ZIP[category]

            if stooq_id not in extracted_cache:
                extract_dir = find_and_extract_zip(download_dir, stooq_id)
                if extract_dir is None:
                    log.error("Cannot find zip for category '%s' (stooq_id=%s)", category, stooq_id)
                    summary[category] = {"error": "zip not found"}
                    continue
                extracted_cache[stooq_id] = extract_dir
            else:
                extract_dir = extracted_cache[stooq_id]

            log.info("=== Ingesting category: %s ===", category)
            try:
                if category == "macro":
                    result = ingest_macro(extract_dir, conn)
                elif category == "bonds":
                    result = ingest_bonds(extract_dir, conn)
                elif category == "commodities":
                    result = ingest_commodities(extract_dir, conn)
                elif category == "etfs":
                    result = ingest_etfs(extract_dir, conn)
                else:
                    result = {"error": f"unknown category {category}"}
            except Exception as exc:
                conn.rollback()
                log.exception("Error ingesting category '%s': %s", category, exc)
                result = {"error": str(exc)}

            summary[category] = result

    finally:
        conn.close()

    elapsed = time.time() - t0
    log.info("=== Ingest complete in %.1fs ===", elapsed)
    for cat, result in summary.items():
        if "error" in result:
            log.error("  [%s] ERROR: %s", cat, result["error"])
        else:
            log.info(
                "  [%s] files=%d  rows=%d",
                cat,
                result.get("files_processed", 0),
                result.get("rows_inserted", 0),
            )


if __name__ == "__main__":
    main()
