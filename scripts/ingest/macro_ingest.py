"""Ingest stooq macro data files into de_macro_master + de_macro_values.

Usage:
    python scripts/ingest/macro_ingest.py [--data-dir /path/to/macro]

Data path can also be set via MACRO_DATA_DIR environment variable.
Defaults to /Users/nimishshah/Downloads/data/daily/macro.

File naming convention: {indicator4}{country2}.m.txt
  e.g. cpiyus.m.txt = CPI YoY (cpiy) for United States (us)

Values of -2 in the source data are null markers and are skipped.
OPEN/HIGH/LOW always equal CLOSE in this dataset — only CLOSE is ingested.
"""

from __future__ import annotations

import argparse
import gc
import io
import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2

# ---------------------------------------------------------------------------
# Bootstrap: load .env from repo root so DATABASE_URL_SYNC is available
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
# Default data directory (overridable via env or CLI arg)
# ---------------------------------------------------------------------------
DEFAULT_MACRO_DIR = "/Users/nimishshah/Downloads/data/daily/macro"

# ---------------------------------------------------------------------------
# Indicator → (human name, unit, frequency)
# Frequency values must match de_macro_master CHECK constraint:
#   'daily','weekly','monthly','quarterly','annual'
# The source 'event' is mapped to 'monthly' (closest valid value).
# ---------------------------------------------------------------------------
INDICATOR_MAP: dict[str, tuple[str, str, str]] = {
    "gdpq": ("GDP Quarterly", "pct", "quarterly"),
    "gdpy": ("GDP Annual", "pct", "annual"),
    "cpim": ("CPI MoM", "pct", "monthly"),
    "cpiy": ("CPI YoY", "pct", "monthly"),
    "cpcm": ("Core CPI MoM", "pct", "monthly"),
    "cpcy": ("Core CPI YoY", "pct", "monthly"),
    "unrt": ("Unemployment Rate", "pct", "monthly"),
    "inrt": ("Interest Rate", "pct", "monthly"),   # event → monthly
    "inpm": ("Industrial Production MoM", "pct", "monthly"),
    "inpy": ("Industrial Production YoY", "pct", "monthly"),
    "pmmn": ("PMI Manufacturing", "index", "monthly"),
    "pmsr": ("PMI Services", "index", "monthly"),
    "pmcp": ("PMI Composite", "index", "monthly"),
    "trbn": ("Trade Balance", "value", "monthly"),
    "expr": ("Exports", "value", "monthly"),
    "impr": ("Imports", "value", "monthly"),
    "rsam": ("Retail Sales MoM", "pct", "monthly"),
    "rsay": ("Retail Sales YoY", "pct", "monthly"),
    "ppim": ("PPI MoM", "pct", "monthly"),
    "ppiy": ("PPI YoY", "pct", "monthly"),
    "whsy": ("Wholesale Price YoY", "pct", "monthly"),
    "whim": ("Wholesale Price MoM", "pct", "monthly"),
    "nfpm": ("Non-Farm Payrolls", "value", "monthly"),
    "fdrh": ("Fed Funds Rate", "pct", "monthly"),  # event → monthly
    "fdph": ("Fed Funds Probability", "pct", "monthly"),
    "ismn": ("ISM Manufacturing", "index", "monthly"),
    "isnf": ("ISM Non-Manufacturing", "index", "monthly"),
    "hosm": ("Housing Starts", "value", "monthly"),
    "hons": ("New Home Sales", "value", "monthly"),
    "nahb": ("NAHB Housing Index", "index", "monthly"),
    "emci": ("Employment Cost Index", "pct", "quarterly"),
    "avhe": ("Average Hourly Earnings", "pct", "monthly"),
    "injc": ("Initial Jobless Claims", "value", "weekly"),
    "ctcl": ("Continuing Claims", "value", "weekly"),
    "cbci": ("Central Bank Rate", "pct", "monthly"),  # event → monthly
    "fnrs": ("Foreign Reserves", "value", "monthly"),
    "m3sy": ("M3 Money Supply YoY", "pct", "monthly"),
}

# ---------------------------------------------------------------------------
# Country code → full name
# ---------------------------------------------------------------------------
COUNTRY_MAP: dict[str, str] = {
    "us": "United States",
    "in": "India",
    "uk": "United Kingdom",
    "de": "Germany",
    "fr": "France",
    "jp": "Japan",
    "cn": "China",
    "au": "Australia",
    "ca": "Canada",
    "br": "Brazil",
    "kr": "South Korea",
    "eu": "European Union",
    "ch": "Switzerland",
    "mx": "Mexico",
    "sg": "Singapore",
    "my": "Malaysia",
    "za": "South Africa",
    "tr": "Turkey",
    "pl": "Poland",
    "se": "Sweden",
    "no": "Norway",
    "dk": "Denmark",
    "it": "Italy",
    "es": "Spain",
    "nl": "Netherlands",
    "be": "Belgium",
    "at": "Austria",
    "gr": "Greece",
    "ie": "Ireland",
    "pt": "Portugal",
    "cz": "Czech Republic",
    "hu": "Hungary",
    "ro": "Romania",
    "nz": "New Zealand",
    "ph": "Philippines",
    "sk": "Slovakia",
    "lt": "Lithuania",
    "is": "Iceland",
}

# Stooq source constant — 'manual' is the closest valid CHECK value for
# de_macro_master.source when stooq is not in the allowed set.
SOURCE = "manual"

# Null sentinel used by stooq in macro files
STOOQ_NULL = -2.0


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_ticker(stem: str) -> tuple[str, str, str] | None:
    """Parse a macro file stem (e.g. 'cpiyus') into (ticker, indicator_code, country_code).

    Convention: last 2 characters are the country code, preceding chars are
    the indicator code. Indicator codes in INDICATOR_MAP are 4 characters but
    unknown codes may be longer — we try the longest prefix match first.

    Returns None if the stem is too short to contain a valid country code.
    """
    stem = stem.lower()
    if len(stem) < 3:
        return None
    country_code = stem[-2:]
    indicator_code = stem[:-2]
    # ticker stored in DB is the full stem (e.g. 'cpiyus')
    return stem, indicator_code, country_code


def decode_indicator(
    ticker: str, indicator_code: str, country_code: str
) -> tuple[str, str, str]:
    """Return (full_name, unit, frequency) for a given indicator+country.

    Falls back to (raw_ticker, '', 'monthly') for unknown indicator codes.
    """
    country_name = COUNTRY_MAP.get(country_code, country_code.upper())

    if indicator_code in INDICATOR_MAP:
        base_name, unit, frequency = INDICATOR_MAP[indicator_code]
        full_name = f"{base_name} — {country_name}"
        return full_name, unit, frequency

    # Unknown indicator: use raw ticker as name
    full_name = f"{ticker.upper()} — {country_name}"
    return full_name, "", "monthly"


def parse_macro_file(path: Path) -> pd.DataFrame | None:
    """Parse a single stooq macro .txt file.

    Returns a DataFrame with columns [ticker, date, value] or None on error.
    Rows where value == STOOQ_NULL are dropped (missing data sentinel).
    """
    stem = path.stem  # e.g. 'cpiyus.m' — strip '.m' suffix
    if stem.endswith(".m"):
        stem = stem[:-2]

    parsed = parse_ticker(stem)
    if parsed is None:
        log.warning("Cannot parse ticker from filename: %s", path)
        return None

    ticker, _indicator_code, _country_code = parsed

    try:
        df = pd.read_csv(
            path,
            header=0,
            names=["t", "per", "date", "time", "open", "high", "low", "close", "volume", "oi"],
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

    # Parse close as numeric; drop null sentinel rows
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"])
    df = df[df["close"] != STOOQ_NULL]
    if df.empty:
        return None

    df["date"] = df["date"].dt.date
    df["ticker"] = ticker

    return df[["ticker", "date", "close"]].rename(columns={"close": "value"}).copy()


# ---------------------------------------------------------------------------
# Database helpers
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
            "DATABASE_URL_SYNC or DATABASE_URL must be set. "
            "Check your .env file or environment."
        )
    return url


def upsert_macro_master(cur: "psycopg2.extensions.cursor", records: list[dict]) -> int:
    """Upsert a list of macro master records.

    Each record: {ticker, name, source, unit, frequency}
    Returns count of rows processed.
    """
    if not records:
        return 0
    sql = """
        INSERT INTO de_macro_master (ticker, name, source, unit, frequency)
        VALUES (%(ticker)s, %(name)s, %(source)s, %(unit)s, %(frequency)s)
        ON CONFLICT (ticker) DO UPDATE
            SET name      = EXCLUDED.name,
                unit      = EXCLUDED.unit,
                frequency = EXCLUDED.frequency,
                updated_at = NOW()
        WHERE de_macro_master.name      IS DISTINCT FROM EXCLUDED.name
           OR de_macro_master.unit      IS DISTINCT FROM EXCLUDED.unit
           OR de_macro_master.frequency IS DISTINCT FROM EXCLUDED.frequency
    """
    cur.executemany(sql, records)
    return len(records)


def bulk_upsert_values(
    cur: "psycopg2.extensions.cursor",
    df: pd.DataFrame,
    staging_table: str = "tmp_macro_values_stage",
) -> int:
    """COPY-stage df into a temp table then upsert into de_macro_values.

    df must have columns: ticker, date, value
    Returns row count inserted/updated.
    """
    if df.empty:
        return 0

    cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
    cur.execute(
        f"""CREATE TEMP TABLE {staging_table} (
            ticker VARCHAR(20),
            date   DATE,
            value  NUMERIC(18,4)
        ) ON COMMIT DROP"""
    )

    buf = io.StringIO()
    df[["ticker", "date", "value"]].to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)

    cur.copy_expert(
        f"COPY {staging_table} (ticker, date, value) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
        buf,
    )

    cur.execute(
        f"""INSERT INTO de_macro_values (date, ticker, value)
            SELECT date, ticker, value FROM {staging_table}
            ON CONFLICT (date, ticker) DO UPDATE
                SET value      = EXCLUDED.value,
                    updated_at = NOW()
            WHERE de_macro_values.value IS DISTINCT FROM EXCLUDED.value
        """
    )
    # rowcount from INSERT … ON CONFLICT is reliable in psycopg2
    inserted = cur.rowcount
    cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
    return inserted


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def scan_macro_files(data_dir: Path) -> list[Path]:
    """Return all .txt files under data_dir, recursively."""
    return sorted(data_dir.rglob("*.txt"))


def build_master_records(files: list[Path]) -> list[dict]:
    """Derive unique de_macro_master records from a list of macro files."""
    seen: dict[str, dict] = {}
    for path in files:
        stem = path.stem
        if stem.endswith(".m"):
            stem = stem[:-2]
        parsed = parse_ticker(stem)
        if parsed is None:
            continue
        ticker, indicator_code, country_code = parsed
        if ticker in seen:
            continue
        name, unit, frequency = decode_indicator(ticker, indicator_code, country_code)
        seen[ticker] = {
            "ticker": ticker,
            "name": name,
            "source": SOURCE,
            "unit": unit or None,
            "frequency": frequency,
        }
    return list(seen.values())


def main() -> None:  # noqa: C901
    parser = argparse.ArgumentParser(
        description="Ingest stooq macro files into de_macro_master + de_macro_values"
    )
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("MACRO_DATA_DIR", DEFAULT_MACRO_DIR),
        help="Root directory containing country-code subdirs with macro .txt files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse files and report row counts without writing to DB",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        log.error("Data directory does not exist: %s", data_dir)
        sys.exit(1)

    t0 = time.time()

    # ------------------------------------------------------------------ scan
    log.info("Scanning %s ...", data_dir)
    files = scan_macro_files(data_dir)
    log.info("Found %d .txt files", len(files))
    if not files:
        log.error("No .txt files found under %s", data_dir)
        sys.exit(1)

    # ---------------------------------------------------------- build master
    master_records = build_master_records(files)
    log.info("Unique tickers (de_macro_master candidates): %d", len(master_records))

    if args.dry_run:
        log.info("Dry run — skipping DB writes")
        # Still parse all files to report row counts
        total_rows = 0
        skipped = 0
        for path in files:
            df = parse_macro_file(path)
            if df is None:
                skipped += 1
                continue
            total_rows += len(df)
        log.info("Dry run complete: %d value rows across %d files (%d skipped)",
                 total_rows, len(files) - skipped, skipped)
        return

    # ------------------------------------------------------ connect to DB
    db_url = get_db_url()
    conn = psycopg2.connect(db_url)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # ---------------------------------------- upsert master records
            log.info("Upserting %d records into de_macro_master ...", len(master_records))
            upsert_macro_master(cur, master_records)
            conn.commit()
            log.info("de_macro_master upsert complete")

            # ---------------------------------------- verify master row count
            cur.execute("SELECT COUNT(*) FROM de_macro_master")
            master_count = cur.fetchone()[0]
            log.info("de_macro_master now has %d rows", master_count)

        # ------------------------------------ ingest values file by file
        total_value_rows = 0
        total_inserted = 0
        files_ok = 0
        files_skipped = 0
        unknown_tickers: list[str] = []

        log.info("Ingesting values from %d files ...", len(files))

        for path in files:
            df = parse_macro_file(path)
            if df is None:
                files_skipped += 1
                continue

            ticker = df["ticker"].iloc[0]
            row_count_before = len(df)

            with conn.cursor() as cur:
                # Only insert rows for tickers that exist in master
                # (should be all of them, but guard against any parse mismatches)
                cur.execute(
                    "SELECT ticker FROM de_macro_master WHERE ticker = %s", (ticker,)
                )
                if cur.fetchone() is None:
                    log.warning("Ticker %s not in de_macro_master — skipping %d rows", ticker, row_count_before)
                    unknown_tickers.append(ticker)
                    files_skipped += 1
                    continue

                inserted = bulk_upsert_values(cur, df)
                conn.commit()

            total_value_rows += row_count_before
            total_inserted += inserted
            files_ok += 1

            del df
            gc.collect()

        # ---------------------------------------- final verification
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT ticker) FROM de_macro_values")
            row = cur.fetchone()
            final_rows, final_tickers = row[0], row[1]

        elapsed = time.time() - t0
        log.info(
            "Ingest complete in %.1fs: %d files processed, %d skipped",
            elapsed, files_ok, files_skipped,
        )
        log.info(
            "Value rows parsed: %d | rows upserted (changed): %d",
            total_value_rows, total_inserted,
        )
        log.info(
            "de_macro_values final state: %d rows, %d distinct tickers",
            final_rows, final_tickers,
        )
        if unknown_tickers:
            log.warning("Tickers not in master (skipped): %s", ", ".join(unknown_tickers))

    except Exception:
        conn.rollback()
        log.exception("Fatal error — rolled back")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
