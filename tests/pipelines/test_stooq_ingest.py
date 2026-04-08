"""Unit tests for scripts/ingest/stooq_ingest.py.

Tests cover:
  - parse_stooq_file: valid CSV, empty file, bad date, null sentinel preservation
  - resolve_bond_ticker: known tickers, pattern parsing, fallback
  - resolve_commodity_ticker: known tickers, unknown fallback
  - bulk_upsert_global_prices: mock cursor verifies COPY + INSERT called
  - upsert_global_instruments: mock cursor verifies executemany called
  - ensure_global_instrument_master_table / ensure_global_prices_table: DDL executed
  - get_db_url: env var resolution
  - find_latest_zips: finds matching files, returns sorted list
  - extract_zip: extracts to expected directory
  - CATEGORY_TO_STOOQ_ZIP mapping completeness
"""

import io
import os
import textwrap
import zipfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

os.environ.setdefault("DATABASE_URL_SYNC", "postgresql://test:test@localhost/test")

from scripts.ingest.stooq_ingest import (
    BOND_TICKER_MAP,
    CATEGORY_TO_STOOQ_ZIP,
    COMMODITY_TICKER_MAP,
    STOOQ_NULL,
    get_db_url,
    resolve_bond_ticker,
    resolve_commodity_ticker,
    parse_stooq_file,
    upsert_global_instruments,
    bulk_upsert_global_prices,
    ensure_global_instrument_master_table,
    ensure_global_prices_table,
    find_latest_zips,
    extract_zip,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_stooq_txt(tmp_path: Path, filename: str, rows: list[str]) -> Path:
    """Write a stooq-format .txt file with given data rows."""
    header = "TICKER,PER,DATE,TIME,OPEN,HIGH,LOW,CLOSE,VOL,OPENINT"
    content = header + "\n" + "\n".join(rows)
    p = tmp_path / filename
    p.write_text(content)
    return p


# ---------------------------------------------------------------------------
# parse_stooq_file
# ---------------------------------------------------------------------------

class TestParseStooqFile:
    def test_valid_bond_file(self, tmp_path):
        path = make_stooq_txt(tmp_path, "10yusy.b.txt", [
            "10YUSY.B,D,20240101,000000,4.50,4.55,4.45,4.50,0,0",
            "10YUSY.B,D,20240102,000000,4.51,4.60,4.48,4.53,0,0",
        ])
        df = parse_stooq_file(path, ticker_override="10YUSY.B")
        assert df is not None
        assert len(df) == 2
        assert list(df.columns) == ["ticker", "date", "open", "high", "low", "close", "volume"]
        assert df["ticker"].iloc[0] == "10YUSY.B"
        assert df["close"].iloc[0] == pytest.approx(4.50)

    def test_valid_commodity_file(self, tmp_path):
        path = make_stooq_txt(tmp_path, "gc.f.txt", [
            "GC.F,D,20240101,000000,2000.0,2020.0,1995.0,2010.0,12345,0",
        ])
        df = parse_stooq_file(path, ticker_override="GC.F")
        assert df is not None
        assert len(df) == 1
        assert df["ticker"].iloc[0] == "GC.F"
        assert df["high"].iloc[0] == pytest.approx(2020.0)

    def test_null_sentinel_rows_preserved_for_caller(self, tmp_path):
        """parse_stooq_file does NOT filter nulls — caller must do that."""
        path = make_stooq_txt(tmp_path, "test.b.txt", [
            "TEST.B,D,20240101,000000,-2,-2,-2,-2,0,0",
            "TEST.B,D,20240102,000000,4.5,4.6,4.4,4.5,0,0",
        ])
        df = parse_stooq_file(path, ticker_override="TEST.B")
        assert df is not None
        # -2 rows are NOT dropped by parse_stooq_file; both rows present
        assert len(df) == 2

    def test_empty_file_returns_none(self, tmp_path):
        path = tmp_path / "empty.b.txt"
        path.write_text("TICKER,PER,DATE,TIME,OPEN,HIGH,LOW,CLOSE,VOL,OPENINT\n")
        result = parse_stooq_file(path, ticker_override="EMPTY.B")
        assert result is None

    def test_bad_date_rows_dropped(self, tmp_path):
        path = make_stooq_txt(tmp_path, "test.f.txt", [
            "GC.F,D,BADDATE,000000,2000.0,2020.0,1995.0,2010.0,0,0",
            "GC.F,D,20240101,000000,2000.0,2020.0,1995.0,2010.0,0,0",
        ])
        df = parse_stooq_file(path, ticker_override="GC.F")
        assert df is not None
        assert len(df) == 1
        assert df["date"].iloc[0] == date(2024, 1, 1)

    def test_ticker_override_applied(self, tmp_path):
        path = make_stooq_txt(tmp_path, "gc.f.txt", [
            "gc.f,D,20240101,000000,2000.0,2020.0,1995.0,2010.0,0,0",
        ])
        df = parse_stooq_file(path, ticker_override="GC.F")
        assert df["ticker"].iloc[0] == "GC.F"

    def test_ticker_from_file_content_when_no_override(self, tmp_path):
        path = make_stooq_txt(tmp_path, "gc.f.txt", [
            "GC.F,D,20240101,000000,2000.0,2020.0,1995.0,2010.0,0,0",
        ])
        df = parse_stooq_file(path)
        assert df is not None
        assert df["ticker"].iloc[0] == "GC.F"

    def test_unreadable_file_returns_none(self, tmp_path):
        missing = tmp_path / "nonexistent.b.txt"
        result = parse_stooq_file(missing, ticker_override="X.B")
        assert result is None

    def test_date_column_is_python_date(self, tmp_path):
        path = make_stooq_txt(tmp_path, "10yusy.b.txt", [
            "10YUSY.B,D,20240315,000000,4.5,4.6,4.4,4.5,0,0",
        ])
        df = parse_stooq_file(path, ticker_override="10YUSY.B")
        assert df is not None
        assert df["date"].iloc[0] == date(2024, 3, 15)


# ---------------------------------------------------------------------------
# resolve_bond_ticker
# ---------------------------------------------------------------------------

class TestResolveBondTicker:
    def test_known_us_10y(self):
        name, country, instrument_type = resolve_bond_ticker("10YUSY.B")
        assert "10" in name or "US" in name
        assert country == "US"
        assert instrument_type == "bond"

    def test_known_india_10y(self):
        name, country, instrument_type = resolve_bond_ticker("10YINY.B")
        assert country == "IN"
        assert instrument_type == "bond"
        assert "India" in name or "10" in name

    def test_case_insensitive_lookup(self):
        name_upper, country_upper, _ = resolve_bond_ticker("10YUSY.B")
        name_lower, country_lower, _ = resolve_bond_ticker("10yusy.b")
        assert name_upper == name_lower
        assert country_upper == country_lower

    def test_known_germany_10y(self):
        name, country, instrument_type = resolve_bond_ticker("10YDEY.B")
        assert country == "DE"
        assert instrument_type == "bond"

    def test_unknown_ticker_falls_back_gracefully(self):
        name, country, instrument_type = resolve_bond_ticker("99XXYY.B")
        assert isinstance(name, str) and len(name) > 0
        assert isinstance(country, str)
        assert instrument_type == "bond"

    def test_all_known_tickers_have_bond_type(self):
        for ticker in BOND_TICKER_MAP:
            _, _, instrument_type = resolve_bond_ticker(ticker)
            assert instrument_type == "bond", f"{ticker} should have type 'bond'"

    def test_all_known_tickers_have_non_empty_country(self):
        for ticker in BOND_TICKER_MAP:
            _, country, _ = resolve_bond_ticker(ticker)
            assert len(country) >= 2, f"{ticker} should have country code"

    def test_pattern_parsed_short_ticker(self):
        # 2YUSY.B is in BOND_TICKER_MAP; test falls through correctly
        name, country, instrument_type = resolve_bond_ticker("2YUSY.B")
        assert instrument_type == "bond"
        assert country == "US"


# ---------------------------------------------------------------------------
# resolve_commodity_ticker
# ---------------------------------------------------------------------------

class TestResolveCommodityTicker:
    def test_known_gold(self):
        name, instrument_type = resolve_commodity_ticker("GC.F")
        assert "Gold" in name
        assert instrument_type == "commodity"

    def test_known_crude_oil(self):
        name, instrument_type = resolve_commodity_ticker("CL.F")
        assert "Crude" in name or "Oil" in name or "WTI" in name
        assert instrument_type == "commodity"

    def test_case_insensitive(self):
        name_upper, _ = resolve_commodity_ticker("GC.F")
        name_lower, _ = resolve_commodity_ticker("gc.f")
        assert name_upper == name_lower

    def test_unknown_ticker_fallback(self):
        name, instrument_type = resolve_commodity_ticker("XX.F")
        assert isinstance(name, str) and len(name) > 0
        assert instrument_type == "commodity"

    def test_all_known_tickers_return_commodity_type(self):
        for ticker in COMMODITY_TICKER_MAP:
            _, instrument_type = resolve_commodity_ticker(ticker)
            assert instrument_type == "commodity", f"{ticker} should be commodity"


# ---------------------------------------------------------------------------
# upsert_global_instruments (mock cursor)
# ---------------------------------------------------------------------------

class TestUpsertGlobalInstruments:
    def test_empty_records_returns_zero(self):
        cur = MagicMock()
        result = upsert_global_instruments(cur, [])
        assert result == 0
        cur.executemany.assert_not_called()

    def test_calls_executemany_with_correct_count(self):
        cur = MagicMock()
        records = [
            {"ticker": "10YUSY.B", "name": "US 10-Year Yield", "instrument_type": "bond",
             "country": "US", "currency": "USD", "source": "stooq"},
            {"ticker": "GC.F", "name": "Gold Futures", "instrument_type": "commodity",
             "country": None, "currency": "USD", "source": "stooq"},
        ]
        result = upsert_global_instruments(cur, records)
        assert result == 2
        cur.executemany.assert_called_once()
        # First arg is SQL, second is records
        call_args = cur.executemany.call_args
        assert len(call_args[0][1]) == 2

    def test_returns_length_of_records(self):
        cur = MagicMock()
        records = [
            {"ticker": f"T{i}.B", "name": f"Name {i}", "instrument_type": "bond",
             "country": "US", "currency": "USD", "source": "stooq"}
            for i in range(5)
        ]
        result = upsert_global_instruments(cur, records)
        assert result == 5


# ---------------------------------------------------------------------------
# bulk_upsert_global_prices (mock cursor)
# ---------------------------------------------------------------------------

class TestBulkUpsertGlobalPrices:
    def _make_df(self, n_rows: int = 3) -> pd.DataFrame:
        return pd.DataFrame({
            "ticker": ["10YUSY.B"] * n_rows,
            "date": [date(2024, 1, i + 1) for i in range(n_rows)],
            "open": [4.5] * n_rows,
            "high": [4.6] * n_rows,
            "low": [4.4] * n_rows,
            "close": [4.5] * n_rows,
            "volume": [None] * n_rows,
        })

    def test_empty_df_returns_zero(self):
        cur = MagicMock()
        df = pd.DataFrame(columns=["ticker", "date", "open", "high", "low", "close", "volume"])
        result = bulk_upsert_global_prices(cur, df)
        assert result == 0
        cur.copy_expert.assert_not_called()

    def test_calls_copy_expert(self):
        cur = MagicMock()
        cur.rowcount = 3
        df = self._make_df(3)
        result = bulk_upsert_global_prices(cur, df)
        assert cur.copy_expert.called
        copy_sql = cur.copy_expert.call_args[0][0]
        assert "COPY" in copy_sql
        assert "FROM STDIN" in copy_sql

    def test_calls_insert_on_conflict(self):
        cur = MagicMock()
        cur.rowcount = 2
        df = self._make_df(2)
        bulk_upsert_global_prices(cur, df)
        # Find the INSERT call (not CREATE TEMP TABLE or DROP)
        insert_calls = [
            str(c) for c in cur.execute.call_args_list
            if "INSERT" in str(c)
        ]
        assert len(insert_calls) >= 1
        assert "ON CONFLICT" in insert_calls[0]

    def test_creates_and_drops_staging_table(self):
        cur = MagicMock()
        cur.rowcount = 1
        df = self._make_df(1)
        bulk_upsert_global_prices(cur, df)
        all_sql = " ".join(str(c) for c in cur.execute.call_args_list)
        assert "CREATE TEMP TABLE" in all_sql
        assert "DROP TABLE" in all_sql

    def test_returns_rowcount(self):
        cur = MagicMock()
        cur.rowcount = 7
        df = self._make_df(7)
        result = bulk_upsert_global_prices(cur, df)
        assert result == 7


# ---------------------------------------------------------------------------
# ensure_* table DDL functions
# ---------------------------------------------------------------------------

class TestEnsureTableDDL:
    def test_ensure_global_instrument_master_executes_create(self):
        cur = MagicMock()
        ensure_global_instrument_master_table(cur)
        all_sql = " ".join(str(c) for c in cur.execute.call_args_list)
        assert "de_global_instrument_master" in all_sql
        assert "CREATE TABLE IF NOT EXISTS" in all_sql

    def test_ensure_global_prices_executes_create(self):
        cur = MagicMock()
        ensure_global_prices_table(cur)
        all_sql = " ".join(str(c) for c in cur.execute.call_args_list)
        assert "de_global_prices" in all_sql
        assert "CREATE TABLE IF NOT EXISTS" in all_sql

    def test_ensure_global_instrument_master_creates_index(self):
        cur = MagicMock()
        ensure_global_instrument_master_table(cur)
        all_sql = " ".join(str(c) for c in cur.execute.call_args_list)
        assert "CREATE INDEX IF NOT EXISTS" in all_sql

    def test_ensure_global_prices_creates_index(self):
        cur = MagicMock()
        ensure_global_prices_table(cur)
        all_sql = " ".join(str(c) for c in cur.execute.call_args_list)
        assert "CREATE INDEX IF NOT EXISTS" in all_sql


# ---------------------------------------------------------------------------
# get_db_url
# ---------------------------------------------------------------------------

class TestGetDbUrl:
    def test_uses_database_url_sync(self):
        with patch.dict(os.environ, {"DATABASE_URL_SYNC": "postgresql://user:pass@host/db"}):
            url = get_db_url()
        assert url == "postgresql://user:pass@host/db"

    def test_converts_asyncpg_prefix(self):
        with patch.dict(os.environ, {
            "DATABASE_URL_SYNC": "",
            "DATABASE_URL": "postgresql+asyncpg://user:pass@host/db",
        }):
            url = get_db_url()
        assert url.startswith("postgresql://")
        assert "asyncpg" not in url

    def test_converts_psycopg2_prefix(self):
        with patch.dict(os.environ, {
            "DATABASE_URL_SYNC": "postgresql+psycopg2://user:pass@host/db",
        }):
            url = get_db_url()
        assert url.startswith("postgresql://")
        assert "psycopg2" not in url

    def test_raises_when_no_url(self):
        with patch.dict(os.environ, {"DATABASE_URL_SYNC": "", "DATABASE_URL": ""}):
            with pytest.raises(RuntimeError, match="DATABASE_URL"):
                get_db_url()


# ---------------------------------------------------------------------------
# find_latest_zips
# ---------------------------------------------------------------------------

class TestFindLatestZips:
    def test_finds_matching_zips(self, tmp_path):
        (tmp_path / "2026-04-08_d_macro_txt.zip").touch()
        (tmp_path / "2026-04-07_d_macro_txt.zip").touch()
        (tmp_path / "2026-04-08_d_world_txt.zip").touch()

        result = find_latest_zips(tmp_path, "d_macro_txt")
        assert len(result) == 2
        # Newest filename first (sorted reverse)
        assert "2026-04-08" in result[0].name

    def test_returns_empty_when_no_match(self, tmp_path):
        (tmp_path / "2026-04-08_d_world_txt.zip").touch()
        result = find_latest_zips(tmp_path, "d_macro_txt")
        assert result == []

    def test_empty_directory_returns_empty(self, tmp_path):
        result = find_latest_zips(tmp_path, "d_macro_txt")
        assert result == []


# ---------------------------------------------------------------------------
# extract_zip
# ---------------------------------------------------------------------------

class TestExtractZip:
    def test_extracts_txt_files(self, tmp_path):
        # Create a minimal zip with a .txt file inside
        zip_path = tmp_path / "2026-04-08_d_macro_txt.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data/daily/macro/cpiyus.m.txt",
                        "CPIYUS,D,20240101,000000,3.1,3.1,3.1,3.1,0,0\n")
            zf.writestr("data/daily/macro/cpiyuk.m.txt",
                        "CPIYUK,D,20240101,000000,2.5,2.5,2.5,2.5,0,0\n")

        extract_dir = tmp_path / "extracted"
        result = extract_zip(zip_path, extract_dir)

        assert result.exists()
        txt_files = list(result.rglob("*.txt"))
        assert len(txt_files) == 2

    def test_extraction_dir_named_from_zip_stem(self, tmp_path):
        zip_path = tmp_path / "2026-04-08_d_macro_txt.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("test.txt", "data")

        extract_dir = tmp_path / "extracted"
        result = extract_zip(zip_path, extract_dir)
        # Result should be under extract_dir / zip stem
        assert result.parent == extract_dir
        assert "2026-04-08_d_macro_txt" in result.name


# ---------------------------------------------------------------------------
# CATEGORY_TO_STOOQ_ZIP mapping
# ---------------------------------------------------------------------------

class TestCategoryMapping:
    def test_all_categories_mapped(self):
        from scripts.ingest.stooq_ingest import ALL_CATEGORIES
        for cat in ALL_CATEGORIES:
            assert cat in CATEGORY_TO_STOOQ_ZIP, f"Category '{cat}' missing from mapping"

    def test_macro_maps_to_macro_zip(self):
        assert CATEGORY_TO_STOOQ_ZIP["macro"] == "d_macro_txt"

    def test_bonds_maps_to_world_zip(self):
        assert CATEGORY_TO_STOOQ_ZIP["bonds"] == "d_world_txt"

    def test_commodities_maps_to_world_zip(self):
        assert CATEGORY_TO_STOOQ_ZIP["commodities"] == "d_world_txt"

    def test_etfs_maps_to_world_zip(self):
        assert CATEGORY_TO_STOOQ_ZIP["etfs"] == "d_world_txt"
