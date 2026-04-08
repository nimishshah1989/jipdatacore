"""Unit tests for scripts/ingest/macro_ingest.py.

Tests cover:
  - parse_ticker: valid stems, edge cases, too-short stems
  - decode_indicator: known codes, unknown codes, country name resolution
  - parse_macro_file: valid CSV, null-sentinel filtering, bad file
  - build_master_records: uniqueness, master record shape
  - scan_macro_files: finds .txt recursively

All DB-touching functions (upsert_macro_master, bulk_upsert_values) are
tested with a mock cursor — no real DB connection required.
"""

import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Patch DATABASE_URL so import does not fail in CI (no .env present)
# ---------------------------------------------------------------------------
import os
os.environ.setdefault("DATABASE_URL_SYNC", "postgresql://test:test@localhost/test")

from scripts.ingest.macro_ingest import (  # noqa: E402
    STOOQ_NULL,
    parse_ticker,
    decode_indicator,
    parse_macro_file,
    build_master_records,
    scan_macro_files,
    upsert_macro_master,
    bulk_upsert_values,
    get_db_url,
)


# ---------------------------------------------------------------------------
# parse_ticker
# ---------------------------------------------------------------------------

class TestParseTicker:
    def test_standard_us_cpi(self):
        result = parse_ticker("cpiyus")
        assert result is not None
        ticker, indicator, country = result
        assert ticker == "cpiyus"
        assert indicator == "cpiy"
        assert country == "us"

    def test_india_interest_rate(self):
        result = parse_ticker("inrtin")
        assert result is not None
        ticker, indicator, country = result
        assert ticker == "inrtin"
        assert indicator == "inrt"
        assert country == "in"

    def test_uppercase_is_lowercased(self):
        result = parse_ticker("CPIYUS")
        assert result is not None
        ticker, indicator, country = result
        assert ticker == "cpiyus"
        assert indicator == "cpiy"
        assert country == "us"

    def test_too_short_returns_none(self):
        assert parse_ticker("ab") is None
        assert parse_ticker("a") is None
        assert parse_ticker("") is None

    def test_minimum_length_three(self):
        result = parse_ticker("abc")
        assert result is not None
        ticker, indicator, country = result
        assert ticker == "abc"
        assert indicator == "a"
        assert country == "bc"

    def test_longer_unknown_indicator(self):
        # 6-char stem: 4-char indicator + 2-char country
        result = parse_ticker("ismnjp")
        assert result is not None
        _, indicator, country = result
        assert indicator == "ismn"
        assert country == "jp"


# ---------------------------------------------------------------------------
# decode_indicator
# ---------------------------------------------------------------------------

class TestDecodeIndicator:
    def test_known_indicator_known_country(self):
        name, unit, freq = decode_indicator("cpiyus", "cpiy", "us")
        assert "CPI YoY" in name
        assert "United States" in name
        assert unit == "pct"
        assert freq == "monthly"

    def test_known_indicator_unknown_country(self):
        name, unit, freq = decode_indicator("cpiyxx", "cpiy", "xx")
        assert "CPI YoY" in name
        # Unknown country code → uppercase fallback
        assert "XX" in name
        assert unit == "pct"

    def test_unknown_indicator_known_country(self):
        name, unit, freq = decode_indicator("zzzzin", "zzzz", "in")
        # Should fall back to raw ticker
        assert "ZZZZIN" in name
        assert "India" in name
        assert freq == "monthly"
        assert unit == ""

    def test_pmi_is_index_unit(self):
        _name, unit, freq = decode_indicator("pmmnde", "pmmn", "de")
        assert unit == "index"
        assert freq == "monthly"

    def test_gdpq_is_quarterly(self):
        _name, unit, freq = decode_indicator("gdpqus", "gdpq", "us")
        assert freq == "quarterly"

    def test_injc_is_weekly(self):
        _name, unit, freq = decode_indicator("injcus", "injc", "us")
        assert freq == "weekly"

    def test_event_indicators_mapped_to_monthly(self):
        # inrt, fdrh, cbci were 'event' in spec → mapped to 'monthly'
        for code in ("inrt", "fdrh", "cbci"):
            _n, _u, freq = decode_indicator(f"{code}us", code, "us")
            assert freq == "monthly", f"{code} should be monthly, got {freq}"


# ---------------------------------------------------------------------------
# parse_macro_file
# ---------------------------------------------------------------------------

SAMPLE_CSV = textwrap.dedent("""\
    <TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>
    CPIYUS.M,D,19900131,000000,5.2,5.2,5.2,5.2,0,0
    CPIYUS.M,D,19900228,000000,5.3,5.3,5.3,5.3,0,0
    CPIYUS.M,D,19900331,000000,-2,-2,-2,-2,0,0
    CPIYUS.M,D,20230131,000000,6.4,6.4,6.4,6.4,0,0
    CPIYUS.M,D,20230228,000000,bad,bad,bad,bad,0,0
""")


class TestParseMacroFile:
    def _write_sample(self, tmp_path: Path, content: str, name: str = "cpiyus.m.txt") -> Path:
        p = tmp_path / name
        p.write_text(content)
        return p

    def test_parses_valid_rows(self, tmp_path):
        path = self._write_sample(tmp_path, SAMPLE_CSV)
        df = parse_macro_file(path)
        assert df is not None
        # 5 rows in CSV: 1 null sentinel (-2), 1 bad value → 3 valid
        assert len(df) == 3

    def test_null_sentinel_removed(self, tmp_path):
        path = self._write_sample(tmp_path, SAMPLE_CSV)
        df = parse_macro_file(path)
        assert df is not None
        assert (df["value"] == STOOQ_NULL).sum() == 0

    def test_ticker_column_set(self, tmp_path):
        path = self._write_sample(tmp_path, SAMPLE_CSV)
        df = parse_macro_file(path)
        assert df is not None
        assert (df["ticker"] == "cpiyus").all()

    def test_columns_are_ticker_date_value(self, tmp_path):
        path = self._write_sample(tmp_path, SAMPLE_CSV)
        df = parse_macro_file(path)
        assert df is not None
        assert list(df.columns) == ["ticker", "date", "value"]

    def test_all_null_rows_returns_none(self, tmp_path):
        all_null = textwrap.dedent("""\
            <TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>
            UNRTUS.M,D,20200131,000000,-2,-2,-2,-2,0,0
            UNRTUS.M,D,20200229,000000,-2,-2,-2,-2,0,0
        """)
        path = self._write_sample(tmp_path, all_null, "unrtus.m.txt")
        df = parse_macro_file(path)
        assert df is None

    def test_empty_file_returns_none(self, tmp_path):
        path = self._write_sample(tmp_path, "", "ismnus.m.txt")
        df = parse_macro_file(path)
        assert df is None

    def test_missing_file_returns_none(self, tmp_path):
        path = tmp_path / "nonexistent.m.txt"
        df = parse_macro_file(path)
        assert df is None

    def test_file_without_m_suffix_parses(self, tmp_path):
        """File stem without .m still parses correctly."""
        csv = textwrap.dedent("""\
            <TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>
            UNRTDE.M,D,20230131,000000,5.5,5.5,5.5,5.5,0,0
        """)
        # Name it without .m so stem = 'unrtde'
        path = self._write_sample(tmp_path, csv, "unrtde.txt")
        df = parse_macro_file(path)
        assert df is not None
        assert df["ticker"].iloc[0] == "unrtde"

    def test_date_column_is_python_date(self, tmp_path):
        path = self._write_sample(tmp_path, SAMPLE_CSV)
        df = parse_macro_file(path)
        assert df is not None
        from datetime import date
        assert isinstance(df["date"].iloc[0], date)


# ---------------------------------------------------------------------------
# build_master_records
# ---------------------------------------------------------------------------

class TestBuildMasterRecords:
    def _make_file_list(self, stems: list[str], tmp_path: Path) -> list[Path]:
        files = []
        for s in stems:
            p = tmp_path / f"{s}.m.txt"
            p.write_text("")
            files.append(p)
        return files

    def test_unique_tickers(self, tmp_path):
        files = self._make_file_list(["cpiyus", "cpiyus", "unrtde"], tmp_path)
        records = build_master_records(files)
        tickers = [r["ticker"] for r in records]
        assert len(tickers) == len(set(tickers))
        assert "cpiyus" in tickers
        assert "unrtde" in tickers

    def test_record_has_required_keys(self, tmp_path):
        files = self._make_file_list(["cpiyus"], tmp_path)
        records = build_master_records(files)
        assert len(records) == 1
        rec = records[0]
        assert set(rec.keys()) == {"ticker", "name", "source", "unit", "frequency"}

    def test_source_is_manual(self, tmp_path):
        files = self._make_file_list(["unrtus"], tmp_path)
        records = build_master_records(files)
        assert records[0]["source"] == "manual"

    def test_known_indicator_frequency(self, tmp_path):
        files = self._make_file_list(["gdpqus"], tmp_path)
        records = build_master_records(files)
        assert records[0]["frequency"] == "quarterly"

    def test_unknown_indicator_defaults_monthly(self, tmp_path):
        files = self._make_file_list(["zzzzus"], tmp_path)
        records = build_master_records(files)
        assert records[0]["frequency"] == "monthly"

    def test_empty_unit_stored_as_none(self, tmp_path):
        """Unknown indicators have empty string unit — should be stored as None."""
        files = self._make_file_list(["zzzzus"], tmp_path)
        records = build_master_records(files)
        assert records[0]["unit"] is None

    def test_too_short_filename_skipped(self, tmp_path):
        files = self._make_file_list(["ab"], tmp_path)
        # 'ab' stem → 'a' + 'b' — only 1 char indicator, 1 char country
        # parse_ticker returns None for len < 3
        records = build_master_records(files)
        assert records == []


# ---------------------------------------------------------------------------
# scan_macro_files
# ---------------------------------------------------------------------------

class TestScanMacroFiles:
    def test_finds_txt_files_recursively(self, tmp_path):
        (tmp_path / "us").mkdir()
        (tmp_path / "us" / "cpiyus.m.txt").write_text("")
        (tmp_path / "in").mkdir()
        (tmp_path / "in" / "inrtin.m.txt").write_text("")
        (tmp_path / "notes.md").write_text("")  # should be excluded

        files = scan_macro_files(tmp_path)
        names = {f.name for f in files}
        assert "cpiyus.m.txt" in names
        assert "inrtin.m.txt" in names
        assert "notes.md" not in names

    def test_empty_directory_returns_empty(self, tmp_path):
        files = scan_macro_files(tmp_path)
        assert files == []


# ---------------------------------------------------------------------------
# upsert_macro_master (mock cursor)
# ---------------------------------------------------------------------------

class TestUpsertMacroMaster:
    def test_calls_executemany_with_records(self):
        cur = MagicMock()
        records = [
            {"ticker": "cpiyus", "name": "CPI YoY — United States", "source": "manual",
             "unit": "pct", "frequency": "monthly"},
            {"ticker": "unrtde", "name": "Unemployment Rate — Germany", "source": "manual",
             "unit": "pct", "frequency": "monthly"},
        ]
        count = upsert_macro_master(cur, records)
        assert cur.executemany.call_count == 1
        assert count == 2

    def test_empty_records_returns_zero(self):
        cur = MagicMock()
        count = upsert_macro_master(cur, [])
        assert count == 0
        cur.executemany.assert_not_called()


# ---------------------------------------------------------------------------
# bulk_upsert_values (mock cursor)
# ---------------------------------------------------------------------------

class TestBulkUpsertValues:
    def _make_df(self) -> pd.DataFrame:
        from datetime import date
        return pd.DataFrame({
            "ticker": ["cpiyus", "cpiyus", "cpiyus"],
            "date": [date(2023, 1, 31), date(2023, 2, 28), date(2023, 3, 31)],
            "value": [6.4, 6.0, 5.0],
        })

    def test_empty_df_returns_zero(self):
        cur = MagicMock()
        df = pd.DataFrame(columns=["ticker", "date", "value"])
        result = bulk_upsert_values(cur, df)
        assert result == 0
        cur.execute.assert_not_called()

    def test_creates_staging_table_and_copies(self):
        cur = MagicMock()
        cur.rowcount = 3
        df = self._make_df()
        result = bulk_upsert_values(cur, df)
        # Should have called execute at least for CREATE TEMP TABLE, COPY insert, DROP
        assert cur.execute.call_count >= 3
        cur.copy_expert.assert_called_once()
        assert result == 3

    def test_copy_expert_uses_csv_null(self):
        cur = MagicMock()
        cur.rowcount = 1
        df = self._make_df().head(1)
        bulk_upsert_values(cur, df, staging_table="tmp_test_stage")
        call_args = cur.copy_expert.call_args[0][0]
        assert "NULL '\\N'" in call_args
        assert "tmp_test_stage" in call_args


# ---------------------------------------------------------------------------
# get_db_url
# ---------------------------------------------------------------------------

class TestGetDbUrl:
    def test_returns_postgresql_url(self):
        with patch.dict(os.environ, {"DATABASE_URL_SYNC": "postgresql://u:p@host/db"}):
            url = get_db_url()
            assert url.startswith("postgresql://")

    def test_converts_asyncpg_url(self):
        with patch.dict(
            os.environ,
            {"DATABASE_URL_SYNC": "", "DATABASE_URL": "postgresql+asyncpg://u:p@host/db"},
        ):
            url = get_db_url()
            assert "asyncpg" not in url
            assert url.startswith("postgresql://")

    def test_raises_when_no_url(self):
        with patch.dict(os.environ, {"DATABASE_URL_SYNC": "", "DATABASE_URL": ""}):
            with pytest.raises(RuntimeError, match="DATABASE_URL"):
                get_db_url()


# ---------------------------------------------------------------------------
# Integration: parse + build_master_records round-trip (no DB)
# ---------------------------------------------------------------------------

class TestRoundTrip:
    def test_parse_and_master_consistency(self, tmp_path):
        """Parsed file ticker matches the master record built from the same file."""
        csv = textwrap.dedent("""\
            <TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>
            UNRTIN.M,D,20230131,000000,7.5,7.5,7.5,7.5,0,0
            UNRTIN.M,D,20230228,000000,7.4,7.4,7.4,7.4,0,0
        """)
        path = tmp_path / "unrtin.m.txt"
        path.write_text(csv)

        df = parse_macro_file(path)
        assert df is not None
        assert len(df) == 2

        records = build_master_records([path])
        assert len(records) == 1
        rec = records[0]

        # Ticker in data matches master
        assert (df["ticker"] == rec["ticker"]).all()
        assert rec["ticker"] == "unrtin"
        assert "Unemployment Rate" in rec["name"]
        assert "India" in rec["name"]
        assert rec["frequency"] == "monthly"
        assert rec["unit"] == "pct"
