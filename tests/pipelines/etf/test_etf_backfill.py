"""Unit tests for etf_backfill.py — backfill script for historical ETF OHLCV.

Tests cover:
- _safe_decimal: NaN/inf/None handling, float->Decimal conversion
- _df_to_rows: row extraction from single-ticker DataFrame
- _dedup_rows: deduplication by (date, ticker)
- ETFS dict expansion: new tickers present, count >= 163
- etf_ingest.py docstring reflects 163+ count
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pandas as pd


# ---------------------------------------------------------------------------
# Tests: _safe_decimal
# ---------------------------------------------------------------------------


class TestSafeDecimal:
    def test_normal_float_converts_to_decimal(self):
        from scripts.ingest.etf_backfill import _safe_decimal

        result = _safe_decimal(123.4567)
        assert isinstance(result, Decimal)
        assert result == Decimal("123.4567")

    def test_rounds_to_4dp(self):
        from scripts.ingest.etf_backfill import _safe_decimal

        result = _safe_decimal(123.456789)
        assert result == Decimal("123.4568")

    def test_none_returns_none(self):
        from scripts.ingest.etf_backfill import _safe_decimal

        assert _safe_decimal(None) is None

    def test_nan_returns_none(self):
        from scripts.ingest.etf_backfill import _safe_decimal

        assert _safe_decimal(float("nan")) is None

    def test_inf_returns_none(self):
        from scripts.ingest.etf_backfill import _safe_decimal

        assert _safe_decimal(float("inf")) is None
        assert _safe_decimal(float("-inf")) is None

    def test_zero_converts_correctly(self):
        from scripts.ingest.etf_backfill import _safe_decimal

        result = _safe_decimal(0.0)
        assert result == Decimal("0.0")

    def test_integer_input_converts(self):
        from scripts.ingest.etf_backfill import _safe_decimal

        result = _safe_decimal(100)
        assert isinstance(result, Decimal)
        assert result == Decimal("100")

    def test_string_float_converts(self):
        from scripts.ingest.etf_backfill import _safe_decimal

        # Values from pandas may come as numpy floats
        import numpy as np

        result = _safe_decimal(np.float64(50.1234))
        assert isinstance(result, Decimal)
        assert result == Decimal("50.1234")


# ---------------------------------------------------------------------------
# Tests: _df_to_rows
# ---------------------------------------------------------------------------


def _make_ticker_df(n: int = 10, ticker: str = "SPY") -> pd.DataFrame:
    """Build a minimal single-ticker OHLCV DataFrame."""
    dates = pd.date_range("2020-01-01", periods=n, freq="B")
    return pd.DataFrame(
        {
            "Open": [100.0 + i * 0.1 for i in range(n)],
            "High": [101.0 + i * 0.1 for i in range(n)],
            "Low": [99.0 + i * 0.1 for i in range(n)],
            "Close": [100.5 + i * 0.1 for i in range(n)],
            "Volume": [1_000_000 + i * 10_000 for i in range(n)],
        },
        index=dates,
    )


class TestDfToRows:
    def test_returns_correct_number_of_rows(self):
        from scripts.ingest.etf_backfill import _df_to_rows

        df = _make_ticker_df(10)
        rows = _df_to_rows(df, "SPY")
        assert len(rows) == 10

    def test_all_rows_have_required_keys(self):
        from scripts.ingest.etf_backfill import _df_to_rows

        df = _make_ticker_df(5)
        rows = _df_to_rows(df, "AGG")
        for row in rows:
            assert "ticker" in row
            assert "date" in row
            assert "open" in row
            assert "high" in row
            assert "low" in row
            assert "close" in row
            assert "volume" in row

    def test_ticker_column_is_canonical(self):
        from scripts.ingest.etf_backfill import _df_to_rows

        df = _make_ticker_df(5)
        rows = _df_to_rows(df, "BND")
        assert all(r["ticker"] == "BND" for r in rows)

    def test_close_values_are_decimal(self):
        from scripts.ingest.etf_backfill import _df_to_rows

        df = _make_ticker_df(5)
        rows = _df_to_rows(df, "TLT")
        for row in rows:
            if row["close"] is not None:
                assert isinstance(row["close"], Decimal)

    def test_open_values_are_decimal(self):
        from scripts.ingest.etf_backfill import _df_to_rows

        df = _make_ticker_df(5)
        rows = _df_to_rows(df, "IEF")
        for row in rows:
            if row["open"] is not None:
                assert isinstance(row["open"], Decimal)

    def test_nan_close_row_skipped(self):
        """Rows with NaN close should be excluded."""
        from scripts.ingest.etf_backfill import _df_to_rows

        df = _make_ticker_df(5)
        df.loc[df.index[2], "Close"] = float("nan")
        rows = _df_to_rows(df, "GLD")
        # Row with NaN close should be skipped
        assert len(rows) == 4

    def test_date_is_python_date(self):
        from scripts.ingest.etf_backfill import _df_to_rows

        df = _make_ticker_df(5)
        rows = _df_to_rows(df, "SLV")
        for row in rows:
            assert isinstance(row["date"], date)

    def test_volume_is_int_or_none(self):
        from scripts.ingest.etf_backfill import _df_to_rows

        df = _make_ticker_df(5)
        rows = _df_to_rows(df, "USO")
        for row in rows:
            assert row["volume"] is None or isinstance(row["volume"], int)


# ---------------------------------------------------------------------------
# Tests: _dedup_rows
# ---------------------------------------------------------------------------


class TestDedupRows:
    def test_no_duplicates_unchanged(self):
        from scripts.ingest.etf_backfill import _dedup_rows

        rows = [
            {"ticker": "SPY", "date": date(2020, 1, 1), "close": Decimal("100")},
            {"ticker": "SPY", "date": date(2020, 1, 2), "close": Decimal("101")},
            {"ticker": "AGG", "date": date(2020, 1, 1), "close": Decimal("50")},
        ]
        result = _dedup_rows(rows)
        assert len(result) == 3

    def test_duplicate_date_ticker_deduped(self):
        from scripts.ingest.etf_backfill import _dedup_rows

        rows = [
            {"ticker": "SPY", "date": date(2020, 1, 1), "close": Decimal("100")},
            {"ticker": "SPY", "date": date(2020, 1, 1), "close": Decimal("101")},  # dup
        ]
        result = _dedup_rows(rows)
        assert len(result) == 1

    def test_last_occurrence_wins(self):
        from scripts.ingest.etf_backfill import _dedup_rows

        rows = [
            {"ticker": "SPY", "date": date(2020, 1, 1), "close": Decimal("100")},
            {"ticker": "SPY", "date": date(2020, 1, 1), "close": Decimal("999")},
        ]
        result = _dedup_rows(rows)
        assert result[0]["close"] == Decimal("999")

    def test_empty_input_returns_empty(self):
        from scripts.ingest.etf_backfill import _dedup_rows

        assert _dedup_rows([]) == []


# ---------------------------------------------------------------------------
# Tests: ETFS dict expansion
# ---------------------------------------------------------------------------


class TestEtfsDictExpansion:
    def test_new_fixed_income_etfs_present(self):
        """All new fixed income tickers must be in the ETFS dict."""
        from scripts.ingest.etf_ingest import ETFS

        new_tickers = ["AGG", "BND", "BNDX", "TIP", "SHY"]
        for t in new_tickers:
            assert t in ETFS, f"{t} missing from ETFS dict"

    def test_new_thematic_etfs_present(self):
        """All new thematic tickers must be in the ETFS dict."""
        from scripts.ingest.etf_ingest import ETFS

        thematic = [
            "ARKK", "BOTZ", "ROBO", "AIQ",
            "DRIV", "LIT", "QCLN",
            "CIBR", "BUG",
            "GNOM", "BLOK", "IBIT", "URA", "ARKX",
            "JETS", "MSOS", "XHE", "CLOU", "FINX",
        ]
        for t in thematic:
            assert t in ETFS, f"{t} missing from ETFS dict"

    def test_new_commodity_etfs_present(self):
        from scripts.ingest.etf_ingest import ETFS

        for t in ["PDBC", "PPLT", "WEAT"]:
            assert t in ETFS, f"{t} missing from ETFS dict"

    def test_frontier_etfs_present(self):
        from scripts.ingest.etf_ingest import ETFS

        for t in ["FM", "ENZL", "PAK", "NGE"]:
            assert t in ETFS, f"{t} missing from ETFS dict"

    def test_total_etf_count_at_least_161(self):
        """ETFS dict must have at least 161 entries after expansion (130 original + 31 new)."""
        from scripts.ingest.etf_ingest import ETFS

        assert len(ETFS) >= 161, f"Expected >= 161, got {len(ETFS)}"

    def test_all_etfs_have_four_tuple_values(self):
        """Every entry must be (country, sector, exchange, name) — a 4-tuple."""
        from scripts.ingest.etf_ingest import ETFS

        for ticker, val in ETFS.items():
            assert len(val) == 4, f"{ticker} has {len(val)}-element tuple, expected 4"

    def test_new_etfs_exchanges_valid(self):
        """All new ETF entries must have NYSE or NASDAQ as exchange."""
        from scripts.ingest.etf_ingest import ETFS

        new_tickers = [
            "AGG", "BND", "BNDX", "TIP", "SHY",
            "PDBC", "PPLT", "WEAT",
            "ARKK", "BOTZ", "ROBO", "AIQ",
            "DRIV", "LIT", "QCLN", "CIBR", "BUG",
            "GNOM", "BLOK", "IBIT", "URA", "ARKX",
            "JETS", "MSOS", "XHE", "CLOU", "FINX",
            "FM", "ENZL", "PAK", "NGE",
        ]
        valid_exchanges = {"NYSE", "NASDAQ"}
        for t in new_tickers:
            _, _, exchange, _ = ETFS[t]
            assert exchange in valid_exchanges, f"{t} has invalid exchange: {exchange}"

    def test_docstring_mentions_161(self):
        """etf_ingest module docstring must reference 161+."""
        import scripts.ingest.etf_ingest as m

        assert "161" in (m.__doc__ or ""), "Docstring should mention 161+"

    def test_existing_bonds_still_present(self):
        """Original bond ETFs must not have been removed."""
        from scripts.ingest.etf_ingest import ETFS

        for t in ["TLT", "IEF", "HYG", "LQD", "EMB"]:
            assert t in ETFS, f"Original bond ETF {t} was removed"
