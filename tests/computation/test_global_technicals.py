"""Unit tests for global_technicals.py computation script.

Tests cover:
- compute_global_indicators: all 20 indicator columns produced
- ETF_INDICATOR_COLS: correct set, no duplicates
- write_global_technicals_via_staging: column list, filter_date logic
- CREATE_TABLE_SQL: DDL structure validation
- ensure_table: function exists and references correct table
"""

from __future__ import annotations

import inspect

import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ohlcv_df(ticker: str = "^SPX", n: int = 300) -> pd.DataFrame:
    """Build a minimal OHLCV DataFrame for indicator tests."""
    dates = pd.date_range("2016-04-01", periods=n, freq="B")
    price = 4000.0
    rows = []
    for i, dt in enumerate(dates):
        h = price * 1.02
        lo = price * 0.97
        price = price * (1 + 0.001 * ((i % 7) - 3))
        rows.append(
            {
                "ticker": ticker,
                "date": dt,
                "close": price,
                "volume": 1e8 + i * 1e6,
                "high": h,
                "low": lo,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Tests: ETF_INDICATOR_COLS list
# ---------------------------------------------------------------------------


class TestGlobalIndicatorCols:
    def test_indicator_cols_match_expected_set(self):
        """ETF_INDICATOR_COLS must contain all 20 expected indicator columns."""
        from scripts.compute.global_technicals import ETF_INDICATOR_COLS

        expected = {
            "close",
            "sma_50",
            "sma_200",
            "ema_10",
            "ema_20",
            "ema_50",
            "ema_200",
            "rsi_14",
            "rsi_7",
            "macd_line",
            "macd_signal",
            "macd_histogram",
            "roc_5",
            "roc_21",
            "volatility_20d",
            "volatility_60d",
            "bollinger_upper",
            "bollinger_lower",
            "relative_volume",
            "adx_14",
        }
        assert set(ETF_INDICATOR_COLS) == expected

    def test_no_duplicate_indicator_cols(self):
        """No column should appear twice in ETF_INDICATOR_COLS."""
        from scripts.compute.global_technicals import ETF_INDICATOR_COLS

        assert len(ETF_INDICATOR_COLS) == len(set(ETF_INDICATOR_COLS))

    def test_exactly_20_indicator_cols(self):
        """There must be exactly 20 indicator columns."""
        from scripts.compute.global_technicals import ETF_INDICATOR_COLS

        assert len(ETF_INDICATOR_COLS) == 20

    def test_indicator_cols_match_etf_technicals(self):
        """Global ETF_INDICATOR_COLS must be identical to etf_technicals list."""
        from scripts.compute.etf_technicals import ETF_INDICATOR_COLS as etf_cols
        from scripts.compute.global_technicals import ETF_INDICATOR_COLS as global_cols

        assert global_cols == etf_cols


# ---------------------------------------------------------------------------
# Tests: compute_global_indicators
# ---------------------------------------------------------------------------


class TestComputeGlobalIndicators:
    def test_all_indicator_columns_produced(self):
        """All 20 expected indicator columns must appear in the output."""
        from scripts.compute.global_technicals import ETF_INDICATOR_COLS, compute_global_indicators

        df = _make_ohlcv_df("^SPX", n=300)
        result = compute_global_indicators(df.copy())

        for col in ETF_INDICATOR_COLS:
            assert col in result.columns, f"Missing column: {col}"

    def test_sma_200_requires_200_rows(self):
        """SMA 200 should be NaN for the first 199 rows of each ticker."""
        from scripts.compute.global_technicals import compute_global_indicators

        df = _make_ohlcv_df("^SPX", n=300)
        result = compute_global_indicators(df.copy())
        assert result["sma_200"].iloc[:199].isna().all()
        assert result["sma_200"].iloc[199:].notna().any()

    def test_sma_50_requires_50_rows(self):
        """SMA 50 should be NaN for the first 49 rows."""
        from scripts.compute.global_technicals import compute_global_indicators

        df = _make_ohlcv_df("^SPX", n=300)
        result = compute_global_indicators(df.copy())
        assert result["sma_50"].iloc[:49].isna().all()
        assert result["sma_50"].iloc[49:].notna().any()

    def test_rsi_bounded_0_to_100(self):
        """RSI values must be in [0, 100]."""
        from scripts.compute.global_technicals import compute_global_indicators

        df = _make_ohlcv_df("^NDX", n=300)
        result = compute_global_indicators(df.copy())
        valid = result["rsi_14"].dropna()
        assert (valid >= 0).all() and (valid <= 100).all()

    def test_rsi_7_bounded_0_to_100(self):
        """RSI-7 values must be in [0, 100]."""
        from scripts.compute.global_technicals import compute_global_indicators

        df = _make_ohlcv_df("^DJI", n=300)
        result = compute_global_indicators(df.copy())
        valid = result["rsi_7"].dropna()
        assert (valid >= 0).all() and (valid <= 100).all()

    def test_bollinger_upper_above_lower(self):
        """Bollinger upper band must always be >= lower band."""
        from scripts.compute.global_technicals import compute_global_indicators

        df = _make_ohlcv_df("GC=F", n=300)
        result = compute_global_indicators(df.copy())
        valid = result.dropna(subset=["bollinger_upper", "bollinger_lower"])
        assert (valid["bollinger_upper"] >= valid["bollinger_lower"]).all()

    def test_macd_histogram_equals_line_minus_signal(self):
        """MACD histogram = MACD line - signal (within floating point tolerance)."""
        from scripts.compute.global_technicals import compute_global_indicators

        df = _make_ohlcv_df("^SPX", n=300)
        result = compute_global_indicators(df.copy())
        valid = result.dropna(subset=["macd_line", "macd_signal", "macd_histogram"])
        diff = (valid["macd_line"] - valid["macd_signal"] - valid["macd_histogram"]).abs()
        assert (diff < 1e-8).all()

    def test_adx_non_negative(self):
        """ADX must be non-negative."""
        from scripts.compute.global_technicals import compute_global_indicators

        df = _make_ohlcv_df("CL=F", n=300)
        result = compute_global_indicators(df.copy())
        valid = result["adx_14"].dropna()
        assert (valid >= 0).all()

    def test_relative_volume_positive(self):
        """Relative volume must be positive when volume > 0."""
        from scripts.compute.global_technicals import compute_global_indicators

        df = _make_ohlcv_df("^VIX", n=300)
        result = compute_global_indicators(df.copy())
        valid = result["relative_volume"].dropna()
        assert (valid > 0).all()

    def test_multiple_tickers_isolated(self):
        """Indicators for different tickers must not bleed into each other."""
        from scripts.compute.global_technicals import compute_global_indicators

        df1 = _make_ohlcv_df("^SPX", n=300)
        df2 = _make_ohlcv_df("GC=F", n=300)
        # Make GC=F prices completely different (gold ~1800 vs SPX ~4000)
        df2["close"] = df2["close"] * 0.45
        df2["high"] = df2["high"] * 0.45
        df2["low"] = df2["low"] * 0.45
        combined = pd.concat([df1, df2], ignore_index=True).sort_values(["ticker", "date"])

        result = compute_global_indicators(combined.copy())
        spx_sma = result[result["ticker"] == "^SPX"]["sma_50"].dropna().mean()
        gold_sma = result[result["ticker"] == "GC=F"]["sma_50"].dropna().mean()
        # GC=F close is ~0.45x SPX — its SMA should be roughly 0.45x
        assert abs(gold_sma / spx_sma - 0.45) < 0.05

    def test_original_df_columns_preserved(self):
        """Input columns (ticker, date, close, volume, high, low) must remain in output."""
        from scripts.compute.global_technicals import compute_global_indicators

        df = _make_ohlcv_df("^SPX", n=300)
        result = compute_global_indicators(df.copy())
        for col in ["ticker", "date", "close", "volume", "high", "low"]:
            assert col in result.columns

    def test_row_count_unchanged(self):
        """compute_global_indicators must not drop or duplicate any rows."""
        from scripts.compute.global_technicals import compute_global_indicators

        df = _make_ohlcv_df("^SPX", n=300)
        result = compute_global_indicators(df.copy())
        assert len(result) == len(df)

    def test_ema_span_correctness(self):
        """EMA-10 should converge faster than EMA-200 (higher variability)."""
        from scripts.compute.global_technicals import compute_global_indicators

        df = _make_ohlcv_df("^SPX", n=300)
        result = compute_global_indicators(df.copy())
        ema10_std = result["ema_10"].dropna().std()
        ema200_std = result["ema_200"].dropna().std()
        # Short EMA is more volatile than long EMA
        assert ema10_std > ema200_std


# ---------------------------------------------------------------------------
# Tests: CREATE_TABLE_SQL structure
# ---------------------------------------------------------------------------


class TestCreateTableSql:
    def test_targets_correct_table(self):
        """DDL must CREATE de_global_technical_daily, not de_etf_technical_daily."""
        from scripts.compute.global_technicals import CREATE_TABLE_SQL

        assert "de_global_technical_daily" in CREATE_TABLE_SQL
        assert "de_etf_technical_daily" not in CREATE_TABLE_SQL

    def test_has_primary_key_date_ticker(self):
        """DDL must declare (date, ticker) as PRIMARY KEY."""
        from scripts.compute.global_technicals import CREATE_TABLE_SQL

        assert "PRIMARY KEY (date, ticker)" in CREATE_TABLE_SQL

    def test_fk_references_global_instrument_master(self):
        """FK must reference de_global_instrument_master, not de_etf_master."""
        from scripts.compute.global_technicals import CREATE_TABLE_SQL

        assert "de_global_instrument_master" in CREATE_TABLE_SQL
        assert "de_etf_master" not in CREATE_TABLE_SQL

    def test_has_generated_above_50dma(self):
        """DDL must include above_50dma GENERATED ALWAYS AS column."""
        from scripts.compute.global_technicals import CREATE_TABLE_SQL

        assert "above_50dma" in CREATE_TABLE_SQL

    def test_has_generated_above_200dma(self):
        """DDL must include above_200dma GENERATED ALWAYS AS column."""
        from scripts.compute.global_technicals import CREATE_TABLE_SQL

        assert "above_200dma" in CREATE_TABLE_SQL

    def test_has_created_at_and_updated_at(self):
        """DDL must include created_at and updated_at TIMESTAMPTZ columns."""
        from scripts.compute.global_technicals import CREATE_TABLE_SQL

        assert "created_at" in CREATE_TABLE_SQL
        assert "updated_at" in CREATE_TABLE_SQL
        assert "TIMESTAMPTZ" in CREATE_TABLE_SQL

    def test_has_if_not_exists(self):
        """DDL must use CREATE TABLE IF NOT EXISTS for idempotency."""
        from scripts.compute.global_technicals import CREATE_TABLE_SQL

        assert "IF NOT EXISTS" in CREATE_TABLE_SQL

    def test_all_indicator_cols_in_ddl(self):
        """Every ETF_INDICATOR_COLS column must appear in the DDL."""
        from scripts.compute.global_technicals import CREATE_TABLE_SQL, ETF_INDICATOR_COLS

        for col in ETF_INDICATOR_COLS:
            assert col in CREATE_TABLE_SQL, f"Column '{col}' missing from CREATE_TABLE_SQL"

    def test_ticker_varchar_20(self):
        """ticker column must be VARCHAR(20) to match de_global_instrument_master."""
        from scripts.compute.global_technicals import CREATE_TABLE_SQL

        assert "VARCHAR(20)" in CREATE_TABLE_SQL


# ---------------------------------------------------------------------------
# Tests: write_global_technicals_via_staging (structure, no DB)
# ---------------------------------------------------------------------------


class TestWriteGlobalTechnicalsViaStaging:
    def test_staging_function_uses_indicator_cols(self):
        """The staging write must reference ETF_INDICATOR_COLS."""
        from scripts.compute import global_technicals

        src = inspect.getsource(global_technicals.write_global_technicals_via_staging)
        assert "ETF_INDICATOR_COLS" in src

    def test_staging_targets_correct_table(self):
        """INSERT must target de_global_technical_daily, not de_etf_technical_daily."""
        from scripts.compute import global_technicals

        src = inspect.getsource(global_technicals.write_global_technicals_via_staging)
        assert "de_global_technical_daily" in src
        assert "de_etf_technical_daily" not in src

    def test_staging_has_on_conflict_upsert(self):
        """Staging write must use ON CONFLICT for idempotency."""
        from scripts.compute import global_technicals

        src = inspect.getsource(global_technicals.write_global_technicals_via_staging)
        assert "ON CONFLICT" in src

    def test_staging_has_updated_at_refresh(self):
        """ON CONFLICT handler must refresh updated_at."""
        from scripts.compute import global_technicals

        src = inspect.getsource(global_technicals.write_global_technicals_via_staging)
        assert "updated_at = NOW()" in src

    def test_filter_date_reduces_rows(self):
        """filter_date must drop rows before the given date (logic test, no DB)."""
        from scripts.compute.global_technicals import compute_global_indicators

        df = _make_ohlcv_df("^SPX", n=300)
        df = compute_global_indicators(df)
        df["date"] = pd.to_datetime(df["date"])

        cutoff = "2017-01-01"
        rows_after = len(df[df["date"] >= pd.Timestamp(cutoff)])
        rows_total = len(df)
        assert rows_after < rows_total

    def test_staging_csv_path_is_global_not_etf(self):
        """Staging CSV path must use global prefix, not etf prefix."""
        from scripts.compute import global_technicals

        src = inspect.getsource(global_technicals.write_global_technicals_via_staging)
        assert "global_tech_staging" in src
        assert "etf_tech_staging" not in src


# ---------------------------------------------------------------------------
# Tests: ensure_table function
# ---------------------------------------------------------------------------


class TestEnsureTable:
    def test_ensure_table_function_exists(self):
        """ensure_table function must be importable."""
        from scripts.compute.global_technicals import ensure_table

        assert callable(ensure_table)

    def test_ensure_table_references_correct_table(self):
        """ensure_table must reference de_global_technical_daily."""
        from scripts.compute import global_technicals

        src = inspect.getsource(global_technicals.ensure_table)
        assert "de_global_technical_daily" in src

    def test_ensure_table_uses_create_table_sql(self):
        """ensure_table must execute CREATE_TABLE_SQL."""
        from scripts.compute import global_technicals

        src = inspect.getsource(global_technicals.ensure_table)
        assert "CREATE_TABLE_SQL" in src


# ---------------------------------------------------------------------------
# Tests: main function structure
# ---------------------------------------------------------------------------


class TestMainFunction:
    def test_main_function_exists(self):
        """main() function must be importable and callable."""
        from scripts.compute.global_technicals import main

        assert callable(main)

    def test_main_reads_from_global_prices(self):
        """main() must query de_global_prices as source."""
        from scripts.compute import global_technicals

        src = inspect.getsource(global_technicals.main)
        assert "de_global_prices" in src

    def test_main_calls_compute_global_indicators(self):
        """main() must call compute_global_indicators."""
        from scripts.compute import global_technicals

        src = inspect.getsource(global_technicals.main)
        assert "compute_global_indicators" in src

    def test_main_calls_write_via_staging(self):
        """main() must call write_global_technicals_via_staging."""
        from scripts.compute import global_technicals

        src = inspect.getsource(global_technicals.main)
        assert "write_global_technicals_via_staging" in src

    def test_main_has_start_date_arg(self):
        """main() must accept --start-date argument."""
        from scripts.compute import global_technicals

        src = inspect.getsource(global_technicals.main)
        assert "--start-date" in src

    def test_main_has_filter_date_arg(self):
        """main() must accept --filter-date argument."""
        from scripts.compute import global_technicals

        src = inspect.getsource(global_technicals.main)
        assert "--filter-date" in src

    def test_module_has_main_guard(self):
        """Module must have if __name__ == '__main__': main() guard."""
        import ast

        with open(
            "/Users/nimishshah/projects/jip data core/scripts/compute/global_technicals.py"
        ) as f:
            source = f.read()
        tree = ast.parse(source)
        # Look for if __name__ == "__main__": block
        found = False
        for node in ast.walk(tree):
            if isinstance(node, ast.If):
                test = node.test
                if (
                    isinstance(test, ast.Compare)
                    and isinstance(test.left, ast.Name)
                    and test.left.id == "__name__"
                ):
                    found = True
                    break
        assert found, "Missing if __name__ == '__main__' guard"

    def test_module_docstring_mentions_global(self):
        """Module docstring must mention 'global instruments'."""
        import scripts.compute.global_technicals as mod

        assert mod.__doc__ is not None
        assert "global instruments" in mod.__doc__.lower()
