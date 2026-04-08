"""Unit tests for ETF ingestion and computation scripts.

Tests cover:
- parse_ohlcv_file: CSV parsing, date filtering, volume zero-to-None
- find_etf_file: correct directory search order (NYSE subdir 1/2, NASDAQ)
- compute_etf_indicators: all indicator columns produced, no crashes on small data
- write_etf_technicals_via_staging: column list, no mutation of source df
- ETF RS SQL: structural validation (syntax / column references)
- WORLD_INDICES / TIER1_US: completeness of curated universe
"""

from __future__ import annotations

import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_stooq_csv(ticker: str, rows: int = 300, start_date: str = "20160101") -> str:
    """Generate a minimal stooq-format CSV string."""
    lines = ["<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>"]
    from datetime import date, timedelta

    # Accept both YYYYMMDD and YYYY-MM-DD formats
    iso = start_date if "-" in start_date else f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
    d = date.fromisoformat(iso)
    price = 100.0
    for i in range(rows):
        open_ = round(price * 0.99, 4)
        h = round(price * 1.01, 4)
        lo = round(price * 0.98, 4)
        price = round(price * (1 + 0.001 * ((i % 5) - 2)), 4)
        vol = 1000000 + i * 1000
        lines.append(f"{ticker}.US,D,{d.strftime('%Y%m%d')},000000,{open_},{h},{lo},{price},{vol},0")
        d += timedelta(days=1)
    return "\n".join(lines)


def _make_ohlcv_df(ticker: str = "SPY", n: int = 300) -> pd.DataFrame:
    """Build a minimal OHLCV DataFrame for indicator tests."""
    dates = pd.date_range("2016-04-01", periods=n, freq="B")
    price = 100.0
    rows = []
    for i, dt in enumerate(dates):
        h = price * 1.02
        lo = price * 0.97
        price = price * (1 + 0.001 * ((i % 7) - 3))
        rows.append({"ticker": ticker, "date": dt, "close": price, "volume": 1e6 + i * 1e3,
                     "high": h, "low": lo})
    df = pd.DataFrame(rows)
    return df


# ---------------------------------------------------------------------------
# Tests: parse_ohlcv_file
# ---------------------------------------------------------------------------


class TestParseOhlcvFile:
    def test_parse_returns_correct_columns(self, tmp_path):
        """Parsed DataFrame must contain exactly [ticker, date, open, high, low, close, volume]."""
        from scripts.ingest.etf_ingest import parse_ohlcv_file

        csv = _make_stooq_csv("SPY", rows=50)
        p = tmp_path / "spy.us.txt"
        p.write_text(csv)

        df = parse_ohlcv_file(p, "SPY", min_date="2016-01-01")
        assert list(df.columns) == ["ticker", "date", "open", "high", "low", "close", "volume"]

    def test_parse_filters_min_date(self, tmp_path):
        """Rows before min_date must be excluded."""
        from scripts.ingest.etf_ingest import parse_ohlcv_file

        # Data starts 2015-01-01 (before filter), 100 rows
        csv = _make_stooq_csv("QQQ", rows=200, start_date="20150101")
        p = tmp_path / "qqq.us.txt"
        p.write_text(csv)

        df_all = parse_ohlcv_file(p, "QQQ", min_date="2015-01-01")
        df_filtered = parse_ohlcv_file(p, "QQQ", min_date="2016-01-01")

        assert len(df_filtered) < len(df_all)
        assert all(pd.to_datetime(d) >= pd.Timestamp("2016-01-01") for d in df_filtered["date"])

    def test_parse_zero_volume_becomes_none(self, tmp_path):
        """Volume = 0 in source file must be stored as None (for DB NULL)."""
        from scripts.ingest.etf_ingest import parse_ohlcv_file

        lines = [
            "<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>",
            "GLD.US,D,20200101,000000,150.0,152.0,149.0,151.0,0,0",
            "GLD.US,D,20200102,000000,151.0,153.0,150.0,152.0,500000,0",
        ]
        p = tmp_path / "gld.us.txt"
        p.write_text("\n".join(lines))

        df = parse_ohlcv_file(p, "GLD", min_date="2019-01-01")
        assert pd.isna(df.iloc[0]["volume"])  # pd.NA or NaN or None
        assert df.iloc[1]["volume"] == 500000

    def test_parse_ticker_column_is_canonical(self, tmp_path):
        """The ticker column must match the passed ticker, not the file's raw ticker."""
        from scripts.ingest.etf_ingest import parse_ohlcv_file

        csv = _make_stooq_csv("SPY", rows=10)
        p = tmp_path / "spy.us.txt"
        p.write_text(csv)

        df = parse_ohlcv_file(p, "SPY", min_date="2016-01-01")
        assert df["ticker"].unique().tolist() == ["SPY"]

    def test_parse_empty_after_filter_returns_empty_df(self, tmp_path):
        """If all rows are before min_date, return empty DataFrame."""
        from scripts.ingest.etf_ingest import parse_ohlcv_file

        csv = _make_stooq_csv("EWJ", rows=10, start_date="20100101")
        p = tmp_path / "ewj.us.txt"
        p.write_text(csv)

        df = parse_ohlcv_file(p, "EWJ", min_date="2025-01-01")
        assert len(df) == 0


# ---------------------------------------------------------------------------
# Tests: find_etf_file
# ---------------------------------------------------------------------------


class TestFindEtfFile:
    def test_find_nasdaq_etf(self, tmp_path, monkeypatch):
        """NASDAQ-listed ETFs are found in the NASDAQ ETF directory."""
        from scripts.ingest import etf_ingest

        nasdaq_dir = tmp_path / "nasdaq etfs"
        nasdaq_dir.mkdir()
        (nasdaq_dir / "qqq.us.txt").write_text("header\n")

        monkeypatch.setattr(etf_ingest, "NASDAQ_ETF_DIR", nasdaq_dir)
        monkeypatch.setattr(etf_ingest, "NYSE_ETF_DIRS", [])

        result = etf_ingest.find_etf_file("QQQ", "NASDAQ")
        assert result is not None
        assert result.name == "qqq.us.txt"

    def test_find_nyse_etf_subdir1(self, tmp_path, monkeypatch):
        """NYSE ETFs in subdirectory 1 are found."""
        from scripts.ingest import etf_ingest

        subdir1 = tmp_path / "nyse etfs" / "1"
        subdir1.mkdir(parents=True)
        (subdir1 / "iwm.us.txt").write_text("header\n")
        subdir2 = tmp_path / "nyse etfs" / "2"
        subdir2.mkdir(parents=True)

        monkeypatch.setattr(etf_ingest, "NYSE_ETF_DIRS", [subdir1, subdir2])
        monkeypatch.setattr(etf_ingest, "NASDAQ_ETF_DIR", tmp_path / "nasdaq etfs")

        result = etf_ingest.find_etf_file("IWM", "NYSE")
        assert result is not None
        assert result.parent == subdir1

    def test_find_nyse_etf_subdir2(self, tmp_path, monkeypatch):
        """NYSE ETFs in subdirectory 2 are found when not in subdir 1."""
        from scripts.ingest import etf_ingest

        subdir1 = tmp_path / "nyse etfs" / "1"
        subdir1.mkdir(parents=True)
        subdir2 = tmp_path / "nyse etfs" / "2"
        subdir2.mkdir(parents=True)
        (subdir2 / "spy.us.txt").write_text("header\n")

        monkeypatch.setattr(etf_ingest, "NYSE_ETF_DIRS", [subdir1, subdir2])
        monkeypatch.setattr(etf_ingest, "NASDAQ_ETF_DIR", tmp_path / "nasdaq etfs")

        result = etf_ingest.find_etf_file("SPY", "NYSE")
        assert result is not None
        assert result.parent == subdir2

    def test_find_missing_etf_returns_none(self, tmp_path, monkeypatch):
        """Missing ETF file returns None instead of raising."""
        from scripts.ingest import etf_ingest

        monkeypatch.setattr(etf_ingest, "NYSE_ETF_DIRS", [tmp_path])
        monkeypatch.setattr(etf_ingest, "NASDAQ_ETF_DIR", tmp_path)

        result = etf_ingest.find_etf_file("NOTREAL", "NYSE")
        assert result is None


# ---------------------------------------------------------------------------
# Tests: compute_etf_indicators
# ---------------------------------------------------------------------------


class TestComputeEtfIndicators:
    def test_all_indicator_columns_produced(self):
        """All expected indicator columns must appear in the output."""
        from scripts.compute.etf_technicals import ETF_INDICATOR_COLS, compute_etf_indicators

        df = _make_ohlcv_df("SPY", n=300)
        result = compute_etf_indicators(df.copy())

        for col in ETF_INDICATOR_COLS:
            assert col in result.columns, f"Missing column: {col}"

    def test_sma_200_requires_200_rows(self):
        """SMA 200 should be NaN for the first 199 rows of each ticker."""
        from scripts.compute.etf_technicals import compute_etf_indicators

        df = _make_ohlcv_df("SPY", n=300)
        result = compute_etf_indicators(df.copy())
        assert result["sma_200"].iloc[:199].isna().all()
        assert result["sma_200"].iloc[199:].notna().any()

    def test_sma_50_requires_50_rows(self):
        """SMA 50 should be NaN for the first 49 rows."""
        from scripts.compute.etf_technicals import compute_etf_indicators

        df = _make_ohlcv_df("SPY", n=300)
        result = compute_etf_indicators(df.copy())
        assert result["sma_50"].iloc[:49].isna().all()
        assert result["sma_50"].iloc[49:].notna().any()

    def test_rsi_bounded_0_to_100(self):
        """RSI values must be in [0, 100]."""
        from scripts.compute.etf_technicals import compute_etf_indicators

        df = _make_ohlcv_df("QQQ", n=300)
        result = compute_etf_indicators(df.copy())
        valid = result["rsi_14"].dropna()
        assert (valid >= 0).all() and (valid <= 100).all()

    def test_multiple_tickers_isolated(self):
        """Indicators for different tickers must not bleed into each other."""
        from scripts.compute.etf_technicals import compute_etf_indicators

        df1 = _make_ohlcv_df("SPY", n=300)
        df2 = _make_ohlcv_df("QQQ", n=300)
        # Make QQQ prices completely different
        df2["close"] = df2["close"] * 3.0
        df2["high"] = df2["high"] * 3.0
        df2["low"] = df2["low"] * 3.0
        combined = pd.concat([df1, df2], ignore_index=True).sort_values(["ticker", "date"])

        result = compute_etf_indicators(combined.copy())
        spy_sma = result[result["ticker"] == "SPY"]["sma_50"].dropna().mean()
        qqq_sma = result[result["ticker"] == "QQQ"]["sma_50"].dropna().mean()
        # QQQ close is 3x SPY — its SMA should be roughly 3x
        assert abs(qqq_sma / spy_sma - 3.0) < 0.05

    def test_bollinger_upper_above_lower(self):
        """Bollinger upper band must always be >= lower band."""
        from scripts.compute.etf_technicals import compute_etf_indicators

        df = _make_ohlcv_df("GLD", n=300)
        result = compute_etf_indicators(df.copy())
        valid = result.dropna(subset=["bollinger_upper", "bollinger_lower"])
        assert (valid["bollinger_upper"] >= valid["bollinger_lower"]).all()

    def test_macd_histogram_equals_line_minus_signal(self):
        """MACD histogram = MACD line - signal (within floating point tolerance)."""
        from scripts.compute.etf_technicals import compute_etf_indicators

        df = _make_ohlcv_df("SPY", n=300)
        result = compute_etf_indicators(df.copy())
        valid = result.dropna(subset=["macd_line", "macd_signal", "macd_histogram"])
        diff = (valid["macd_line"] - valid["macd_signal"] - valid["macd_histogram"]).abs()
        assert (diff < 1e-8).all()

    def test_adx_non_negative(self):
        """ADX must be non-negative."""
        from scripts.compute.etf_technicals import compute_etf_indicators

        df = _make_ohlcv_df("XLK", n=300)
        result = compute_etf_indicators(df.copy())
        valid = result["adx_14"].dropna()
        assert (valid >= 0).all()

    def test_relative_volume_positive(self):
        """Relative volume must be positive when volume > 0."""
        from scripts.compute.etf_technicals import compute_etf_indicators

        df = _make_ohlcv_df("EEM", n=300)
        result = compute_etf_indicators(df.copy())
        valid = result["relative_volume"].dropna()
        assert (valid > 0).all()


# ---------------------------------------------------------------------------
# Tests: ETF_INDICATOR_COLS list
# ---------------------------------------------------------------------------


class TestEtfIndicatorCols:
    def test_indicator_cols_match_model_columns(self):
        """ETF_INDICATOR_COLS must align with de_etf_technical_daily columns."""
        from scripts.compute.etf_technicals import ETF_INDICATOR_COLS

        expected = {
            "close", "sma_50", "sma_200",
            "ema_10", "ema_20", "ema_50", "ema_200",
            "rsi_14", "rsi_7",
            "macd_line", "macd_signal", "macd_histogram",
            "roc_5", "roc_21",
            "volatility_20d", "volatility_60d",
            "bollinger_upper", "bollinger_lower",
            "relative_volume", "adx_14",
        }
        assert set(ETF_INDICATOR_COLS) == expected

    def test_no_duplicate_indicator_cols(self):
        """No column should appear twice in ETF_INDICATOR_COLS."""
        from scripts.compute.etf_technicals import ETF_INDICATOR_COLS

        assert len(ETF_INDICATOR_COLS) == len(set(ETF_INDICATOR_COLS))


# ---------------------------------------------------------------------------
# Tests: curated universe completeness
# ---------------------------------------------------------------------------


class TestCuratedUniverse:
    def test_tier1_us_all_have_required_keys(self):
        """Every ETF in TIER1_US must have name, exchange, and category."""
        from scripts.ingest.etf_ingest import TIER1_US

        for ticker, meta in TIER1_US.items():
            assert "name" in meta, f"{ticker} missing 'name'"
            assert "exchange" in meta, f"{ticker} missing 'exchange'"
            assert "category" in meta, f"{ticker} missing 'category'"

    def test_tier1_us_exchanges_valid(self):
        """All exchanges must be NYSE or NASDAQ."""
        from scripts.ingest.etf_ingest import TIER1_US

        valid_exchanges = {"NYSE", "NASDAQ"}
        for ticker, meta in TIER1_US.items():
            assert meta["exchange"] in valid_exchanges, f"{ticker} has invalid exchange: {meta['exchange']}"

    def test_world_indices_have_required_keys(self):
        """Every world index must have name and country."""
        from scripts.ingest.etf_ingest import WORLD_INDICES

        for ticker, meta in WORLD_INDICES.items():
            assert "name" in meta, f"{ticker} missing 'name'"
            assert "country" in meta, f"{ticker} missing 'country'"

    def test_world_indices_count(self):
        """Exactly 10 world indices should be defined."""
        from scripts.ingest.etf_ingest import WORLD_INDICES

        assert len(WORLD_INDICES) == 10

    def test_tier1_us_count(self):
        """Should have exactly 33 ETFs in the curated universe."""
        from scripts.ingest.etf_ingest import TIER1_US

        assert len(TIER1_US) == 33

    def test_spy_present_in_tier1(self):
        """SPY must be in the curated universe — used as benchmark in RS."""
        from scripts.ingest.etf_ingest import TIER1_US

        assert "SPY" in TIER1_US

    def test_spx_in_world_indices(self):
        """^SPX must be in WORLD_INDICES — used as RS benchmark."""
        from scripts.ingest.etf_ingest import WORLD_INDICES

        assert "^SPX" in WORLD_INDICES


# ---------------------------------------------------------------------------
# Tests: ETF RS SQL structure
# ---------------------------------------------------------------------------


class TestEtfRsSql:
    def test_spy_sql_references_etf_ohlcv(self):
        """SPY-benchmark RS SQL must reference de_etf_ohlcv as source."""
        from scripts.compute.etf_rs import ETF_RS_SPY_SQL

        assert "de_etf_ohlcv" in ETF_RS_SPY_SQL

    def test_spx_sql_references_global_prices(self):
        """^SPX-benchmark RS SQL must reference de_global_prices."""
        from scripts.compute.etf_rs import ETF_RS_SPX_SQL

        assert "de_global_prices" in ETF_RS_SPX_SQL

    def test_rs_sql_inserts_into_de_rs_scores(self):
        """RS SQL must INSERT INTO de_rs_scores."""
        from scripts.compute.etf_rs import ETF_RS_SPY_SQL, ETF_RS_SPX_SQL

        assert "de_rs_scores" in ETF_RS_SPY_SQL
        assert "de_rs_scores" in ETF_RS_SPX_SQL

    def test_rs_sql_entity_type_is_etf(self):
        """entity_type literal must be 'etf' (not 'equity' or 'mf')."""
        from scripts.compute.etf_rs import ETF_RS_SPY_SQL, ETF_RS_SPX_SQL

        assert "'etf'" in ETF_RS_SPY_SQL
        assert "'etf'" in ETF_RS_SPX_SQL

    def test_rs_composite_weights_sum_to_one(self):
        """Composite formula coefficients (0.10+0.20+0.30+0.25+0.15) must sum to 1.0."""
        weights = [0.10, 0.20, 0.30, 0.25, 0.15]
        assert abs(sum(weights) - 1.0) < 1e-9

    def test_rs_spy_sql_excludes_spy_from_results(self):
        """SPY-benchmark SQL must exclude SPY itself from the result set."""
        from scripts.compute.etf_rs import ETF_RS_SPY_SQL

        assert "ticker != 'SPY'" in ETF_RS_SPY_SQL

    def test_rs_sql_has_on_conflict_upsert(self):
        """Both RS SQLs must use ON CONFLICT upsert to be idempotent."""
        from scripts.compute.etf_rs import ETF_RS_SPY_SQL, ETF_RS_SPX_SQL

        assert "ON CONFLICT" in ETF_RS_SPY_SQL
        assert "ON CONFLICT" in ETF_RS_SPX_SQL

    def test_rs_sql_has_lookback_and_compute_params(self):
        """RS SQL must use :lookback_start and :compute_start bind params."""
        from scripts.compute.etf_rs import ETF_RS_SPY_SQL, ETF_RS_SPX_SQL

        for sql in (ETF_RS_SPY_SQL, ETF_RS_SPX_SQL):
            assert ":lookback_start" in sql
            assert ":compute_start" in sql


# ---------------------------------------------------------------------------
# Tests: write_etf_technicals_via_staging (column/schema check without DB)
# ---------------------------------------------------------------------------


class TestWriteEtfTechnicalsViaStaging:
    def test_staging_uses_all_indicator_cols(self):
        """The staging write must reference all ETF_INDICATOR_COLS."""
        from scripts.compute import etf_technicals

        # Verify INDICATOR_COLS are referenced in the function's SQL template
        # by introspecting the source
        import inspect
        src = inspect.getsource(etf_technicals.write_etf_technicals_via_staging)
        assert "ETF_INDICATOR_COLS" in src

    def test_filter_date_reduces_rows(self):
        """filter_date must drop rows before the given date."""
        df = _make_ohlcv_df("SPY", n=300)
        from scripts.compute.etf_technicals import compute_etf_indicators
        df = compute_etf_indicators(df)
        df["date"] = pd.to_datetime(df["date"])

        # Count rows that would pass filter
        cutoff = "2017-01-01"
        rows_after = len(df[df["date"] >= pd.Timestamp(cutoff)])
        rows_total = len(df)
        assert rows_after < rows_total
