"""Unit tests for scripts/ingest/etf_enrich.py.

Tests cover:
- _safe_decimal: None/NaN/inf/zero handling, float->Decimal
- _safe_date_from_unix: unix timestamp -> date, None/bad input
- _derive_asset_class: quoteType + category combinations
- _extract_fields: field selection from info dict, None-exclusion
- _build_update_sql: dynamic SET clause construction
- NSE symbol suffix logic (via _fetch_yfinance_info symbol construction)
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Tests: _safe_decimal
# ---------------------------------------------------------------------------


class TestSafeDecimal:
    def test_normal_float_converts(self):
        from scripts.ingest.etf_enrich import _safe_decimal

        result = _safe_decimal(0.0050)
        assert isinstance(result, Decimal)
        assert result == Decimal("0.005")

    def test_zero_is_valid(self):
        """expense_ratio of 0 must NOT be treated as None."""
        from scripts.ingest.etf_enrich import _safe_decimal

        result = _safe_decimal(0.0)
        assert result == Decimal("0.0")

    def test_none_returns_none(self):
        from scripts.ingest.etf_enrich import _safe_decimal

        assert _safe_decimal(None) is None

    def test_nan_returns_none(self):
        from scripts.ingest.etf_enrich import _safe_decimal

        assert _safe_decimal(float("nan")) is None

    def test_inf_returns_none(self):
        from scripts.ingest.etf_enrich import _safe_decimal

        assert _safe_decimal(float("inf")) is None

    def test_neg_inf_returns_none(self):
        from scripts.ingest.etf_enrich import _safe_decimal

        assert _safe_decimal(float("-inf")) is None

    def test_rounds_to_4dp(self):
        from scripts.ingest.etf_enrich import _safe_decimal

        result = _safe_decimal(0.00049999)
        assert isinstance(result, Decimal)
        # round to 4dp: 0.0005
        assert result == Decimal("0.0005")

    def test_string_float_converts(self):
        from scripts.ingest.etf_enrich import _safe_decimal

        result = _safe_decimal("0.0025")
        assert result == Decimal("0.0025")

    def test_bad_string_returns_none(self):
        from scripts.ingest.etf_enrich import _safe_decimal

        assert _safe_decimal("not_a_number") is None


# ---------------------------------------------------------------------------
# Tests: _safe_date_from_unix
# ---------------------------------------------------------------------------


class TestSafeDateFromUnix:
    def test_valid_unix_timestamp(self):
        from scripts.ingest.etf_enrich import _safe_date_from_unix

        # 2000-01-01 00:00:00 UTC = 946684800
        result = _safe_date_from_unix(946684800)
        assert isinstance(result, date)
        # Allow for timezone offset — just check year
        assert result.year in (1999, 2000)

    def test_none_returns_none(self):
        from scripts.ingest.etf_enrich import _safe_date_from_unix

        assert _safe_date_from_unix(None) is None

    def test_invalid_string_returns_none(self):
        from scripts.ingest.etf_enrich import _safe_date_from_unix

        assert _safe_date_from_unix("not_a_timestamp") is None

    def test_negative_timestamp_handled(self):
        """Negative timestamps (pre-1970) should either return a date or None — not raise."""
        from scripts.ingest.etf_enrich import _safe_date_from_unix

        # Should not raise — return date or None
        result = _safe_date_from_unix(-86400)
        assert result is None or isinstance(result, date)

    def test_float_timestamp_converts(self):
        """yfinance sometimes returns float timestamps."""
        from scripts.ingest.etf_enrich import _safe_date_from_unix

        result = _safe_date_from_unix(946684800.0)
        assert isinstance(result, date)


# ---------------------------------------------------------------------------
# Tests: _derive_asset_class
# ---------------------------------------------------------------------------


class TestDeriveAssetClass:
    def test_etf_no_category_returns_equity(self):
        from scripts.ingest.etf_enrich import _derive_asset_class

        assert _derive_asset_class("ETF", None) == "Equity"

    def test_bond_category_returns_fixed_income(self):
        from scripts.ingest.etf_enrich import _derive_asset_class

        assert _derive_asset_class("ETF", "Corporate Bond") == "Fixed Income"

    def test_treasury_category_returns_fixed_income(self):
        from scripts.ingest.etf_enrich import _derive_asset_class

        assert _derive_asset_class("ETF", "Treasury Inflation-Protected") == "Fixed Income"

    def test_gold_category_returns_commodity(self):
        from scripts.ingest.etf_enrich import _derive_asset_class

        assert _derive_asset_class("ETF", "Trading--Leveraged Gold") == "Commodity"

    def test_commodity_category_returns_commodity(self):
        from scripts.ingest.etf_enrich import _derive_asset_class

        assert _derive_asset_class("ETF", "Commodities Broad Basket") == "Commodity"

    def test_reit_category_returns_real_estate(self):
        from scripts.ingest.etf_enrich import _derive_asset_class

        assert _derive_asset_class("ETF", "Real Estate") == "Real Estate"

    def test_non_etf_quote_type_returns_none(self):
        from scripts.ingest.etf_enrich import _derive_asset_class

        assert _derive_asset_class("EQUITY", "Technology") is None

    def test_none_quote_type_returns_none(self):
        from scripts.ingest.etf_enrich import _derive_asset_class

        assert _derive_asset_class(None, "Technology") is None

    def test_etf_tech_sector_returns_equity(self):
        from scripts.ingest.etf_enrich import _derive_asset_class

        assert _derive_asset_class("ETF", "Technology") == "Equity"

    def test_case_insensitive_bond_check(self):
        from scripts.ingest.etf_enrich import _derive_asset_class

        # category comparison is upper-cased internally
        assert _derive_asset_class("ETF", "Short-Term Bond") == "Fixed Income"


# ---------------------------------------------------------------------------
# Tests: _extract_fields
# ---------------------------------------------------------------------------


class TestExtractFields:
    def test_all_fields_present(self):
        from scripts.ingest.etf_enrich import _extract_fields

        info = {
            "category": "Large Blend",
            "sectorDisp": "Technology",
            "annualReportExpenseRatio": 0.0003,
            "benchmark": "S&P 500",
            "fundInceptionDate": 946684800,
            "currency": "USD",
            "quoteType": "ETF",
        }
        fields = _extract_fields(info)
        assert fields["category"] == "Large Blend"
        assert fields["sector"] == "Technology"
        assert isinstance(fields["expense_ratio"], Decimal)
        assert fields["expense_ratio"] == Decimal("0.0003")
        assert fields["benchmark"] == "S&P 500"
        assert isinstance(fields["inception_date"], date)
        assert fields["currency"] == "USD"
        assert fields["asset_class"] == "Equity"

    def test_empty_info_returns_empty_dict(self):
        from scripts.ingest.etf_enrich import _extract_fields

        fields = _extract_fields({})
        assert fields == {}

    def test_none_values_not_included(self):
        """Fields with None values must not appear in result dict."""
        from scripts.ingest.etf_enrich import _extract_fields

        info = {
            "category": None,
            "sectorDisp": None,
            "annualReportExpenseRatio": None,
            "benchmark": None,
            "fundInceptionDate": None,
            "currency": None,
            "quoteType": None,
        }
        fields = _extract_fields(info)
        assert fields == {}

    def test_zero_expense_ratio_included(self):
        """expense_ratio=0.0 is a valid value and must be included."""
        from scripts.ingest.etf_enrich import _extract_fields

        info = {"annualReportExpenseRatio": 0.0, "quoteType": "ETF"}
        fields = _extract_fields(info)
        assert "expense_ratio" in fields
        assert fields["expense_ratio"] == Decimal("0.0")

    def test_sector_fallback_to_sector_key(self):
        """If sectorDisp absent, fall back to 'sector' key."""
        from scripts.ingest.etf_enrich import _extract_fields

        info = {"sector": "Healthcare", "quoteType": "ETF"}
        fields = _extract_fields(info)
        assert fields.get("sector") == "Healthcare"

    def test_sectordisp_preferred_over_sector(self):
        from scripts.ingest.etf_enrich import _extract_fields

        info = {"sectorDisp": "Financial Services", "sector": "Financials", "quoteType": "ETF"}
        fields = _extract_fields(info)
        assert fields["sector"] == "Financial Services"

    def test_benchmark_fallback_to_benchmarksymbol(self):
        from scripts.ingest.etf_enrich import _extract_fields

        info = {"benchmarkSymbol": "^GSPC", "quoteType": "ETF"}
        fields = _extract_fields(info)
        assert fields.get("benchmark") == "^GSPC"

    def test_bond_etf_asset_class(self):
        from scripts.ingest.etf_enrich import _extract_fields

        info = {"category": "Intermediate-Term Bond", "quoteType": "ETF"}
        fields = _extract_fields(info)
        assert fields["asset_class"] == "Fixed Income"


# ---------------------------------------------------------------------------
# Tests: _build_update_sql
# ---------------------------------------------------------------------------


class TestBuildUpdateSql:
    def test_single_field(self):
        from scripts.ingest.etf_enrich import _build_update_sql

        sql, params = _build_update_sql({"category": "Large Blend"})
        assert "category = %s" in sql
        assert params == ["Large Blend"]
        assert "UPDATE de_etf_master SET" in sql
        assert "WHERE ticker = %s" in sql

    def test_multiple_fields(self):
        from scripts.ingest.etf_enrich import _build_update_sql

        fields = {"category": "Large Blend", "currency": "USD"}
        sql, params = _build_update_sql(fields)
        assert "category = %s" in sql
        assert "currency = %s" in sql
        assert len(params) == 2

    def test_params_order_matches_set_clauses(self):
        """params list count must match SET clause count (WHERE %s is separate — caller appends ticker)."""
        from scripts.ingest.etf_enrich import _build_update_sql

        fields = {"sector": "Technology", "expense_ratio": Decimal("0.0003")}
        sql, params = _build_update_sql(fields)
        # The SQL has SET clauses + 1 WHERE clause, params only contains SET values
        # Total %s count = len(fields) + 1 (WHERE ticker = %s)
        total_placeholders = sql.count("%s")
        assert total_placeholders == len(params) + 1  # +1 for WHERE ticker


# ---------------------------------------------------------------------------
# Tests: _fetch_yfinance_info (symbol construction, error handling)
# ---------------------------------------------------------------------------


class TestFetchYfinanceInfo:
    def test_nse_ticker_gets_ns_suffix(self):
        """NSE exchange tickers must become TICKER.NS for yfinance."""
        from scripts.ingest.etf_enrich import _fetch_yfinance_info

        with patch("scripts.ingest.etf_enrich.yf") as mock_yf:
            mock_ticker = MagicMock()
            mock_ticker.info = {"category": "Broad Index", "quoteType": "ETF", "currency": "INR", "longName": "Test"}
            mock_yf.Ticker.return_value = mock_ticker

            _fetch_yfinance_info("NIFTYBEES", "NSE")
            mock_yf.Ticker.assert_called_once_with("NIFTYBEES.NS")

    def test_non_nse_ticker_no_suffix(self):
        """Non-NSE tickers must NOT get .NS suffix."""
        from scripts.ingest.etf_enrich import _fetch_yfinance_info

        with patch("scripts.ingest.etf_enrich.yf") as mock_yf:
            mock_ticker = MagicMock()
            mock_ticker.info = {"category": "Large Blend", "quoteType": "ETF", "currency": "USD", "longName": "SPY"}
            mock_yf.Ticker.return_value = mock_ticker

            _fetch_yfinance_info("SPY", "NYSE")
            mock_yf.Ticker.assert_called_once_with("SPY")

    def test_exception_returns_none(self):
        """If yfinance raises, return None (don't propagate)."""
        from scripts.ingest.etf_enrich import _fetch_yfinance_info

        with patch("scripts.ingest.etf_enrich.yf") as mock_yf:
            mock_yf.Ticker.side_effect = Exception("timeout")
            result = _fetch_yfinance_info("SPY", "NYSE")
            assert result is None

    def test_empty_info_returns_none(self):
        """An empty info dict (delisted ticker) should return None."""
        from scripts.ingest.etf_enrich import _fetch_yfinance_info

        with patch("scripts.ingest.etf_enrich.yf") as mock_yf:
            mock_ticker = MagicMock()
            mock_ticker.info = {}
            mock_yf.Ticker.return_value = mock_ticker

            result = _fetch_yfinance_info("DEAD", "NYSE")
            assert result is None

    def test_sparse_info_returns_none(self):
        """Only 2 keys in info — likely a delisted/bad ticker. Return None."""
        from scripts.ingest.etf_enrich import _fetch_yfinance_info

        with patch("scripts.ingest.etf_enrich.yf") as mock_yf:
            mock_ticker = MagicMock()
            mock_ticker.info = {"symbol": "DEAD", "quoteType": "NONE"}
            mock_yf.Ticker.return_value = mock_ticker

            result = _fetch_yfinance_info("DEAD", "NYSE")
            assert result is None
