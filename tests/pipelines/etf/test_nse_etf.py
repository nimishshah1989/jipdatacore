"""Tests for NSE ETF master seed script and NseEtfSyncPipeline.

Covers:
- NSE_ETFS dict: completeness, format, valid field values
- nse_etf_master.main(): DB interactions via mocked psycopg2
- NseEtfSyncPipeline.execute(): zero-ticker early return, normal upsert path
"""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BUSINESS_DATE = date(2026, 4, 10)


# ---------------------------------------------------------------------------
# NSE_ETFS dict: structure and content
# ---------------------------------------------------------------------------


class TestNseEtfsDict:
    def test_count_is_67(self) -> None:
        """Dict must contain exactly 67 NSE ETF definitions."""
        from scripts.ingest.nse_etf_master import NSE_ETFS

        assert len(NSE_ETFS) == 67

    def test_all_values_are_4_tuples(self) -> None:
        """Every value must be a 4-tuple of (country, category, exchange, name)."""
        from scripts.ingest.nse_etf_master import NSE_ETFS

        for ticker, meta in NSE_ETFS.items():
            assert len(meta) == 4, f"{ticker}: expected 4-tuple, got {len(meta)}"

    def test_all_exchange_values_are_nse(self) -> None:
        """Every ETF's exchange field must be 'NSE'."""
        from scripts.ingest.nse_etf_master import NSE_ETFS

        for ticker, (country, category, exchange, name) in NSE_ETFS.items():
            assert exchange == "NSE", f"{ticker}: exchange is '{exchange}', expected 'NSE'"

    def test_all_country_values_are_in(self) -> None:
        """Every ETF's country field must be 'IN' (India)."""
        from scripts.ingest.nse_etf_master import NSE_ETFS

        for ticker, (country, category, exchange, name) in NSE_ETFS.items():
            assert country == "IN", f"{ticker}: country is '{country}', expected 'IN'"

    def test_no_duplicate_tickers(self) -> None:
        """No ticker should appear more than once."""
        from scripts.ingest.nse_etf_master import NSE_ETFS

        assert len(NSE_ETFS) == len(set(NSE_ETFS.keys()))

    def test_all_names_non_empty(self) -> None:
        """Every ETF must have a non-empty name string."""
        from scripts.ingest.nse_etf_master import NSE_ETFS

        for ticker, (country, category, exchange, name) in NSE_ETFS.items():
            assert name and isinstance(name, str), f"{ticker}: name is empty or not a string"

    def test_all_categories_non_empty(self) -> None:
        """Every ETF must have a non-empty category string."""
        from scripts.ingest.nse_etf_master import NSE_ETFS

        for ticker, (country, category, exchange, name) in NSE_ETFS.items():
            assert category and isinstance(category, str), f"{ticker}: category is empty"

    def test_benchmark_tickers_present(self) -> None:
        """Key benchmark ETFs (NIFTYBEES, BANKBEES, GOLDBEES) must be present."""
        from scripts.ingest.nse_etf_master import NSE_ETFS

        for expected in ("NIFTYBEES", "BANKBEES", "GOLDBEES", "LIQUIDBEES"):
            assert expected in NSE_ETFS, f"Required ticker {expected} missing from NSE_ETFS"

    def test_broad_index_category_count(self) -> None:
        """Broad Index category must have exactly 20 ETFs."""
        from scripts.ingest.nse_etf_master import NSE_ETFS

        broad = [t for t, (_, cat, _, _) in NSE_ETFS.items() if cat == "Broad Index"]
        assert len(broad) == 20, f"Expected 20 Broad Index ETFs, got {len(broad)}"

    def test_gold_category_count(self) -> None:
        """Gold category must have exactly 8 ETFs."""
        from scripts.ingest.nse_etf_master import NSE_ETFS

        gold = [t for t, (_, cat, _, _) in NSE_ETFS.items() if cat == "Gold"]
        assert len(gold) == 8, f"Expected 8 Gold ETFs, got {len(gold)}"

    def test_ticker_lengths_within_varchar30(self) -> None:
        """All tickers must fit in VARCHAR(30)."""
        from scripts.ingest.nse_etf_master import NSE_ETFS

        for ticker in NSE_ETFS:
            assert len(ticker) <= 30, f"Ticker '{ticker}' exceeds VARCHAR(30)"

    def test_no_whitespace_in_tickers(self) -> None:
        """Tickers must not contain whitespace."""
        from scripts.ingest.nse_etf_master import NSE_ETFS

        for ticker in NSE_ETFS:
            assert ticker == ticker.strip() and " " not in ticker, (
                f"Ticker '{ticker}' contains whitespace"
            )


# ---------------------------------------------------------------------------
# nse_etf_master.main(): DB interaction via mocked psycopg2
# ---------------------------------------------------------------------------


class TestNseEtfMasterMain:
    def test_main_connects_to_db_and_upserts(self) -> None:
        """main() must connect to DB, query before count, upsert all ETFs, query after count."""
        from scripts.ingest.nse_etf_master import NSE_ETFS

        mock_cur = MagicMock()
        # before-count returns 0, after-count returns len(NSE_ETFS)
        mock_cur.fetchone.side_effect = [(0,), (len(NSE_ETFS),)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        with patch("scripts.ingest.nse_etf_master.psycopg2.connect", return_value=mock_conn):
            from scripts.ingest import nse_etf_master

            nse_etf_master.main()

        # autocommit must be set True
        assert mock_conn.autocommit is True

        # Two SELECT COUNT(*) calls + len(NSE_ETFS) upsert calls
        execute_calls = mock_cur.execute.call_args_list
        count_calls = [c for c in execute_calls if "COUNT" in str(c)]
        upsert_calls = [c for c in execute_calls if "INSERT" in str(c)]

        assert len(count_calls) == 2, f"Expected 2 COUNT queries, got {len(count_calls)}"
        assert len(upsert_calls) == len(NSE_ETFS), (
            f"Expected {len(NSE_ETFS)} upserts, got {len(upsert_calls)}"
        )

    def test_main_upsert_includes_inr_currency(self) -> None:
        """Each upsert must include 'INR' currency value."""
        mock_cur = MagicMock()
        mock_cur.fetchone.side_effect = [(0,), (1,)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        with patch("scripts.ingest.nse_etf_master.psycopg2.connect", return_value=mock_conn):
            from scripts.ingest import nse_etf_master

            nse_etf_master.main()

        upsert_calls = [c for c in mock_cur.execute.call_args_list if "INSERT" in str(c)]
        # Each call's second arg (params tuple) should contain 'INR' via SQL literal
        # The SQL has 'INR' hardcoded in the query string itself
        sql_strs = [str(c) for c in upsert_calls]
        for s in sql_strs:
            # We check the SQL template contains INR (hardcoded)
            break  # template is shared; just verify it once via SQL text
        assert "'INR'" in mock_cur.execute.call_args_list[1][0][0]

    def test_main_closes_connection_on_success(self) -> None:
        """Connection and cursor must be closed after successful run."""
        mock_cur = MagicMock()
        mock_cur.fetchone.side_effect = [(0,), (5,)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        with patch("scripts.ingest.nse_etf_master.psycopg2.connect", return_value=mock_conn):
            from scripts.ingest import nse_etf_master

            nse_etf_master.main()

        mock_cur.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_main_closes_connection_on_db_error(self) -> None:
        """Connection must be closed even when a DB error is raised."""
        mock_cur = MagicMock()
        mock_cur.execute.side_effect = Exception("DB connection error")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        with patch("scripts.ingest.nse_etf_master.psycopg2.connect", return_value=mock_conn):
            from scripts.ingest import nse_etf_master

            with pytest.raises(Exception, match="DB connection error"):
                nse_etf_master.main()

        mock_cur.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_main_uses_env_db_url(self, monkeypatch) -> None:
        """main() must use DATABASE_URL_SYNC env var when set."""
        custom_url = "postgresql://test_user:test_pass@localhost:5432/test_db"
        monkeypatch.setenv("DATABASE_URL_SYNC", custom_url)

        mock_cur = MagicMock()
        mock_cur.fetchone.side_effect = [(0,), (1,)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        captured_url = []

        def fake_connect(url):
            captured_url.append(url)
            return mock_conn

        with patch("scripts.ingest.nse_etf_master.psycopg2.connect", side_effect=fake_connect):
            import importlib
            import scripts.ingest.nse_etf_master as mod

            # Reload so DB constant picks up env var
            importlib.reload(mod)
            mod.main()

        assert captured_url[0] == custom_url


# ---------------------------------------------------------------------------
# NseEtfSyncPipeline.execute()
# ---------------------------------------------------------------------------


class TestNseEtfSyncPipelineExecute:
    @pytest.mark.asyncio
    async def test_execute_returns_zero_when_no_active_tickers(self) -> None:
        """When de_etf_master has no active NSE tickers, return rows_processed=0."""
        from app.pipelines.etf.nse_etf_sync import NseEtfSyncPipeline

        session = AsyncMock()
        run_log = MagicMock()

        # master query returns no rows
        master_result = MagicMock()
        master_result.fetchall.return_value = []
        session.execute = AsyncMock(return_value=master_result)

        pipeline = NseEtfSyncPipeline()
        result = await pipeline.execute(BUSINESS_DATE, session, run_log)

        assert result.rows_processed == 0
        assert result.rows_failed == 0
        # Sync SQL must NOT be executed when ticker list is empty
        assert session.execute.call_count == 1  # only the master query

    @pytest.mark.asyncio
    async def test_execute_returns_rowcount_from_upsert(self) -> None:
        """rows_processed must equal the rowcount returned by the INSERT...SELECT."""
        from app.pipelines.etf.nse_etf_sync import NseEtfSyncPipeline

        session = AsyncMock()
        run_log = MagicMock()

        tickers = ["NIFTYBEES", "BANKBEES", "GOLDBEES"]
        master_result = MagicMock()
        master_result.fetchall.return_value = [(t,) for t in tickers]

        upsert_result = MagicMock()
        upsert_result.rowcount = 3

        session.execute = AsyncMock(side_effect=[master_result, upsert_result])

        pipeline = NseEtfSyncPipeline()
        result = await pipeline.execute(BUSINESS_DATE, session, run_log)

        assert result.rows_processed == 3
        assert result.rows_failed == 0

    @pytest.mark.asyncio
    async def test_execute_passes_date_object_to_sql(self) -> None:
        """business_date must be passed as a date object (not string) to avoid asyncpg rejection."""
        from app.pipelines.etf.nse_etf_sync import NseEtfSyncPipeline

        session = AsyncMock()
        run_log = MagicMock()

        master_result = MagicMock()
        master_result.fetchall.return_value = [("NIFTYBEES",)]

        upsert_result = MagicMock()
        upsert_result.rowcount = 1

        session.execute = AsyncMock(side_effect=[master_result, upsert_result])

        pipeline = NseEtfSyncPipeline()
        await pipeline.execute(BUSINESS_DATE, session, run_log)

        # Second call is the upsert — extract the bind params dict
        second_call = session.execute.call_args_list[1]
        bind_params = second_call[0][1]  # positional arg 2

        assert isinstance(bind_params["business_date"], date), (
            "business_date must be a date object, not a string (asyncpg rejects strings)"
        )

    @pytest.mark.asyncio
    async def test_execute_passes_ticker_list_for_any_param(self) -> None:
        """nse_tickers bind param must be a Python list for ANY() operator."""
        from app.pipelines.etf.nse_etf_sync import NseEtfSyncPipeline

        session = AsyncMock()
        run_log = MagicMock()

        tickers = ["NIFTYBEES", "BANKBEES"]
        master_result = MagicMock()
        master_result.fetchall.return_value = [(t,) for t in tickers]

        upsert_result = MagicMock()
        upsert_result.rowcount = 2

        session.execute = AsyncMock(side_effect=[master_result, upsert_result])

        pipeline = NseEtfSyncPipeline()
        await pipeline.execute(BUSINESS_DATE, session, run_log)

        second_call = session.execute.call_args_list[1]
        bind_params = second_call[0][1]

        assert isinstance(bind_params["nse_tickers"], list), (
            "nse_tickers must be a Python list for ANY(:nse_tickers) to work"
        )
        assert bind_params["nse_tickers"] == tickers

    @pytest.mark.asyncio
    async def test_execute_rowcount_none_treated_as_zero(self) -> None:
        """If rowcount is None (some drivers), treat as 0 without crashing."""
        from app.pipelines.etf.nse_etf_sync import NseEtfSyncPipeline

        session = AsyncMock()
        run_log = MagicMock()

        master_result = MagicMock()
        master_result.fetchall.return_value = [("NIFTYBEES",)]

        upsert_result = MagicMock()
        upsert_result.rowcount = None  # some drivers return None

        session.execute = AsyncMock(side_effect=[master_result, upsert_result])

        pipeline = NseEtfSyncPipeline()
        result = await pipeline.execute(BUSINESS_DATE, session, run_log)

        assert result.rows_processed == 0
        assert result.rows_failed == 0

    def test_pipeline_name_is_nse_etf_sync(self) -> None:
        """pipeline_name must match the registered name used in crontab/triggers."""
        from app.pipelines.etf.nse_etf_sync import NseEtfSyncPipeline

        assert NseEtfSyncPipeline.pipeline_name == "nse_etf_sync"

    def test_requires_trading_day_is_true(self) -> None:
        """Pipeline must only run on NSE trading days."""
        from app.pipelines.etf.nse_etf_sync import NseEtfSyncPipeline

        assert NseEtfSyncPipeline.requires_trading_day is True

    def test_exchange_is_nse(self) -> None:
        """exchange must be 'NSE' so trading calendar check uses NSE holidays."""
        from app.pipelines.etf.nse_etf_sync import NseEtfSyncPipeline

        assert NseEtfSyncPipeline.exchange == "NSE"


# ---------------------------------------------------------------------------
# __init__.py exports
# ---------------------------------------------------------------------------


class TestEtfPackageExports:
    def test_etf_price_pipeline_exported(self) -> None:
        """EtfPricePipeline must be importable from app.pipelines.etf."""
        from app.pipelines.etf import EtfPricePipeline

        assert EtfPricePipeline is not None

    def test_nse_etf_sync_pipeline_exported(self) -> None:
        """NseEtfSyncPipeline must be importable from app.pipelines.etf."""
        from app.pipelines.etf import NseEtfSyncPipeline

        assert NseEtfSyncPipeline is not None

    def test_all_list_contains_both_pipelines(self) -> None:
        """__all__ must include both pipeline class names."""
        import app.pipelines.etf as pkg

        assert "EtfPricePipeline" in pkg.__all__
        assert "NseEtfSyncPipeline" in pkg.__all__
