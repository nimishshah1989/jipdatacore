"""Tests for NSE equity master refresh pipeline."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.pipelines.equity.master_refresh import (
    parse_equity_listing_csv,
    MasterRefreshPipeline,
    handle_suspension,
    handle_delisting,
)


# ---------------------------------------------------------------------------
# parse_equity_listing_csv
# ---------------------------------------------------------------------------

SAMPLE_EQUITY_CSV = """\
SYMBOL,NAME OF COMPANY,SERIES,DATE OF LISTING,PAID UP VALUE,MARKET LOT,ISIN NUMBER,FACE VALUE
RELIANCE,Reliance Industries Limited,EQ,29-Nov-1995,10,1,INE002A01018,10
INFY,Infosys Limited,EQ,08-Feb-1993,5,1,INE009A01021,5
TCS,Tata Consultancy Services Limited,EQ,25-Aug-2004,1,1,INE467B01029,1
HDFCBANK,HDFC Bank Limited,EQ,23-May-1995,1,1,INE040A01034,1
"""


class TestParseEquityListingCsv:
    def test_parse_returns_correct_count(self) -> None:
        result = parse_equity_listing_csv(SAMPLE_EQUITY_CSV)
        assert len(result) == 4

    def test_parse_symbol_uppercased(self) -> None:
        result = parse_equity_listing_csv(SAMPLE_EQUITY_CSV)
        assert result[0]["symbol"] == "RELIANCE"

    def test_parse_company_name_preserved(self) -> None:
        result = parse_equity_listing_csv(SAMPLE_EQUITY_CSV)
        assert result[0]["company_name"] == "Reliance Industries Limited"

    def test_parse_isin_extracted(self) -> None:
        result = parse_equity_listing_csv(SAMPLE_EQUITY_CSV)
        assert result[0]["isin"] == "INE002A01018"

    def test_parse_listing_date_parsed(self) -> None:
        result = parse_equity_listing_csv(SAMPLE_EQUITY_CSV)
        assert result[0]["listing_date"] == date(1995, 11, 29)

    def test_parse_series_extracted(self) -> None:
        result = parse_equity_listing_csv(SAMPLE_EQUITY_CSV)
        assert result[0]["series"] == "EQ"

    def test_parse_exchange_set_to_nse(self) -> None:
        result = parse_equity_listing_csv(SAMPLE_EQUITY_CSV)
        assert result[0]["exchange"] == "NSE"

    def test_parse_is_active_true(self) -> None:
        result = parse_equity_listing_csv(SAMPLE_EQUITY_CSV)
        assert result[0]["is_active"] is True

    def test_parse_empty_csv_returns_empty_list(self) -> None:
        result = parse_equity_listing_csv("")
        assert result == []

    def test_parse_skips_rows_without_symbol(self) -> None:
        csv = """\
SYMBOL,NAME OF COMPANY,SERIES,DATE OF LISTING,PAID UP VALUE,MARKET LOT,ISIN NUMBER,FACE VALUE
,Missing Symbol Company,EQ,01-Jan-2020,1,1,INE000X00001,1
VALIDCO,Valid Company,EQ,01-Jan-2020,1,1,INE000X00002,1
"""
        result = parse_equity_listing_csv(csv)
        assert len(result) == 1
        assert result[0]["symbol"] == "VALIDCO"

    def test_parse_header_only_returns_empty_list(self) -> None:
        csv = "SYMBOL,NAME OF COMPANY,SERIES,DATE OF LISTING,PAID UP VALUE,MARKET LOT,ISIN NUMBER,FACE VALUE\n"
        result = parse_equity_listing_csv(csv)
        assert result == []

    def test_parse_listing_date_none_if_invalid(self) -> None:
        csv = """\
SYMBOL,NAME OF COMPANY,SERIES,DATE OF LISTING,PAID UP VALUE,MARKET LOT,ISIN NUMBER,FACE VALUE
TESTCO,Test Company,EQ,INVALID_DATE,1,1,INE000X00001,1
"""
        result = parse_equity_listing_csv(csv)
        assert len(result) == 1
        assert result[0]["listing_date"] is None

    def test_parse_multiple_formats_listing_date(self) -> None:
        csv = """\
SYMBOL,NAME OF COMPANY,SERIES,DATE OF LISTING,PAID UP VALUE,MARKET LOT,ISIN NUMBER,FACE VALUE
TESTCO,Test Company,EQ,2020-01-15,1,1,INE000X00001,1
"""
        result = parse_equity_listing_csv(csv)
        assert result[0]["listing_date"] == date(2020, 1, 15)


# ---------------------------------------------------------------------------
# MasterRefreshPipeline.execute() — unit test with mocked HTTP
# ---------------------------------------------------------------------------

class TestMasterRefreshPipelineExecute:
    @pytest.mark.asyncio
    async def test_execute_inserts_new_instruments(self) -> None:
        """New symbols from NSE listing are inserted into de_instrument."""
        pipeline = MasterRefreshPipeline()
        business_date = date(2026, 4, 5)

        mock_session = AsyncMock()
        mock_run_log = MagicMock()
        mock_run_log.id = 1

        # Mock _download_equity_listing to return sample CSV
        # Mock _load_existing_instruments to return empty dict (all instruments are new)
        # Mock _insert_instrument to succeed

        with (
            patch(
                "app.pipelines.equity.master_refresh._download_equity_listing",
                new=AsyncMock(return_value=SAMPLE_EQUITY_CSV),
            ),
            patch(
                "app.pipelines.equity.master_refresh._load_existing_instruments",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "app.pipelines.equity.master_refresh._insert_instrument",
                new=AsyncMock(),
            ) as mock_insert,
        ):
            result = await pipeline.execute(business_date, mock_session, mock_run_log)

        assert result.rows_processed == 4
        assert result.rows_failed == 0
        assert mock_insert.call_count == 4

    @pytest.mark.asyncio
    async def test_execute_skips_existing_symbols(self) -> None:
        """Existing symbols without changes are counted but not re-inserted."""
        import uuid

        pipeline = MasterRefreshPipeline()
        business_date = date(2026, 4, 5)

        mock_session = AsyncMock()
        mock_run_log = MagicMock()
        mock_run_log.id = 1

        existing_id = uuid.uuid4()
        existing_map = {
            "RELIANCE": (existing_id, "RELIANCE"),
            "INFY": (uuid.uuid4(), "INFY"),
            "TCS": (uuid.uuid4(), "TCS"),
            "HDFCBANK": (uuid.uuid4(), "HDFCBANK"),
        }

        with (
            patch(
                "app.pipelines.equity.master_refresh._download_equity_listing",
                new=AsyncMock(return_value=SAMPLE_EQUITY_CSV),
            ),
            patch(
                "app.pipelines.equity.master_refresh._load_existing_instruments",
                new=AsyncMock(return_value=existing_map),
            ),
        ):
            result = await pipeline.execute(business_date, mock_session, mock_run_log)

        # All 4 are existing → updated_count = 4, no failures
        assert result.rows_failed == 0

    @pytest.mark.asyncio
    async def test_execute_raises_if_empty_listing(self) -> None:
        """Empty NSE listing raises ValueError."""
        pipeline = MasterRefreshPipeline()
        business_date = date(2026, 4, 5)
        mock_session = AsyncMock()
        mock_run_log = MagicMock()

        with (
            patch(
                "app.pipelines.equity.master_refresh._download_equity_listing",
                new=AsyncMock(return_value=""),
            ),
            patch(
                "app.pipelines.equity.master_refresh._load_existing_instruments",
                new=AsyncMock(return_value={}),
            ),
        ):
            with pytest.raises(ValueError, match="0 instruments"):
                await pipeline.execute(business_date, mock_session, mock_run_log)

    @pytest.mark.asyncio
    async def test_execute_counts_row_failures_on_error(self) -> None:
        """Instruments that fail to insert increment rows_failed."""
        pipeline = MasterRefreshPipeline()
        business_date = date(2026, 4, 5)
        mock_session = AsyncMock()
        mock_run_log = MagicMock()

        with (
            patch(
                "app.pipelines.equity.master_refresh._download_equity_listing",
                new=AsyncMock(return_value=SAMPLE_EQUITY_CSV),
            ),
            patch(
                "app.pipelines.equity.master_refresh._load_existing_instruments",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "app.pipelines.equity.master_refresh._insert_instrument",
                side_effect=RuntimeError("DB error"),
            ),
        ):
            result = await pipeline.execute(business_date, mock_session, mock_run_log)

        assert result.rows_failed == 4


# ---------------------------------------------------------------------------
# handle_suspension
# ---------------------------------------------------------------------------

class TestHandleSuspension:
    @pytest.mark.asyncio
    async def test_suspension_returns_false_if_instrument_not_found(self) -> None:
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.first.return_value = None
        mock_session.execute.return_value = mock_result

        result = await handle_suspension(mock_session, "UNKNOWN", date(2026, 4, 5))
        assert result is False

    @pytest.mark.asyncio
    async def test_suspension_returns_true_if_instrument_found(self) -> None:
        import uuid

        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.first.return_value = (uuid.uuid4(),)
        mock_session.execute.return_value = mock_result

        result = await handle_suspension(mock_session, "RELIANCE", date(2026, 4, 5))
        assert result is True
        # Should have called execute twice: once for SELECT, once for UPDATE
        assert mock_session.execute.call_count == 2


# ---------------------------------------------------------------------------
# handle_delisting
# ---------------------------------------------------------------------------

class TestHandleDelisting:
    @pytest.mark.asyncio
    async def test_delisting_returns_false_if_instrument_not_found(self) -> None:
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.first.return_value = None
        mock_session.execute.return_value = mock_result

        result = await handle_delisting(mock_session, "UNKNOWN", date(2026, 4, 5))
        assert result is False

    @pytest.mark.asyncio
    async def test_delisting_returns_true_if_instrument_found(self) -> None:
        import uuid

        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.first.return_value = (uuid.uuid4(),)
        mock_session.execute.return_value = mock_result

        result = await handle_delisting(mock_session, "DELISTEDCO", date(2026, 4, 5))
        assert result is True
        assert mock_session.execute.call_count == 2
