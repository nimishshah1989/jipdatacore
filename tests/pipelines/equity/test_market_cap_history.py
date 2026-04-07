"""Tests for the AMFI market cap history pipeline."""

from __future__ import annotations

import io
import uuid
from datetime import date
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import openpyxl
import pytest

from app.pipelines.equity.market_cap_history import (
    MarketCapHistoryPipeline,
    _build_amfi_date_str,
    _build_amfi_urls,
    _normalise_category,
    _parse_text,
    determine_effective_from,
    fetch_amfi_cap_list,
    parse_amfi_cap_list,
    parse_amfi_xlsx,
    rank_to_cap_category,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_amfi_xlsx(
    rows: list[tuple],  # (rank, company_name, isin, bse_symbol, bse_cap, nse_symbol, nse_cap, category)
    include_header: bool = True,
) -> bytes:
    """Build a minimal AMFI AverageMarketCapitalization XLSX in memory.

    Columns match AMFI spec:
      0: Sr. No.
      1: Company Name
      2: ISIN
      3: BSE Symbol
      4: BSE 6-month Avg Market Cap (crore)
      5: NSE Symbol
      6: NSE 6-month Avg Market Cap (crore)
      7: MSEI Symbol (ignored)
      8: MSEI 6-month Avg Market Cap (ignored)
      9: Average of All Exchanges (crore)
      10: Categorization
    """
    wb = openpyxl.Workbook()
    ws = wb.active
    if include_header:
        ws.append([
            "Sr. No.",
            "Company Name",
            "ISIN",
            "BSE Symbol",
            "BSE 6-month Avg Market Cap (crore)",
            "NSE Symbol",
            "NSE 6-month Avg Market Cap (crore)",
            "MSEI Symbol",
            "MSEI 6-month Avg Market Cap",
            "Average of All Exchanges (crore)",
            "Categorization",
        ])
    for row in rows:
        ws.append(list(row))

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# rank_to_cap_category
# ---------------------------------------------------------------------------

class TestRankToCapCategory:
    def test_rank_1_returns_large(self) -> None:
        assert rank_to_cap_category(1) == "large"

    def test_rank_100_returns_large(self) -> None:
        assert rank_to_cap_category(100) == "large"

    def test_rank_101_returns_mid(self) -> None:
        assert rank_to_cap_category(101) == "mid"

    def test_rank_250_returns_mid(self) -> None:
        assert rank_to_cap_category(250) == "mid"

    def test_rank_251_returns_small(self) -> None:
        assert rank_to_cap_category(251) == "small"

    def test_rank_500_returns_small(self) -> None:
        assert rank_to_cap_category(500) == "small"

    def test_rank_501_returns_micro(self) -> None:
        assert rank_to_cap_category(501) == "micro"

    def test_rank_9999_returns_micro(self) -> None:
        assert rank_to_cap_category(9999) == "micro"

    def test_boundary_large_to_mid(self) -> None:
        assert rank_to_cap_category(100) == "large"
        assert rank_to_cap_category(101) == "mid"

    def test_boundary_mid_to_small(self) -> None:
        assert rank_to_cap_category(250) == "mid"
        assert rank_to_cap_category(251) == "small"

    def test_boundary_small_to_micro(self) -> None:
        assert rank_to_cap_category(500) == "small"
        assert rank_to_cap_category(501) == "micro"


# ---------------------------------------------------------------------------
# determine_effective_from
# ---------------------------------------------------------------------------

class TestDetermineEffectiveFrom:
    def test_january_date_returns_jan_1(self) -> None:
        result = determine_effective_from(date(2025, 1, 15))
        assert result == date(2025, 1, 1)

    def test_june_30_returns_jan_1(self) -> None:
        result = determine_effective_from(date(2025, 6, 30))
        assert result == date(2025, 1, 1)

    def test_july_1_returns_jul_1(self) -> None:
        result = determine_effective_from(date(2025, 7, 1))
        assert result == date(2025, 7, 1)

    def test_december_date_returns_jul_1(self) -> None:
        result = determine_effective_from(date(2025, 12, 31))
        assert result == date(2025, 7, 1)

    def test_jan_1_returns_jan_1(self) -> None:
        result = determine_effective_from(date(2025, 1, 1))
        assert result == date(2025, 1, 1)

    def test_july_2_returns_jul_1(self) -> None:
        result = determine_effective_from(date(2025, 7, 2))
        assert result == date(2025, 7, 1)

    def test_year_preserved(self) -> None:
        result_jan = determine_effective_from(date(2024, 3, 1))
        assert result_jan.year == 2024

        result_jul = determine_effective_from(date(2024, 9, 1))
        assert result_jul.year == 2024

    def test_h1_returns_jan_1_same_year(self) -> None:
        for month in range(1, 7):
            result = determine_effective_from(date(2025, month, 15))
            assert result == date(2025, 1, 1), f"Month {month} failed"

    def test_h2_returns_jul_1_same_year(self) -> None:
        for month in range(7, 13):
            result = determine_effective_from(date(2025, month, 15))
            assert result == date(2025, 7, 1), f"Month {month} failed"


# ---------------------------------------------------------------------------
# _build_amfi_date_str
# ---------------------------------------------------------------------------

class TestBuildAmfiDateStr:
    def test_h1_period_returns_30jun(self) -> None:
        """effective_from = Jan 1 → H1 → period ends 30Jun."""
        result = _build_amfi_date_str(date(2025, 1, 1))
        assert result == "30Jun2025"

    def test_h2_period_returns_31dec(self) -> None:
        """effective_from = Jul 1 → H2 → period ends 31Dec."""
        result = _build_amfi_date_str(date(2025, 7, 1))
        assert result == "31Dec2025"

    def test_year_is_preserved_h1(self) -> None:
        result = _build_amfi_date_str(date(2022, 1, 1))
        assert "2022" in result
        assert result == "30Jun2022"

    def test_year_is_preserved_h2(self) -> None:
        result = _build_amfi_date_str(date(2020, 7, 1))
        assert result == "31Dec2020"


# ---------------------------------------------------------------------------
# _build_amfi_urls
# ---------------------------------------------------------------------------

class TestBuildAmfiUrls:
    def test_returns_two_urls(self) -> None:
        urls = _build_amfi_urls(date(2025, 7, 1))
        assert len(urls) == 2

    def test_short_url_is_first(self) -> None:
        urls = _build_amfi_urls(date(2025, 7, 1))
        assert "AverageMarketCapitalization31Dec2025.xlsx" in urls[0]
        assert "oflistedcompanies" not in urls[0]

    def test_long_url_is_second(self) -> None:
        urls = _build_amfi_urls(date(2025, 7, 1))
        assert "oflistedcompanies" in urls[1]
        assert "31Dec2025" in urls[1]

    def test_h1_urls_contain_30jun(self) -> None:
        urls = _build_amfi_urls(date(2024, 1, 1))
        for url in urls:
            assert "30Jun2024" in url

    def test_urls_are_strings(self) -> None:
        urls = _build_amfi_urls(date(2025, 1, 1))
        for url in urls:
            assert isinstance(url, str)
            assert url.startswith("https://")


# ---------------------------------------------------------------------------
# _normalise_category
# ---------------------------------------------------------------------------

class TestNormaliseCategory:
    def test_large_cap_returns_large(self) -> None:
        assert _normalise_category("Large Cap") == "large"

    def test_mid_cap_returns_mid(self) -> None:
        assert _normalise_category("Mid Cap") == "mid"

    def test_small_cap_returns_small(self) -> None:
        assert _normalise_category("Small Cap") == "small"

    def test_case_insensitive(self) -> None:
        assert _normalise_category("LARGE CAP") == "large"
        assert _normalise_category("mid cap") == "mid"
        assert _normalise_category("SMALL CAP") == "small"

    def test_strips_whitespace(self) -> None:
        assert _normalise_category("  Large Cap  ") == "large"

    def test_unrecognised_returns_none(self) -> None:
        assert _normalise_category("Micro Cap") is None
        assert _normalise_category("unknown") is None
        assert _normalise_category("") is None


# ---------------------------------------------------------------------------
# parse_amfi_xlsx
# ---------------------------------------------------------------------------

class TestParseAmfiXlsx:
    def test_basic_parse_returns_rows(self) -> None:
        content = _make_amfi_xlsx([
            (1, "Reliance Industries", "INE002A01018",
             "RELIANCE", 100000, "RELIANCE", 100000, "", "", 100000, "Large Cap"),
            (101, "Tata Motors", "INE155A01022",
             "TATAMOTORS", 20000, "TATAMOTORS", 20000, "", "", 20000, "Mid Cap"),
            (251, "Small Corp", "INE999Z01011",
             "SMALLCO", 5000, "SMALLCO", 5000, "", "", 5000, "Small Cap"),
        ])
        rows = parse_amfi_xlsx(content)
        assert len(rows) == 3

    def test_large_cap_category_parsed(self) -> None:
        content = _make_amfi_xlsx([
            (1, "Big Co", "INE002A01018", "BIGCO", 100000, "BIGCO", 100000, "", "", 100000, "Large Cap"),
        ])
        rows = parse_amfi_xlsx(content)
        assert rows[0]["cap_category"] == "large"

    def test_mid_cap_category_parsed(self) -> None:
        content = _make_amfi_xlsx([
            (150, "Mid Co", "INE111A01011", "MIDCO", 20000, "MIDCO", 20000, "", "", 20000, "Mid Cap"),
        ])
        rows = parse_amfi_xlsx(content)
        assert rows[0]["cap_category"] == "mid"

    def test_small_cap_category_parsed(self) -> None:
        content = _make_amfi_xlsx([
            (300, "Small Co", "INE222B01011", "SMCO", 5000, "SMCO", 5000, "", "", 5000, "Small Cap"),
        ])
        rows = parse_amfi_xlsx(content)
        assert rows[0]["cap_category"] == "small"

    def test_nse_symbol_extracted(self) -> None:
        content = _make_amfi_xlsx([
            (1, "Test Corp", "INE002A01018", "TESTBSE", 50000, "TESTNSE", 50000, "", "", 50000, "Large Cap"),
        ])
        rows = parse_amfi_xlsx(content)
        assert rows[0]["nse_symbol"] == "TESTNSE"

    def test_isin_extracted(self) -> None:
        content = _make_amfi_xlsx([
            (1, "Test Corp", "INE002A01018", "TESTBSE", 50000, "TESTNSE", 50000, "", "", 50000, "Large Cap"),
        ])
        rows = parse_amfi_xlsx(content)
        assert rows[0]["isin"] == "INE002A01018"

    def test_rank_extracted(self) -> None:
        content = _make_amfi_xlsx([
            (42, "Test Corp", "INE002A01018", "TESTBSE", 50000, "TESTNSE", 50000, "", "", 50000, "Large Cap"),
        ])
        rows = parse_amfi_xlsx(content)
        assert rows[0]["rank"] == 42

    def test_company_name_extracted(self) -> None:
        content = _make_amfi_xlsx([
            (1, "Reliance Industries Ltd", "INE002A01018",
             "RELIANCE", 100000, "RELIANCE", 100000, "", "", 100000, "Large Cap"),
        ])
        rows = parse_amfi_xlsx(content)
        assert rows[0]["company_name"] == "Reliance Industries Ltd"

    def test_empty_bytes_returns_empty(self) -> None:
        rows = parse_amfi_xlsx(b"")
        assert rows == []

    def test_garbage_bytes_returns_empty(self) -> None:
        rows = parse_amfi_xlsx(b"not an excel file at all")
        assert rows == []

    def test_unknown_category_falls_back_to_rank(self) -> None:
        """If categorization is unrecognised, rank-based category is used."""
        content = _make_amfi_xlsx([
            (1, "Test Corp", "INE002A01018", "TESTBSE", 50000, "TESTNSE", 50000, "", "", 50000, "Unknown"),
        ])
        rows = parse_amfi_xlsx(content)
        # Rank 1 → large (rank-based fallback)
        assert rows[0]["cap_category"] == "large"

    def test_multiple_rows_all_returned(self) -> None:
        data = [
            (i, f"Company {i}", f"INE{i:06d}A{i:04d}"[:12].ljust(12, "0"),
             f"BSE{i}", i * 1000, f"NSE{i}", i * 1000, "", "", i * 1000,
             "Large Cap" if i <= 100 else ("Mid Cap" if i <= 250 else "Small Cap"))
            for i in range(1, 11)
        ]
        content = _make_amfi_xlsx(data)
        rows = parse_amfi_xlsx(content)
        assert len(rows) == 10


# ---------------------------------------------------------------------------
# _parse_text
# ---------------------------------------------------------------------------

class TestParseText:
    def _make_csv(self, rows: list[tuple]) -> bytes:
        lines = ["Rank,ISIN,Company Name"]
        for rank, isin, name in rows:
            lines.append(f"{rank},{isin},{name}")
        return "\n".join(lines).encode("utf-8")

    def test_parse_basic_csv(self) -> None:
        content = self._make_csv([
            (1, "INE123A01011", "RELIANCE"),
            (101, "INE456B01022", "MIDCAP CORP"),
            (251, "INE789C01033", "SMALL CO"),
            (501, "INE000D01044", "MICRO CORP"),
        ])
        rows = _parse_text(content)
        assert len(rows) == 4
        assert rows[0]["cap_category"] == "large"
        assert rows[1]["cap_category"] == "mid"
        assert rows[2]["cap_category"] == "small"
        assert rows[3]["cap_category"] == "micro"

    def test_parse_returns_correct_isin(self) -> None:
        content = self._make_csv([(50, "INE123A01011", "TEST CORP")])
        rows = _parse_text(content)
        assert rows[0]["isin"] == "INE123A01011"

    def test_parse_isin_uppercased(self) -> None:
        lines = "Rank,ISIN,Company Name\n1,ine123a01011,Test Corp\n"
        rows = _parse_text(lines.encode("utf-8"))
        assert rows[0]["isin"] == "INE123A01011"

    def test_parse_invalid_rank_skipped(self) -> None:
        lines = "Rank,ISIN,Company Name\nnot_a_number,INE123A01011,Test Corp\n"
        rows = _parse_text(lines.encode("utf-8"))
        assert len(rows) == 0

    def test_parse_short_isin_skipped(self) -> None:
        lines = "Rank,ISIN,Company Name\n1,TOOSHORT,Test Corp\n"
        rows = _parse_text(lines.encode("utf-8"))
        assert len(rows) == 0

    def test_parse_no_header_returns_empty(self) -> None:
        lines = "1,INE123A01011,Test Corp\n"
        rows = _parse_text(lines.encode("utf-8"))
        assert rows == []

    def test_parse_tab_delimited(self) -> None:
        lines = "Rank\tISIN\tCompany Name\n1\tINE123A01011\tTest Corp\n"
        rows = _parse_text(lines.encode("utf-8"))
        assert len(rows) == 1
        assert rows[0]["isin"] == "INE123A01011"

    def test_parse_pipe_delimited(self) -> None:
        lines = "Rank|ISIN|Company Name\n1|INE123A01011|Test Corp\n"
        rows = _parse_text(lines.encode("utf-8"))
        assert len(rows) == 1

    def test_parse_empty_bytes_returns_empty(self) -> None:
        rows = _parse_text(b"")
        assert rows == []

    def test_nse_symbol_field_present(self) -> None:
        """Text-parsed rows should have nse_symbol key (empty string)."""
        content = self._make_csv([(1, "INE123A01011", "Test Corp")])
        rows = _parse_text(content)
        assert "nse_symbol" in rows[0]


# ---------------------------------------------------------------------------
# parse_amfi_cap_list
# ---------------------------------------------------------------------------

class TestParseAmfiCapList:
    def test_xlsx_content_parsed_first(self) -> None:
        content = _make_amfi_xlsx([
            (1, "Test Corp", "INE002A01018", "TESTBSE", 50000, "TESTNSE", 50000, "", "", 50000, "Large Cap"),
        ])
        rows = parse_amfi_cap_list(content)
        assert len(rows) == 1
        assert rows[0]["cap_category"] == "large"

    def test_delegates_to_text_when_xlsx_fails(self) -> None:
        content = "Rank,ISIN,Company Name\n1,INE123A01011,Test Corp\n".encode("utf-8")
        rows = parse_amfi_cap_list(content)
        assert len(rows) == 1
        assert rows[0]["cap_category"] == "large"

    def test_empty_content_returns_empty(self) -> None:
        rows = parse_amfi_cap_list(b"")
        assert rows == []

    def test_returns_list(self) -> None:
        rows = parse_amfi_cap_list(b"no valid content")
        assert isinstance(rows, list)


# ---------------------------------------------------------------------------
# fetch_amfi_cap_list
# ---------------------------------------------------------------------------

class TestFetchAmfiCapList:
    @pytest.mark.asyncio
    async def test_returns_content_on_success(self) -> None:
        mock_response = MagicMock()
        mock_response.content = b"fake content"
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        result = await fetch_amfi_cap_list(mock_client, date(2025, 1, 1))
        assert result == b"fake content"

    @pytest.mark.asyncio
    async def test_raises_on_all_failures(self) -> None:
        import httpx

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("timeout"))

        with pytest.raises((httpx.TimeoutException, RuntimeError)):
            await fetch_amfi_cap_list(mock_client, date(2025, 1, 1))

    @pytest.mark.asyncio
    async def test_empty_response_tries_next_url(self) -> None:
        """Empty content from first URL should try fallback URL."""

        call_count = 0
        fake_content = b"Rank,ISIN,Company Name\n1,INE123A01011,Test\n"

        async def mock_get(url: str, **kwargs: Any) -> MagicMock:
            nonlocal call_count
            call_count += 1
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if call_count <= 1:
                resp.content = b""
            else:
                resp.content = fake_content
            return resp

        mock_client = AsyncMock()
        mock_client.get = mock_get

        result = await fetch_amfi_cap_list(mock_client, date(2025, 1, 1))
        assert result == fake_content
        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_uses_dated_url_for_h1(self) -> None:
        """H1 period should request URL containing 30Jun."""
        called_urls: list[str] = []

        async def mock_get(url: str, **kwargs: Any) -> MagicMock:
            called_urls.append(url)
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            resp.content = b"xlsx content"
            return resp

        mock_client = AsyncMock()
        mock_client.get = mock_get

        await fetch_amfi_cap_list(mock_client, date(2025, 1, 1))
        assert any("30Jun2025" in url for url in called_urls)

    @pytest.mark.asyncio
    async def test_uses_dated_url_for_h2(self) -> None:
        """H2 period should request URL containing 31Dec."""
        called_urls: list[str] = []

        async def mock_get(url: str, **kwargs: Any) -> MagicMock:
            called_urls.append(url)
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            resp.content = b"xlsx content"
            return resp

        mock_client = AsyncMock()
        mock_client.get = mock_get

        await fetch_amfi_cap_list(mock_client, date(2025, 7, 1))
        assert any("31Dec2025" in url for url in called_urls)


# ---------------------------------------------------------------------------
# MarketCapHistoryPipeline — attributes
# ---------------------------------------------------------------------------

class TestMarketCapHistoryPipelineAttributes:
    def test_pipeline_name(self) -> None:
        pipeline = MarketCapHistoryPipeline()
        assert pipeline.pipeline_name == "market_cap_history"

    def test_requires_trading_day_false(self) -> None:
        pipeline = MarketCapHistoryPipeline()
        assert pipeline.requires_trading_day is False

    def test_is_base_pipeline_subclass(self) -> None:
        from app.pipelines.framework import BasePipeline
        assert issubclass(MarketCapHistoryPipeline, BasePipeline)


# ---------------------------------------------------------------------------
# MarketCapHistoryPipeline.execute — mock DB session tests
# ---------------------------------------------------------------------------

class TestMarketCapHistoryExecute:
    def _make_session(
        self,
        symbol_rows: list[tuple[str, uuid.UUID]] | None = None,
        isin_rows: list[tuple[str, uuid.UUID]] | None = None,
    ) -> AsyncMock:
        """Build a mock AsyncSession for execute() tests.

        Args:
            symbol_rows: List of (symbol, uuid) for symbol map query.
            isin_rows: List of (isin, uuid) for ISIN map query.
        """
        session = AsyncMock()

        symbol_rows_real = symbol_rows or []
        isin_rows_real = isin_rows or []

        symbol_result = MagicMock()
        symbol_result.__iter__ = MagicMock(
            return_value=iter([
                MagicMock(current_symbol=sym, id=uid)
                for sym, uid in symbol_rows_real
            ])
        )

        isin_result = MagicMock()
        isin_result.__iter__ = MagicMock(
            return_value=iter([
                MagicMock(isin=isin, id=uid)
                for isin, uid in isin_rows_real
            ])
        )

        update_result = MagicMock()
        update_result.rowcount = 0

        call_count = 0

        async def mock_execute(stmt: Any, *args: Any, **kwargs: Any) -> MagicMock:
            nonlocal call_count
            call_count += 1
            # 1: symbol map, 2: isin map, 3: UPDATE close, 4: INSERT upsert
            if call_count == 1:
                return symbol_result
            elif call_count == 2:
                return isin_result
            elif call_count == 3:
                return update_result
            else:
                return MagicMock()

        session.execute = mock_execute
        return session

    @pytest.mark.asyncio
    async def test_execute_returns_zero_rows_when_parse_fails(self) -> None:
        """If AMFI returns unparseable content, rows_processed = 0."""
        pipeline = MarketCapHistoryPipeline()
        session = self._make_session()
        run_log = MagicMock()

        with patch(
            "app.pipelines.equity.market_cap_history.fetch_amfi_cap_list",
            new_callable=AsyncMock,
            return_value=b"garbage content",
        ):
            result = await pipeline.execute(date(2025, 4, 1), session, run_log)

        assert result.rows_processed == 0

    @pytest.mark.asyncio
    async def test_execute_counts_unmatched_symbols_and_isins_as_failed(self) -> None:
        """Rows with no symbol/ISIN match in de_instrument → rows_failed."""
        pipeline = MarketCapHistoryPipeline()
        session = self._make_session(symbol_rows=[], isin_rows=[])
        run_log = MagicMock()

        xlsx_content = _make_amfi_xlsx([
            (1, "Large Corp", "INE123A01011", "LARGECO", 100000, "LARGECO", 100000, "", "", 100000, "Large Cap"),
            (101, "Mid Corp", "INE456B01022", "MIDCO", 20000, "MIDCO", 20000, "", "", 20000, "Mid Cap"),
        ])

        with patch(
            "app.pipelines.equity.market_cap_history.fetch_amfi_cap_list",
            new_callable=AsyncMock,
            return_value=xlsx_content,
        ):
            result = await pipeline.execute(date(2025, 4, 1), session, run_log)

        assert result.rows_failed == 2
        assert result.rows_processed == 0

    @pytest.mark.asyncio
    async def test_execute_processes_symbol_matched_instruments(self) -> None:
        """NSE symbol matches → rows_processed incremented."""
        pipeline = MarketCapHistoryPipeline()
        uid1 = uuid.uuid4()
        uid2 = uuid.uuid4()
        session = self._make_session(
            symbol_rows=[("LARGECO", uid1), ("MIDCO", uid2)],
            isin_rows=[],
        )
        run_log = MagicMock()

        xlsx_content = _make_amfi_xlsx([
            (1, "Large Corp", "INE123A01011", "LARGEBSE", 100000, "LARGECO", 100000, "", "", 100000, "Large Cap"),
            (101, "Mid Corp", "INE456B01022", "MIDBSE", 20000, "MIDCO", 20000, "", "", 20000, "Mid Cap"),
        ])

        with patch(
            "app.pipelines.equity.market_cap_history.fetch_amfi_cap_list",
            new_callable=AsyncMock,
            return_value=xlsx_content,
        ):
            result = await pipeline.execute(date(2025, 4, 1), session, run_log)

        assert result.rows_processed == 2
        assert result.rows_failed == 0

    @pytest.mark.asyncio
    async def test_execute_falls_back_to_isin_when_no_symbol_match(self) -> None:
        """If NSE symbol not found, ISIN fallback should match."""
        pipeline = MarketCapHistoryPipeline()
        uid1 = uuid.uuid4()
        # Symbol map is empty; ISIN map has the match
        session = self._make_session(
            symbol_rows=[],
            isin_rows=[("INE123A01011", uid1)],
        )
        run_log = MagicMock()

        xlsx_content = _make_amfi_xlsx([
            (1, "Large Corp", "INE123A01011", "LARGEBSE", 100000, "UNKNOWNSYMBOL", 100000, "", "", 100000, "Large Cap"),
        ])

        with patch(
            "app.pipelines.equity.market_cap_history.fetch_amfi_cap_list",
            new_callable=AsyncMock,
            return_value=xlsx_content,
        ):
            result = await pipeline.execute(date(2025, 4, 1), session, run_log)

        assert result.rows_processed == 1
        assert result.rows_failed == 0

    @pytest.mark.asyncio
    async def test_execute_mixed_match_counts(self) -> None:
        """Some rows match by symbol, one fails entirely."""
        pipeline = MarketCapHistoryPipeline()
        uid1 = uuid.uuid4()
        session = self._make_session(
            symbol_rows=[("MATCHEDCO", uid1)],
            isin_rows=[],
        )
        run_log = MagicMock()

        xlsx_content = _make_amfi_xlsx([
            (1, "Matched Corp", "INE001A01011", "MATCHEDBSE", 100000, "MATCHEDCO", 100000, "", "", 100000, "Large Cap"),
            (102, "Unmatched Corp", "INE999Z01011", "NOBSE", 5000, "NOSYMBOL", 5000, "", "", 5000, "Mid Cap"),
        ])

        with patch(
            "app.pipelines.equity.market_cap_history.fetch_amfi_cap_list",
            new_callable=AsyncMock,
            return_value=xlsx_content,
        ):
            result = await pipeline.execute(date(2025, 4, 1), session, run_log)

        assert result.rows_processed == 1
        assert result.rows_failed == 1


# ---------------------------------------------------------------------------
# Integration: parse then category check
# ---------------------------------------------------------------------------

class TestParseAndCategorize:
    def test_100_large_caps_parsed_correctly_from_xlsx(self) -> None:
        data = [
            (i, f"Company {i}", f"INE{i:06d}A{i:04d}"[:12].ljust(12, "0"),
             f"BSE{i}", i * 1000, f"NSE{i}", i * 1000, "", "", i * 1000, "Large Cap")
            for i in range(1, 101)
        ]
        content = _make_amfi_xlsx(data)
        rows = parse_amfi_cap_list(content)
        large_caps = [r for r in rows if r["cap_category"] == "large"]
        assert len(large_caps) == 100

    def test_mixed_categories_classified_correctly_from_xlsx(self) -> None:
        test_data = [
            (50, "Large Cap"),
            (100, "Large Cap"),
            (101, "Mid Cap"),
            (200, "Mid Cap"),
            (250, "Mid Cap"),
            (251, "Small Cap"),
            (400, "Small Cap"),
            (500, "Small Cap"),
        ]
        expected_cats = ["large", "large", "mid", "mid", "mid", "small", "small", "small"]
        xlsx_rows = [
            (rank, f"Co {rank}", f"INE{i:06d}A{rank:04d}"[:12].ljust(12, "0"),
             f"BSE{i}", rank * 100, f"NSE{i}", rank * 100, "", "", rank * 100, cat_label)
            for i, (rank, cat_label) in enumerate(test_data, start=1)
        ]
        content = _make_amfi_xlsx(xlsx_rows)
        rows = parse_amfi_cap_list(content)
        assert len(rows) == len(test_data)
        for row, expected_cat in zip(rows, expected_cats):
            assert row["cap_category"] == expected_cat

    def test_100_large_caps_from_text_csv(self) -> None:
        lines = ["Rank,ISIN,Company Name"]
        for i in range(1, 101):
            isin = f"INE{i:06d}A{i:04d}"[:12].ljust(12, "0")
            lines.append(f"{i},{isin},Company {i}")
        content = "\n".join(lines).encode("utf-8")

        rows = parse_amfi_cap_list(content)
        large_caps = [r for r in rows if r["cap_category"] == "large"]
        assert len(large_caps) == 100
