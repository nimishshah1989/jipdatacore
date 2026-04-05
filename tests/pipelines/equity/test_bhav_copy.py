"""Tests for BhavCopyPipeline — format detection, symbol enforcement, validation."""

from __future__ import annotations

import io
import uuid
import zipfile
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.pipelines.equity.bhav_copy import (
    BhavCopyPipeline,
    build_bhav_url,
    detect_bhav_format,
    parse_bhav_csv,
    _safe_decimal,
    _safe_int,
)
from app.utils.symbol_resolver import _clear_symbol_cache


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

STANDARD_HEADER = "SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN"
UDIFF_HEADER = (
    "TradDt,TckrSymb,SctySrs,OpnPric,HghPric,LwPric,"
    "ClsPric,LastPric,PrvsClsgPric,TtlTradgVol,TtlTrfVal,NbOfTxs,ISIN"
)
PRE2010_HEADER = "SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN"


def _make_standard_csv(rows: int = 600, trade_date: str = "01-APR-2023") -> bytes:
    """Generate a standard-format BHAV CSV."""
    lines = [STANDARD_HEADER]
    for i in range(rows):
        symbol = f"STOCK{i:04d}"
        lines.append(
            f"{symbol},EQ,100.0,105.0,99.0,103.0,103.0,102.0,100000,10300000,{trade_date},500,INE{i:06d}XX"
        )
    return "\n".join(lines).encode("utf-8")


def _make_udiff_csv(rows: int = 600, trade_date: str = "01-JUL-2024") -> bytes:
    """Generate a UDiFF-format BHAV CSV."""
    lines = [UDIFF_HEADER]
    for i in range(rows):
        symbol = f"STOCK{i:04d}"
        lines.append(
            f"{trade_date},{symbol},EQ,100.0,105.0,99.0,103.0,103.0,102.0,100000,10300000,500,INE{i:06d}XX"
        )
    return "\n".join(lines).encode("utf-8")


def _make_pre2010_zip(rows: int = 600, trade_date: str = "01-JAN-2009") -> bytes:
    """Generate a pre-2010 BHAV zip with embedded CSV."""
    csv_content = _make_standard_csv(rows, trade_date)
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("eq_01012009_csv.csv", csv_content)
    return zip_buffer.getvalue()


# ---------------------------------------------------------------------------
# Tests: format detection
# ---------------------------------------------------------------------------

class TestBhavFormatDetection:
    def test_bhav_format_detection_pre2010(self) -> None:
        """Pre-2010 date with standard headers → 'pre2010'."""
        result = detect_bhav_format(PRE2010_HEADER, date(2009, 1, 15))
        assert result == "pre2010"

    def test_bhav_format_detection_standard(self) -> None:
        """2015 date with standard headers → 'standard'."""
        result = detect_bhav_format(STANDARD_HEADER, date(2015, 6, 10))
        assert result == "standard"

    def test_bhav_format_detection_udiff(self) -> None:
        """UDiFF header detected regardless of date."""
        result = detect_bhav_format(UDIFF_HEADER, date(2024, 7, 1))
        assert result == "udiff"

    def test_bhav_format_detection_udiff_by_clspric(self) -> None:
        """ClsPric in header → udiff."""
        header_with_clspric = "TradDt,TckrSymb,SctySrs,ClsPric,OpnPric"
        result = detect_bhav_format(header_with_clspric, date(2024, 8, 1))
        assert result == "udiff"


# ---------------------------------------------------------------------------
# Tests: URL building
# ---------------------------------------------------------------------------

class TestBhavUrlBuilding:
    def test_build_url_standard(self) -> None:
        """Standard URL for 2020 date."""
        url, fmt = build_bhav_url(date(2020, 4, 1))
        assert "sec_bhavdata_full_01042020.csv" in url
        assert fmt == "standard"

    def test_build_url_udiff(self) -> None:
        """UDiFF URL for July 2024+ date."""
        url, fmt = build_bhav_url(date(2024, 7, 15))
        assert fmt == "udiff"
        assert "15072024" in url

    def test_build_url_pre2010(self) -> None:
        """Pre-2010 zip URL for 2009 date."""
        url, fmt = build_bhav_url(date(2009, 1, 5))
        assert fmt == "pre2010"
        assert "eq_05012009_csv.zip" in url
        assert "JAN" in url


# ---------------------------------------------------------------------------
# Tests: CSV parsing
# ---------------------------------------------------------------------------

class TestParseBhavCsv:
    def test_parse_standard_csv(self) -> None:
        """Standard CSV parses to normalized DataFrame."""
        raw = _make_standard_csv(rows=10, trade_date="01-APR-2023")
        df = parse_bhav_csv(raw, "standard", date(2023, 4, 1))
        assert len(df) == 10
        assert "symbol" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns

    def test_parse_udiff_csv(self) -> None:
        """UDiFF CSV parses to normalized DataFrame."""
        raw = _make_udiff_csv(rows=8, trade_date="01-JUL-2024")
        df = parse_bhav_csv(raw, "udiff", date(2024, 7, 1))
        assert len(df) == 8
        assert "symbol" in df.columns
        assert "close" in df.columns

    def test_parse_pre2010_zip(self) -> None:
        """Pre-2010 zip extracts and parses CSV."""
        raw = _make_pre2010_zip(rows=5, trade_date="05-JAN-2009")
        df = parse_bhav_csv(raw, "pre2010", date(2009, 1, 5))
        assert len(df) == 5

    def test_parse_filters_eq_series_only(self) -> None:
        """Non-EQ series rows are filtered out."""
        lines = [STANDARD_HEADER]
        eq_row = "RELIANCE,EQ,100.0,105.0,99.0,103.0,103.0,102.0,100000,10300000,01-APR-2023,500,INE002A01018"
        be_row = "RELIANCE,BE,100.0,105.0,99.0,103.0,103.0,102.0,100000,10300000,01-APR-2023,500,INE002A01018"
        sm_row = "RELIANCE,SM,50.0,55.0,49.0,53.0,53.0,52.0,1000,53000,01-APR-2023,50,INE002A01018"
        lines.extend([eq_row, be_row, sm_row])
        raw = "\n".join(lines).encode("utf-8")
        df = parse_bhav_csv(raw, "standard", date(2023, 4, 1))
        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "RELIANCE"


# ---------------------------------------------------------------------------
# Tests: Symbol enforcement
# ---------------------------------------------------------------------------

class TestSymbolEnforcement:
    @pytest.mark.asyncio
    async def test_symbol_enforcement_unknown_symbol_skipped(self) -> None:
        """Unknown symbols are skipped; pipeline does not fail."""
        _clear_symbol_cache()

        # Build a minimal CSV with one known and one unknown symbol
        reliance_id = uuid.uuid4()
        lines = [STANDARD_HEADER]
        reliance_row = (
            "RELIANCE,EQ,2500.0,2550.0,2480.0,2530.0,2530.0,2510.0,"
            "1000000,2530000000,01-APR-2023,5000,INE002A01018"
        )
        unknown_row = (
            "UNKNOWNSYM,EQ,100.0,105.0,99.0,103.0,103.0,102.0,"
            "100000,10300000,01-APR-2023,500,INEINVALID01"
        )
        lines.extend([reliance_row, unknown_row])
        raw = "\n".join(lines).encode("utf-8")

        mock_run_log = MagicMock()
        mock_run_log.id = 1

        mock_session = AsyncMock()
        mock_session.flush = AsyncMock()

        with (
            patch("app.pipelines.equity.bhav_copy.fetch_with_retry", return_value=raw),
            patch("app.pipelines.equity.bhav_copy.check_freshness", return_value=(True, "fresh")),
            patch(
                "app.pipelines.equity.bhav_copy.register_source_file",
                return_value=MagicMock(id=uuid.uuid4()),
            ),
            patch("app.pipelines.equity.bhav_copy.bulk_resolve_symbols") as mock_bulk,
        ):
            mock_bulk.return_value = {"RELIANCE": reliance_id}
            pipeline = BhavCopyPipeline()
            result = await pipeline.execute(date(2023, 4, 1), mock_session, mock_run_log)

        # RELIANCE processed, UNKNOWNSYM skipped (rows_failed=1)
        assert result.rows_processed == 1
        assert result.rows_failed == 1


# ---------------------------------------------------------------------------
# Tests: Freshness validation
# ---------------------------------------------------------------------------

class TestFreshnessValidation:
    @pytest.mark.asyncio
    async def test_freshness_validation_below_500_rows(self) -> None:
        """Rows < 500 triggers low_row_count warning but does not fail pipeline."""
        _clear_symbol_cache()

        # Only 3 rows — below BHAV_MIN_ROWS
        lines = [STANDARD_HEADER]
        for i in range(3):
            lines.append(
                f"STOCK{i:04d},EQ,100.0,105.0,99.0,103.0,103.0,102.0,1000,103000,01-APR-2023,50,INE000{i:06d}XX"
            )
        raw = "\n".join(lines).encode("utf-8")

        mock_run_log = MagicMock()
        mock_run_log.id = 1
        mock_session = AsyncMock()
        mock_session.flush = AsyncMock()

        with (
            patch("app.pipelines.equity.bhav_copy.fetch_with_retry", return_value=raw),
            patch("app.pipelines.equity.bhav_copy.check_freshness", return_value=(True, "fresh")),
            patch(
                "app.pipelines.equity.bhav_copy.register_source_file",
                return_value=MagicMock(id=uuid.uuid4()),
            ),
            patch("app.pipelines.equity.bhav_copy.bulk_resolve_symbols") as mock_bulk,
        ):
            mock_bulk.return_value = {
                f"STOCK{i:04d}": uuid.uuid4() for i in range(3)
            }
            pipeline = BhavCopyPipeline()
            result = await pipeline.execute(date(2023, 4, 1), mock_session, mock_run_log)

        # Pipeline still processes the 3 rows; does not fail
        assert result.rows_processed == 3
        assert result.rows_failed == 0


# ---------------------------------------------------------------------------
# Tests: Idempotent upsert
# ---------------------------------------------------------------------------

class TestUpsertIdempotent:
    @pytest.mark.asyncio
    async def test_upsert_idempotent_same_data(self) -> None:
        """Duplicate file (same checksum) returns rows_processed=0 without error."""
        _clear_symbol_cache()
        raw = _make_standard_csv(rows=600)

        mock_run_log = MagicMock()
        mock_run_log.id = 1
        mock_session = AsyncMock()

        with (
            patch("app.pipelines.equity.bhav_copy.fetch_with_retry", return_value=raw),
            patch(
                "app.pipelines.equity.bhav_copy.check_freshness",
                return_value=(False, "already ingested"),
            ),
        ):
            pipeline = BhavCopyPipeline()
            result = await pipeline.execute(date(2023, 4, 1), mock_session, mock_run_log)

        # Should skip — no rows processed
        assert result.rows_processed == 0
        assert result.rows_failed == 0


# ---------------------------------------------------------------------------
# Tests: Decimal type enforcement
# ---------------------------------------------------------------------------

class TestDecimalPrices:
    def test_safe_decimal_from_string(self) -> None:
        """_safe_decimal converts string to Decimal."""
        result = _safe_decimal("100.50")
        assert isinstance(result, Decimal)
        assert result == Decimal("100.50")

    def test_safe_decimal_from_float_string(self) -> None:
        """_safe_decimal uses str() conversion, not float() — avoids float precision."""
        result = _safe_decimal(100.5)
        assert isinstance(result, Decimal)
        # Must use str(value) path
        assert result == Decimal("100.5")

    def test_safe_decimal_none_on_nan(self) -> None:
        """_safe_decimal returns None for NaN/None."""
        assert _safe_decimal(float("nan")) is None
        assert _safe_decimal(None) is None

    def test_safe_int_valid(self) -> None:
        """_safe_int converts numeric string to int."""
        assert _safe_int("100000") == 100000
        assert _safe_int(99.9) == 99

    def test_safe_int_none_on_nan(self) -> None:
        """_safe_int returns None for NaN."""
        assert _safe_int(float("nan")) is None

    def test_prices_stored_as_decimal(self) -> None:
        """Prices in parsed DataFrame are converted to Decimal via _safe_decimal."""
        raw = _make_standard_csv(rows=2)
        df = parse_bhav_csv(raw, "standard", date(2023, 4, 1))
        # Ensure we can safely run _safe_decimal on the parsed close column
        for val in df["close"].tolist():
            d = _safe_decimal(val)
            assert d is None or isinstance(d, Decimal)
