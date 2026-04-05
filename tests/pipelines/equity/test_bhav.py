"""Tests for BHAV copy parsing and format detection."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.pipelines.equity.bhav import (
    BhavFormat,
    detect_bhav_format,
    parse_bhav_csv,
    _safe_decimal,
    _safe_int,
    _compute_checksum,
    _extract_zip_csv,
)


# ---------------------------------------------------------------------------
# detect_bhav_format
# ---------------------------------------------------------------------------

class TestDetectBhavFormat:
    def test_detect_format_pre2010_header_returns_pre2010(self) -> None:
        header = "SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN"
        result = detect_bhav_format(header)
        assert result == BhavFormat.PRE2010

    def test_detect_format_standard_header_returns_standard(self) -> None:
        header = (
            "SYMBOL,SERIES,DATE1,PREV_CLOSE,OPEN_PRICE,HIGH_PRICE,"
            "LOW_PRICE,LAST_PRICE,CLOSE_PRICE,AVG_PRICE,TTL_TRD_QNTY"
        )
        result = detect_bhav_format(header)
        assert result == BhavFormat.STANDARD

    def test_detect_format_udiff_header_traddt_returns_udiff(self) -> None:
        header = "TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb"
        result = detect_bhav_format(header)
        assert result == BhavFormat.UDIFF

    def test_detect_format_udiff_header_bizdt_returns_udiff(self) -> None:
        header = "BizDt,Sgmt,FinInstrmId,OpnPric,HghPric,LwPric,ClsPric,TtlTrdQty"
        result = detect_bhav_format(header)
        assert result == BhavFormat.UDIFF

    def test_detect_format_unknown_defaults_to_standard(self) -> None:
        header = "COL1,COL2,COL3"
        result = detect_bhav_format(header)
        assert result == BhavFormat.STANDARD

    def test_detect_format_case_insensitive(self) -> None:
        header = "symbol,series,TOTTRDQTY,PREVCLOSE"
        result = detect_bhav_format(header)
        assert result == BhavFormat.PRE2010


# ---------------------------------------------------------------------------
# _safe_decimal
# ---------------------------------------------------------------------------

class TestSafeDecimal:
    def test_safe_decimal_valid_integer_string(self) -> None:
        result = _safe_decimal("1234")
        assert result == Decimal("1234")

    def test_safe_decimal_valid_float_string(self) -> None:
        result = _safe_decimal("1234.5678")
        assert result == Decimal("1234.5678")

    def test_safe_decimal_empty_string_returns_none(self) -> None:
        result = _safe_decimal("")
        assert result is None

    def test_safe_decimal_none_input_returns_none(self) -> None:
        result = _safe_decimal(None)
        assert result is None

    def test_safe_decimal_dash_returns_none(self) -> None:
        result = _safe_decimal("-")
        assert result is None

    def test_safe_decimal_na_returns_none(self) -> None:
        result = _safe_decimal("NA")
        assert result is None

    def test_safe_decimal_not_a_float(self) -> None:
        """Decimal must come from str, never float."""
        val = _safe_decimal("123.456789")
        assert val == Decimal("123.456789")
        # Verify it's Decimal not float
        assert isinstance(val, Decimal)


# ---------------------------------------------------------------------------
# _safe_int
# ---------------------------------------------------------------------------

class TestSafeInt:
    def test_safe_int_valid_integer_string(self) -> None:
        result = _safe_int("50000")
        assert result == 50000

    def test_safe_int_decimal_notation_rounds_down(self) -> None:
        result = _safe_int("50000.0")
        assert result == 50000

    def test_safe_int_empty_returns_none(self) -> None:
        result = _safe_int("")
        assert result is None

    def test_safe_int_none_returns_none(self) -> None:
        result = _safe_int(None)
        assert result is None

    def test_safe_int_na_returns_none(self) -> None:
        result = _safe_int("NA")
        assert result is None


# ---------------------------------------------------------------------------
# parse_bhav_csv — PRE2010 format
# ---------------------------------------------------------------------------

PRE2010_CSV = """\
SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN
RELIANCE,EQ,2400.00,2450.00,2390.00,2430.00,2430.00,2390.00,1234567,300000000,01-Apr-2009,,INE002A01018
INFY,EQ,1500.00,1520.00,1490.00,1510.00,1510.00,1495.00,500000,75000000,01-Apr-2009,,INE009A01021
TCS,EQ,3200.00,3250.00,3180.00,3220.00,3220.00,3190.00,800000,256000000,01-Apr-2009,,INE467B01029
"""


class TestParseBhavCsvPre2010:
    def test_parse_pre2010_returns_correct_row_count(self) -> None:
        rows = parse_bhav_csv(PRE2010_CSV, BhavFormat.PRE2010)
        assert len(rows) == 3

    def test_parse_pre2010_symbol_uppercased(self) -> None:
        rows = parse_bhav_csv(PRE2010_CSV, BhavFormat.PRE2010)
        assert rows[0]["symbol"] == "RELIANCE"

    def test_parse_pre2010_close_is_decimal(self) -> None:
        rows = parse_bhav_csv(PRE2010_CSV, BhavFormat.PRE2010)
        assert isinstance(rows[0]["close"], Decimal)
        assert rows[0]["close"] == Decimal("2430.00")

    def test_parse_pre2010_open_is_decimal(self) -> None:
        rows = parse_bhav_csv(PRE2010_CSV, BhavFormat.PRE2010)
        assert rows[0]["open"] == Decimal("2400.00")

    def test_parse_pre2010_high_is_decimal(self) -> None:
        rows = parse_bhav_csv(PRE2010_CSV, BhavFormat.PRE2010)
        assert rows[0]["high"] == Decimal("2450.00")

    def test_parse_pre2010_low_is_decimal(self) -> None:
        rows = parse_bhav_csv(PRE2010_CSV, BhavFormat.PRE2010)
        assert rows[0]["low"] == Decimal("2390.00")

    def test_parse_pre2010_volume_is_int(self) -> None:
        rows = parse_bhav_csv(PRE2010_CSV, BhavFormat.PRE2010)
        assert rows[0]["volume"] == 1234567

    def test_parse_pre2010_series_preserved(self) -> None:
        rows = parse_bhav_csv(PRE2010_CSV, BhavFormat.PRE2010)
        assert rows[0]["series"] == "EQ"

    def test_parse_pre2010_empty_csv_returns_empty_list(self) -> None:
        rows = parse_bhav_csv("", BhavFormat.PRE2010)
        assert rows == []

    def test_parse_pre2010_skips_row_with_no_close(self) -> None:
        csv = """\
SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN
BADSTOCK,EQ,100.00,110.00,90.00,,100.00,99.00,1000,100000,01-Apr-2009,,
"""
        rows = parse_bhav_csv(csv, BhavFormat.PRE2010)
        assert len(rows) == 0


# ---------------------------------------------------------------------------
# parse_bhav_csv — STANDARD format
# ---------------------------------------------------------------------------

_STANDARD_HEADER = (
    "SYMBOL,SERIES,DATE1,PREV_CLOSE,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,"
    "LAST_PRICE,CLOSE_PRICE,AVG_PRICE,TTL_TRD_QNTY,TURNOVER_LACS,NO_OF_TRADES,DELIV_QTY,DELIV_PER"
)
_STANDARD_ROW1 = "RELIANCE,EQ,01-Apr-2024,2390.00,2400.00,2450.00,2390.00,2430.00,2430.00,2420.00,1234567,300000.00,12345,900000,72.9"  # noqa: E501
_STANDARD_ROW2 = "INFY,EQ,01-Apr-2024,1495.00,1500.00,1520.00,1490.00,1510.00,1510.00,1505.00,500000,75000.00,8000,400000,80.0"  # noqa: E501
STANDARD_CSV = "\n".join([_STANDARD_HEADER, _STANDARD_ROW1, _STANDARD_ROW2]) + "\n"


class TestParseBhavCsvStandard:
    def test_parse_standard_returns_correct_row_count(self) -> None:
        rows = parse_bhav_csv(STANDARD_CSV, BhavFormat.STANDARD)
        assert len(rows) == 2

    def test_parse_standard_close_price_is_decimal(self) -> None:
        rows = parse_bhav_csv(STANDARD_CSV, BhavFormat.STANDARD)
        assert rows[0]["close"] == Decimal("2430.00")

    def test_parse_standard_open_price_is_decimal(self) -> None:
        rows = parse_bhav_csv(STANDARD_CSV, BhavFormat.STANDARD)
        assert rows[0]["open"] == Decimal("2400.00")

    def test_parse_standard_volume_is_int(self) -> None:
        rows = parse_bhav_csv(STANDARD_CSV, BhavFormat.STANDARD)
        assert rows[0]["volume"] == 1234567

    def test_parse_standard_date_parsed(self) -> None:
        rows = parse_bhav_csv(STANDARD_CSV, BhavFormat.STANDARD)
        assert rows[0]["date"] == date(2024, 4, 1)

    def test_parse_standard_symbol_uppercased(self) -> None:
        rows = parse_bhav_csv(STANDARD_CSV, BhavFormat.STANDARD)
        assert rows[0]["symbol"] == "RELIANCE"


# ---------------------------------------------------------------------------
# parse_bhav_csv — UDIFF format
# ---------------------------------------------------------------------------

_UDIFF_HEADER = (
    "TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,"
    "TckrSymb,SctySrs,OpnPric,HghPric,LwPric,ClsPric,LastPric,TtlTrdQty,TtlTrdVal"
)
_UDIFF_ROW1 = "2024-07-15,2024-07-15,CM,NSE,EQ,RELIANCE,INE002A01018,RELIANCE,EQ,2400.00,2450.00,2390.00,2430.00,2430.00,1234567,3000000000.00"  # noqa: E501
_UDIFF_ROW2 = "2024-07-15,2024-07-15,CM,NSE,EQ,INFY,INE009A01021,INFY,EQ,1500.00,1520.00,1490.00,1510.00,1510.00,500000,755000000.00"  # noqa: E501
UDIFF_CSV = "\n".join([_UDIFF_HEADER, _UDIFF_ROW1, _UDIFF_ROW2]) + "\n"


class TestParseBhavCsvUdiff:
    def test_parse_udiff_returns_correct_row_count(self) -> None:
        rows = parse_bhav_csv(UDIFF_CSV, BhavFormat.UDIFF)
        assert len(rows) == 2

    def test_parse_udiff_close_price_is_decimal(self) -> None:
        rows = parse_bhav_csv(UDIFF_CSV, BhavFormat.UDIFF)
        # UDIFF uses CLSPRIC or LASTPRIC for close
        assert isinstance(rows[0]["close"], Decimal)

    def test_parse_udiff_volume_is_int(self) -> None:
        rows = parse_bhav_csv(UDIFF_CSV, BhavFormat.UDIFF)
        assert rows[0]["volume"] == 1234567

    def test_parse_udiff_date_parsed(self) -> None:
        rows = parse_bhav_csv(UDIFF_CSV, BhavFormat.UDIFF)
        assert rows[0]["date"] == date(2024, 7, 15)


# ---------------------------------------------------------------------------
# _compute_checksum
# ---------------------------------------------------------------------------

class TestComputeChecksum:
    def test_compute_checksum_returns_64_char_hex(self) -> None:
        result = _compute_checksum(b"test content")
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_compute_checksum_deterministic(self) -> None:
        content = b"same content"
        assert _compute_checksum(content) == _compute_checksum(content)

    def test_compute_checksum_different_content_different_hash(self) -> None:
        assert _compute_checksum(b"content A") != _compute_checksum(b"content B")


# ---------------------------------------------------------------------------
# _extract_zip_csv
# ---------------------------------------------------------------------------

class TestExtractZipCsv:
    def test_extract_zip_csv_returns_csv_content(self) -> None:
        import io
        import zipfile

        csv_text = "SYMBOL,SERIES\nRELIANCE,EQ\n"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("eq_01042009_csv.csv", csv_text)
        zip_bytes = buf.getvalue()

        result = _extract_zip_csv(zip_bytes)
        assert "RELIANCE" in result
        assert "SYMBOL" in result

    def test_extract_zip_csv_raises_if_no_csv(self) -> None:
        import io
        import zipfile

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "no csv here")
        zip_bytes = buf.getvalue()

        with pytest.raises(ValueError, match="No CSV file found"):
            _extract_zip_csv(zip_bytes)
