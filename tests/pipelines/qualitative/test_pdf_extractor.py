"""Tests for pdf_extractor: classify_report_type() and extract_pdf_text()."""

from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.pipelines.qualitative.pdf_extractor import classify_report_type, extract_pdf_text


# ---------------------------------------------------------------------------
# classify_report_type tests
# ---------------------------------------------------------------------------
class TestClassifyReportType:
    """Tests for classify_report_type() covering all 10 types + None cases."""

    def test_trend_friend_from_title(self) -> None:
        assert classify_report_type("Trend Friend Daily 2nd Apr 2026") == "trend_friend"

    def test_big_picture_from_title(self) -> None:
        assert classify_report_type("Big Picture Weekly") == "big_picture"

    def test_big_catch_from_title(self) -> None:
        assert classify_report_type("Big Catch Monthly") == "big_catch"

    def test_stock_bullet_from_title(self) -> None:
        assert classify_report_type("Stock Bullet — HDFC Bank") == "stock_bullet"

    def test_sector_trends_from_title(self) -> None:
        assert classify_report_type("Sector Trends Report Q1") == "sector_trends"

    def test_fortnightly_from_title(self) -> None:
        assert classify_report_type("Fortnightly Market Update") == "fortnightly"

    def test_concall_from_monthly_con(self) -> None:
        assert classify_report_type("Monthly Concall Notes Feb 2026") == "concall"

    def test_concall_from_concall_keyword(self) -> None:
        assert classify_report_type("ConcAll Transcript") == "concall"

    def test_concall_from_con_call_hyphen(self) -> None:
        assert classify_report_type("Con-Call Discussion") == "concall"

    def test_sound_byte_from_title(self) -> None:
        assert classify_report_type("Sound Byte — Nifty Outlook") == "sound_byte"

    def test_sound_byte_hyphen(self) -> None:
        assert classify_report_type("Sound-Byte Macro View") == "sound_byte"

    def test_qa_ampersand(self) -> None:
        assert classify_report_type("Q&A with Gautam Shah") == "qa"

    def test_qa_spaced(self) -> None:
        assert classify_report_type("Q & A Session") == "qa"

    def test_qa_written_out(self) -> None:
        assert classify_report_type("Question and Answer Compilation") == "qa"

    def test_market_snippet_from_title(self) -> None:
        assert classify_report_type("Market Snippet Intraday") == "snippet"

    def test_case_insensitive_title(self) -> None:
        """Classification must be case-insensitive."""
        assert classify_report_type("TREND FRIEND DAILY") == "trend_friend"
        assert classify_report_type("big picture report") == "big_picture"

    def test_disclaimer_returns_none(self) -> None:
        assert classify_report_type("Disclaimer") is None

    def test_privacy_policy_returns_none(self) -> None:
        assert classify_report_type("Privacy Policy") is None

    def test_terms_and_conditions_returns_none(self) -> None:
        assert classify_report_type("Terms and Conditions") is None

    def test_terms_hyphen_returns_none(self) -> None:
        assert classify_report_type("Terms-and-Conditions") is None

    def test_unclassified_returns_none(self) -> None:
        """Unknown document title returns None."""
        assert classify_report_type("Unknown Report Q4 2025") is None

    def test_empty_title_returns_none(self) -> None:
        assert classify_report_type("") is None

    def test_classification_from_text_fallback(self) -> None:
        """When title has no match, check first 500 chars of text."""
        title = "Research Update"
        text = "This is the Trend Friend daily briefing for the week. Nifty outlook..."
        assert classify_report_type(title, text) == "trend_friend"

    def test_title_takes_priority_over_text(self) -> None:
        """Title match should win over text match."""
        title = "Big Picture Monthly"
        text = "Today's sound byte from Gautam Shah..."
        assert classify_report_type(title, text) == "big_picture"

    def test_text_beyond_500_chars_not_checked(self) -> None:
        """Keyword buried beyond 500 chars in text should NOT match."""
        title = "Unnamed Document"
        padding = "x" * 600
        text = padding + " trend friend report here"
        assert classify_report_type(title, text) is None

    def test_text_within_500_chars_checked(self) -> None:
        """Keyword within first 500 chars of text should match."""
        title = "Unnamed Document"
        text = "a" * 100 + " big catch analysis for week"
        assert classify_report_type(title, text) == "big_catch"

    def test_monthly_con_keyword(self) -> None:
        """'monthly con' in title should classify as concall."""
        assert classify_report_type("Monthly Con Call March") == "concall"


# ---------------------------------------------------------------------------
# extract_pdf_text tests (using mocked fitz)
# ---------------------------------------------------------------------------
class TestExtractPdfText:
    """Tests for extract_pdf_text() using mocked PyMuPDF (fitz)."""

    def _make_fitz_doc(
        self,
        pages: list[str],
        is_encrypted: bool = False,
        auth_result: int = 1,
    ) -> MagicMock:
        """Create a mock fitz document with given page texts."""
        mock_doc = MagicMock()
        mock_doc.is_encrypted = is_encrypted
        mock_doc.authenticate.return_value = auth_result
        mock_doc.page_count = len(pages)

        mock_pages = []
        for text in pages:
            page = MagicMock()
            page.get_text.return_value = text
            mock_pages.append(page)

        mock_doc.__iter__ = lambda self: iter(mock_pages)
        mock_doc.__enter__ = lambda self: self
        mock_doc.__exit__ = MagicMock(return_value=False)
        return mock_doc

    def test_extract_single_page(self, tmp_path: Path) -> None:
        """Single-page PDF returns text with page separator."""
        fake_pdf = tmp_path / "test.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4 fake")

        mock_doc = self._make_fitz_doc(["Nifty at 23,000 support zone."])
        with patch("app.pipelines.qualitative.pdf_extractor.fitz.open", return_value=mock_doc):
            result = extract_pdf_text(fake_pdf)

        assert "Nifty at 23,000 support zone." in result
        assert "--- Page 1 ---" in result

    def test_extract_multi_page(self, tmp_path: Path) -> None:
        """Multi-page PDF has separator between each page."""
        fake_pdf = tmp_path / "multi.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4 fake")

        mock_doc = self._make_fitz_doc(["Page one text.", "Page two text."])
        with patch("app.pipelines.qualitative.pdf_extractor.fitz.open", return_value=mock_doc):
            result = extract_pdf_text(fake_pdf)

        assert "--- Page 1 ---" in result
        assert "--- Page 2 ---" in result
        assert "Page one text." in result
        assert "Page two text." in result

    def test_encrypted_pdf_correct_password(self, tmp_path: Path) -> None:
        """Encrypted PDF with correct password authenticates and returns text."""
        fake_pdf = tmp_path / "encrypted.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4 fake")

        mock_doc = self._make_fitz_doc(
            ["Sensitive market report text."],
            is_encrypted=True,
            auth_result=4,  # non-zero = success
        )
        with patch("app.pipelines.qualitative.pdf_extractor.fitz.open", return_value=mock_doc):
            result = extract_pdf_text(fake_pdf, password="AICPJ9616P")

        mock_doc.authenticate.assert_called_once_with("AICPJ9616P")
        assert "Sensitive market report text." in result

    def test_encrypted_pdf_wrong_password_raises(self, tmp_path: Path) -> None:
        """Encrypted PDF with wrong password raises ValueError."""
        fake_pdf = tmp_path / "locked.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4 fake")

        mock_doc = self._make_fitz_doc(
            ["Locked content"],
            is_encrypted=True,
            auth_result=0,  # 0 = authentication failed
        )
        with patch("app.pipelines.qualitative.pdf_extractor.fitz.open", return_value=mock_doc):
            with pytest.raises(ValueError, match="Wrong password"):
                extract_pdf_text(fake_pdf, password="WRONGPASS")

    def test_encrypted_pdf_no_password_raises(self, tmp_path: Path) -> None:
        """Encrypted PDF with no password raises ValueError."""
        fake_pdf = tmp_path / "nopass.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4 fake")

        mock_doc = self._make_fitz_doc([], is_encrypted=True)
        with patch("app.pipelines.qualitative.pdf_extractor.fitz.open", return_value=mock_doc):
            with pytest.raises(ValueError, match="no password provided"):
                extract_pdf_text(fake_pdf, password=None)

    def test_zero_page_pdf_raises(self, tmp_path: Path) -> None:
        """PDF with zero pages raises ValueError."""
        fake_pdf = tmp_path / "empty.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4 fake")

        mock_doc = self._make_fitz_doc([])  # zero pages
        with patch("app.pipelines.qualitative.pdf_extractor.fitz.open", return_value=mock_doc):
            with pytest.raises(ValueError, match="zero pages"):
                extract_pdf_text(fake_pdf)

    def test_corrupt_pdf_raises_runtime_error(self, tmp_path: Path) -> None:
        """Corrupt PDF that fitz cannot open raises RuntimeError."""
        fake_pdf = tmp_path / "corrupt.pdf"
        fake_pdf.write_bytes(b"not a pdf at all")

        with patch(
            "app.pipelines.qualitative.pdf_extractor.fitz.open",
            side_effect=Exception("invalid PDF"),
        ):
            with pytest.raises(RuntimeError, match="Failed to open PDF"):
                extract_pdf_text(fake_pdf)

    def test_page_text_stripped(self, tmp_path: Path) -> None:
        """Whitespace around page text is stripped before joining."""
        fake_pdf = tmp_path / "whitespace.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4 fake")

        mock_doc = self._make_fitz_doc(["  Trend Friend report.  "])
        with patch("app.pipelines.qualitative.pdf_extractor.fitz.open", return_value=mock_doc):
            result = extract_pdf_text(fake_pdf)

        # The full_text is stripped, so leading \n\n is removed from the single-page result
        assert result.startswith("--- Page 1 ---\n\nTrend Friend report.")


# ---------------------------------------------------------------------------
# extract_filename_from_url tests (testing the script helper)
# ---------------------------------------------------------------------------
class TestExtractFilenameFromUrl:
    """Tests for the URL filename extraction helper in the backfill script."""

    def _fn(self, url: str):
        # Import here to avoid module-level side effects
        from scripts.ingest.extract_goldilocks_pdfs import extract_filename_from_url
        return extract_filename_from_url(url)

    def test_standard_url(self) -> None:
        url = (
            "https://www.goldilocksresearch.com/set_password.php"
            "?FileName=reports/y0LITpTrend%20Friend%20Daily%202nd%20Apr%202026.pdf&id=3923"
        )
        result = self._fn(url)
        assert result == "y0LITpTrend Friend Daily 2nd Apr 2026.pdf"

    def test_url_with_unencoded_spaces(self) -> None:
        url = (
            "https://www.goldilocksresearch.com/set_password.php"
            "?FileName=reports/y0LITpTrend Friend Daily 2nd Apr 2026.pdf&id=3923"
        )
        result = self._fn(url)
        assert result == "y0LITpTrend Friend Daily 2nd Apr 2026.pdf"

    def test_url_without_filename_param(self) -> None:
        url = "https://www.goldilocksresearch.com/some_page.php?id=123"
        result = self._fn(url)
        assert result is None

    def test_empty_url_returns_none(self) -> None:
        result = self._fn("")
        assert result is None

    def test_url_no_reports_prefix(self) -> None:
        """FileName without reports/ prefix still returns filename."""
        url = (
            "https://www.goldilocksresearch.com/set_password.php"
            "?FileName=BigPicture.pdf&id=100"
        )
        result = self._fn(url)
        assert result == "BigPicture.pdf"
