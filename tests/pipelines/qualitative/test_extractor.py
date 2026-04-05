"""Tests for ContentExtractor: audio, PDF, URL, and text extraction."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.pipelines.qualitative.extractor import ContentExtractor, ExtractionError


@pytest.fixture
def extractor() -> ContentExtractor:
    """Return a ContentExtractor with test settings."""
    with patch("app.pipelines.qualitative.extractor.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            openai_api_key="test-openai-key",
            anthropic_api_key="test-anthropic-key",
        )
        return ContentExtractor()


class TestTextExtraction:
    """Tests for plain text and pass-through extraction."""

    @pytest.mark.asyncio
    async def test_text_passthrough_from_content(self, extractor: ContentExtractor) -> None:
        """Text content should be returned as-is."""
        content = "RBI rate hike expected in Q2 FY26."
        result = await extractor.extract("text", text_content=content)
        assert result == content

    @pytest.mark.asyncio
    async def test_text_extraction_from_file(
        self, extractor: ContentExtractor, tmp_path: Path
    ) -> None:
        """Text extraction from file should return file contents."""
        content = "SEBI board meeting outcome: new F&O rules from June 2026."
        txt_file = tmp_path / "report.txt"
        txt_file.write_text(content, encoding="utf-8")

        result = await extractor.extract("text", file_path=txt_file)
        assert result == content

    @pytest.mark.asyncio
    async def test_text_extraction_missing_args_raises(
        self, extractor: ContentExtractor
    ) -> None:
        """Missing both text_content and file_path should raise ValueError."""
        with pytest.raises(ValueError, match="text_content or file_path"):
            await extractor.extract("text")

    @pytest.mark.asyncio
    async def test_unsupported_format_raises(self, extractor: ContentExtractor) -> None:
        """Unsupported format_type should raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported format_type"):
            await extractor.extract("docx")


class TestUrlExtraction:
    """Tests for URL content extraction via BeautifulSoup."""

    @pytest.mark.asyncio
    async def test_url_extraction_returns_text(self, extractor: ContentExtractor) -> None:
        """URL extraction should strip HTML tags and return clean text."""
        html = "<html><body><article><p>Gold prices rise on Fed uncertainty.</p></article></body></html>"

        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("app.pipelines.qualitative.extractor.httpx.AsyncClient", return_value=mock_client):
            result = await extractor.extract("url", url="https://example.com/article")

        assert "Gold prices rise on Fed uncertainty" in result

    @pytest.mark.asyncio
    async def test_url_extraction_removes_script_tags(self, extractor: ContentExtractor) -> None:
        """Script tags should be stripped from URL content."""
        html = "<html><body><script>alert('xss')</script><p>Market update.</p></body></html>"

        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("app.pipelines.qualitative.extractor.httpx.AsyncClient", return_value=mock_client):
            result = await extractor.extract("url", url="https://example.com/")

        assert "alert" not in result
        assert "Market update" in result

    @pytest.mark.asyncio
    async def test_url_extraction_retries_on_error(self, extractor: ContentExtractor) -> None:
        """URL extraction should raise ExtractionError after 3 failed attempts."""
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(side_effect=Exception("connection refused"))

        with patch("app.pipelines.qualitative.extractor.httpx.AsyncClient", return_value=mock_client):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with pytest.raises(ExtractionError, match="url"):
                    await extractor.extract("url", url="https://unreachable.example.com/")


class TestPdfExtraction:
    """Tests for PDF text extraction."""

    @pytest.mark.asyncio
    async def test_pdf_extraction_via_pymupdf(
        self, extractor: ContentExtractor, tmp_path: Path
    ) -> None:
        """PDF with extractable text should return content via PyMuPDF."""
        pdf_file = tmp_path / "report.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 dummy content")

        mock_page = MagicMock()
        mock_page.get_text.return_value = "Equity markets outlook bullish for H2 FY26."
        mock_doc = MagicMock()
        mock_doc.__iter__ = MagicMock(return_value=iter([mock_page]))
        mock_doc.close = MagicMock()

        mock_fitz = MagicMock()
        mock_fitz.open = MagicMock(return_value=mock_doc)

        with patch.dict("sys.modules", {"fitz": mock_fitz}):
            result = await extractor.extract("pdf", file_path=pdf_file)

        assert "Equity markets" in result

    @pytest.mark.asyncio
    async def test_pdf_empty_falls_back_to_claude_vision(
        self, extractor: ContentExtractor, tmp_path: Path
    ) -> None:
        """Empty PyMuPDF extraction should trigger Claude vision fallback."""
        pdf_file = tmp_path / "scanned.pdf"
        pdf_file.write_bytes(b"%PDF-1.4")

        mock_page = MagicMock()
        mock_page.get_text.return_value = ""  # Empty — triggers fallback
        mock_doc = MagicMock()
        mock_doc.__iter__ = MagicMock(return_value=iter([mock_page]))
        mock_doc.close = MagicMock()
        mock_doc.__len__ = MagicMock(return_value=1)

        mock_fitz = MagicMock()
        mock_fitz.open = MagicMock(return_value=mock_doc)

        claude_response_text = "Macro outlook: neutral on equities, bullish on gold."

        with patch.dict("sys.modules", {"fitz": mock_fitz}):
            with patch.object(
                extractor,
                "_extract_pdf_with_claude_vision",
                new=AsyncMock(return_value=claude_response_text),
            ):
                result = await extractor.extract("pdf", file_path=pdf_file)

        assert "Macro outlook" in result
