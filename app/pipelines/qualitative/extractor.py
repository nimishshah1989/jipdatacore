"""Content extractor: text, PDF, URL, audio formats.

Extracts raw text from documents before passing to Claude for structured extraction.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from app.config import get_settings
from app.logging import get_logger

logger = get_logger(__name__)

_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 2.0  # seconds
_HTTP_TIMEOUT = 30.0  # seconds


class ExtractionError(Exception):
    """Raised when content extraction fails after retries."""

    def __init__(self, format_type: str, message: str) -> None:
        super().__init__(f"Extraction failed for format={format_type!r}: {message}")
        self.format_type = format_type


class ContentExtractor:
    """Extracts raw text from various content types.

    Supported formats: text, pdf, url, audio
    """

    def __init__(self) -> None:
        self._settings = get_settings()

    async def extract(
        self,
        format_type: str,
        *,
        file_path: Optional[Path] = None,
        url: Optional[str] = None,
        text_content: Optional[str] = None,
    ) -> str:
        """Extract raw text from the given content.

        Args:
            format_type: One of 'text', 'pdf', 'url', 'audio'.
            file_path: Path to a local file (for 'text', 'pdf', 'audio').
            url: URL to fetch (for 'url').
            text_content: Inline text string (for 'text').

        Returns:
            Extracted raw text.

        Raises:
            ValueError: For unsupported format types or missing required arguments.
            ExtractionError: When extraction fails after retries.
        """
        if format_type == "text":
            return await self._extract_text(file_path=file_path, text_content=text_content)
        elif format_type == "pdf":
            return await self._extract_pdf(file_path=file_path)
        elif format_type == "url":
            return await self._extract_url(url=url)
        elif format_type == "audio":
            return await self._extract_audio(file_path=file_path)
        else:
            raise ValueError(f"Unsupported format_type: {format_type!r}")

    async def _extract_text(
        self,
        file_path: Optional[Path] = None,
        text_content: Optional[str] = None,
    ) -> str:
        """Return text content directly or read from file."""
        if text_content is not None:
            return text_content
        if file_path is not None:
            return file_path.read_text(encoding="utf-8")
        raise ValueError(
            "Must provide either text_content or file_path for text extraction"
        )

    async def _extract_pdf(self, file_path: Optional[Path] = None) -> str:
        """Extract text from PDF using PyMuPDF.

        Falls back to Claude vision for scanned/image PDFs.
        """
        if file_path is None:
            raise ValueError("file_path required for pdf extraction")

        try:
            import fitz  # PyMuPDF

            doc = fitz.open(str(file_path))
            pages_text: list[str] = []
            for page in doc:
                pages_text.append(page.get_text())
            doc.close()

            full_text = "\n".join(pages_text).strip()
            if full_text:
                logger.info("pdf_extracted_via_pymupdf", file=file_path.name, chars=len(full_text))
                return full_text

            # Empty — fall back to Claude vision
            logger.info("pdf_empty_text_falling_back_to_claude_vision", file=file_path.name)
            return await self._extract_pdf_with_claude_vision(file_path, doc)

        except Exception as exc:
            raise ExtractionError("pdf", str(exc)) from exc

    async def _extract_pdf_with_claude_vision(
        self, file_path: Path, doc: object
    ) -> str:
        """Use Claude vision API to extract text from scanned PDF pages."""
        import base64

        import fitz  # type: ignore[import]

        pages_texts: list[str] = []
        for page in doc:  # type: ignore[attr-defined]
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
            img_bytes = pix.tobytes("png")
            img_b64 = base64.b64encode(img_bytes).decode()

            try:
                import anthropic  # type: ignore[import]

                client = anthropic.AsyncAnthropic(api_key=self._settings.anthropic_api_key)
                response = await client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=4096,
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "image",
                                    "source": {
                                        "type": "base64",
                                        "media_type": "image/png",
                                        "data": img_b64,
                                    },
                                },
                                {
                                    "type": "text",
                                    "text": (
                                        "Extract all text from this document page. "
                                        "Return only the extracted text, no commentary."
                                    ),
                                },
                            ],
                        }
                    ],
                )
                for block in response.content:
                    if hasattr(block, "text"):
                        pages_texts.append(block.text)
            except Exception as exc:
                logger.warning("claude_vision_page_failed", error=str(exc))
                continue

        return "\n".join(pages_texts).strip()

    async def _extract_url(self, url: Optional[str] = None) -> str:
        """Fetch URL and extract clean text using BeautifulSoup."""
        if url is None:
            raise ValueError("url required for url extraction")

        last_exc: Optional[Exception] = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                async with httpx.AsyncClient(
                    timeout=_HTTP_TIMEOUT,
                    follow_redirects=True,
                    headers={"User-Agent": "JIP-DataEngine/2.0 (+https://jhaveri.com)"},
                ) as client:
                    response = await client.get(url)
                    response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")

                # Remove script and style tags
                for tag in soup(["script", "style", "noscript", "head"]):
                    tag.decompose()

                text = soup.get_text(separator=" ", strip=True)
                logger.info("url_extracted", url=url[:80], chars=len(text))
                return text

            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "url_extraction_attempt_failed",
                    url=url[:80],
                    attempt=attempt,
                    error=str(exc),
                )
                if attempt < _MAX_RETRIES:
                    await asyncio.sleep(_RETRY_BACKOFF_BASE ** attempt)

        raise ExtractionError("url", f"Failed after {_MAX_RETRIES} attempts: {last_exc}")

    async def _extract_audio(self, file_path: Optional[Path] = None) -> str:
        """Transcribe audio using OpenAI Whisper API."""
        if file_path is None:
            raise ValueError("file_path required for audio extraction")

        try:
            import openai  # type: ignore[import]

            client = openai.AsyncOpenAI(api_key=self._settings.openai_api_key)
            with open(file_path, "rb") as audio_file:
                response = await client.audio.transcriptions.create(
                    model="whisper-1",
                    file=audio_file,
                    response_format="text",
                )
            transcript = str(response)
            logger.info("audio_transcribed", file=file_path.name, chars=len(transcript))
            return transcript
        except Exception as exc:
            raise ExtractionError("audio", str(exc)) from exc
