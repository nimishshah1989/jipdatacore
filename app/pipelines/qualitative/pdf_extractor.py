"""PDF text extraction and Goldilocks report classification.

Reusable sync module — imported by the backfill script and future daily scraper (C10).
Must remain sync (not async) — used in subprocess contexts on EC2.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import fitz  # PyMuPDF — must be installed: pip install pymupdf

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Keyword → report_type mapping (checked in order, first match wins)
# ---------------------------------------------------------------------------
_REPORT_TYPE_KEYWORDS: list[tuple[str, str]] = [
    ("trend friend", "trend_friend"),
    ("big picture", "big_picture"),
    ("open calls", "big_catch"),
    ("big catch", "big_catch"),
    ("stock bullet", "stock_bullet"),
    ("sector trends", "sector_trends"),
    ("sector trend", "sector_trends"),
    ("fortnightly", "fortnightly"),
    ("monthly con", "concall"),
    ("concall", "concall"),
    ("con-call", "concall"),
    ("con call", "concall"),
    ("sound byte", "sound_byte"),
    ("sound-byte", "sound_byte"),
    ("q&a", "qa"),
    ("q & a", "qa"),
    ("question and answer", "qa"),
    ("market snippet", "snippet"),
    ("buy ", "big_catch"),  # "Buy NTPC", "Buy Lloyds Metals" — catch-all for stock ideas
]

# Non-content PDFs — return None to signal skip
_SKIP_KEYWORDS: list[str] = [
    "disclaimer",
    "privacy policy",
    "terms and conditions",
    "terms-and-conditions",
]


def extract_pdf_text(file_path: Path, password: Optional[str] = None) -> str:
    """Extract text from a PDF file, handling encryption.

    Args:
        file_path: Path to the PDF file on disk.
        password: Optional decryption password. Required for encrypted PDFs.

    Returns:
        Full text of all pages joined with page separators.

    Raises:
        ValueError: If the PDF is encrypted and password is wrong/missing,
                    or if the PDF has zero pages.
        RuntimeError: If fitz (PyMuPDF) raises during open/read.
    """
    try:
        doc = fitz.open(str(file_path))
    except Exception as exc:
        raise RuntimeError(f"Failed to open PDF {file_path}: {exc}") from exc

    try:
        if doc.is_encrypted:
            if password is None:
                raise ValueError(f"PDF is encrypted but no password provided: {file_path}")
            result = doc.authenticate(password)
            if result == 0:
                raise ValueError(
                    f"Wrong password or unrecognised encryption for PDF: {file_path}"
                )

        page_count = doc.page_count
        if page_count == 0:
            raise ValueError(f"PDF has zero pages: {file_path}")

        pages_text: list[str] = []
        for n, page in enumerate(doc, start=1):
            page_text = page.get_text("text").strip()
            pages_text.append(f"\n\n--- Page {n} ---\n\n{page_text}")

        full_text = "".join(pages_text).strip()
        char_count = len(full_text)

        logger.info(
            "pdf_extracted",
            extra={"file": file_path.name, "pages": page_count, "chars": char_count},
        )
        return full_text

    finally:
        doc.close()


def classify_report_type(title: str, text: str = "") -> Optional[str]:
    """Classify a Goldilocks report type from title and/or content.

    Checks title first, then the first 500 chars of text. First match wins.

    Args:
        title: Document title string.
        text: Optional document text (used as fallback if title has no match).

    Returns:
        One of: "trend_friend", "big_picture", "big_catch", "stock_bullet",
        "sector_trends", "fortnightly", "concall", "sound_byte", "qa", "snippet".
        Returns None for non-content PDFs (Disclaimer, Privacy, Terms) or unclassified.
    """
    title_lower = title.lower()
    text_prefix = text[:500].lower()

    # Check non-content / skip keywords first
    for skip_kw in _SKIP_KEYWORDS:
        if skip_kw in title_lower or skip_kw in text_prefix:
            logger.debug("classify_skip", extra={"title": title, "matched_skip": skip_kw})
            return None

    # Try title first, then text prefix
    for search_text in (title_lower, text_prefix):
        for keyword, report_type in _REPORT_TYPE_KEYWORDS:
            if keyword in search_text:
                logger.debug(
                    "classify_matched",
                    extra={"title": title, "keyword": keyword, "report_type": report_type},
                )
                return report_type

    logger.debug("classify_unclassified", extra={"title": title})
    return None
