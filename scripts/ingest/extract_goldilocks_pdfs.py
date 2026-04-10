"""Extract text from Goldilocks PDF files on disk and update de_qual_documents.

Standalone script — runs directly on EC2 or locally. Uses psycopg2 + plain SQL,
not SQLAlchemy async. Loads DATABASE_URL_SYNC (or DATABASE_URL) from environment.

Usage:
    python3 scripts/ingest/extract_goldilocks_pdfs.py
    python3 scripts/ingest/extract_goldilocks_pdfs.py --dry-run
    python3 scripts/ingest/extract_goldilocks_pdfs.py --pdf-dir /custom/path
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import urllib.parse
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Bootstrap: load .env from repo root so DATABASE_URL_SYNC is available
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).parent.parent.parent


def _load_env() -> None:
    """Load key=value pairs from .env into os.environ (does not overwrite)."""
    env_path = _REPO_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key not in os.environ:
            os.environ[key] = val


_load_env()

# ---------------------------------------------------------------------------
# Add repo root to path so we can import app modules
# ---------------------------------------------------------------------------
sys.path.insert(0, str(_REPO_ROOT))

# ---------------------------------------------------------------------------
# Third-party imports
# ---------------------------------------------------------------------------
try:
    import psycopg2
    import psycopg2.extras
except ImportError as exc:
    print(f"[ERROR] psycopg2 not available: {exc}", flush=True)
    sys.exit(1)

# Import reusable extractor — use importlib to avoid triggering app.__init__
try:
    import importlib.util as _ilu
    _spec = _ilu.spec_from_file_location(
        "pdf_extractor",
        str(_REPO_ROOT / "app" / "pipelines" / "qualitative" / "pdf_extractor.py"),
    )
    _mod = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    extract_pdf_text = _mod.extract_pdf_text
    classify_report_type = _mod.classify_report_type
except Exception as exc:
    print(f"[ERROR] Could not import pdf_extractor: {exc}", flush=True)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_PDF_DIR = Path("/home/ubuntu/jip-data-engine/data/goldilocks/pdfs")

# Goldilocks PAN card — PDF encryption password
GOLDILOCKS_PDF_PASSWORD = "AICPJ9616P"

# Skip update if raw_text already looks extracted (idempotency threshold)
ALREADY_EXTRACTED_MIN_CHARS = 200


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def ts() -> str:
    """Return current UTC timestamp string for print logs."""
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


def _err(msg: str) -> None:
    print(f"[{ts()}] [ERROR] {msg}", file=sys.stderr, flush=True)


def get_db_conn() -> "psycopg2.connection":
    """Return a psycopg2 connection from DATABASE_URL_SYNC or DATABASE_URL."""
    url = os.environ.get("DATABASE_URL_SYNC") or os.environ.get("DATABASE_URL", "")
    # Normalise: strip async driver prefix so psycopg2 accepts it
    if url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
    elif url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql+psycopg2://", "postgresql://", 1)
    if not url:
        raise RuntimeError("DATABASE_URL_SYNC / DATABASE_URL not set in environment")
    return psycopg2.connect(url)


def extract_filename_from_url(source_url: str) -> Optional[str]:
    """Extract the PDF filename from a Goldilocks source_url.

    URL pattern::

        https://goldilocksresearch.com/set_password.php?FileName=reports/y0LITpTrend%20Friend.pdf&id=3923

    Returns the filename portion after "reports/" (e.g., "y0LITpTrend Friend Daily 2nd Apr 2026.pdf"),
    URL-decoded. Returns None if parsing fails.
    """
    if not source_url:
        return None
    try:
        parsed = urllib.parse.urlparse(source_url)
        qs = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        file_name_values = qs.get("FileName")
        if not file_name_values:
            return None
        raw_value = file_name_values[0]  # e.g. "reports/y0LITpTrend Friend Daily 2nd Apr 2026.pdf"
        # URL-decode spaces and special chars
        decoded = urllib.parse.unquote(raw_value)
        # Strip the "reports/" prefix
        if "/" in decoded:
            filename = decoded.rsplit("/", 1)[-1]
        else:
            filename = decoded
        return filename if filename else None
    except Exception:
        return None


def find_pdf_on_disk(pdf_dir: Path, filename: str) -> Optional[Path]:
    """Find the PDF file in pdf_dir, handling &id=NNNN suffix on disk filenames.

    Files on disk have names like 'y0LITpTrend Friend Daily 2nd Apr 2026.pdf&id=3923'
    but the parsed filename is 'y0LITpTrend Friend Daily 2nd Apr 2026.pdf'.

    Returns the full Path if found, None otherwise.
    """
    # Try exact match first
    candidate = pdf_dir / filename
    if candidate.exists():
        return candidate
    # Try with &id= suffix (files on disk have this from the download URL)
    for f in pdf_dir.iterdir():
        if f.name.startswith(filename):
            return f
    return None


def fetch_goldilocks_pdf_docs(
    conn: "psycopg2.connection",
) -> list[dict]:
    """Query de_qual_documents for all Goldilocks PDF rows."""
    sql = """
        SELECT
            d.id::text AS id,
            d.title,
            d.source_url,
            d.raw_text,
            d.report_type
        FROM de_qual_documents d
        WHERE d.source_id IN (
            SELECT id FROM de_qual_sources
            WHERE source_name ILIKE '%goldilocks%'
        )
          AND d.original_format = 'pdf'
        ORDER BY d.created_at
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def update_document(
    conn: "psycopg2.connection",
    doc_id: str,
    raw_text: str,
    report_type: Optional[str],
) -> None:
    """UPDATE de_qual_documents with extracted text and classified report_type."""
    sql = """
        UPDATE de_qual_documents
        SET raw_text = %s,
            report_type = %s,
            updated_at = NOW()
        WHERE id = %s::uuid
    """
    with conn.cursor() as cur:
        cur.execute(sql, (raw_text, report_type, doc_id))
    conn.commit()


# ---------------------------------------------------------------------------
# Main extraction logic
# ---------------------------------------------------------------------------
def run(pdf_dir: Path, dry_run: bool) -> None:
    """Main extraction loop.

    Args:
        pdf_dir: Directory containing downloaded Goldilocks PDFs.
        dry_run: If True, extract and classify but do not UPDATE the database.
    """
    _log(f"Starting Goldilocks PDF extraction — pdf_dir={pdf_dir} dry_run={dry_run}")

    conn = get_db_conn()
    docs = fetch_goldilocks_pdf_docs(conn)
    _log(f"Fetched {len(docs)} Goldilocks PDF documents from de_qual_documents")

    # Counters for summary
    total_processed = 0
    total_chars = 0
    skipped_already_done = 0
    skipped_file_missing = 0
    failed = 0
    report_type_counts: Counter = Counter()

    for doc in docs:
        doc_id = doc["id"]
        title = doc["title"] or ""
        source_url = doc["source_url"] or ""
        existing_text = doc["raw_text"] or ""

        # Idempotency: skip if already extracted
        if len(existing_text) > ALREADY_EXTRACTED_MIN_CHARS:
            _log(f"SKIP (already extracted) id={doc_id[:8]}... title={title!r}")
            skipped_already_done += 1
            continue

        # Derive filename from source_url
        filename = extract_filename_from_url(source_url)
        if not filename:
            _log(f"WARN no filename in source_url — id={doc_id[:8]}... url={source_url!r}")
            skipped_file_missing += 1
            continue

        # Find file on disk
        pdf_path = find_pdf_on_disk(pdf_dir, filename)
        if pdf_path is None:
            _log(f"WARN file not found on disk — id={doc_id[:8]}... filename={filename!r}")
            skipped_file_missing += 1
            continue

        try:
            t0 = time.monotonic()

            # Extract text from PDF
            extracted_text = extract_pdf_text(pdf_path, password=GOLDILOCKS_PDF_PASSWORD)
            elapsed_ms = int((time.monotonic() - t0) * 1000)

            # Classify report type
            report_type = classify_report_type(title, extracted_text)

            # Skip update for non-content PDFs (Disclaimer, Privacy, Terms)
            if report_type is None and _is_non_content_title(title):
                _log(
                    f"SKIP (non-content PDF) id={doc_id[:8]}... "
                    f"title={title!r} chars={len(extracted_text)}"
                )
                skipped_already_done += 1
                continue

            _log(
                f"EXTRACTED id={doc_id[:8]}... title={title!r} "
                f"chars={len(extracted_text)} report_type={report_type!r} elapsed={elapsed_ms}ms"
            )

            if not dry_run:
                update_document(conn, doc_id, extracted_text, report_type)
            else:
                _log(f"  [DRY-RUN] would UPDATE id={doc_id[:8]}...")

            total_processed += 1
            total_chars += len(extracted_text)
            if report_type:
                report_type_counts[report_type] += 1
            else:
                report_type_counts["unclassified"] += 1

        except Exception as exc:
            _err(f"FAILED id={doc_id[:8]}... title={title!r} — {exc}")
            failed += 1

    conn.close()

    # Summary
    _log("=" * 60)
    _log("SUMMARY")
    _log(f"  Total PDFs processed  : {total_processed}")
    _log(f"  Total chars extracted : {total_chars:,}")
    _log(f"  Skipped (done/skip)   : {skipped_already_done}")
    _log(f"  Skipped (file missing): {skipped_file_missing}")
    _log(f"  Failed (exception)    : {failed}")
    _log("  By report_type:")
    for rtype, count in sorted(report_type_counts.items()):
        _log(f"    {rtype:<25} {count}")
    _log("=" * 60)


def _is_non_content_title(title: str) -> bool:
    """Return True if the title indicates a non-content PDF (Disclaimer, Privacy, Terms)."""
    title_lower = title.lower()
    return any(
        kw in title_lower
        for kw in ("disclaimer", "privacy policy", "terms and conditions", "terms-and-conditions")
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract text from Goldilocks PDFs and update de_qual_documents."
    )
    parser.add_argument(
        "--pdf-dir",
        type=Path,
        default=DEFAULT_PDF_DIR,
        help=f"Directory containing Goldilocks PDFs (default: {DEFAULT_PDF_DIR})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract and classify but do not UPDATE the database",
    )
    args = parser.parse_args()

    run(pdf_dir=args.pdf_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
