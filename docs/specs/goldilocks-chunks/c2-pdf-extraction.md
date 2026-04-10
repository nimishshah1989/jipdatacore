# C2: PDF Text Extraction

**Complexity:** Low
**Dependencies:** C1 (report_type column on DeQualDocuments must exist)
**Status:** pending

## Files
- scripts/ingest/extract_goldilocks_pdfs.py (new — one-time backfill script)
- app/pipelines/qualitative/pdf_extractor.py (new — reusable PDF extraction module)

## What To Build

### pdf_extractor.py — Reusable Module

This module is imported by both the backfill script and the daily scraper (future C10).
Must be sync (not async) — used in subprocess contexts on EC2.

**Function: extract_pdf_text(file_path: Path, password: str | None = None) -> str**
- Open PDF with fitz.open() (pymupdf)
- If doc.is_encrypted: call doc.authenticate(password)
  - If authenticate returns 0: raise ValueError("Wrong password or unencrypted PDF")
- Iterate pages: page.get_text("text")
- Join pages with "\n\n--- Page {n} ---\n\n" separators
- Strip leading/trailing whitespace per page before joining
- Return full text string
- Raise on: corrupt PDF (fitz exception), wrong password, zero pages
- Log: file_path, page_count, char_count

**Function: classify_report_type(title: str, text: str) -> str | None**
- Normalize: title.lower()
- Keyword → report_type mapping (check title first, then first 500 chars of text):
  - "trend friend" → "trend_friend"
  - "big picture" → "big_picture"
  - "big catch" → "big_catch"
  - "stock bullet" → "stock_bullet"
  - "sector trends" → "sector_trends"
  - "fortnightly" → "fortnightly"
  - "monthly con" or "concall" or "con-call" → "concall"
  - "sound byte" or "sound-byte" → "sound_byte"
  - "q&a" or "q & a" or "question and answer" → "qa"
  - "market snippet" → "snippet"
- Non-content PDFs to skip (return None):
  - "disclaimer", "privacy policy", "terms and conditions", "terms-and-conditions"
- If no match: return None (log as unclassified, do not fail)

### extract_goldilocks_pdfs.py — Backfill Script

Standalone script — runs directly on EC2, not via FastAPI. Use psycopg2 + plain SQL,
not SQLAlchemy async. Load DATABASE_URL from environment (python-dotenv or os.environ).

**Script flow:**
1. Connect to DB via psycopg2 (DATABASE_URL env var, or DATABASE_URL_SYNC)
2. Query: SELECT id, title, source_url, raw_text FROM de_qual_documents
           WHERE source_name = 'Goldilocks Research'
           AND original_format = 'pdf'
           ORDER BY created_at
3. For each document row:
   a. Determine file path:
      - PDF base dir: /home/ubuntu/jip-data-engine/data/goldilocks/pdfs/
      - Derive filename from source_url (urllib.parse.urlparse → path → basename)
      - full_path = pdf_dir / filename
   b. If file does not exist: log warning, skip (do not fail entire batch)
   c. Check if already extracted: if raw_text is not None and len(raw_text) > 200: skip (idempotent)
   d. Call extract_pdf_text(full_path, password="AICPJ9616P")
   e. Call classify_report_type(title, extracted_text)
   f. If report_type is None and title contains known non-content keywords: skip update
   g. UPDATE de_qual_documents SET
        raw_text = %s,
        report_type = %s,
        updated_at = NOW()
      WHERE id = %s
   h. Log: document_id, title, chars_extracted, report_type, elapsed_ms
4. Print summary table:
   - Total PDFs processed
   - Total chars extracted
   - By report_type: count
   - Skipped (already done): count
   - Skipped (file missing): count
   - Failed (exception): count

**Password handling:**
- Password "AICPJ9616P" is Goldilocks PAN card — hardcoded in this script only
- Comment: # Goldilocks PAN card — PDF encryption password
- Do NOT commit to git without confirming .env approach is not viable
  (PDFs are encrypted with a fixed PAN, this is the only way)

**Error handling:**
- Wrap each document in try/except
- Log exception to stderr with document_id and title
- Continue to next document — never abort the batch

**Local dev support:**
- Accept --pdf-dir argument to override default path (for running on Mac with downloaded PDFs)
- Accept --dry-run flag: extract and classify but do not UPDATE db

## Acceptance Criteria
- [ ] All 51 PDFs have raw_text populated (>200 chars — not the 122-char HTML snippet)
- [ ] All PDFs have report_type classified correctly
- [ ] Encrypted PDFs (password AICPJ9616P) decrypted and extracted successfully
- [ ] Non-content PDFs (Disclaimer, Privacy-Policy, Terms-Conditions) marked report_type=null or skipped cleanly
- [ ] Script is idempotent: re-running skips already-extracted docs
- [ ] Logging shows per-document extraction stats (doc_id, title, chars, report_type)
- [ ] Missing files logged as warnings, batch continues
- [ ] `ruff check . --select E,F,W` passes on both files
- [ ] Unit tests for classify_report_type() covering all 10 report types + None cases
- [ ] Unit test for extract_pdf_text() with a small real or synthetic test PDF
