# C4: HTML Content Extraction

**Complexity:** Medium
**Dependencies:** C1 (report_type column on DeQualDocuments must exist)
**Status:** pending

## Files
- scripts/ingest/extract_goldilocks_html.py (new — clean HTML content stored in DB)
- app/pipelines/qualitative/html_cleaner.py (new — reusable HTML cleaning module)

## Context

The existing goldilocks_scraper.py stores raw HTML page content in de_qual_documents.raw_text
for non-PDF pages. This raw HTML includes navigation, footer, sidebar, disclaimer boilerplate.
The existing entries have report_type=NULL and dirty raw_text with nav/footer noise.

Known HTML document types in de_qual_documents (source_name='Goldilocks Research', original_format='html'):
- monthly_con_call.php — listing page for con-call videos (not the transcripts themselves)
- sound_byte.php — listing page for audio files
- q_a_gautam.php — Q&A with Gautam Shah (accordion panels)
- market_snippets.php — timestamped short views
- cus_dashboard.php — main dashboard (India reports)
- cus_dashboard_us.php — US market reports
- video_update.php — video update listings

Note: HTML documents from listing pages (monthly_con_call.php, sound_byte.php) have limited
extractable text — the real content is in the downloaded media files (C3). These listing pages
should be classified correctly but their cleaned text will be sparse (index/listing content).

## What To Build

### html_cleaner.py — Reusable Module

Sync module using BeautifulSoup4. No async needed — called from scripts only.

**Function: clean_goldilocks_html(raw_html: str, page_type: str) -> str**
- Parse with BeautifulSoup(raw_html, "html.parser")
- Remove these tags entirely (decompose): nav, header, footer, script, style, noscript
- Remove elements by common CSS selectors for Goldilocks site:
  - .navbar, .footer, .sidebar, .disclaimer, .cookie-banner
  - Any element with id containing "nav", "footer", "sidebar", "cookie"
- Page-type-specific extraction:
  - page_type == "qa": extract Q&A pairs (see below)
  - page_type == "snippet": extract timestamped entries (see below)
  - page_type == "listing": extract link text + dates (for con-call/sound_byte listing pages)
  - page_type == "generic": soup.get_text(separator="\n", strip=True)
- Strip lines that are purely whitespace
- Strip lines shorter than 3 chars (navigation artifacts)
- Collapse 3+ consecutive blank lines to 2
- Return cleaned plain text

**Q&A extraction (page_type="qa"):**
- Goldilocks Q&A page uses accordion/panel structure
- Find question containers: look for elements with class "question", "accordion-header",
  "panel-heading" or similar (inspect actual HTML to confirm selectors)
- Find answer containers: "answer", "accordion-body", "panel-body", "collapse show"
- Format output as:
  ```
  Q: {question text}
  A: {answer text}

  Q: {next question}
  A: {next answer}
  ```
- If accordion structure not found: fall back to generic get_text()
- Log count of Q&A pairs extracted

**Snippet extraction (page_type="snippet"):**
- Market Snippets page has timestamped short entries
- Find timestamp elements: look for date/time patterns (DD-Mon-YYYY HH:MM or similar)
- Format output preserving timestamps:
  ```
  [2024-03-15 14:30] Nifty holding 22000 — bullish bias intact...

  [2024-03-14 11:00] Bank Nifty support at 46500...
  ```
- If timestamp elements not identifiable: preserve order but use generic extraction
- Log count of snippet entries extracted

**Function: classify_html_report_type(source_url: str, title: str) -> str**
- Check source_url path (case-insensitive):
  - monthly_con_call.php → 'concall'
  - sound_byte.php → 'sound_byte'
  - q_a_gautam.php → 'qa'
  - market_snippets.php → 'snippet'
  - video_update.php → 'concall' (video updates are similar format)
  - cus_dashboard_us.php → 'usa_report'
  - cus_dashboard.php → infer from title (or 'big_picture' as default for India dashboard)
- If URL not matched, check title keywords (same as pdf_extractor.classify_report_type logic)
- Return matched type or None if truly unclassifiable

**Function: get_page_type_from_report_type(report_type: str | None) -> str**
- Maps report_type to the page_type parameter for clean_goldilocks_html:
  - 'qa' → 'qa'
  - 'snippet' → 'snippet'
  - 'concall', 'sound_byte' → 'listing'
  - All others → 'generic'

### extract_goldilocks_html.py — Backfill Script

Standalone script, sync, psycopg2.

**Flow:**
1. Connect to DB
2. Query:
   SELECT id, source_url, title, raw_text
   FROM de_qual_documents
   WHERE original_format = 'html'
   AND source_name = 'Goldilocks Research'
   ORDER BY created_at
3. For each document:
   a. Check if raw_text is already cleaned:
      - If raw_text is None or len < 50: log warning (no content), skip UPDATE
      - If raw_text does NOT contain HTML tags (<html>, <nav>, <div>, etc.): already clean, skip
   b. Classify: report_type = classify_html_report_type(source_url, title)
   c. Determine page_type = get_page_type_from_report_type(report_type)
   d. Clean: cleaned_text = clean_goldilocks_html(raw_text, page_type)
   e. If len(cleaned_text) < 20: log warning "Cleaning produced nearly empty text", skip UPDATE
   f. UPDATE de_qual_documents SET
        raw_text = cleaned_text,
        report_type = report_type,
        updated_at = NOW()
      WHERE id = document_id
   g. Log: document_id, source_url, report_type, page_type, original_chars, cleaned_chars
4. Print summary: total processed, by report_type count, skipped (already clean), skipped (empty result)

**Idempotency:**
- Script detects already-cleaned documents by absence of HTML tags in raw_text
- Re-running is safe: already-clean docs are skipped

## HTML Selector Risk

The Goldilocks site's exact HTML structure is not known until runtime inspection.
The implementer MUST:
1. Print a sample of the raw_text from the DB (first 2000 chars) for 2-3 documents
2. Inspect the actual selectors used for nav/footer/accordion
3. Adjust class names / tag selectors in html_cleaner.py to match real HTML
4. Document actual selectors found in a comment block at top of html_cleaner.py

## Acceptance Criteria
- [ ] All HTML documents (expected ~22) have cleaned raw_text (no <html>, <nav>, <script> tags)
- [ ] All HTML documents have report_type classified and populated
- [ ] Q&A entries formatted as "Q: ... / A: ..." (verify by reading a few)
- [ ] Market Snippets preserve timestamps in cleaned output
- [ ] Con-call/sound_byte listing pages: classified correctly, cleaned text has listing content
- [ ] Script is idempotent: re-run skips already-clean documents
- [ ] No document has cleaned_text shorter than 20 chars without a logged warning
- [ ] Unit tests for classify_html_report_type() covering all URL patterns
- [ ] Unit test for clean_goldilocks_html() with a small fixture of real-looking Goldilocks HTML
- [ ] `ruff check . --select E,F,W` passes on both files
