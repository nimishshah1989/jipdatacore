# C10: Daily Pipeline Wiring + Scraper Enhancement

**Complexity:** Medium
**Dependencies:** All previous chunks (C1 through C9). C10 wires them together into a scheduled daily pipeline.
**Status:** pending

## Files
- scripts/ingest/goldilocks_scraper.py (modify — add --extract and --transcribe flags; auto-call pdf_extractor after download)
- app/pipelines/qualitative/goldilocks_daily.py (new — BasePipeline subclass orchestrating the full daily flow)
- app/pipelines/registry.py (modify — register goldilocks_daily pipeline)

## Context

### Existing files to read before modifying

**scripts/ingest/goldilocks_scraper.py:** Standalone psycopg2 script. Read its current argument parser, its download functions, and its DB update logic before modifying. The enhancement adds two new CLI flags and calls into the existing pipeline modules for post-download processing.

**app/pipelines/qualitative/playwright_goldilocks.py:** The existing Playwright-based Goldilocks pipeline. This is a `BasePipeline` subclass that runs the scraper via subprocess or direct calls. Read its execute() method before building `goldilocks_daily.py` — avoid duplicating its functionality.

**app/pipelines/framework.py:** `BasePipeline` base class. Read for: constructor signature, `execute()` override protocol, `log_run()` or equivalent logging method, and `flush()` / session management pattern. All new pipelines must follow this exactly.

**app/pipelines/registry.py:** Read the existing `_PIPELINE_CLASSES` dict and `SCHEDULE_REGISTRY` dict pattern. Adds must follow the same lazy import tuple format `(module_path, class_name)`.

### Existing qualitative pipeline modules
- `app/pipelines/qualitative/claude_extract.py` — Claude API extraction pipeline
- `app/pipelines/qualitative/extractor.py` — extraction orchestration

### New pipeline modules from earlier chunks
- `app/pipelines/qualitative/pdf_extractor.py` (from C2) — `extract_pdf_text()` and `classify_report_type()`
- `app/computation/outcome_tracker.py` (from C7) — `track_goldilocks_outcomes()`

Check that these exist before referencing them. If C2/C7 are not yet implemented, use placeholder calls with clear TODO comments.

### Data flow timing (from PRD section 6)
```
Daily 19:30 IST: BHAV copy → OHLCV
Daily 23:00 IST: Technicals + Stochastic + Disparity + BollingerWidth
Daily 23:15 IST: RS scores + Divergence detection
Daily 23:20 IST: Breadth + Regime + Pivot points + Intermarket ratios
Daily 23:30 IST: Goldilocks scraper → new PDFs/audio
Daily 23:45 IST: PDF extraction → Claude extraction → structured tables
Daily 23:50 IST: Outcome tracker checks active ideas
```

The `goldilocks_daily` pipeline runs at 23:30 IST and covers steps: scrape → extract → Claude → outcome track.

## What To Build

### goldilocks_daily.py — Daily Pipeline Orchestrator

```python
class GoldilocksDailyPipeline(BasePipeline):
    pipeline_name = "goldilocks_daily"
```

**execute() flow:**

The pipeline executes 4 phases in sequence. Each phase is wrapped in try/except so a failure in one phase does not block subsequent phases.

**Phase 1: Scrape**
```python
# Run the existing Playwright scraper (playwright_goldilocks.py) to check for new content.
# This downloads new PDFs to /home/ubuntu/jip-data-engine/data/goldilocks/pdfs/
# and inserts new rows into de_qual_documents with processing_status='pending'.
#
# If playwright_goldilocks.py is a BasePipeline subclass, instantiate and call execute().
# If it exposes a run() function: call it directly.
# Read playwright_goldilocks.py to determine the right invocation pattern.
scrape_result = await self._run_scraper()
self.logger.info("goldilocks_scrape_complete", new_docs=scrape_result.get("new_documents", 0))
```

**Phase 2: PDF Text Extraction**
```python
# Find all de_qual_documents WHERE source_name = 'Goldilocks Research'
#   AND original_format = 'pdf'
#   AND (raw_text IS NULL OR length(raw_text) < 200)
#   AND processing_status = 'pending'
# For each: call extract_pdf_text() and classify_report_type() from pdf_extractor.py
# Update raw_text, report_type, processing_status='text_extracted'
# Count: rows_extracted
```

Implementation note: `pdf_extractor.py` is sync (not async). Call it in a thread executor:
```python
import asyncio
loop = asyncio.get_event_loop()
text = await loop.run_in_executor(None, extract_pdf_text, file_path, password)
```

**Phase 3: Claude Extraction**
```python
# Find all de_qual_documents WHERE source_name = 'Goldilocks Research'
#   AND raw_text IS NOT NULL AND length(raw_text) >= 200
#   AND processing_status = 'text_extracted'
#   AND report_type IS NOT NULL
# For each: call the appropriate Claude extractor based on report_type:
#   - 'trend_friend' → extract to de_goldilocks_market_view
#   - 'stock_bullet' / 'big_catch' → extract to de_goldilocks_stock_ideas
#   - 'sector_trends' / 'fortnightly' → extract to de_goldilocks_sector_view
#   - All types → extract to de_qual_extracts (general views)
# Update processing_status='processed' on success, 'failed' on error.
# Log quality_score for each extraction.
```

Read `app/pipelines/qualitative/extractor.py` and `claude_extract.py` to understand the exact calling convention. Use those existing modules — do not re-implement Claude API calls. The goldilocks_daily pipeline is an orchestrator, not a re-implementation.

**Phase 4: Outcome Tracking**
```python
# Import and call track_goldilocks_outcomes(session) from outcome_tracker.py
# Log the result summary dict.
result = await track_goldilocks_outcomes(self.session)
self.logger.info("outcome_tracking_complete", **result)
```

**Private helper: _run_scraper() -> dict**

Runs the Playwright scraper. Returns a dict with at minimum: `{"new_documents": int, "errors": int}`.

Determine whether to:
a) Import and call `playwright_goldilocks.py` directly as a Python module, OR
b) Run it as a subprocess via `asyncio.create_subprocess_exec`

Prefer (a) if `playwright_goldilocks.py` is already a BasePipeline that can be instantiated independently. Read the file first. If it uses its own session management that would conflict: prefer (b) subprocess approach to isolate it.

**Session management:**
Follow the BasePipeline pattern exactly as used by existing pipelines. Read `framework.py` for the correct pattern — do not invent a new session lifecycle.

**Logging:**
Use `self.logger` (structlog) throughout. Log at start and end of each phase. Log counts: new documents found, texts extracted, Claude extractions run, outcomes checked.

**Return value:**
Match the return type expected by BasePipeline.execute(). Inspect the framework and existing pipelines for the exact return type (likely a dict or a result dataclass).

---

### goldilocks_scraper.py — Enhancements

**IMPORTANT: Read the entire existing file before modifying. Preserve all existing behavior.**

Add two new CLI flags to the existing argparse setup:

**Flag: --extract**
When present: after each PDF is downloaded (or for all unextracted PDFs in the DB), call `extract_pdf_text()` from `app/pipelines/qualitative/pdf_extractor.py` and `classify_report_type()`, then UPDATE de_qual_documents.raw_text and report_type.

Implementation note: `goldilocks_scraper.py` uses psycopg2 (sync), not SQLAlchemy async. `pdf_extractor.py` is also sync. The --extract path does not need async.

Password for PDF decryption: read from environment variable `GOLDILOCKS_PDF_PASSWORD`. Fall back to the hardcoded value only if env var not set. Log a warning if using fallback.

**Flag: --transcribe**
When present: for new audio/video files downloaded this session, trigger transcription using faster-whisper.

Implementation approach:
```python
if args.transcribe:
    for audio_file in newly_downloaded_audio:
        transcript = transcribe_audio(audio_file)  # new helper function
        update_document_transcript(conn, doc_id, transcript)
```

**New helper function: transcribe_audio(file_path: Path) -> str**
- Import `faster_whisper.WhisperModel` inside the function (guard with try/except ImportError — faster-whisper may not be installed on dev machine)
- Model: `WhisperModel("small", device="cpu", compute_type="int8")`
- Language: `"hi"` (Hindi — handles code-switched Hindi/English per PRD)
- Return transcript as single string (join all segments with space)
- If ImportError: log error, return "" (do not crash the entire script)

**New helper: detect_new_concall_videos(conn, page_html: str) -> list[dict]**
When scraping the con-call page, detect video entries not yet present in de_qual_documents:
```python
# Parse page_html for video links
# Query: SELECT source_url FROM de_qual_documents WHERE source_name='Goldilocks Research' AND original_format='video'
# Return list of {"url": ..., "title": ..., "date": ...} for URLs not in DB
```

**Backward compatibility:**
- Without --extract or --transcribe flags: existing behavior is completely unchanged.
- All existing CLI flags and their behavior must continue to work.
- The script must still run successfully on a machine without faster-whisper installed (--transcribe flag logs error and skips gracefully).

---

### registry.py — Add goldilocks_daily

In `app/pipelines/registry.py`, add to `_PIPELINE_CLASSES`:
```python
"goldilocks_daily": ("app.pipelines.qualitative.goldilocks_daily", "GoldilocksDailyPipeline"),
```

Position: after the existing `"qualitative_goldilocks"` entry, within the `# Qualitative` section.

Add to `SCHEDULE_REGISTRY` — add a new schedule group:
```python
"goldilocks_daily": ["goldilocks_daily"],
```

This allows the pipeline_trigger API (app/api/v1/pipeline_trigger.py) to trigger it by group name. Do not add it to the `"eod"` group — it runs on its own schedule (23:30 IST) triggered separately.

Do not modify any existing entries in `_PIPELINE_CLASSES` or `SCHEDULE_REGISTRY`.

## Edge Cases

- **No new Goldilocks content today:** Scraper finds nothing new. Phase 1 returns `{"new_documents": 0}`. Phases 2 and 3 find nothing to process (all docs already have raw_text or processing_status != 'pending'). Phase 4 runs outcome tracker regardless. Total pipeline: completes successfully in < 30 seconds.
- **faster-whisper not installed:** `--transcribe` flag used but faster-whisper not pip-installed. transcribe_audio() catches ImportError, logs error, returns "". Script continues. The pipeline logs a warning but does not fail.
- **PDF file not found on disk:** PDF row in de_qual_documents but file not at expected path. Log warning with document_id and path, skip this document, continue to next. Do not fail the phase.
- **Claude extraction error:** One document causes Claude extraction to fail (rate limit, bad response, etc.). Update processing_status='failed' for that document. Log error. Continue to next document. Outcome tracking still runs.
- **Outcome tracker error:** track_goldilocks_outcomes raises an exception. Log error with full traceback. Pipeline still returns success for phases 1-3.
- **Playwright scraper session conflict:** If playwright_goldilocks.py manages its own DB session and goldilocks_daily.py also has a session, they must not share a session or transaction. The existing BasePipeline isolation pattern (each pipeline gets its own session) should prevent this — verify by reading framework.py.
- **PDF password env var not set:** Log a one-time warning, use fallback. The hardcoded fallback is acceptable per C2 spec — PDF encryption password is a fixed PAN, not a secret in the traditional sense, but prefer env var.

## Acceptance Criteria
- [ ] GoldilocksDailyPipeline.execute() runs all 4 phases in sequence
- [ ] Phase failure does not prevent subsequent phases from running
- [ ] Phase 1: Playwright scraper runs, new documents counted and logged
- [ ] Phase 2: PDF extraction runs for all pending PDFs, raw_text and report_type updated
- [ ] Phase 3: Claude extraction runs for all text_extracted documents, structured tables populated
- [ ] Phase 4: track_goldilocks_outcomes runs for all active ideas
- [ ] Pipeline logs show counts at each phase (new_docs, texts_extracted, claude_extracted, outcomes_checked)
- [ ] goldilocks_scraper.py --extract flag calls extract_pdf_text + classify_report_type and updates DB
- [ ] goldilocks_scraper.py --transcribe flag calls transcribe_audio with faster-whisper
- [ ] faster-whisper not installed: --transcribe logs warning, exits gracefully (no crash)
- [ ] Backward compatibility: existing scraper modes (daily, historical) work unchanged
- [ ] goldilocks_daily registered in registry.py _PIPELINE_CLASSES
- [ ] goldilocks_daily schedule group added to SCHEDULE_REGISTRY
- [ ] Pipeline is triggerable via GET /api/v1/pipeline/trigger/goldilocks_daily (existing pipeline_trigger.py API)
- [ ] `ruff check . --select E,F,W` passes on all 3 modified/created files
