# Chunk 13: Qualitative Pipeline

**Layer:** 4
**Dependencies:** C4
**Complexity:** High
**Status:** pending

## Files

- `app/pipelines/qualitative/__init__.py`
- `app/pipelines/qualitative/rss.py`
- `app/pipelines/qualitative/upload.py`
- `app/pipelines/qualitative/security_gate.py`
- `app/pipelines/qualitative/extractor.py`
- `app/pipelines/qualitative/embeddings.py`
- `app/pipelines/qualitative/deduplication.py`
- `app/pipelines/qualitative/archival.py`
- `app/pipelines/qualitative/playwright_goldilocks.py`
- `app/api/v1/qualitative.py`
- `tests/pipelines/qualitative/test_rss.py`
- `tests/pipelines/qualitative/test_security_gate.py`
- `tests/pipelines/qualitative/test_extractor.py`
- `tests/pipelines/qualitative/test_deduplication.py`

## Acceptance Criteria

### RSS Feed Polling (every 30 minutes)

- [ ] Poll configured RSS sources: RBI, SEBI, ET Markets, Business Standard, Fed press releases, Mint Markets
- [ ] For each new item: compute SHA-256 `content_hash`; check `de_qual_documents(source_id, content_hash)` — skip if exists (exact deduplication)
- [ ] Lock granularity (v1.7): per-document `pg_advisory_lock('qual', hashtext(content_hash))` — allows parallel ingestion

### Upload Endpoint

- [ ] `POST /api/v1/qualitative/upload` — admin JWT only, rate limited to 10 uploads/hour
- [ ] Accepts: PDF, audio (MP3/WAV/M4A), plain text, URL
- [ ] Returns: document_id, processing_status, estimated_cost

### Security Gate (upload files only — ALL must pass before processing)

- [ ] **Magic byte verification:** Declared MIME type must match actual file bytes using `python-magic`; mismatch → reject with 422
- [ ] **ClamAV scan:** `subprocess.run(['clamdscan', file_path])` — infected → `status='quarantine'`, alert admin, STOP
- [ ] ClamAV daemon pre-flight check at orchestrator startup (v1.7)
- [ ] Move file from quarantine folder to processing folder only after both checks pass

### Semantic Deduplication (v1.8)

- [ ] Compute embedding of `title + first 500 chars` for each new document
- [ ] Query existing documents ingested in past 48 hours
- [ ] If `1 - cosine_distance(new_embedding, existing_embedding) > 0.92`: skip, log as `semantic_duplicate`
- [ ] OpenAI `text-embedding-3-small` for embedding computation
- [ ] ivfflat index on embeddings deferred until 10,000+ rows — do NOT create in migration

### Cost Guardrails (v1.8)

- [ ] Daily cap: `SELECT COUNT(*) FROM de_qual_documents WHERE DATE(ingested_at) = :today AND processing_status = 'complete'` — if >200, pause and alert admin
- [ ] Per-source rate limit: max 50 documents/day per `source_id`
- [ ] Audio transcription: max 10 files/day via Whisper API
- [ ] Log API call counts and estimated costs in `de_pipeline_log.track_status` JSONB

### Content Extraction

- [ ] **Audio (MP3/WAV/M4A):** OpenAI Whisper API → `raw_text`; estimated cost ~$0.35 for 35-minute recording
- [ ] **PDF:** PyMuPDF `doc.get_text()` → `raw_text`; fallback to Claude vision API if extraction empty
- [ ] **Text:** file content → `raw_text` directly
- [ ] **URL:** `httpx.get()` → BeautifulSoup text extraction → `raw_text`

### Claude API Extraction

- [ ] Call Claude API (`claude-sonnet-4-20250514`) for structured market view extraction from `raw_text`
- [ ] Extract: `asset_class`, `entity_ref`, `direction` (bullish/bearish/neutral), `timeframe`, `conviction` (high/medium/low), `view_text`, `source_quote`, `quality_score`
- [ ] `quality_score` (0-1): Claude's confidence in the extraction; downstream uses only scores >= 0.70
- [ ] INSERT each extract into `de_qual_extracts`

### Embeddings

- [ ] Compute OpenAI `text-embedding-3-small` embeddings for both document and extract records
- [ ] Store as `vector(1536)` in `embedding` column

### S3 Archival (v1.7)

- [ ] After processing completes, upload original file to `s3://jsl-data-engine-archive/qualitative/YYYY/MM/DD/`
- [ ] Confirm S3 upload before deleting local file
- [ ] Log archival in `de_pipeline_log`

### Playwright Automation

- [ ] `playwright_goldilocks.py`: Automated scraping of Goldilocks Research content
- [ ] Headless Chromium browser via Playwright
- [ ] Handles login/session management for authenticated content
- [ ] Error handling: if Goldilocks site changes structure, fail gracefully with alert

### Retry and Error Handling

- [ ] Retry 3 times with exponential backoff (1m/5m/15m) on any failure
- [ ] After 3 failures: `status='failed'`, write `processing_error`, alert admin
- [ ] On any failure: release per-document lock in `finally` block

## Notes

**Pipeline trigger:** Every 30 minutes via orchestrator cron.

**Source list for RSS:** `de_qual_sources` table must be seeded with:
- RBI press releases (RSS/scrape)
- SEBI circulars (RSS/scrape)
- ET Markets (RSS)
- Business Standard Markets (RSS)
- Fed press releases (RSS)
- Mint Markets (RSS)
- Goldilocks Research (Playwright)

**Claude API model:** `claude-sonnet-4-20250514` for extraction. Use structured output / tool calling for deterministic JSON extraction of market views.

**Cosine similarity threshold (0.92):** Initial estimate — tune after observing 1,000+ documents. Too high = misses duplicates. Too low = false positives (different articles on same topic treated as duplicates).

**ivfflat index note:** Create manually after initial load reaches 10,000+ rows:
```sql
CREATE INDEX idx_qual_docs_embedding ON de_qual_documents
  USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
```
Schedule monthly `REINDEX` to maintain quality.

**Security gate order is mandatory:** magic bytes → ClamAV → processing. Never process a file that fails either check. Quarantine folder is separate from processing folder.
