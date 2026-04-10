# C3: Video Download + Transcription

**Complexity:** High
**Dependencies:** C1 (audio_duration_s and report_type columns on DeQualDocuments must exist)
**Status:** pending

## Files
- scripts/ingest/download_goldilocks_media.py (new — download MP4/MP3 from site)
- scripts/ingest/transcribe_goldilocks.py (new — faster-whisper transcription runner)
- app/pipelines/qualitative/transcriber.py (new — reusable transcription module)

## EC2 Prerequisites (document in script header comments)

```bash
# One-time setup on EC2
sudo apt install -y ffmpeg
pip3 install --break-system-packages faster-whisper
```

faster-whisper model will auto-download on first run (~150 MB for small model).
Store model cache at: /home/ubuntu/.cache/huggingface/hub/ (default).

## What To Build

### transcriber.py — Reusable Module

Sync module (not async). Designed to run on EC2 t3.large (2 vCPU, 8 GB RAM).
Uses CPU inference only — no GPU available.

**Module-level singleton:**
```python
_model: WhisperModel | None = None

def _get_model(model_size: str = "small") -> WhisperModel:
    global _model
    if _model is None:
        _model = WhisperModel(model_size, device="cpu", compute_type="int8")
    return _model
```
Lazy init — model not loaded until first transcribe call.

**Function: transcribe_audio(audio_path: Path, language: str = "hi") -> tuple[str, int]**
- Call _get_model() to get or init the singleton
- segments, info = model.transcribe(str(audio_path), language=language)
- Join all segment.text values with " " to produce full transcript
- duration_seconds = int(info.duration)
- Return (transcript_text, duration_seconds)
- OOM fallback: if WhisperModel raises MemoryError or RuntimeError with "out of memory":
  - Reload with model_size="tiny" and retry once
  - Log: "Fell back to tiny model due to OOM"
- Log: audio_path, model_size, language, duration_s, transcript_chars, elapsed_s
- Raise on: file not found, corrupt audio (after logging)

**Function: extract_audio_from_video(video_path: Path, output_path: Path) -> Path**
- Run via subprocess:
  ```
  ffmpeg -i {video_path} -vn -ar 16000 -ac 1 -acodec pcm_s16le {output_path} -y
  ```
- Check return code — raise RuntimeError if ffmpeg fails (include stderr in message)
- Verify output_path exists and size > 0 after ffmpeg
- Return output_path
- Handle: ffmpeg not installed (FileNotFoundError → clear message), corrupt video

**Function: get_audio_duration(audio_path: Path) -> int**
- Use ffprobe or mutagen to get duration without loading model
- Fallback: run ffprobe -v quiet -print_format json -show_streams and parse JSON
- Return int (seconds)

### download_goldilocks_media.py — Download Script

Authenticate using Playwright (reuse pattern from existing goldilocks_scraper.py).
Check app/pipelines/qualitative/goldilocks_scraper.py for the auth/session pattern
and replicate it — do not reinvent.

**Con-call download (monthly_con_call.php):**
1. Playwright: navigate to https://goldilocks.co.in/monthly_con_call.php (authenticated)
2. Parse page HTML with BeautifulSoup:
   - Find all <video> tags → <source src="data-temp/XXX.mp4">
   - Find associated date: look for nearest <p><b>YYYY-MM-DD</b></p> or similar heading
3. For each MP4 URL found:
   - Build full URL: https://goldilocks.co.in/data-temp/XXX.mp4
   - Target path: /home/ubuntu/jip-data-engine/data/goldilocks/video/XXX.mp4
   - Skip if: file exists AND os.path.getsize matches Content-Length header
   - Download: requests.get(url, cookies=playwright_cookies, stream=True)
   - Stream to disk in 1 MB chunks
   - Verify final size matches Content-Length
4. After download, upsert into de_qual_documents:
   - source_url = full MP4 URL
   - original_format = 'video'
   - title = "Goldilocks Con-Call {date}"
   - report_type = 'concall'
   - source_name = 'Goldilocks Research'
   - ON CONFLICT (source_url) DO UPDATE set report_type, updated_at

**Sound byte download (sound_byte.php):**
1. Navigate to https://goldilocks.co.in/sound_byte.php
2. Parse <audio> tags → <source src="data-temp/XXX.mp3">
3. Dedup URLs (same MP3 may appear multiple times on page — use set())
4. For each MP3 URL:
   - Target path: /home/ubuntu/jip-data-engine/data/goldilocks/audio/XXX.mp3
   - Skip if already downloaded and size matches
   - Download and stream to disk
5. Upsert into de_qual_documents:
   - original_format = 'audio'
   - report_type = 'sound_byte'
   - title derived from filename (e.g., "Goldilocks Sound Byte {date from filename}")

**Directory creation:**
- Create /home/ubuntu/jip-data-engine/data/goldilocks/video/ if not exists
- Create /home/ubuntu/jip-data-engine/data/goldilocks/audio/ if not exists
- Accept --base-dir argument to override for local testing

**DB connection:**
- Sync psycopg2 via DATABASE_URL env var
- Use ON CONFLICT DO UPDATE (not INSERT OR IGNORE) so we can update report_type

### transcribe_goldilocks.py — Transcription Script

Standalone script. Imports transcriber.py module. Sync execution.

**Flow:**
1. Connect to DB (psycopg2)
2. Query documents needing transcription:
   SELECT id, source_url, original_format, title
   FROM de_qual_documents
   WHERE original_format IN ('video', 'audio')
   AND source_name = 'Goldilocks Research'
   AND (raw_text IS NULL OR LENGTH(raw_text) < 100)
   ORDER BY created_at
3. For each document:
   a. Derive local file path from source_url filename
      - video → /home/ubuntu/jip-data-engine/data/goldilocks/video/{filename}
      - audio → /home/ubuntu/jip-data-engine/data/goldilocks/audio/{filename}
   b. If file does not exist on disk: log warning, skip
   c. If MP4 video: call extract_audio_from_video(mp4_path, wav_path)
      - wav_path = video_path.with_suffix(".wav") (same dir)
      - Clean up WAV after transcription (disk space)
   d. If MP3 audio: use directly
   e. Call transcribe_audio(audio_path, language="hi")
   f. UPDATE de_qual_documents SET
        raw_text = transcript_text,
        audio_duration_s = duration_seconds,
        updated_at = NOW()
      WHERE id = document_id
   g. If WAV was created: os.remove(wav_path)
   h. Log: document_id, title, duration_s, transcript_chars, elapsed_s
4. Process SERIALLY — never parallel. RAM constraint: one model load at a time.
   Peak RAM during transcription: ~2-3 GB (small model + audio buffer).
5. Print summary: files transcribed, total duration, total chars, failed count

**RAM guard:**
- Log available RAM before starting (psutil or /proc/meminfo)
- If available RAM < 3 GB: warn and proceed with tiny model only
- Do not process multiple files concurrently under any circumstance

## Acceptance Criteria
- [ ] All 3 con-call MP4s downloaded to /home/ubuntu/jip-data-engine/data/goldilocks/video/
- [ ] All MP3 sound bytes downloaded (count from sound_byte.php deduped)
- [ ] Audio extracted from MP4s via ffmpeg (WAV files created and deleted after transcription)
- [ ] All audio/video documents transcribed with faster-whisper
- [ ] Transcripts stored in de_qual_documents.raw_text (>1000 chars for a 60-min con-call)
- [ ] audio_duration_s populated (seconds, integer)
- [ ] report_type set to 'concall' or 'sound_byte' in de_qual_documents
- [ ] Download script is idempotent (re-run skips already-downloaded files by size check)
- [ ] Transcription script is idempotent (skips docs with raw_text already populated)
- [ ] Peak RAM stays under 6 GB during transcription (verified with `free -h` before/after)
- [ ] Unit tests for extract_audio_from_video() and transcribe_audio() with mocked subprocess/model
- [ ] `ruff check . --select E,F,W` passes on all 3 files
