"""Transcribe downloaded Goldilocks audio/video files via Groq Whisper.

The scraper already downloads .mp3 / .mp4 / .wav files to
/home/ubuntu/jip-data-engine/data/goldilocks/{audio,video}/. This script
walks those directories, finds any whose de_qual_documents.raw_text is
still empty (i.e. not yet transcribed), transcribes them via Groq
whisper-large-v3, and UPDATEs raw_text in place.

Groq free tier has a 25MB per-file upload limit. Con-call MP4s from
Goldilocks are ~70MB, so we:
  1. ffmpeg-extract the audio track at mono/16kHz/32kbps mp3
     (Whisper is trained on 16kHz and ignores higher quality anyway).
     A 1-hour video compresses to ~14MB — fits under the limit.
  2. If the compressed audio is STILL over 24MB (very long con-call),
     ffmpeg-split into 10-minute segments, transcribe each, concatenate.
  3. POST the file (or each chunk) to
     POST https://api.groq.com/openai/v1/audio/transcriptions
     with multipart/form-data, model=whisper-large-v3.

Usage:
    python -m scripts.ingest.transcribe_goldilocks_media
    python -m scripts.ingest.transcribe_goldilocks_media --max-files 3
    python -m scripts.ingest.transcribe_goldilocks_media --dry-run
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_REPO_ROOT = Path(__file__).parent.parent.parent


def _load_env() -> None:
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
sys.path.insert(0, str(_REPO_ROOT))

import httpx  # noqa: E402
import psycopg2  # noqa: E402
import psycopg2.extras  # noqa: E402


MEDIA_ROOT = Path(os.environ.get(
    "GOLDILOCKS_MEDIA_ROOT",
    "/home/ubuntu/jip-data-engine/data/goldilocks",
))
GROQ_URL = "https://api.groq.com/openai/v1/audio/transcriptions"
GROQ_MODEL = "whisper-large-v3"
MAX_UPLOAD_BYTES = 24 * 1024 * 1024  # 25MB limit, keep 1MB headroom
SEGMENT_SECONDS = 10 * 60  # 10-minute chunks when splitting


def _ts() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)


def _db_url() -> str:
    url = os.environ.get("DATABASE_URL_SYNC") or os.environ.get("DATABASE_URL", "")
    for prefix in ("postgresql+asyncpg://", "postgresql+psycopg2://"):
        if url.startswith(prefix):
            url = url.replace(prefix, "postgresql://", 1)
    return url


def _run(cmd: list[str]) -> None:
    """Run a shell command, raise on non-zero exit."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"{' '.join(cmd[:2])} failed (exit {result.returncode}): {result.stderr[-500:]}"
        )


def _extract_audio(src: Path, dst: Path) -> None:
    """ffmpeg: extract mono 16kHz 32kbps mp3 from an mp3/mp4/wav input.
    Whisper is 16kHz-trained — higher quality is wasted bits."""
    _run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(src),
        "-vn",                 # drop any video stream
        "-ac", "1",            # mono
        "-ar", "16000",        # 16 kHz
        "-c:a", "libmp3lame",
        "-b:a", "32k",         # 32 kbps
        str(dst),
    ])


def _split_audio(src: Path, out_dir: Path) -> list[Path]:
    """ffmpeg: segment an mp3 into N-minute chunks; return chunk paths."""
    pattern = out_dir / "chunk_%03d.mp3"
    _run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(src),
        "-f", "segment",
        "-segment_time", str(SEGMENT_SECONDS),
        "-c", "copy",
        str(pattern),
    ])
    return sorted(out_dir.glob("chunk_*.mp3"))


def _transcribe_file(path: Path, api_key: str) -> str:
    """POST a single audio file to Groq Whisper. Returns the transcript text."""
    with open(path, "rb") as f:
        files = {"file": (path.name, f, "audio/mpeg")}
        data = {"model": GROQ_MODEL, "response_format": "json"}
        headers = {"Authorization": f"Bearer {api_key}"}
        resp = httpx.post(
            GROQ_URL, headers=headers, files=files, data=data, timeout=600.0
        )
    if resp.status_code != 200:
        raise RuntimeError(f"Groq Whisper {resp.status_code}: {resp.text[:500]}")
    return resp.json().get("text", "")


def transcribe_media_file(src: Path, api_key: str) -> str:
    """Full pipeline for one media file: extract → maybe split → transcribe.
    Returns the concatenated transcript."""
    if not src.exists():
        raise FileNotFoundError(str(src))

    with tempfile.TemporaryDirectory(prefix="gl_tx_") as tmp:
        tmp_dir = Path(tmp)
        compressed = tmp_dir / "audio.mp3"
        _log(f"  extracting audio: {src.name} ({src.stat().st_size // 1024 // 1024} MB)")
        _extract_audio(src, compressed)
        size = compressed.stat().st_size
        _log(f"  compressed: {size // 1024} KB")

        if size <= MAX_UPLOAD_BYTES:
            _log("  transcribing as single file")
            return _transcribe_file(compressed, api_key).strip()

        _log(f"  > {MAX_UPLOAD_BYTES // 1024 // 1024}MB, splitting into {SEGMENT_SECONDS//60}-min chunks")
        chunks = _split_audio(compressed, tmp_dir)
        _log(f"  {len(chunks)} chunks")
        parts: list[str] = []
        for i, ch in enumerate(chunks, 1):
            ch_size = ch.stat().st_size
            if ch_size > MAX_UPLOAD_BYTES:
                _log(f"  chunk {i} still {ch_size // 1024 // 1024}MB — skip")
                continue
            _log(f"  chunk {i}/{len(chunks)}: transcribing")
            text = _transcribe_file(ch, api_key)
            parts.append(text.strip())
            time.sleep(1)  # breathe between Groq calls
        return "\n\n".join(parts)


def _find_docs_needing_transcription(cur) -> list[dict]:
    """Return docs whose original_format is audio/video and whose raw_text
    either is empty or is too short to plausibly be a real transcript
    (< 2000 chars — a 1-hour con-call produces 10k-30k chars).

    Whisper output never contains the '--- Page N ---' marker that PDF
    extraction prepends, so we also skip anything that already has that
    marker (already processed by the PDF path)."""
    cur.execute(
        """
        SELECT id::text, title, source_url, audio_url, original_format,
               COALESCE(LENGTH(raw_text), 0) AS text_len
        FROM de_qual_documents
        WHERE source_id IN (SELECT id FROM de_qual_sources WHERE source_name ILIKE '%goldilocks%')
          AND original_format IN ('audio', 'video')
          AND (raw_text IS NULL OR LENGTH(raw_text) < 2000)
          AND (raw_text IS NULL OR raw_text NOT LIKE '%--- Page %')
        ORDER BY created_at DESC
        """
    )
    return cur.fetchall()


def _find_local_file(doc: dict) -> Optional[Path]:
    """Map a document row to a downloaded file on disk by matching the
    filename portion of its URL."""
    for url_field in ("source_url", "audio_url"):
        url = doc.get(url_field) or ""
        if not url:
            continue
        fname = url.split("/")[-1].split("?")[0]
        if not fname:
            continue
        for sub in ("video", "audio"):
            candidate = MEDIA_ROOT / sub / fname
            if candidate.exists():
                return candidate
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Transcribe goldilocks audio/video via Groq Whisper")
    parser.add_argument("--max-files", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    api_key = os.environ.get("GROQ_API_KEY", "").strip()
    if not api_key:
        _log("[ERROR] GROQ_API_KEY not set")
        sys.exit(1)

    # ffmpeg must be present
    try:
        subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
    except Exception:
        _log("[ERROR] ffmpeg not found. Install with: apt-get install -y ffmpeg")
        sys.exit(1)

    conn = psycopg2.connect(_db_url())
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    docs = _find_docs_needing_transcription(cur)
    _log(f"Found {len(docs)} docs with audio/video and missing transcript")

    processed = 0
    failed = 0
    skipped = 0
    for doc in docs:
        if processed >= args.max_files:
            break
        local = _find_local_file(doc)
        if local is None:
            _log(f"  SKIP [{doc['id'][:8]}] {doc['title'][:60]} — file not on disk")
            skipped += 1
            continue

        _log(f"Processing [{doc['original_format']}] {doc['title'][:60]}")

        if args.dry_run:
            _log(f"  [DRY-RUN] would transcribe {local}")
            processed += 1
            continue

        try:
            transcript = transcribe_media_file(local, api_key)
        except Exception as exc:
            failed += 1
            _log(f"  FAIL: {exc}")
            continue

        if not transcript or len(transcript) < 50:
            _log(f"  SKIP: transcript too short ({len(transcript)} chars)")
            skipped += 1
            continue

        cur.execute(
            "UPDATE de_qual_documents "
            "SET raw_text = %s, processing_status = 'pending', updated_at = NOW() "
            "WHERE id = %s::uuid",
            (transcript, doc["id"]),
        )
        conn.commit()
        processed += 1
        _log(f"  OK: {len(transcript)} chars stored, marked pending for LLM extraction")

    _log(f"=== DONE: processed={processed} skipped={skipped} failed={failed} ===")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
