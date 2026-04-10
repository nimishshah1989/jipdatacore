"""Transcribe Goldilocks audio/video content using faster-whisper.

Queries de_qual_documents for untranscribed Goldilocks audio/video files,
extracts audio from MP4s via ffmpeg, transcribes with faster-whisper,
and stores transcripts in de_qual_documents.raw_text.

Usage:
    python3 scripts/ingest/transcribe_goldilocks.py
    python3 scripts/ingest/transcribe_goldilocks.py --dry-run
    python3 scripts/ingest/transcribe_goldilocks.py --base-dir /tmp/goldilocks

EC2 prerequisites:
    sudo apt install -y ffmpeg
    pip3 install --break-system-packages faster-whisper psycopg2-binary

RAM usage: ~2-3 GB for small model. Use tiny model if <3 GB available.
Process serially — NEVER parallel.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Bootstrap: load .env from repo root (pattern from goldilocks_scraper.py)
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).parent.parent.parent


def _load_env() -> None:
    env_path = _REPO_ROOT / ".env"
    if env_path.exists():
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
# Add app/ to path so transcriber module can be imported
# ---------------------------------------------------------------------------
_APP_ROOT = str(_REPO_ROOT)
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

# ---------------------------------------------------------------------------
# Third-party imports
# ---------------------------------------------------------------------------
try:
    import psycopg2
    import psycopg2.extras
except ImportError as exc:
    print(f"[ERROR] psycopg2 not available: {exc}", flush=True)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
def ts() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_db_conn():
    """Return a psycopg2 connection from DATABASE_URL_SYNC."""
    url = os.environ.get("DATABASE_URL_SYNC") or os.environ.get("DATABASE_URL", "")
    if url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
    elif url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql+psycopg2://", "postgresql://", 1)
    if not url:
        raise RuntimeError("DATABASE_URL_SYNC / DATABASE_URL not set in environment")
    return psycopg2.connect(url)


# ---------------------------------------------------------------------------
# RAM guard
# ---------------------------------------------------------------------------
def get_available_ram_mb() -> int:
    """Return available RAM in MB. Reads /proc/meminfo or falls back to psutil."""
    # Try psutil first (cleaner)
    try:
        import psutil
        return psutil.virtual_memory().available // (1024 * 1024)
    except ImportError:
        pass

    # Fall back to /proc/meminfo (Linux)
    try:
        meminfo = Path("/proc/meminfo").read_text()
        for line in meminfo.splitlines():
            if line.startswith("MemAvailable:"):
                kb = int(line.split()[1])
                return kb // 1024
    except (OSError, ValueError):
        pass

    # Cannot determine — assume enough RAM
    return 8192


def check_ram_and_warn() -> str:
    """Check available RAM and return recommended model size.

    Returns 'small' if >=3 GB available, 'tiny' otherwise.
    """
    available_mb = get_available_ram_mb()
    available_gb = available_mb / 1024
    _log(f"Available RAM: {available_gb:.1f} GB")

    if available_mb < 3072:  # 3 GB
        _log(
            f"[WARN] Available RAM ({available_gb:.1f} GB) below 3 GB threshold. "
            "Forcing tiny model to avoid OOM."
        )
        return "tiny"
    return "small"


# ---------------------------------------------------------------------------
# Document querying
# ---------------------------------------------------------------------------
def fetch_pending_documents(cur) -> list[dict]:
    """Query de_qual_documents for untranscribed Goldilocks audio/video.

    Selects documents where:
    - original_format IN ('video', 'audio')
    - source is Goldilocks Research
    - raw_text is NULL or shorter than 100 chars (not yet transcribed)

    Returns list of dicts with keys: id, source_url, original_format, title.
    """
    cur.execute("""
        SELECT
            d.id,
            d.source_url,
            d.original_format,
            d.title
        FROM de_qual_documents d
        JOIN de_qual_sources s ON s.id = d.source_id
        WHERE d.original_format IN ('video', 'audio')
          AND s.source_name = 'Goldilocks Research'
          AND (d.raw_text IS NULL OR LENGTH(d.raw_text) < 100)
        ORDER BY d.created_at
    """)
    rows = cur.fetchall()
    return [
        {
            "id": str(row[0]),
            "source_url": row[1],
            "original_format": row[2],
            "title": row[3] or "(untitled)",
        }
        for row in rows
    ]


def update_transcript(
    cur,
    doc_id: str,
    raw_text: str,
    duration_seconds: int,
    dry_run: bool,
) -> None:
    """Update de_qual_documents with transcript and duration."""
    if dry_run:
        _log(
            f"  [DRY-RUN] Would update doc {doc_id}: "
            f"{len(raw_text)} chars, {duration_seconds}s"
        )
        return

    cur.execute("""
        UPDATE de_qual_documents
        SET raw_text = %s,
            audio_duration_s = %s,
            processing_status = 'done',
            updated_at = NOW()
        WHERE id = %s
    """, (raw_text, duration_seconds, doc_id))


# ---------------------------------------------------------------------------
# File path resolution
# ---------------------------------------------------------------------------
def resolve_local_path(source_url: str, original_format: str, base_dir: Path) -> Path:
    """Derive local file path from source_url.

    Video files -> base_dir/video/{filename}
    Audio files -> base_dir/audio/{filename}
    """
    filename = source_url.split("/")[-1].split("?")[0]
    if original_format == "video":
        return base_dir / "video" / filename
    else:
        return base_dir / "audio" / filename


# ---------------------------------------------------------------------------
# Main transcription loop
# ---------------------------------------------------------------------------
def transcribe_documents(
    documents: list[dict],
    cur,
    base_dir: Path,
    model_size: str,
    dry_run: bool,
) -> dict:
    """Process each document serially.

    Returns stats dict: transcribed, skipped_missing, failed, total_duration_s, total_chars.
    """
    # Import via importlib to avoid triggering app.__init__ → sqlalchemy chain
    import importlib.util as _ilu
    _spec = _ilu.spec_from_file_location(
        "transcriber",
        str(_REPO_ROOT / "app" / "pipelines" / "qualitative" / "transcriber.py"),
    )
    _mod = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    extract_audio_from_video = _mod.extract_audio_from_video
    transcribe_audio = _mod.transcribe_audio

    stats = {
        "transcribed": 0,
        "skipped_missing": 0,
        "failed": 0,
        "total_duration_s": 0,
        "total_chars": 0,
    }

    for doc in documents:
        doc_id = doc["id"]
        source_url = doc["source_url"] or ""
        original_format = doc["original_format"]
        title = doc["title"]

        _log(f"Processing: {title[:70]!r} [{original_format}]")

        # Resolve local file path
        local_path = resolve_local_path(source_url, original_format, base_dir)

        if not local_path.exists():
            _log(f"  [SKIP] File not found on disk: {local_path}")
            stats["skipped_missing"] += 1
            continue

        wav_path: Path | None = None
        audio_path = local_path

        try:
            t0 = time.monotonic()

            # Extract audio from video if needed
            if original_format == "video":
                wav_path = local_path.with_suffix(".wav")
                _log(f"  Extracting audio: {local_path.name} -> {wav_path.name}")
                if not dry_run:
                    extract_audio_from_video(local_path, wav_path)
                    audio_path = wav_path
                else:
                    _log(f"  [DRY-RUN] Would extract audio from {local_path.name}")

            # Transcribe
            _log(f"  Transcribing: {audio_path.name} (model={model_size}, lang=hi)")
            if not dry_run:
                transcript, duration_s = transcribe_audio(audio_path, language="hi")
            else:
                transcript = "[DRY-RUN transcript placeholder]"
                duration_s = 0

            elapsed = time.monotonic() - t0

            # Update DB
            update_transcript(cur, doc_id, transcript, duration_s, dry_run)

            stats["transcribed"] += 1
            stats["total_duration_s"] += duration_s
            stats["total_chars"] += len(transcript)

            _log(
                f"  Done: duration={duration_s}s chars={len(transcript)} elapsed={elapsed:.1f}s"
            )

        except Exception as exc:
            _log(f"  [ERROR] Failed for {title[:60]!r}: {exc}")
            stats["failed"] += 1

        finally:
            # Always clean up temp WAV file
            if wav_path is not None and wav_path.exists() and not dry_run:
                try:
                    wav_path.unlink()
                    _log(f"  Cleaned up: {wav_path.name}")
                except OSError as e:
                    _log(f"  [WARN] Could not delete WAV: {e}")

    return stats


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Transcribe Goldilocks audio/video using faster-whisper"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Query DB and list files to transcribe without writing anything",
    )
    parser.add_argument(
        "--base-dir",
        default="/home/ubuntu/jip-data-engine/data/goldilocks",
        help="Root directory for downloaded media files",
    )
    parser.add_argument(
        "--model",
        choices=["small", "tiny"],
        default=None,
        help="Force specific Whisper model size (default: auto based on available RAM)",
    )
    args = parser.parse_args()

    dry_run: bool = args.dry_run
    base_dir = Path(args.base_dir)

    _log(f"=== Goldilocks Transcription Runner | dry_run={dry_run} ===")
    _log(f"Base dir: {base_dir}")

    # RAM check — determine model size
    if args.model:
        model_size = args.model
        _log(f"Model size forced: {model_size}")
    else:
        model_size = check_ram_and_warn()
        _log(f"Model size selected: {model_size}")

    # DB connection
    conn = get_db_conn()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # Fetch pending documents
        documents = fetch_pending_documents(cur)
        _log(f"Documents pending transcription: {len(documents)}")

        if not documents:
            _log("Nothing to transcribe. Exiting.")
            return

        for doc in documents:
            _log(f"  - [{doc['original_format']}] {doc['title'][:70]!r}")

        if dry_run:
            _log("[DRY-RUN] Stopping before transcription.")
            return

        # Transcribe serially
        stats = transcribe_documents(
            documents=documents,
            cur=cur,
            base_dir=base_dir,
            model_size=model_size,
            dry_run=dry_run,
        )

        conn.commit()

        # Summary
        _log("=== Transcription Summary ===")
        _log(f"Transcribed:      {stats['transcribed']}")
        _log(f"Skipped (missing): {stats['skipped_missing']}")
        _log(f"Failed:           {stats['failed']}")
        total_min = stats["total_duration_s"] // 60
        _log(f"Total audio:      {total_min} min ({stats['total_duration_s']}s)")
        _log(f"Total chars:      {stats['total_chars']:,}")

    except Exception as exc:
        conn.rollback()
        _log(f"[ERROR] Unhandled exception: {exc}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
