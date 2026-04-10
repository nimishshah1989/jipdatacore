"""Audio transcription via faster-whisper (open source, CPU-only).

EC2 prerequisites (one-time setup):
    sudo apt install -y ffmpeg
    pip3 install --break-system-packages faster-whisper

Model auto-downloads on first run (~150 MB for small, ~75 MB for tiny).
Model cache: /home/ubuntu/.cache/huggingface/hub/ (default HuggingFace location).

RAM usage: ~2-3 GB for small model + audio buffer on t3.large (2 vCPU, 8 GB).
"""

from __future__ import annotations

import json
import logging
import subprocess
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Singleton model — lazy init, never reloaded unless OOM fallback triggers
# ---------------------------------------------------------------------------
_model = None
_model_size: str = "small"


def _get_model(model_size: str = "small"):
    """Load faster-whisper WhisperModel (singleton, lazy init).

    On first call, downloads and loads the model (~30s for small on t3.large).
    Subsequent calls return the cached instance immediately.

    OOM fallback: if MemoryError or RuntimeError containing 'out of memory'
    is raised during load, falls back to 'tiny' model and retries once.
    """
    global _model, _model_size

    if _model is not None:
        return _model

    try:
        from faster_whisper import WhisperModel  # type: ignore[import]
    except ImportError as exc:
        raise RuntimeError(
            "faster-whisper not installed. Run: "
            "pip3 install --break-system-packages faster-whisper"
        ) from exc

    try:
        logger.info("Loading WhisperModel size=%s device=cpu compute_type=int8", model_size)
        _model = WhisperModel(model_size, device="cpu", compute_type="int8")
        _model_size = model_size
        logger.info("WhisperModel loaded: size=%s", model_size)
    except (MemoryError, RuntimeError) as exc:
        if "out of memory" in str(exc).lower() or isinstance(exc, MemoryError):
            logger.warning(
                "Fell back to tiny model due to OOM (original error: %s)", exc
            )
            _model = WhisperModel("tiny", device="cpu", compute_type="int8")
            _model_size = "tiny"
            logger.info("WhisperModel loaded: size=tiny (fallback)")
        else:
            raise

    return _model


def transcribe_audio(audio_path: Path, language: str = "hi") -> tuple[str, int]:
    """Transcribe an audio file using faster-whisper.

    Args:
        audio_path: Path to WAV, MP3, or other audio file supported by ffmpeg/whisper.
        language: ISO-639-1 language code. Default 'hi' (Hindi) for Goldilocks content.

    Returns:
        (transcript_text, duration_seconds) tuple.
        transcript_text: Full transcript joined with spaces.
        duration_seconds: Audio duration as integer seconds.

    Raises:
        FileNotFoundError: If audio_path does not exist.
        RuntimeError: If transcription fails (corrupt audio, etc.).
    """
    if not audio_path.exists():
        raise FileNotFoundError(f"Audio file not found: {audio_path}")

    model = _get_model()
    t0 = time.monotonic()

    try:
        segments, info = model.transcribe(str(audio_path), language=language)
        text_parts: list[str] = []
        for segment in segments:
            part = segment.text.strip()
            if part:
                text_parts.append(part)
    except Exception as exc:
        raise RuntimeError(
            f"Transcription failed for {audio_path.name}: {exc}"
        ) from exc

    transcript = " ".join(text_parts)
    duration = int(info.duration)
    elapsed = time.monotonic() - t0

    logger.info(
        "Transcribed: file=%s model=%s language=%s duration_s=%d chars=%d elapsed_s=%.1f",
        audio_path.name,
        _model_size,
        language,
        duration,
        len(transcript),
        elapsed,
    )

    return transcript, duration


def extract_audio_from_video(video_path: Path, output_path: Path) -> Path:
    """Extract audio from MP4/video file using ffmpeg.

    Produces a 16kHz mono WAV file — optimal format for faster-whisper.

    Args:
        video_path: Path to input video file (MP4, etc.).
        output_path: Destination path for WAV output.

    Returns:
        output_path (verified to exist and be non-empty).

    Raises:
        FileNotFoundError: If ffmpeg is not installed.
        RuntimeError: If ffmpeg extraction fails or output is empty.
    """
    if not video_path.exists():
        raise FileNotFoundError(f"Video file not found: {video_path}")

    cmd = [
        "ffmpeg",
        "-i", str(video_path),
        "-vn",                    # no video stream
        "-ar", "16000",           # 16kHz sample rate (whisper optimal)
        "-ac", "1",               # mono channel
        "-acodec", "pcm_s16le",   # 16-bit PCM WAV
        "-y",                     # overwrite output if exists
        str(output_path),
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minutes max for long videos
        )
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            "ffmpeg not found. Install with: sudo apt install -y ffmpeg"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"ffmpeg timed out after 300s for {video_path.name}"
        ) from exc

    if result.returncode != 0:
        raise RuntimeError(
            f"ffmpeg failed (exit {result.returncode}) for {video_path.name}: "
            f"{result.stderr[:500]}"
        )

    # Verify output was actually created and has content
    if not output_path.exists():
        raise RuntimeError(
            f"ffmpeg completed but output file missing: {output_path}"
        )

    output_size = output_path.stat().st_size
    if output_size == 0:
        raise RuntimeError(
            f"ffmpeg produced empty WAV file for {video_path.name}"
        )

    logger.info(
        "Extracted audio: %s -> %s (%d bytes)",
        video_path.name,
        output_path.name,
        output_size,
    )
    return output_path


def get_audio_duration(audio_path: Path) -> int:
    """Get audio/video duration in seconds using ffprobe.

    Lightweight — does not load the whisper model.

    Args:
        audio_path: Path to audio or video file.

    Returns:
        Duration in integer seconds. Returns 0 if ffprobe fails.
    """
    if not audio_path.exists():
        logger.warning("get_audio_duration: file not found: %s", audio_path)
        return 0

    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        str(audio_path),
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except FileNotFoundError:
        logger.warning("ffprobe not found — cannot get duration without loading model")
        return 0
    except subprocess.TimeoutExpired:
        logger.warning("ffprobe timed out for %s", audio_path.name)
        return 0

    if result.returncode != 0:
        logger.warning(
            "ffprobe failed for %s: %s", audio_path.name, result.stderr[:200]
        )
        return 0

    try:
        data = json.loads(result.stdout)
        streams = data.get("streams", [])
        for stream in streams:
            duration_str: Optional[str] = stream.get("duration")
            if duration_str:
                return int(float(duration_str))
    except (json.JSONDecodeError, ValueError, KeyError) as exc:
        logger.warning("ffprobe JSON parse error for %s: %s", audio_path.name, exc)

    return 0
