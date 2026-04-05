"""Security gate: magic byte verification + ClamAV scanning.

Mandatory order: magic bytes → ClamAV → processing.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Optional

from app.logging import get_logger

logger = get_logger(__name__)

# Allowed MIME types and their canonical format names
_MIME_TO_FORMAT: dict[str, str] = {
    "application/pdf": "pdf",
    "audio/mpeg": "audio",
    "audio/mp3": "audio",
    "audio/wav": "audio",
    "audio/x-wav": "audio",
    "audio/ogg": "audio",
    "audio/flac": "audio",
    "audio/aac": "audio",
    "audio/mp4": "audio",
    "audio/x-m4a": "audio",
    "text/plain": "text",
    "text/html": "html",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
}

_AUDIO_MIMES: frozenset[str] = frozenset(
    mime for mime in _MIME_TO_FORMAT if mime.startswith("audio/")
)

_CLAMAV_TIMEOUT_SECONDS = 60


class SecurityGateError(Exception):
    """Base exception for security gate failures."""

    def __init__(self, message: str, reason: str) -> None:
        super().__init__(message)
        self.reason = reason


class MagicByteMismatchError(SecurityGateError):
    """Raised when actual MIME type does not match declared MIME type."""

    def __init__(self, declared: str, actual: str) -> None:
        super().__init__(
            f"Magic byte mismatch: declared={declared!r}, actual={actual!r}",
            reason="magic_byte_mismatch",
        )
        self.declared = declared
        self.actual = actual


class ClamAVInfectedError(SecurityGateError):
    """Raised when ClamAV detects malware in a file."""

    def __init__(self, scan_output: str) -> None:
        super().__init__(f"ClamAV detected infection: {scan_output}", reason="infected")
        self.scan_output = scan_output


def detect_mime_type(file_path: Path) -> str:
    """Detect MIME type using python-magic (libmagic).

    This function is a thin wrapper so tests can patch it easily.
    """
    try:
        import magic  # type: ignore[import]

        return magic.from_file(str(file_path), mime=True)
    except ImportError:
        # Fallback: use file extension heuristic when libmagic is unavailable
        suffix = file_path.suffix.lower()
        _ext_map = {
            ".pdf": "application/pdf",
            ".mp3": "audio/mpeg",
            ".wav": "audio/wav",
            ".txt": "text/plain",
            ".html": "text/html",
            ".htm": "text/html",
        }
        return _ext_map.get(suffix, "application/octet-stream")


def verify_magic_bytes(file_path: Path, declared_mime: str) -> str:
    """Verify that actual magic bytes match the declared MIME type.

    Args:
        file_path: Path to the file to inspect.
        declared_mime: MIME type declared by the uploader.

    Returns:
        Actual (normalized) MIME type string.

    Raises:
        MagicByteMismatchError: When actual MIME does not match declared.
    """
    actual_mime = detect_mime_type(file_path)
    if actual_mime.lower() != declared_mime.lower():
        raise MagicByteMismatchError(declared=declared_mime, actual=actual_mime)
    return actual_mime.lower()


def run_clamav_scan(file_path: Path) -> None:
    """Run ClamAV scan on the given file.

    Raises:
        ClamAVInfectedError: When ClamAV reports an infection (returncode=1).
        SecurityGateError: When clamdscan is unavailable or times out.
    """
    try:
        result = subprocess.run(
            ["clamdscan", "--no-summary", str(file_path)],
            capture_output=True,
            text=True,
            timeout=_CLAMAV_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as exc:
        raise SecurityGateError(
            f"ClamAV binary not found: {exc}",
            reason="clamav_unavailable",
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise SecurityGateError(
            f"ClamAV scan timed out after {_CLAMAV_TIMEOUT_SECONDS}s",
            reason="clamav_timeout",
        ) from exc

    if result.returncode == 1:
        # returncode 1 = virus found
        raise ClamAVInfectedError(scan_output=result.stdout)
    elif result.returncode > 1:
        # returncode 2 = error (daemon not running etc.)
        raise SecurityGateError(
            f"ClamAV returned error code {result.returncode}: {result.stderr}",
            reason="clamav_error",
        )

    logger.info("clamav_scan_clean", file=str(file_path))


def run_security_gate(
    file_path: Path,
    declared_mime: str,
    skip_clamav: bool = False,
) -> str:
    """Run the full security gate pipeline.

    Mandatory order: magic bytes → ClamAV → return.

    Args:
        file_path: Path to the file to inspect.
        declared_mime: MIME type declared by the uploader.
        skip_clamav: If True, bypass ClamAV scan (dev/test mode only).

    Returns:
        Normalized actual MIME type string.

    Raises:
        MagicByteMismatchError: On magic byte mismatch.
        ClamAVInfectedError: On ClamAV infection detection.
        SecurityGateError: On ClamAV infrastructure failures.
    """
    # Step 1: magic bytes
    actual_mime = verify_magic_bytes(file_path, declared_mime)
    logger.info("security_gate_magic_bytes_ok", mime=actual_mime, file=file_path.name)

    # Step 2: ClamAV
    if not skip_clamav:
        run_clamav_scan(file_path)
        logger.info("security_gate_clamav_ok", file=file_path.name)

    return actual_mime


def is_audio_mime(mime: str) -> bool:
    """Return True if the MIME type is an audio format."""
    return mime.lower() in _AUDIO_MIMES or mime.lower().startswith("audio/")


def mime_to_format(mime: str) -> Optional[str]:
    """Map a MIME type to a canonical format name.

    Returns None for unknown/unsupported types.
    """
    return _MIME_TO_FORMAT.get(mime.lower())
