"""Tests for security gate: magic byte verification and ClamAV scanning."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.pipelines.qualitative.security_gate import (
    ClamAVInfectedError,
    MagicByteMismatchError,
    SecurityGateError,
    is_audio_mime,
    mime_to_format,
    run_clamav_scan,
    run_security_gate,
    verify_magic_bytes,
)


class TestMagicByteValidation:
    """Tests for magic byte (libmagic) file type verification."""

    def test_magic_byte_validation_pdf(self, tmp_path: Path) -> None:
        """A real PDF file should be detected as application/pdf."""
        pdf_file = tmp_path / "report.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")

        # Patch detect_mime_type directly to avoid libmagic dependency
        with patch(
            "app.pipelines.qualitative.security_gate.detect_mime_type",
            return_value="application/pdf",
        ):
            result = verify_magic_bytes(pdf_file, "application/pdf")
        assert result == "application/pdf"

    def test_magic_byte_validation_rejects_executable(self, tmp_path: Path) -> None:
        """An ELF binary declared as PDF must be rejected with MagicByteMismatchError."""
        exe_file = tmp_path / "malware.pdf"
        exe_file.write_bytes(b"\x7fELF\x02\x01\x01\x00" + b"\x00" * 8)

        with patch(
            "app.pipelines.qualitative.security_gate.detect_mime_type",
            return_value="application/x-executable",
        ):
            with pytest.raises(MagicByteMismatchError) as exc_info:
                verify_magic_bytes(exe_file, "application/pdf")

        assert "application/pdf" in str(exc_info.value)
        assert "application/x-executable" in str(exc_info.value)

    def test_magic_byte_validation_audio_mp3(self, tmp_path: Path) -> None:
        """MP3 file declared as audio/mpeg should pass."""
        mp3_file = tmp_path / "podcast.mp3"
        mp3_file.write_bytes(b"\xff\xfb\x90\x00" + b"\x00" * 100)

        with patch(
            "app.pipelines.qualitative.security_gate.detect_mime_type",
            return_value="audio/mpeg",
        ):
            result = verify_magic_bytes(mp3_file, "audio/mpeg")
        assert result == "audio/mpeg"

    def test_magic_byte_validation_case_insensitive(self, tmp_path: Path) -> None:
        """MIME comparison should be case-insensitive."""
        pdf_file = tmp_path / "doc.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n")

        with patch(
            "app.pipelines.qualitative.security_gate.detect_mime_type",
            return_value="application/pdf",
        ):
            result = verify_magic_bytes(pdf_file, "Application/PDF")
        assert result == "application/pdf"

    def test_magic_byte_validation_text_as_pdf_rejected(self, tmp_path: Path) -> None:
        """Plain text file declared as PDF must be rejected."""
        txt_file = tmp_path / "fake.pdf"
        txt_file.write_text("This is actually plain text, not a PDF")

        with patch(
            "app.pipelines.qualitative.security_gate.detect_mime_type",
            return_value="text/plain",
        ):
            with pytest.raises(MagicByteMismatchError):
                verify_magic_bytes(txt_file, "application/pdf")


class TestClamAVScan:
    """Tests for ClamAV virus scanning."""

    def test_clamav_clean_file_passes(self, tmp_path: Path) -> None:
        """Clean file should pass without raising."""
        clean_file = tmp_path / "clean.pdf"
        clean_file.write_bytes(b"%PDF-1.4\n")

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "clean.pdf: OK"
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result):
            run_clamav_scan(clean_file)  # Should not raise

    def test_clamav_infected_raises(self, tmp_path: Path) -> None:
        """Infected file should raise ClamAVInfectedError."""
        infected_file = tmp_path / "virus.pdf"
        infected_file.write_bytes(b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE")

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = "virus.pdf: Eicar-Test-Signature FOUND"
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(ClamAVInfectedError) as exc_info:
                run_clamav_scan(infected_file)

        assert "FOUND" in str(exc_info.value)

    def test_clamav_not_installed_raises_security_gate_error(self, tmp_path: Path) -> None:
        """Missing clamdscan binary should raise SecurityGateError."""
        test_file = tmp_path / "test.pdf"
        test_file.write_bytes(b"%PDF-1.4\n")

        with patch("subprocess.run", side_effect=FileNotFoundError("clamdscan not found")):
            with pytest.raises(SecurityGateError) as exc_info:
                run_clamav_scan(test_file)

        assert exc_info.value.reason == "clamav_unavailable"

    def test_clamav_timeout_raises_security_gate_error(self, tmp_path: Path) -> None:
        """Timeout during scan should raise SecurityGateError."""
        test_file = tmp_path / "test.pdf"
        test_file.write_bytes(b"%PDF-1.4\n")

        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("clamdscan", 60)):
            with pytest.raises(SecurityGateError) as exc_info:
                run_clamav_scan(test_file)

        assert exc_info.value.reason == "clamav_timeout"


class TestSecurityGateIntegration:
    """Integration tests for the full security gate."""

    def test_full_gate_passes_clean_pdf(self, tmp_path: Path) -> None:
        """Clean PDF should pass both magic byte check and ClamAV."""
        pdf_file = tmp_path / "report.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n")

        mock_clamav = MagicMock()
        mock_clamav.returncode = 0
        mock_clamav.stdout = "OK"
        mock_clamav.stderr = ""

        with patch(
            "app.pipelines.qualitative.security_gate.detect_mime_type",
            return_value="application/pdf",
        ), patch("subprocess.run", return_value=mock_clamav):
            result = run_security_gate(pdf_file, "application/pdf")

        assert result == "application/pdf"

    def test_full_gate_skips_clamav_in_dev_mode(self, tmp_path: Path) -> None:
        """skip_clamav=True should bypass ClamAV scan."""
        pdf_file = tmp_path / "report.pdf"
        pdf_file.write_bytes(b"%PDF-1.4\n")

        with patch(
            "app.pipelines.qualitative.security_gate.detect_mime_type",
            return_value="application/pdf",
        ), patch("subprocess.run") as mock_run:
            result = run_security_gate(pdf_file, "application/pdf", skip_clamav=True)
            mock_run.assert_not_called()

        assert result == "application/pdf"


class TestHelperFunctions:
    """Tests for MIME type helper functions."""

    def test_is_audio_mime_mp3(self) -> None:
        assert is_audio_mime("audio/mpeg") is True

    def test_is_audio_mime_wav(self) -> None:
        assert is_audio_mime("audio/wav") is True

    def test_is_audio_mime_pdf_is_false(self) -> None:
        assert is_audio_mime("application/pdf") is False

    def test_mime_to_format_pdf(self) -> None:
        assert mime_to_format("application/pdf") == "pdf"

    def test_mime_to_format_unknown_returns_none(self) -> None:
        assert mime_to_format("application/x-unknown") is None
