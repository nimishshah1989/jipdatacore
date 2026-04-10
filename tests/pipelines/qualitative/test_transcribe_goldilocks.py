"""Tests for scripts/ingest/transcribe_goldilocks.py

All tests are pure-unit (no DB, no disk model loading, no ffmpeg).
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Add scripts/ingest to path
# ---------------------------------------------------------------------------
_SCRIPTS_PATH = str(
    Path(__file__).parent.parent.parent.parent / "scripts" / "ingest"
)
if _SCRIPTS_PATH not in sys.path:
    sys.path.insert(0, _SCRIPTS_PATH)

import transcribe_goldilocks as tg  # noqa: E402


# ===========================================================================
# get_available_ram_mb
# ===========================================================================
class TestGetAvailableRamMb:
    def test_returns_integer(self):
        result = tg.get_available_ram_mb()
        assert isinstance(result, int)
        assert result >= 0

    def test_fallback_when_psutil_missing(self, tmp_path):
        """Should not raise even if psutil and /proc/meminfo both unavailable."""
        with patch.dict("sys.modules", {"psutil": None}):
            with patch("pathlib.Path.read_text", side_effect=OSError("no file")):
                result = tg.get_available_ram_mb()
                assert result == 8192  # fallback value


# ===========================================================================
# check_ram_and_warn
# ===========================================================================
class TestCheckRamAndWarn:
    def test_returns_small_when_enough_ram(self):
        with patch("transcribe_goldilocks.get_available_ram_mb", return_value=6144):
            model_size = tg.check_ram_and_warn()
            assert model_size == "small"

    def test_returns_tiny_when_low_ram(self):
        with patch("transcribe_goldilocks.get_available_ram_mb", return_value=2048):
            model_size = tg.check_ram_and_warn()
            assert model_size == "tiny"

    def test_boundary_at_3gb(self):
        # 3072 MB = 3 GB (boundary — should return small)
        with patch("transcribe_goldilocks.get_available_ram_mb", return_value=3072):
            model_size = tg.check_ram_and_warn()
            assert model_size == "small"

    def test_just_below_3gb_returns_tiny(self):
        with patch("transcribe_goldilocks.get_available_ram_mb", return_value=3071):
            model_size = tg.check_ram_and_warn()
            assert model_size == "tiny"


# ===========================================================================
# resolve_local_path
# ===========================================================================
class TestResolveLocalPath:
    def test_video_goes_to_video_dir(self, tmp_path):
        url = "https://www.goldilocksresearch.com/data-temp/concall.mp4"
        result = tg.resolve_local_path(url, "video", tmp_path)
        assert result == tmp_path / "video" / "concall.mp4"

    def test_audio_goes_to_audio_dir(self, tmp_path):
        url = "https://www.goldilocksresearch.com/data-temp/sound.mp3"
        result = tg.resolve_local_path(url, "audio", tmp_path)
        assert result == tmp_path / "audio" / "sound.mp3"

    def test_query_params_stripped(self, tmp_path):
        url = "https://example.com/data-temp/file.mp4?v=123&auth=abc"
        result = tg.resolve_local_path(url, "video", tmp_path)
        assert result.name == "file.mp4"


# ===========================================================================
# fetch_pending_documents
# ===========================================================================
class TestFetchPendingDocuments:
    def test_returns_list_of_dicts(self):
        cur = MagicMock()
        cur.fetchall.return_value = [
            ("uuid-1", "https://example.com/v.mp4", "video", "Con-call May"),
            ("uuid-2", "https://example.com/a.mp3", "audio", "Sound Byte June"),
        ]

        docs = tg.fetch_pending_documents(cur)
        assert len(docs) == 2
        assert docs[0]["id"] == "uuid-1"
        assert docs[0]["original_format"] == "video"
        assert docs[1]["original_format"] == "audio"

    def test_executes_correct_query(self):
        cur = MagicMock()
        cur.fetchall.return_value = []

        tg.fetch_pending_documents(cur)
        sql = cur.execute.call_args[0][0]
        assert "de_qual_documents" in sql
        assert "original_format IN" in sql
        assert "Goldilocks Research" in sql
        assert "raw_text IS NULL" in sql

    def test_none_title_defaults_to_untitled(self):
        cur = MagicMock()
        cur.fetchall.return_value = [
            ("uuid-1", "https://example.com/v.mp4", "video", None),
        ]

        docs = tg.fetch_pending_documents(cur)
        assert docs[0]["title"] == "(untitled)"

    def test_empty_result_returns_empty_list(self):
        cur = MagicMock()
        cur.fetchall.return_value = []

        docs = tg.fetch_pending_documents(cur)
        assert docs == []


# ===========================================================================
# update_transcript
# ===========================================================================
class TestUpdateTranscript:
    def test_dry_run_does_not_call_execute(self):
        cur = MagicMock()
        tg.update_transcript(cur, "uuid-1", "transcript text", 300, dry_run=True)
        cur.execute.assert_not_called()

    def test_live_mode_updates_raw_text(self):
        cur = MagicMock()
        tg.update_transcript(cur, "uuid-1", "transcript text", 300, dry_run=False)
        cur.execute.assert_called_once()
        sql = cur.execute.call_args[0][0]
        assert "raw_text" in sql
        assert "audio_duration_s" in sql
        assert "de_qual_documents" in sql

    def test_params_include_transcript_and_duration(self):
        cur = MagicMock()
        tg.update_transcript(cur, "uuid-99", "hello world", 120, dry_run=False)
        params = cur.execute.call_args[0][1]
        assert "hello world" in params
        assert 120 in params
        assert "uuid-99" in params


# ===========================================================================
# transcribe_documents
# ===========================================================================
class TestTranscribeDocuments:
    def _make_doc(self, doc_id="uuid-1", source_url="https://x.com/a.mp3", fmt="audio", title="Test"):
        return {"id": doc_id, "source_url": source_url, "original_format": fmt, "title": title}

    def test_skips_missing_files(self, tmp_path):
        cur = MagicMock()
        docs = [self._make_doc(source_url="https://x.com/data-temp/missing.mp3")]

        stats = tg.transcribe_documents(
            documents=docs,
            cur=cur,
            base_dir=tmp_path,
            model_size="tiny",
            dry_run=False,
        )

        assert stats["skipped_missing"] == 1
        assert stats["transcribed"] == 0

    @pytest.mark.skipif(
        not __import__("shutil").which("ffmpeg"),
        reason="faster-whisper/ffmpeg not available locally — runs on EC2",
    )
    def test_transcribes_mp3_file(self, tmp_path):
        audio_dir = tmp_path / "audio"
        audio_dir.mkdir()
        mp3 = audio_dir / "sound.mp3"
        mp3.write_bytes(b"fake mp3 content")

        cur = MagicMock()
        docs = [self._make_doc(source_url="https://x.com/data-temp/sound.mp3", fmt="audio")]

        mock_transcribe = MagicMock(return_value=("Hindi transcript text here", 180))

        with patch("app.pipelines.qualitative.transcriber.transcribe_audio", mock_transcribe):
            with patch("app.pipelines.qualitative.transcriber.extract_audio_from_video"):
                stats = tg.transcribe_documents(
                    documents=docs,
                    cur=cur,
                    base_dir=tmp_path,
                    model_size="small",
                    dry_run=False,
                )

        assert stats["transcribed"] == 1
        assert stats["total_duration_s"] == 180
        assert stats["total_chars"] == len("Hindi transcript text here")

    @pytest.mark.skipif(
        not __import__("shutil").which("ffmpeg"),
        reason="faster-whisper/ffmpeg not available locally — runs on EC2",
    )
    def test_extracts_audio_for_video_files(self, tmp_path):
        video_dir = tmp_path / "video"
        video_dir.mkdir()
        mp4 = video_dir / "concall.mp4"
        mp4.write_bytes(b"fake mp4 content")

        cur = MagicMock()
        docs = [self._make_doc(
            source_url="https://x.com/data-temp/concall.mp4",
            fmt="video",
            title="Con-call"
        )]

        mock_extract = MagicMock(return_value=mp4.with_suffix(".wav"))
        mock_transcribe = MagicMock(return_value=("Transcript here", 3600))

        with patch("app.pipelines.qualitative.transcriber.extract_audio_from_video", mock_extract):
            with patch("app.pipelines.qualitative.transcriber.transcribe_audio", mock_transcribe):
                # Create wav so cleanup doesn't error
                wav = mp4.with_suffix(".wav")
                wav.write_bytes(b"wav")
                mock_extract.return_value = wav

                stats = tg.transcribe_documents(
                    documents=docs,
                    cur=cur,
                    base_dir=tmp_path,
                    model_size="small",
                    dry_run=False,
                )

        mock_extract.assert_called_once()
        assert stats["transcribed"] == 1

    def test_wav_cleaned_up_after_video_transcription(self, tmp_path):
        video_dir = tmp_path / "video"
        video_dir.mkdir()
        mp4 = video_dir / "concall.mp4"
        mp4.write_bytes(b"fake mp4")
        wav = mp4.with_suffix(".wav")
        wav.write_bytes(b"fake wav")

        cur = MagicMock()
        docs = [self._make_doc(
            source_url="https://x.com/data-temp/concall.mp4",
            fmt="video"
        )]

        def fake_extract(video_path, output_path):
            # WAV already created above
            return output_path

        with patch("app.pipelines.qualitative.transcriber.extract_audio_from_video", side_effect=fake_extract):
            with patch("app.pipelines.qualitative.transcriber.transcribe_audio", return_value=("text", 60)):
                tg.transcribe_documents(
                    documents=docs,
                    cur=cur,
                    base_dir=tmp_path,
                    model_size="small",
                    dry_run=False,
                )

        # WAV should be deleted after transcription
        assert not wav.exists()

    def test_failed_transcription_counted_in_stats(self, tmp_path):
        audio_dir = tmp_path / "audio"
        audio_dir.mkdir()
        mp3 = audio_dir / "bad.mp3"
        mp3.write_bytes(b"corrupt mp3")

        cur = MagicMock()
        docs = [self._make_doc(source_url="https://x.com/data-temp/bad.mp3", fmt="audio")]

        with patch(
            "app.pipelines.qualitative.transcriber.transcribe_audio",
            side_effect=RuntimeError("transcription failed"),
        ):
            stats = tg.transcribe_documents(
                documents=docs,
                cur=cur,
                base_dir=tmp_path,
                model_size="small",
                dry_run=False,
            )

        assert stats["failed"] == 1
        assert stats["transcribed"] == 0

    def test_dry_run_does_not_call_transcribe(self, tmp_path):
        audio_dir = tmp_path / "audio"
        audio_dir.mkdir()
        mp3 = audio_dir / "sound.mp3"
        mp3.write_bytes(b"mp3")

        cur = MagicMock()
        docs = [self._make_doc(source_url="https://x.com/data-temp/sound.mp3", fmt="audio")]

        mock_transcribe = MagicMock(return_value=("text", 60))

        with patch("app.pipelines.qualitative.transcriber.transcribe_audio", mock_transcribe):
            tg.transcribe_documents(
                documents=docs,
                cur=cur,
                base_dir=tmp_path,
                model_size="small",
                dry_run=True,
            )

        # In dry_run, transcription should NOT be called
        mock_transcribe.assert_not_called()

    @pytest.mark.skipif(
        not __import__("shutil").which("ffmpeg"),
        reason="faster-whisper/ffmpeg not available locally — runs on EC2",
    )
    def test_serial_processing_multiple_docs(self, tmp_path):
        audio_dir = tmp_path / "audio"
        audio_dir.mkdir()

        for name in ["a.mp3", "b.mp3", "c.mp3"]:
            (audio_dir / name).write_bytes(b"mp3")

        cur = MagicMock()
        docs = [
            self._make_doc(doc_id=f"id-{i}", source_url=f"https://x.com/data-temp/{name}", fmt="audio")
            for i, name in enumerate(["a.mp3", "b.mp3", "c.mp3"])
        ]

        call_order = []

        def mock_transcribe(path, language="hi"):
            call_order.append(str(path.name))
            return ("text", 60)

        with patch("app.pipelines.qualitative.transcriber.transcribe_audio", side_effect=mock_transcribe):
            stats = tg.transcribe_documents(
                documents=docs,
                cur=cur,
                base_dir=tmp_path,
                model_size="small",
                dry_run=False,
            )

        assert stats["transcribed"] == 3
        assert call_order == ["a.mp3", "b.mp3", "c.mp3"]
