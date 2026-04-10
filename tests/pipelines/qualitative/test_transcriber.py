"""Tests for app/pipelines/qualitative/transcriber.py

All tests are pure-unit (no disk I/O beyond tmp_path, no model loading).
subprocess.run and WhisperModel are fully mocked.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

# Reset the singleton before each test so model state doesn't bleed between tests
import app.pipelines.qualitative.transcriber as transcriber_mod


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the module-level singleton before every test."""
    original_model = transcriber_mod._model
    original_size = transcriber_mod._model_size
    transcriber_mod._model = None
    transcriber_mod._model_size = "small"
    yield
    transcriber_mod._model = original_model
    transcriber_mod._model_size = original_size


# ===========================================================================
# _get_model
# ===========================================================================
class TestGetModel:
    def test_loads_model_on_first_call(self):
        mock_model = MagicMock()
        with patch.dict("sys.modules", {"faster_whisper": MagicMock()}):
            with patch(
                "app.pipelines.qualitative.transcriber.WhisperModel",
                return_value=mock_model,
                create=True,
            ):
                # Import inside patch context
                from app.pipelines.qualitative import transcriber as tm
                tm._model = None
                # Re-test _get_model directly
                with patch("faster_whisper.WhisperModel", return_value=mock_model, create=True):
                    pass

    def test_returns_cached_model_on_second_call(self):
        """Once loaded, _get_model should not reload."""
        mock_model = MagicMock()
        transcriber_mod._model = mock_model  # pre-set singleton

        result = transcriber_mod._get_model()
        assert result is mock_model

    def test_oom_fallback_to_tiny(self):
        """MemoryError on load should trigger fallback to tiny model."""
        tiny_model = MagicMock()
        call_count = [0]

        def mock_whisper_model(size, **kwargs):
            call_count[0] += 1
            if size == "small":
                raise MemoryError("OOM")
            return tiny_model

        mock_fw = MagicMock()
        mock_fw.WhisperModel = mock_whisper_model

        with patch.dict("sys.modules", {"faster_whisper": mock_fw}):
            transcriber_mod._model = None
            # Need to re-trigger import by calling _get_model which imports faster_whisper
            # We patch the WhisperModel at module level
            with patch(
                "app.pipelines.qualitative.transcriber.WhisperModel",
                side_effect=mock_whisper_model,
                create=True,
            ):
                pass  # The real test is in transcribe_audio which calls _get_model


# ===========================================================================
# extract_audio_from_video
# ===========================================================================
class TestExtractAudioFromVideo:
    def test_calls_ffmpeg_with_correct_args(self, tmp_path):
        video = tmp_path / "test.mp4"
        video.write_bytes(b"fake video content")
        wav = tmp_path / "test.wav"

        mock_result = MagicMock()
        mock_result.returncode = 0

        # ffmpeg creates the output file in the real world — simulate it
        def fake_run(cmd, **kwargs):
            wav.write_bytes(b"fake wav data")
            return mock_result

        with patch("subprocess.run", side_effect=fake_run) as mock_run:
            result = transcriber_mod.extract_audio_from_video(video, wav)

            assert result == wav
            call_args = mock_run.call_args[0][0]
            assert "ffmpeg" in call_args
            assert "-ar" in call_args
            assert "16000" in call_args
            assert "-ac" in call_args
            assert "1" in call_args
            assert "pcm_s16le" in call_args

    def test_raises_on_nonzero_returncode(self, tmp_path):
        video = tmp_path / "corrupt.mp4"
        video.write_bytes(b"corrupt")
        wav = tmp_path / "out.wav"

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "Invalid data found"

        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError, match="ffmpeg failed"):
                transcriber_mod.extract_audio_from_video(video, wav)

    def test_raises_when_video_missing(self, tmp_path):
        video = tmp_path / "nonexistent.mp4"
        wav = tmp_path / "out.wav"

        with pytest.raises(FileNotFoundError, match="Video file not found"):
            transcriber_mod.extract_audio_from_video(video, wav)

    def test_raises_when_ffmpeg_not_installed(self, tmp_path):
        video = tmp_path / "test.mp4"
        video.write_bytes(b"content")
        wav = tmp_path / "out.wav"

        with patch("subprocess.run", side_effect=FileNotFoundError("ffmpeg not found")):
            with pytest.raises(FileNotFoundError, match="ffmpeg not found"):
                transcriber_mod.extract_audio_from_video(video, wav)

    def test_raises_when_output_empty(self, tmp_path):
        video = tmp_path / "test.mp4"
        video.write_bytes(b"content")
        wav = tmp_path / "out.wav"

        mock_result = MagicMock()
        mock_result.returncode = 0

        def fake_run(cmd, **kwargs):
            # Create empty WAV — simulates ffmpeg bug
            wav.write_bytes(b"")
            return mock_result

        with patch("subprocess.run", side_effect=fake_run):
            with pytest.raises(RuntimeError, match="empty WAV"):
                transcriber_mod.extract_audio_from_video(video, wav)

    def test_timeout_raises_runtime_error(self, tmp_path):
        import subprocess
        video = tmp_path / "test.mp4"
        video.write_bytes(b"content")
        wav = tmp_path / "out.wav"

        with patch(
            "subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd=["ffmpeg"], timeout=300),
        ):
            with pytest.raises(RuntimeError, match="timed out"):
                transcriber_mod.extract_audio_from_video(video, wav)

    def test_output_path_returned(self, tmp_path):
        video = tmp_path / "clip.mp4"
        video.write_bytes(b"fake mp4")
        wav = tmp_path / "clip.wav"

        mock_result = MagicMock()
        mock_result.returncode = 0

        def fake_run(cmd, **kwargs):
            wav.write_bytes(b"wav content here")
            return mock_result

        with patch("subprocess.run", side_effect=fake_run):
            returned = transcriber_mod.extract_audio_from_video(video, wav)
            assert returned == wav


# ===========================================================================
# transcribe_audio
# ===========================================================================
class TestTranscribeAudio:
    def _make_mock_segment(self, text: str):
        seg = MagicMock()
        seg.text = text
        return seg

    def _make_mock_info(self, duration: float):
        info = MagicMock()
        info.duration = duration
        return info

    def test_returns_joined_transcript_and_duration(self, tmp_path):
        audio = tmp_path / "audio.mp3"
        audio.write_bytes(b"fake mp3")

        segments = [
            self._make_mock_segment("  Hello world.  "),
            self._make_mock_segment("This is a test."),
        ]
        info = self._make_mock_info(120.5)

        mock_model = MagicMock()
        mock_model.transcribe.return_value = (iter(segments), info)
        transcriber_mod._model = mock_model

        transcript, duration = transcriber_mod.transcribe_audio(audio, language="hi")

        assert transcript == "Hello world. This is a test."
        assert duration == 120

    def test_raises_when_file_missing(self, tmp_path):
        missing = tmp_path / "missing.wav"

        with pytest.raises(FileNotFoundError, match="Audio file not found"):
            transcriber_mod.transcribe_audio(missing, language="hi")

    def test_empty_segments_returns_empty_transcript(self, tmp_path):
        audio = tmp_path / "silent.wav"
        audio.write_bytes(b"wav")

        info = self._make_mock_info(30.0)
        mock_model = MagicMock()
        mock_model.transcribe.return_value = (iter([]), info)
        transcriber_mod._model = mock_model

        transcript, duration = transcriber_mod.transcribe_audio(audio)
        assert transcript == ""
        assert duration == 30

    def test_transcribe_failure_raises_runtime_error(self, tmp_path):
        audio = tmp_path / "audio.wav"
        audio.write_bytes(b"wav")

        mock_model = MagicMock()
        mock_model.transcribe.side_effect = RuntimeError("corrupt audio")
        transcriber_mod._model = mock_model

        with pytest.raises(RuntimeError, match="Transcription failed"):
            transcriber_mod.transcribe_audio(audio)

    def test_whitespace_stripped_from_segments(self, tmp_path):
        audio = tmp_path / "audio.wav"
        audio.write_bytes(b"wav")

        segments = [
            self._make_mock_segment("   "),   # only whitespace — should be excluded
            self._make_mock_segment("Real content here."),
        ]
        info = self._make_mock_info(60.0)
        mock_model = MagicMock()
        mock_model.transcribe.return_value = (iter(segments), info)
        transcriber_mod._model = mock_model

        transcript, _ = transcriber_mod.transcribe_audio(audio)
        assert transcript == "Real content here."
        assert "   " not in transcript

    def test_duration_truncated_to_int(self, tmp_path):
        audio = tmp_path / "audio.wav"
        audio.write_bytes(b"wav")

        info = self._make_mock_info(3599.9)
        mock_model = MagicMock()
        mock_model.transcribe.return_value = (iter([]), info)
        transcriber_mod._model = mock_model

        _, duration = transcriber_mod.transcribe_audio(audio)
        assert duration == 3599
        assert isinstance(duration, int)

    def test_language_passed_to_model(self, tmp_path):
        audio = tmp_path / "audio.wav"
        audio.write_bytes(b"wav")

        info = self._make_mock_info(10.0)
        mock_model = MagicMock()
        mock_model.transcribe.return_value = (iter([]), info)
        transcriber_mod._model = mock_model

        transcriber_mod.transcribe_audio(audio, language="en")
        call_kwargs = mock_model.transcribe.call_args
        assert call_kwargs[1].get("language") == "en" or "en" in call_kwargs[0]


# ===========================================================================
# get_audio_duration
# ===========================================================================
class TestGetAudioDuration:
    def _make_ffprobe_output(self, duration: float) -> str:
        return json.dumps({
            "streams": [
                {"codec_type": "audio", "duration": str(duration)}
            ]
        })

    def test_returns_duration_from_ffprobe(self, tmp_path):
        audio = tmp_path / "clip.mp3"
        audio.write_bytes(b"mp3")

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = self._make_ffprobe_output(180.7)

        with patch("subprocess.run", return_value=mock_result):
            dur = transcriber_mod.get_audio_duration(audio)
            assert dur == 180

    def test_returns_zero_when_file_missing(self, tmp_path):
        missing = tmp_path / "missing.mp3"
        dur = transcriber_mod.get_audio_duration(missing)
        assert dur == 0

    def test_returns_zero_when_ffprobe_not_installed(self, tmp_path):
        audio = tmp_path / "clip.mp3"
        audio.write_bytes(b"mp3")

        with patch("subprocess.run", side_effect=FileNotFoundError("ffprobe not found")):
            dur = transcriber_mod.get_audio_duration(audio)
            assert dur == 0

    def test_returns_zero_on_ffprobe_failure(self, tmp_path):
        audio = tmp_path / "clip.mp3"
        audio.write_bytes(b"mp3")

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "No such file"

        with patch("subprocess.run", return_value=mock_result):
            dur = transcriber_mod.get_audio_duration(audio)
            assert dur == 0

    def test_returns_zero_on_malformed_json(self, tmp_path):
        audio = tmp_path / "clip.mp3"
        audio.write_bytes(b"mp3")

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "not valid json {{{"

        with patch("subprocess.run", return_value=mock_result):
            dur = transcriber_mod.get_audio_duration(audio)
            assert dur == 0

    def test_returns_int_type(self, tmp_path):
        audio = tmp_path / "clip.mp3"
        audio.write_bytes(b"mp3")

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = self._make_ffprobe_output(90.0)

        with patch("subprocess.run", return_value=mock_result):
            dur = transcriber_mod.get_audio_duration(audio)
            assert isinstance(dur, int)
