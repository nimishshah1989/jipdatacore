"""Tests for scripts/ingest/download_goldilocks_media.py

All tests are pure-unit (no DB, no network, no Playwright).
"""

from __future__ import annotations

import sys
from datetime import timezone
from pathlib import Path
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Add scripts/ingest to path
# ---------------------------------------------------------------------------
_SCRIPTS_PATH = str(
    Path(__file__).parent.parent.parent.parent / "scripts" / "ingest"
)
if _SCRIPTS_PATH not in sys.path:
    sys.path.insert(0, _SCRIPTS_PATH)

import download_goldilocks_media as dlm  # noqa: E402


# ===========================================================================
# compute_content_hash
# ===========================================================================
class TestComputeContentHash:
    def test_returns_64_char_hex(self):
        h = dlm.compute_content_hash("https://example.com/video.mp4")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_deterministic(self):
        url = "https://example.com/audio.mp3"
        assert dlm.compute_content_hash(url) == dlm.compute_content_hash(url)

    def test_different_urls_different_hashes(self):
        h1 = dlm.compute_content_hash("https://a.com/1.mp4")
        h2 = dlm.compute_content_hash("https://a.com/2.mp4")
        assert h1 != h2


# ===========================================================================
# parse_date_from_bold
# ===========================================================================
class TestParseDateFromBold:
    def test_extracts_iso_date(self):
        dt = dlm.parse_date_from_bold("2024-05-15")
        assert dt is not None
        assert dt.year == 2024
        assert dt.month == 5
        assert dt.day == 15

    def test_extracts_date_from_surrounding_text(self):
        dt = dlm.parse_date_from_bold("Con-call from 2024-03-10 recording")
        assert dt is not None
        assert dt.year == 2024

    def test_returns_none_when_no_date(self):
        assert dlm.parse_date_from_bold("no date here") is None

    def test_returns_utc_aware(self):
        dt = dlm.parse_date_from_bold("2024-01-20")
        assert dt is not None
        assert dt.tzinfo == timezone.utc


# ===========================================================================
# extract_concall_entries
# ===========================================================================
class TestExtractConcallEntries:
    def test_extracts_video_src(self):
        html = """
        <html><body>
        <p><b>2024-05-15</b></p>
        <video>
            <source src="data-temp/concall_may2024.mp4" type="video/mp4">
        </video>
        </body></html>
        """
        entries = dlm.extract_concall_entries(html)
        assert len(entries) == 1
        assert "concall_may2024.mp4" in entries[0]["url"]
        assert entries[0]["filename"] == "concall_may2024.mp4"

    def test_full_url_built_correctly(self):
        html = """
        <html><body>
        <video><source src="data-temp/test.mp4"></video>
        </body></html>
        """
        entries = dlm.extract_concall_entries(html)
        assert len(entries) == 1
        assert entries[0]["url"].startswith("https://")
        assert "goldilocksresearch.com" in entries[0]["url"]

    def test_date_associated_with_video(self):
        html = """
        <html><body>
        <p><b>2024-06-01</b></p>
        <video><source src="data-temp/june.mp4"></video>
        </body></html>
        """
        entries = dlm.extract_concall_entries(html)
        assert entries[0]["date"] is not None
        assert entries[0]["date"].year == 2024
        assert entries[0]["date"].month == 6

    def test_deduplicates_same_url(self):
        html = """
        <html><body>
        <video><source src="data-temp/same.mp4"></video>
        <video><source src="data-temp/same.mp4"></video>
        </body></html>
        """
        entries = dlm.extract_concall_entries(html)
        assert len(entries) == 1

    def test_empty_page_returns_empty_list(self):
        html = "<html><body><p>No videos here</p></body></html>"
        entries = dlm.extract_concall_entries(html)
        assert entries == []

    def test_title_contains_date(self):
        html = """
        <html><body>
        <p><b>2024-09-10</b></p>
        <video><source src="data-temp/sep.mp4"></video>
        </body></html>
        """
        entries = dlm.extract_concall_entries(html)
        assert "2024-09-10" in entries[0]["title"]


# ===========================================================================
# extract_soundbyte_entries
# ===========================================================================
class TestExtractSoundbytEntries:
    def test_extracts_audio_src(self):
        html = """
        <html><body>
        <audio><source src="data-temp/sound1.mp3" type="audio/mpeg"></audio>
        </body></html>
        """
        entries = dlm.extract_soundbyte_entries(html)
        assert len(entries) == 1
        assert "sound1.mp3" in entries[0]["url"]

    def test_deduplicates_same_mp3(self):
        html = """
        <html><body>
        <audio><source src="data-temp/dup.mp3"></audio>
        <audio><source src="data-temp/dup.mp3"></audio>
        </body></html>
        """
        entries = dlm.extract_soundbyte_entries(html)
        assert len(entries) == 1

    def test_filters_non_mp3_sources(self):
        html = """
        <html><body>
        <audio><source src="data-temp/video.mp4"></audio>
        <audio><source src="data-temp/sound.mp3"></audio>
        </body></html>
        """
        entries = dlm.extract_soundbyte_entries(html)
        assert len(entries) == 1
        assert "sound.mp3" in entries[0]["url"]

    def test_multiple_different_mp3s(self):
        html = """
        <html><body>
        <audio><source src="data-temp/a.mp3"></audio>
        <audio><source src="data-temp/b.mp3"></audio>
        <audio><source src="data-temp/c.mp3"></audio>
        </body></html>
        """
        entries = dlm.extract_soundbyte_entries(html)
        assert len(entries) == 3

    def test_date_associated_correctly(self):
        html = """
        <html><body>
        <p><b>2024-07-20</b></p>
        <audio><source src="data-temp/july.mp3"></audio>
        </body></html>
        """
        entries = dlm.extract_soundbyte_entries(html)
        assert entries[0]["date"] is not None
        assert entries[0]["date"].month == 7

    def test_empty_page_returns_empty_list(self):
        html = "<html><body><p>Nothing here</p></body></html>"
        entries = dlm.extract_soundbyte_entries(html)
        assert entries == []


# ===========================================================================
# should_skip_download
# ===========================================================================
class TestShouldSkipDownload:
    def test_skips_when_file_exists_and_sizes_match(self, tmp_path):
        f = tmp_path / "video.mp4"
        f.write_bytes(b"x" * 1000)
        assert dlm.should_skip_download(f, remote_size=1000) is True

    def test_does_not_skip_when_file_missing(self, tmp_path):
        missing = tmp_path / "missing.mp4"
        assert dlm.should_skip_download(missing, remote_size=1000) is False

    def test_does_not_skip_when_size_mismatch(self, tmp_path):
        f = tmp_path / "partial.mp4"
        f.write_bytes(b"x" * 500)
        assert dlm.should_skip_download(f, remote_size=1000) is False

    def test_does_not_skip_empty_file(self, tmp_path):
        f = tmp_path / "empty.mp4"
        f.write_bytes(b"")
        assert dlm.should_skip_download(f, remote_size=None) is False

    def test_skips_when_no_remote_size_and_file_exists(self, tmp_path):
        """If Content-Length not available, skip if file exists with non-zero size."""
        f = tmp_path / "video.mp4"
        f.write_bytes(b"x" * 500)
        assert dlm.should_skip_download(f, remote_size=None) is True


# ===========================================================================
# stream_download
# ===========================================================================
class TestStreamDownload:
    def test_dry_run_skips_download(self, tmp_path):
        sess = MagicMock()
        dest = tmp_path / "video.mp4"

        result = dlm.stream_download(sess, "https://example.com/v.mp4", dest, "https://referer.com", dry_run=True)
        assert result is True
        assert not dest.exists()
        sess.get.assert_not_called()

    def test_streams_chunks_to_file(self, tmp_path):
        sess = MagicMock()
        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Length": "12"}
        mock_resp.iter_content.return_value = [b"hello ", b"world!"]
        mock_resp.raise_for_status.return_value = None
        sess.get.return_value = mock_resp
        sess.head.return_value = MagicMock(headers={"Content-Length": "12"})

        dest = tmp_path / "audio.mp3"
        result = dlm.stream_download(sess, "https://example.com/a.mp3", dest, "https://r.com", dry_run=False)

        assert result is True
        assert dest.exists()
        assert dest.read_bytes() == b"hello world!"

    def test_download_failure_returns_false(self, tmp_path):
        sess = MagicMock()
        sess.head.return_value = MagicMock(headers={})
        sess.get.side_effect = Exception("Connection refused")

        dest = tmp_path / "fail.mp4"
        result = dlm.stream_download(sess, "https://example.com/fail.mp4", dest, "https://r.com", dry_run=False)
        assert result is False

    def test_skips_already_downloaded_file(self, tmp_path):
        dest = tmp_path / "video.mp4"
        dest.write_bytes(b"x" * 1000)

        sess = MagicMock()
        sess.head.return_value = MagicMock(headers={"Content-Length": "1000"})

        result = dlm.stream_download(sess, "https://example.com/v.mp4", dest, "https://r.com", dry_run=False)
        assert result is True
        sess.get.assert_not_called()  # Should not re-download


# ===========================================================================
# upsert_media_document
# ===========================================================================
class TestUpsertMediaDocument:
    def test_dry_run_returns_none_without_db_call(self):
        cur = MagicMock()
        result = dlm.upsert_media_document(
            cur=cur,
            source_id=1,
            source_url="https://example.com/v.mp4",
            title="Test Video",
            original_format="video",
            report_type="concall",
            published_at=None,
            dry_run=True,
        )
        assert result is None
        cur.execute.assert_not_called()

    def test_live_mode_executes_upsert(self):
        cur = MagicMock()
        cur.fetchone.return_value = ("some-uuid",)

        dlm.upsert_media_document(
            cur=cur,
            source_id=1,
            source_url="https://example.com/v.mp4",
            title="Test Video",
            original_format="video",
            report_type="concall",
            published_at=None,
            dry_run=False,
        )

        cur.execute.assert_called_once()
        sql = cur.execute.call_args[0][0]
        assert "de_qual_documents" in sql
        assert "ON CONFLICT" in sql
        assert "report_type" in sql

    def test_report_type_included_in_upsert(self):
        cur = MagicMock()
        cur.fetchone.return_value = ("uuid-1",)

        dlm.upsert_media_document(
            cur=cur,
            source_id=1,
            source_url="https://example.com/a.mp3",
            title="Sound Byte",
            original_format="audio",
            report_type="sound_byte",
            published_at=None,
            dry_run=False,
        )

        params = cur.execute.call_args[0][1]
        assert "sound_byte" in params

    def test_long_url_truncated(self):
        cur = MagicMock()
        cur.fetchone.return_value = (None,)
        long_url = "https://example.com/" + "x" * 3000

        dlm.upsert_media_document(
            cur=cur,
            source_id=1,
            source_url=long_url,
            title="Test",
            original_format="video",
            report_type="concall",
            published_at=None,
            dry_run=False,
        )

        params = cur.execute.call_args[0][1]
        url_param = [p for p in params if isinstance(p, str) and p.startswith("https://")]
        assert all(len(p) <= 2000 for p in url_param)

    def test_long_title_truncated(self):
        cur = MagicMock()
        cur.fetchone.return_value = (None,)

        dlm.upsert_media_document(
            cur=cur,
            source_id=1,
            source_url="https://example.com/v.mp4",
            title="T" * 600,
            original_format="video",
            report_type="concall",
            published_at=None,
            dry_run=False,
        )

        params = cur.execute.call_args[0][1]
        title_param = [p for p in params if isinstance(p, str) and p.startswith("T")]
        assert all(len(p) <= 500 for p in title_param)


# ===========================================================================
# ensure_qual_source
# ===========================================================================
class TestEnsureQualSource:
    def test_returns_source_id(self):
        cur = MagicMock()
        cur.fetchone.return_value = (42,)

        source_id = dlm.ensure_qual_source(cur)
        assert source_id == 42
        cur.execute.assert_called_once()
        sql = cur.execute.call_args[0][0]
        assert "de_qual_sources" in sql
        assert "Goldilocks Research" in str(cur.execute.call_args[0][1])
