"""Tests for scripts/ingest/goldilocks_scraper.py

All tests are pure-unit (no DB, no network).
External services (requests.Session, psycopg2) are fully mocked.
"""

from __future__ import annotations

import sys
from datetime import timezone
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Import the module under test.
# The module does _load_env() at import time — that's fine (reads .env if present).
# We guard psycopg2/requests/bs4 so tests don't need the real packages installed
# as long as they are importable (they are in requirements).
# ---------------------------------------------------------------------------
_SCRAPER_PATH = str(
    Path(__file__).parent.parent.parent.parent / "scripts" / "ingest"
)
if _SCRAPER_PATH not in sys.path:
    sys.path.insert(0, _SCRAPER_PATH)

import goldilocks_scraper as gs  # noqa: E402  (after sys.path manipulation)


# ===========================================================================
# compute_content_hash
# ===========================================================================
class TestComputeContentHash:
    def test_returns_64_char_hex(self):
        h = gs.compute_content_hash("hello world")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_deterministic(self):
        h1 = gs.compute_content_hash("test content")
        h2 = gs.compute_content_hash("test content")
        assert h1 == h2

    def test_different_inputs_produce_different_hashes(self):
        h1 = gs.compute_content_hash("abc")
        h2 = gs.compute_content_hash("xyz")
        assert h1 != h2

    def test_empty_string_valid(self):
        h = gs.compute_content_hash("")
        assert len(h) == 64

    def test_unicode_handled(self):
        h = gs.compute_content_hash("Nifty ₹1,23,456 — भारत")
        assert len(h) == 64


# ===========================================================================
# parse_date_text
# ===========================================================================
class TestParseDateText:
    def test_dd_mmmm_yyyy(self):
        dt = gs.parse_date_text("15 March 2024")
        assert dt is not None
        assert dt.year == 2024
        assert dt.month == 3
        assert dt.day == 15

    def test_dd_mmm_yyyy_with_dash(self):
        dt = gs.parse_date_text("15-Mar-2024")
        assert dt is not None
        assert dt.month == 3

    def test_dd_slash_mm_slash_yyyy(self):
        dt = gs.parse_date_text("15/03/2024")
        assert dt is not None
        assert dt.day == 15
        assert dt.month == 3

    def test_iso_format(self):
        dt = gs.parse_date_text("2024-03-15")
        assert dt is not None
        assert dt.year == 2024

    def test_month_dd_comma_yyyy(self):
        dt = gs.parse_date_text("March 15, 2024")
        assert dt is not None

    def test_returns_utc_aware(self):
        dt = gs.parse_date_text("2024-03-15")
        assert dt is not None
        assert dt.tzinfo is not None
        assert dt.tzinfo == timezone.utc

    def test_invalid_text_returns_none(self):
        assert gs.parse_date_text("not a date at all") is None

    def test_empty_string_returns_none(self):
        assert gs.parse_date_text("") is None

    def test_whitespace_stripped(self):
        dt = gs.parse_date_text("  2024-01-20  ")
        assert dt is not None
        assert dt.day == 20


# ===========================================================================
# transcribe_audio
# ===========================================================================
class TestTranscribeAudio:
    def test_returns_empty_string(self):
        """Placeholder must return empty string — not None, not raise."""
        result = gs.transcribe_audio(Path("/tmp/fake_audio.mp3"))
        assert result == ""

    def test_return_type_is_str(self):
        result = gs.transcribe_audio(Path("/tmp/fake_audio.mp3"))
        assert isinstance(result, str)


# ===========================================================================
# extract_items_from_soup
# ===========================================================================
class TestExtractItemsFromSoup:
    """Tests use real BeautifulSoup parsing on synthetic HTML — valid for unit tests
    because this is testing OUR parsing logic, not external data."""

    def _soup(self, html: str):
        from bs4 import BeautifulSoup
        return BeautifulSoup(html, "html.parser")

    def test_extracts_title_from_h3(self):
        html = """
        <table>
          <tr>
            <td><h3>Trend Friend - Week 15</h3></td>
            <td>15 March 2024</td>
            <td><a href="/reports/tf15.pdf">Download PDF</a></td>
          </tr>
        </table>
        """
        items = gs.extract_items_from_soup(self._soup(html), "https://example.com/page.php")
        titles = [i["title"] for i in items]
        assert any("Trend Friend" in t for t in titles)

    def test_extracts_pdf_link(self):
        html = """
        <tr>
          <td><strong>Stock Bullet</strong></td>
          <td><a href="/files/stock_bullet.pdf">Download</a></td>
          <td>20 February 2024</td>
        </tr>
        """
        items = gs.extract_items_from_soup(self._soup(html), "https://example.com/cus_dashboard.php")
        pdf_links = [lnk for i in items for lnk in i["pdf_links"]]
        assert any("stock_bullet.pdf" in lnk for lnk in pdf_links)

    def test_extracts_audio_link(self):
        html = """
        <tr>
          <td><strong>Sound Byte 42</strong></td>
          <td><a href="/audio/sound42.mp3">Listen</a></td>
          <td>01 April 2024</td>
        </tr>
        """
        items = gs.extract_items_from_soup(self._soup(html), "https://example.com/sound_byte.php")
        audio_links = [lnk for i in items for lnk in i["audio_links"]]
        assert any("sound42.mp3" in lnk for lnk in audio_links)

    def test_extracts_youtube_video_link(self):
        html = """
        <tr>
          <td><strong>Video Update</strong></td>
          <td><a href="https://www.youtube.com/watch?v=abc123">Watch</a></td>
          <td>05 April 2024</td>
        </tr>
        """
        items = gs.extract_items_from_soup(self._soup(html), "https://example.com/video_update.php")
        video_links = [lnk for i in items for lnk in i["video_links"]]
        assert any("youtube.com" in lnk for lnk in video_links)

    def test_skips_tiny_rows(self):
        html = "<tr><td>Hi</td></tr><tr><td><h3>Real Report Title</h3><p>Some content here longer</p></td></tr>"
        items = gs.extract_items_from_soup(self._soup(html), "https://example.com/page.php")
        # Should still find the real row; tiny one excluded
        texts = [i["body_text"] for i in items]
        assert any("Real Report Title" in t for t in texts)

    def test_returns_list(self):
        html = "<div>nothing useful</div>"
        result = gs.extract_items_from_soup(self._soup(html), "https://example.com/page.php")
        assert isinstance(result, list)

    def test_relative_pdf_url_resolved(self):
        html = """
        <tr>
          <td><a href="reports/jan24.pdf">January Report</a></td>
        </tr>
        """
        items = gs.extract_items_from_soup(
            self._soup(html), "https://www.goldilocksresearch.com/cus_dashboard.php"
        )
        pdf_links = [lnk for i in items for lnk in i["pdf_links"]]
        assert any(lnk.startswith("https://") for lnk in pdf_links)


# ===========================================================================
# is_duplicate
# ===========================================================================
class TestIsDuplicate:
    def _mock_cursor(self, found: bool):
        cur = MagicMock()
        cur.fetchone.return_value = (1,) if found else None
        return cur

    def test_returns_true_when_row_exists(self):
        cur = self._mock_cursor(found=True)
        assert gs.is_duplicate(cur, source_id=1, content_hash="abc123") is True

    def test_returns_false_when_no_row(self):
        cur = self._mock_cursor(found=False)
        assert gs.is_duplicate(cur, source_id=1, content_hash="abc123") is False

    def test_executes_correct_query(self):
        cur = self._mock_cursor(found=False)
        gs.is_duplicate(cur, source_id=42, content_hash="hashval")
        call_args = cur.execute.call_args[0]
        sql = call_args[0]
        params = call_args[1]
        assert "de_qual_documents" in sql
        assert params == (42, "hashval")


# ===========================================================================
# insert_qual_document — dry_run branch
# ===========================================================================
class TestInsertQualDocument:
    def _mock_cursor(self, returned_id: Optional[str] = None):
        cur = MagicMock()
        cur.fetchone.return_value = (returned_id,) if returned_id else None
        return cur

    def test_dry_run_returns_none(self):
        cur = self._mock_cursor(returned_id="some-uuid")
        result = gs.insert_qual_document(
            cur=cur,
            source_id=1,
            content_hash="abc",
            source_url="https://example.com/doc.pdf",
            title="Test Report",
            raw_text="body text",
            original_format="pdf",
            published_at=None,
            dry_run=True,
        )
        assert result is None
        cur.execute.assert_not_called()

    def test_live_mode_calls_execute(self):
        cur = self._mock_cursor(returned_id="test-uuid-1234")
        gs.insert_qual_document(
            cur=cur,
            source_id=1,
            content_hash="abc",
            source_url="https://example.com/doc.pdf",
            title="Test Report",
            raw_text="body text",
            original_format="pdf",
            published_at=None,
            dry_run=False,
        )
        cur.execute.assert_called_once()
        # SQL must reference de_qual_documents
        sql = cur.execute.call_args[0][0]
        assert "de_qual_documents" in sql

    def test_returns_none_on_conflict_do_nothing(self):
        """ON CONFLICT DO NOTHING → fetchone returns None."""
        cur = self._mock_cursor(returned_id=None)
        result = gs.insert_qual_document(
            cur=cur,
            source_id=1,
            content_hash="abc",
            source_url="https://example.com/x.pdf",
            title="Dup",
            raw_text="body",
            original_format="html",
            published_at=None,
            dry_run=False,
        )
        assert result is None

    def test_long_source_url_truncated(self):
        """source_url longer than 2000 chars must be silently truncated."""
        cur = self._mock_cursor(returned_id="uuid-x")
        long_url = "https://example.com/" + "x" * 3000
        gs.insert_qual_document(
            cur=cur,
            source_id=1,
            content_hash="abc",
            source_url=long_url,
            title="Title",
            raw_text="text",
            original_format="html",
            published_at=None,
            dry_run=False,
        )
        params = cur.execute.call_args[0][1]
        # source_url is the 4th parameter (index 3)
        assert len(params[3]) <= 2000

    def test_long_title_truncated(self):
        cur = self._mock_cursor(returned_id="uuid-y")
        long_title = "T" * 600
        gs.insert_qual_document(
            cur=cur,
            source_id=1,
            content_hash="abc",
            source_url="https://example.com/x",
            title=long_title,
            raw_text="text",
            original_format="html",
            published_at=None,
            dry_run=False,
        )
        params = cur.execute.call_args[0][1]
        # title is 6th parameter (index 5)
        assert len(params[5]) <= 500


# ===========================================================================
# ensure_goldilocks_tables
# ===========================================================================
class TestEnsureGoldilocksTable:
    def test_executes_three_create_statements(self):
        cur = MagicMock()
        gs.ensure_goldilocks_tables(cur)
        assert cur.execute.call_count == 3

    def test_creates_market_view_table(self):
        cur = MagicMock()
        gs.ensure_goldilocks_tables(cur)
        calls_sql = [str(c[0][0]) for c in cur.execute.call_args_list]
        assert any("de_goldilocks_market_view" in sql for sql in calls_sql)

    def test_creates_sector_view_table(self):
        cur = MagicMock()
        gs.ensure_goldilocks_tables(cur)
        calls_sql = [str(c[0][0]) for c in cur.execute.call_args_list]
        assert any("de_goldilocks_sector_view" in sql for sql in calls_sql)

    def test_creates_stock_ideas_table(self):
        cur = MagicMock()
        gs.ensure_goldilocks_tables(cur)
        calls_sql = [str(c[0][0]) for c in cur.execute.call_args_list]
        assert any("de_goldilocks_stock_ideas" in sql for sql in calls_sql)

    def test_all_use_if_not_exists(self):
        cur = MagicMock()
        gs.ensure_goldilocks_tables(cur)
        for call in cur.execute.call_args_list:
            sql = call[0][0]
            assert "IF NOT EXISTS" in sql, f"Missing IF NOT EXISTS in: {sql[:80]}"


# ===========================================================================
# download_file — dry_run
# ===========================================================================
class TestDownloadFile:
    def test_dry_run_returns_true_without_writing(self, tmp_path):
        sess = MagicMock()
        dest = tmp_path / "pdfs" / "report.pdf"
        result = gs.download_file(sess, "https://example.com/r.pdf", dest, dry_run=True)
        assert result is True
        assert not dest.exists()
        sess.get.assert_not_called()

    def test_real_download_creates_file(self, tmp_path):
        sess = MagicMock()
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = [b"PDF content here"]
        mock_resp.raise_for_status.return_value = None
        sess.get.return_value = mock_resp

        dest = tmp_path / "pdfs" / "report.pdf"
        result = gs.download_file(sess, "https://example.com/r.pdf", dest, dry_run=False)
        assert result is True
        assert dest.exists()
        assert dest.read_bytes() == b"PDF content here"

    def test_download_failure_returns_false(self, tmp_path):
        sess = MagicMock()
        sess.get.side_effect = Exception("Connection refused")
        dest = tmp_path / "pdfs" / "fail.pdf"
        result = gs.download_file(sess, "https://example.com/fail.pdf", dest, dry_run=False)
        assert result is False


# ===========================================================================
# Pages constant structure
# ===========================================================================
class TestPagesConstant:
    def test_all_required_pages_present(self):
        page_names = [p["name"] for p in gs.PAGES]
        required = {
            "india_reports",
            "market_snippets",
            "qa_gautam",
            "monthly_concall",
            "video_updates",
            "sound_bytes",
            "usa_reports",
        }
        assert required == set(page_names)

    def test_all_pages_have_url_and_name(self):
        for page in gs.PAGES:
            assert "url" in page
            assert "name" in page
            assert page["url"].startswith("https://")

    def test_all_urls_on_goldilocksresearch_domain(self):
        for page in gs.PAGES:
            assert "goldilocksresearch.com" in page["url"]


# ===========================================================================
# build_session — mocked network
# ===========================================================================
class TestBuildSession:
    @patch("goldilocks_scraper.requests.Session")
    def test_posts_to_login_url(self, mock_session_cls):
        mock_sess = MagicMock()
        mock_resp = MagicMock()
        mock_resp.url = "https://www.goldilocksresearch.com/cus_dashboard.php"
        mock_resp.status_code = 200
        mock_resp.text = "<html>Welcome</html>"
        mock_sess.post.return_value = mock_resp
        mock_session_cls.return_value = mock_sess

        gs.build_session("test@example.com", "pass123")
        mock_sess.post.assert_called_once()
        call_args = mock_sess.post.call_args
        assert gs.LOGIN_URL in call_args[0] or gs.LOGIN_URL == call_args[0][0]

    @patch("goldilocks_scraper.requests.Session")
    def test_sets_user_agent_header(self, mock_session_cls):
        mock_sess = MagicMock()
        mock_resp = MagicMock()
        mock_resp.url = "https://www.goldilocksresearch.com/cus_dashboard.php"
        mock_resp.status_code = 200
        mock_resp.text = "<html>Welcome</html>"
        mock_sess.post.return_value = mock_resp
        mock_session_cls.return_value = mock_sess

        gs.build_session("test@example.com", "pass123")
        update_call = mock_sess.headers.update.call_args[0][0]
        assert "User-Agent" in update_call
        assert "Mozilla" in update_call["User-Agent"]

    @patch("goldilocks_scraper.requests.Session")
    def test_raises_on_auth_failure(self, mock_session_cls):
        mock_sess = MagicMock()
        mock_resp = MagicMock()
        mock_resp.url = "https://www.goldilocksresearch.com/cus_signin.php"
        mock_resp.status_code = 200
        mock_resp.text = "<html>Invalid credentials error</html>"
        mock_sess.post.return_value = mock_resp
        mock_session_cls.return_value = mock_sess

        with pytest.raises(RuntimeError, match="Goldilocks login failed"):
            gs.build_session("bad@example.com", "wrongpass")
