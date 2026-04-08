"""Unit tests for scripts/ingest/stooq_downloader.py.

Tests cover:
  - load_cookies: missing file, valid file, malformed file
  - is_captcha_present: CAPTCHA detected / not detected via mock page
  - CLI argument parsing defaults
  - dry-run mode output (no browser launched)
"""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure DB env var is set so any transitive import doesn't fail
os.environ.setdefault("DATABASE_URL_SYNC", "postgresql://test:test@localhost/test")

from scripts.ingest.stooq_downloader import (
    DEFAULT_CATEGORIES,
    DOWNLOAD_URL_PATTERN,
    STOOQ_DB_PAGE,
    load_cookies,
    is_captcha_present,
)


# ---------------------------------------------------------------------------
# load_cookies
# ---------------------------------------------------------------------------

class TestLoadCookies:
    def test_missing_file_returns_empty_list(self, tmp_path):
        missing = tmp_path / "no_cookies.json"
        result = load_cookies(missing)
        assert result == []

    def test_valid_cookie_file_loaded(self, tmp_path):
        cookie_file = tmp_path / "cookies.json"
        cookies = [
            {"name": "session", "value": "abc123", "domain": "stooq.com"},
            {"name": "cf_clearance", "value": "xyz789", "domain": "stooq.com"},
        ]
        cookie_file.write_text(json.dumps(cookies))
        result = load_cookies(cookie_file)
        assert len(result) == 2
        assert result[0]["name"] == "session"
        assert result[1]["value"] == "xyz789"

    def test_malformed_json_returns_empty_list(self, tmp_path):
        cookie_file = tmp_path / "bad_cookies.json"
        cookie_file.write_text("this is not json {{{{")
        result = load_cookies(cookie_file)
        assert result == []

    def test_empty_cookie_list_file(self, tmp_path):
        cookie_file = tmp_path / "empty_cookies.json"
        cookie_file.write_text("[]")
        result = load_cookies(cookie_file)
        assert result == []

    def test_returns_list_type(self, tmp_path):
        cookie_file = tmp_path / "cookies.json"
        cookie_file.write_text(json.dumps([{"name": "x", "value": "y", "domain": "d.com"}]))
        result = load_cookies(cookie_file)
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# is_captcha_present
# ---------------------------------------------------------------------------

class TestIsCaptchaPresent:
    def _make_page(self, title="", frame_urls=None, has_element=False):
        page = MagicMock()
        page.title.return_value = title
        frames = []
        for url in (frame_urls or []):
            f = MagicMock()
            f.url = url
            frames.append(f)
        page.frames = frames
        element = MagicMock() if has_element else None
        page.query_selector.return_value = element
        return page

    def test_no_captcha_clean_page(self):
        page = self._make_page(title="Stooq Data", frame_urls=["https://stooq.com/db/h/"])
        assert is_captcha_present(page) is False

    def test_captcha_in_title(self):
        page = self._make_page(title="CAPTCHA challenge — stooq")
        assert is_captcha_present(page) is True

    def test_robot_in_title(self):
        page = self._make_page(title="Are you a robot?")
        assert is_captcha_present(page) is True

    def test_recaptcha_frame_detected(self):
        page = self._make_page(
            title="Stooq Data",
            frame_urls=["https://www.google.com/recaptcha/api2/anchor?..."],
        )
        assert is_captcha_present(page) is True

    def test_cloudflare_challenge_frame(self):
        page = self._make_page(
            title="Just a moment...",
            frame_urls=["https://challenges.cloudflare.com/cdn-cgi/challenge-platform"],
        )
        # "robot" not in title — but challenge in frame URL triggers detection
        assert is_captcha_present(page) is True

    def test_captcha_element_present(self):
        page = self._make_page(title="Stooq", frame_urls=[], has_element=True)
        assert is_captcha_present(page) is True

    def test_exception_in_page_call_returns_false(self):
        page = MagicMock()
        page.title.side_effect = Exception("page crashed")
        result = is_captcha_present(page)
        assert result is False


# ---------------------------------------------------------------------------
# Constants / URL patterns
# ---------------------------------------------------------------------------

class TestConstants:
    def test_default_categories_non_empty(self):
        assert len(DEFAULT_CATEGORIES) >= 2

    def test_default_categories_have_three_elements(self):
        for cat_id, desc, size in DEFAULT_CATEGORIES:
            assert isinstance(cat_id, str) and len(cat_id) > 0
            assert isinstance(desc, str) and len(desc) > 0

    def test_download_url_pattern_formats_correctly(self):
        url = DOWNLOAD_URL_PATTERN.format(category="d_macro_txt")
        assert url == "https://stooq.com/db/d/?b=d_macro_txt"

    def test_macro_download_url(self):
        url = DOWNLOAD_URL_PATTERN.format(category="d_macro_txt")
        assert "stooq.com" in url
        assert "d_macro_txt" in url

    def test_world_download_url(self):
        url = DOWNLOAD_URL_PATTERN.format(category="d_world_txt")
        assert "d_world_txt" in url

    def test_stooq_db_page_url(self):
        assert STOOQ_DB_PAGE == "https://stooq.com/db/h/"


# ---------------------------------------------------------------------------
# CLI dry-run (no browser)
# ---------------------------------------------------------------------------

class TestCLIDryRun:
    def test_dry_run_does_not_call_playwright(self, tmp_path, capsys):
        """dry-run should print URLs and exit without launching a browser."""
        import sys
        from unittest.mock import patch

        test_args = [
            "stooq_downloader.py",
            "--dry-run",
            "--download-dir", str(tmp_path),
            "--cookie-file", str(tmp_path / "cookies.json"),
            "--categories", "d_macro_txt",
        ]
        with patch("sys.argv", test_args):
            # If playwright is called this would raise; patch it to catch any leak
            with patch("scripts.ingest.stooq_downloader.download_categories") as mock_dl:
                from scripts.ingest.stooq_downloader import main
                main()
                # download_categories must NOT be called in dry-run
                mock_dl.assert_not_called()

    def test_empty_categories_exits(self, tmp_path):
        import sys
        test_args = [
            "stooq_downloader.py",
            "--categories", "",
            "--download-dir", str(tmp_path),
        ]
        with patch("sys.argv", test_args):
            from scripts.ingest.stooq_downloader import main
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
