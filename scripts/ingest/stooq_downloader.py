"""Download stooq bulk data files using Playwright.

Navigates to https://stooq.com/db/h/ and downloads specified category zip files.
Persists session cookies to avoid repeated CAPTCHA challenges.

Usage:
    python scripts/ingest/stooq_downloader.py \
        [--download-dir /tmp/stooq_downloads] [--cookie-file /tmp/stooq_cookies.json]

Environment variables:
    STOOQ_DOWNLOAD_DIR   — override default download directory
    STOOQ_COOKIE_FILE    — override default cookie file path
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
STOOQ_BASE = "https://stooq.com"
STOOQ_DB_PAGE = "https://stooq.com/db/h/"
DOWNLOAD_URL_PATTERN = "https://stooq.com/db/d/?b={category}"

# Categories to download: (category_id, description, approx_size)
DEFAULT_CATEGORIES: list[tuple[str, str, str]] = [
    ("d_macro_txt", "Macro indicators", "~0.9MB"),
    ("d_world_txt", "World indices, bonds, commodities, forex", "~50MB"),
]

# Stooq uses "rate limit" detection; these delays keep it polite
CLICK_DELAY_S = 2.0
DOWNLOAD_WAIT_S = 120  # seconds to wait for large zip to download
CAPTCHA_CHECK_SELECTOR = "form#captcha, div.g-recaptcha, iframe[src*='recaptcha']"

DEFAULT_DOWNLOAD_DIR = os.environ.get("STOOQ_DOWNLOAD_DIR", "/tmp/stooq_downloads")
DEFAULT_COOKIE_FILE = os.environ.get("STOOQ_COOKIE_FILE", "/tmp/stooq_cookies.json")


# ---------------------------------------------------------------------------
# Cookie helpers
# ---------------------------------------------------------------------------

def load_cookies(path: Path) -> list[dict]:
    """Load cookies from a JSON file. Returns empty list if file doesn't exist."""
    if not path.exists():
        log.info("Cookie file not found at %s — will start a fresh session", path)
        return []
    try:
        cookies = json.loads(path.read_text())
        log.info("Loaded %d cookies from %s", len(cookies), path)
        return cookies
    except Exception as exc:
        log.warning("Failed to load cookies from %s: %s — starting fresh", path, exc)
        return []


def save_cookies(context: object, path: Path) -> None:  # type: ignore[no-untyped-def]
    """Save current browser cookies to JSON file."""
    import asyncio

    async def _save():
        cookies = await context.cookies()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(cookies, indent=2))
        log.info("Saved %d cookies to %s", len(cookies), path)

    asyncio.get_event_loop().run_until_complete(_save())


# ---------------------------------------------------------------------------
# CAPTCHA detection
# ---------------------------------------------------------------------------

def is_captcha_present(page) -> bool:
    """Check whether the current page shows a CAPTCHA."""
    try:
        # Check for recaptcha iframe or common CAPTCHA markers in page title/content
        title = page.title()
        if "captcha" in title.lower() or "robot" in title.lower():
            return True
        # Check for recaptcha frame or Cloudflare challenge
        frames = page.frames
        for frame in frames:
            if "recaptcha" in frame.url or "challenge" in frame.url:
                return True
        # Try element selector (non-blocking)
        element = page.query_selector(CAPTCHA_CHECK_SELECTOR)
        return element is not None
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Core download logic (sync Playwright)
# ---------------------------------------------------------------------------

def download_categories(
    categories: list[str],
    download_dir: Path,
    cookie_file: Path,
) -> dict[str, Path]:
    """Download stooq zip files for the given category IDs.

    Returns mapping of category_id → downloaded file path.
    Raises SystemExit if CAPTCHA detected (manual intervention needed).
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

    download_dir.mkdir(parents=True, exist_ok=True)
    today = date.today().isoformat()
    downloaded: dict[str, Path] = {}

    cookies = load_cookies(cookie_file)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            accept_downloads=True,
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
        )

        # Restore cookies if available
        if cookies:
            context.add_cookies(cookies)

        page = context.new_page()

        log.info("Navigating to %s ...", STOOQ_DB_PAGE)
        try:
            page.goto(STOOQ_DB_PAGE, wait_until="domcontentloaded", timeout=30_000)
        except PlaywrightTimeoutError:
            log.error("Timed out loading %s", STOOQ_DB_PAGE)
            browser.close()
            sys.exit(1)

        # CAPTCHA check
        if is_captcha_present(page):
            log.warning(
                "CAPTCHA detected on stooq.com — cookies may be expired. "
                "Manual intervention required: open %s in a browser, solve CAPTCHA, "
                "export cookies to %s, then retry.",
                STOOQ_DB_PAGE,
                cookie_file,
            )
            browser.close()
            sys.exit(2)

        log.info("No CAPTCHA detected — proceeding with downloads")

        for category_id in categories:
            download_url = DOWNLOAD_URL_PATTERN.format(category=category_id)
            output_path = download_dir / f"{today}_{category_id}.zip"

            if output_path.exists():
                log.info(
                    "File already exists for today: %s — skipping download", output_path
                )
                downloaded[category_id] = output_path
                continue

            log.info("Downloading %s -> %s ...", category_id, output_path)
            try:
                with page.expect_download(timeout=DOWNLOAD_WAIT_S * 1000) as download_info:
                    # Navigate directly to the download URL; stooq serves zip directly
                    page.goto(download_url, wait_until="commit", timeout=15_000)
                    # If navigation triggers a download prompt instead of page nav,
                    # the expect_download context captures it.

                download = download_info.value
                failure = download.failure()
                if failure:
                    log.error("Download failed for %s: %s", category_id, failure)
                    continue

                download.save_as(str(output_path))
                size_mb = output_path.stat().st_size / (1024 * 1024)
                log.info(
                    "Downloaded %s -> %s (%.1f MB)", category_id, output_path, size_mb
                )
                downloaded[category_id] = output_path

            except PlaywrightTimeoutError:
                log.error(
                    "Timeout waiting for download of %s after %ds",
                    category_id,
                    DOWNLOAD_WAIT_S,
                )
                continue
            except Exception as exc:
                log.error("Unexpected error downloading %s: %s", category_id, exc)
                continue

            # Polite delay between downloads
            if category_id != categories[-1]:
                log.debug("Waiting %.1fs before next download ...", CLICK_DELAY_S)
                time.sleep(CLICK_DELAY_S)

        # Persist cookies after successful session
        if downloaded:
            cookies_out = context.cookies()
            cookie_file.parent.mkdir(parents=True, exist_ok=True)
            cookie_file.write_text(json.dumps(cookies_out, indent=2))
            log.info("Saved %d session cookies to %s", len(cookies_out), cookie_file)

        browser.close()

    return downloaded


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download stooq bulk data zip files via Playwright"
    )
    parser.add_argument(
        "--download-dir",
        default=DEFAULT_DOWNLOAD_DIR,
        help=f"Directory to save downloaded zips (default: {DEFAULT_DOWNLOAD_DIR})",
    )
    parser.add_argument(
        "--cookie-file",
        default=DEFAULT_COOKIE_FILE,
        help=f"Path to cookie persistence file (default: {DEFAULT_COOKIE_FILE})",
    )
    parser.add_argument(
        "--categories",
        default=",".join(c[0] for c in DEFAULT_CATEGORIES),
        help=(
            "Comma-separated list of stooq category IDs to download. "
            f"Default: {','.join(c[0] for c in DEFAULT_CATEGORIES)}"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print download URLs without actually downloading",
    )
    args = parser.parse_args()

    download_dir = Path(args.download_dir)
    cookie_file = Path(args.cookie_file)
    categories = [c.strip() for c in args.categories.split(",") if c.strip()]

    if not categories:
        log.error("No categories specified")
        sys.exit(1)

    if args.dry_run:
        log.info("Dry run — would download these categories:")
        for cat in categories:
            url = DOWNLOAD_URL_PATTERN.format(category=cat)
            today = date.today().isoformat()
            out = download_dir / f"{today}_{cat}.zip"
            log.info("  %s -> %s (from %s)", cat, out, url)
        return

    log.info(
        "Starting stooq download: %d categories -> %s", len(categories), download_dir
    )
    downloaded = download_categories(categories, download_dir, cookie_file)

    if not downloaded:
        log.error("No files were downloaded successfully")
        sys.exit(1)

    log.info(
        "Download complete: %d/%d categories downloaded",
        len(downloaded),
        len(categories),
    )
    for cat_id, path in downloaded.items():
        log.info("  %s -> %s", cat_id, path)


if __name__ == "__main__":
    main()
