"""Download Goldilocks con-call videos and sound bytes.

Authenticates against goldilocksresearch.com via Playwright, scrapes
monthly_con_call.php and sound_byte.php, downloads MP4/MP3 files,
and upserts records into de_qual_documents.

Usage:
    python3 scripts/ingest/download_goldilocks_media.py
    python3 scripts/ingest/download_goldilocks_media.py --dry-run
    python3 scripts/ingest/download_goldilocks_media.py --base-dir /tmp/goldilocks

EC2 prerequisites:
    pip3 install playwright requests beautifulsoup4 psycopg2-binary
    playwright install chromium

DB schema required (from C1):
    de_qual_documents.report_type VARCHAR(30)
    de_qual_documents.audio_duration_s INTEGER
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Bootstrap: load .env from repo root
# Pattern from goldilocks_scraper.py lines 38-51
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
# Third-party imports
# ---------------------------------------------------------------------------
try:
    import psycopg2
    import psycopg2.extras
except ImportError as exc:
    print(f"[ERROR] psycopg2 not available: {exc}", flush=True)
    sys.exit(1)

try:
    import requests
    from requests import Session
except ImportError as exc:
    print(f"[ERROR] requests not available: {exc}", flush=True)
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError as exc:
    print(f"[ERROR] beautifulsoup4 not available: {exc}", flush=True)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://www.goldilocksresearch.com"
LOGIN_URL = f"{BASE_URL}/cus_signin.php"

CONCALL_URL = f"{BASE_URL}/monthly_con_call.php"
SOUNDBYTE_URL = f"{BASE_URL}/sound_byte.php"

REAL_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

CHUNK_SIZE = 1024 * 1024  # 1 MB streaming chunks
REQUEST_DELAY_S = 3


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
def ts() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Database helpers (reused from goldilocks_scraper.py pattern)
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


def ensure_qual_source(cur) -> int:
    """Upsert de_qual_sources row for Goldilocks Research. Returns source id."""
    cur.execute("""
        INSERT INTO de_qual_sources (source_name, source_type, feed_url, is_active)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (source_name) DO UPDATE
            SET feed_url = EXCLUDED.feed_url,
                is_active = EXCLUDED.is_active,
                updated_at = NOW()
        RETURNING id
    """, ("Goldilocks Research", "report", BASE_URL, True))
    row = cur.fetchone()
    return row[0]


def compute_content_hash(text: str) -> str:
    """SHA-256 hex digest."""
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()


def upsert_media_document(
    cur,
    source_id: int,
    source_url: str,
    title: str,
    original_format: str,
    report_type: str,
    published_at: Optional[datetime],
    dry_run: bool,
) -> Optional[str]:
    """Upsert a media document into de_qual_documents.

    Uses ON CONFLICT (source_id, content_hash) DO UPDATE so re-runs are safe.
    content_hash = SHA-256 of source_url (stable natural key for media files).

    Returns doc UUID or None on dry-run / conflict with no change.
    """
    content_hash = compute_content_hash(source_url)

    if dry_run:
        _log(f"  [DRY-RUN] Would upsert: {title[:80]!r} format={original_format} type={report_type}")
        return None

    doc_id = str(uuid.uuid4())
    cur.execute("""
        INSERT INTO de_qual_documents (
            id, source_id, content_hash, source_url, published_at,
            title, original_format, report_type, processing_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_id, content_hash) DO UPDATE
            SET report_type = EXCLUDED.report_type,
                title = EXCLUDED.title,
                updated_at = NOW()
        RETURNING id
    """, (
        doc_id,
        source_id,
        content_hash,
        source_url[:2000],
        published_at,
        title[:500],
        original_format,
        report_type,
        "pending",
    ))
    row = cur.fetchone()
    return str(row[0]) if row else None


# ---------------------------------------------------------------------------
# Authentication (reuse Playwright pattern from goldilocks_scraper.py)
# ---------------------------------------------------------------------------
def build_session(email: str, password: str) -> Session:
    """Authenticate via Playwright and return a requests.Session with cookies."""
    import json as _json

    cookie_file = Path(os.environ.get("GOLDILOCKS_COOKIE_FILE", "/tmp/goldilocks_cookies.json"))

    sess = requests.Session()
    sess.headers.update({"User-Agent": REAL_UA})

    # Try saved cookies first
    if cookie_file.exists():
        _log("Trying saved cookies...")
        with open(cookie_file) as f:
            for c in _json.load(f):
                sess.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))
        test = sess.get(f"{BASE_URL}/cus_dashboard.php", timeout=15, allow_redirects=True)
        if "window.location.href" not in test.text[:200] and len(test.text) > 10000:
            _log("Saved cookies valid — reusing session")
            return sess
        _log("Saved cookies expired — re-authenticating via Playwright")
        sess.cookies.clear()

    _log(f"Logging in via Playwright as {email}")
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise RuntimeError(
            "playwright not installed. Run: pip3 install playwright && playwright install chromium"
        )

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=REAL_UA)
        page = context.new_page()

        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
        time.sleep(2)

        page.fill('input[name="Email"]', email)
        page.fill('input[name="Password"]', password)
        page.click('button[type="submit"]')
        time.sleep(5)

        page.goto(f"{BASE_URL}/cus_dashboard.php", wait_until="domcontentloaded", timeout=30000)
        time.sleep(3)
        content = page.content()

        if "window.location.href" in content[:200]:
            browser.close()
            raise RuntimeError("Goldilocks login failed — dashboard redirects. Check credentials.")

        _log("Playwright login success — transferring cookies")
        cookies = context.cookies()

        cookie_file.parent.mkdir(parents=True, exist_ok=True)
        with open(cookie_file, "w") as f:
            _json.dump(cookies, f)

        for c in cookies:
            sess.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))

        browser.close()

    _log(f"Session ready with {len(sess.cookies)} cookies")
    return sess


# ---------------------------------------------------------------------------
# Page fetch helpers
# ---------------------------------------------------------------------------
def fetch_page_html(sess: Session, url: str) -> Optional[str]:
    """Fetch page HTML. Returns None on error."""
    try:
        resp = sess.get(url, timeout=30)
        resp.raise_for_status()
        if "window.location.href" in resp.text[:200]:
            _log(f"[WARN] Session expired fetching {url}")
            return None
        return resp.text
    except Exception as exc:
        _log(f"[WARN] Failed to fetch {url}: {exc}")
        return None


def parse_date_from_bold(text: str) -> Optional[datetime]:
    """Parse YYYY-MM-DD date string. Returns UTC-aware datetime or None."""
    import re
    match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if match:
        try:
            dt = datetime.strptime(match.group(1), "%Y-%m-%d")
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# Media URL extraction
# ---------------------------------------------------------------------------
def extract_concall_entries(html: str) -> list[dict]:
    """Parse monthly_con_call.php HTML.

    Finds all <video> tags with <source src="data-temp/XXX.mp4"> and
    associates the nearest date from <p><b>YYYY-MM-DD</b></p>.

    Returns list of dicts: {url, filename, date, title}
    """
    soup = BeautifulSoup(html, "html.parser")
    entries: list[dict] = []

    # Walk all elements in document order to associate dates with videos
    current_date: Optional[datetime] = None
    current_date_str: str = ""

    for tag in soup.find_all(True):
        # Detect date headings: <p><b>2024-05-15</b></p> or similar
        if tag.name in ("p", "div", "h3", "h4"):
            bold = tag.find("b")
            if bold:
                date_text = bold.get_text(strip=True)
                parsed = parse_date_from_bold(date_text)
                if parsed:
                    current_date = parsed
                    current_date_str = date_text

        # Detect <video> or <source> tags
        if tag.name == "video":
            source_tag = tag.find("source")
            src = None
            if source_tag:
                src = source_tag.get("src", "")
            else:
                src = tag.get("src", "")

            if src and ".mp4" in src.lower():
                # Build full URL
                if src.startswith("http"):
                    full_url = src
                else:
                    full_url = f"{BASE_URL}/{src.lstrip('/')}"

                filename = src.split("/")[-1].split("?")[0]
                date_label = current_date_str or "unknown"
                title = f"Goldilocks Con-Call {date_label}"

                entries.append({
                    "url": full_url,
                    "filename": filename,
                    "date": current_date,
                    "title": title,
                })

    # Deduplicate by URL
    seen: set[str] = set()
    unique: list[dict] = []
    for e in entries:
        if e["url"] not in seen:
            seen.add(e["url"])
            unique.append(e)

    return unique


def extract_soundbyte_entries(html: str) -> list[dict]:
    """Parse sound_byte.php HTML.

    Finds all <audio> / <source src="data-temp/XXX.mp3"> tags.
    Deduplicates URLs via set().

    Returns list of dicts: {url, filename, date, title}
    """
    soup = BeautifulSoup(html, "html.parser")
    seen_urls: set[str] = set()
    entries: list[dict] = []

    current_date: Optional[datetime] = None
    current_date_str: str = ""

    for tag in soup.find_all(True):
        # Detect date headings
        if tag.name in ("p", "div", "h3", "h4"):
            bold = tag.find("b")
            if bold:
                date_text = bold.get_text(strip=True)
                parsed = parse_date_from_bold(date_text)
                if parsed:
                    current_date = parsed
                    current_date_str = date_text

        # Detect <audio> tags
        if tag.name == "audio":
            # Check direct src
            direct_src = tag.get("src", "")
            sources = [s.get("src", "") for s in tag.find_all("source")]
            all_srcs = ([direct_src] if direct_src else []) + sources

            for src in all_srcs:
                if not src or ".mp3" not in src.lower():
                    continue
                if src.startswith("http"):
                    full_url = src
                else:
                    full_url = f"{BASE_URL}/{src.lstrip('/')}"

                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                filename = src.split("/")[-1].split("?")[0]
                date_label = current_date_str or filename.replace(".mp3", "")
                title = f"Goldilocks Sound Byte {date_label}"

                entries.append({
                    "url": full_url,
                    "filename": filename,
                    "date": current_date,
                    "title": title,
                })

    return entries


# ---------------------------------------------------------------------------
# File download
# ---------------------------------------------------------------------------
def get_remote_size(sess: Session, url: str) -> Optional[int]:
    """Get Content-Length via HEAD request. Returns None if not available."""
    try:
        resp = sess.head(url, timeout=15, allow_redirects=True)
        cl = resp.headers.get("Content-Length")
        if cl:
            return int(cl)
    except Exception:
        pass
    return None


def should_skip_download(dest_path: Path, remote_size: Optional[int]) -> bool:
    """Return True if file already exists and size matches remote."""
    if not dest_path.exists():
        return False
    local_size = dest_path.stat().st_size
    if local_size == 0:
        return False
    if remote_size is not None and local_size != remote_size:
        return False
    # File exists with non-zero size and either size matches or we can't check
    return True


def stream_download(
    sess: Session,
    url: str,
    dest_path: Path,
    referer: str,
    dry_run: bool,
) -> bool:
    """Download a binary file to dest_path using streaming.

    Args:
        sess: Authenticated requests.Session.
        url: Full URL to download.
        dest_path: Local destination path.
        referer: Referer header (required by Goldilocks server).
        dry_run: If True, only log what would happen.

    Returns:
        True on success (or simulated success in dry-run), False on error.
    """
    if dry_run:
        _log(f"  [DRY-RUN] Would download: {url} -> {dest_path.name}")
        return True

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    # Check remote size and skip if already downloaded
    remote_size = get_remote_size(sess, url)
    if should_skip_download(dest_path, remote_size):
        _log(f"  [SKIP] Already downloaded: {dest_path.name} ({dest_path.stat().st_size:,} bytes)")
        return True

    headers = {"Referer": referer}
    try:
        resp = sess.get(url, headers=headers, stream=True, timeout=600)
        resp.raise_for_status()

        bytes_written = 0
        with open(dest_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    fh.write(chunk)
                    bytes_written += len(chunk)

        # Verify size if Content-Length was available
        if remote_size is not None and bytes_written != remote_size:
            _log(
                f"  [WARN] Size mismatch: expected {remote_size:,}, got {bytes_written:,} for {dest_path.name}"
            )

        _log(f"  Downloaded: {dest_path.name} ({bytes_written:,} bytes)")
        return True

    except Exception as exc:
        _log(f"  [WARN] Download failed {url}: {exc}")
        # Remove partial file
        if dest_path.exists() and dest_path.stat().st_size == 0:
            dest_path.unlink(missing_ok=True)
        return False


# ---------------------------------------------------------------------------
# Main download orchestration
# ---------------------------------------------------------------------------
def process_concalls(
    sess: Session,
    cur,
    source_id: int,
    video_dir: Path,
    dry_run: bool,
) -> dict:
    """Scrape and download monthly con-call videos."""
    _log(f"Fetching con-call page: {CONCALL_URL}")
    html = fetch_page_html(sess, CONCALL_URL)
    if not html:
        _log("[ERROR] Could not fetch con-call page")
        return {"found": 0, "downloaded": 0, "skipped": 0, "upserted": 0}

    entries = extract_concall_entries(html)
    _log(f"Found {len(entries)} con-call video(s)")

    stats = {"found": len(entries), "downloaded": 0, "skipped": 0, "upserted": 0}

    for entry in entries:
        _log(f"  Video: {entry['filename']} | {entry['title']}")
        dest = video_dir / entry["filename"]

        downloaded = stream_download(
            sess=sess,
            url=entry["url"],
            dest_path=dest,
            referer=CONCALL_URL,
            dry_run=dry_run,
        )

        if downloaded:
            if dest.exists() and dest.stat().st_size > 0:
                stats["downloaded"] += 1
            else:
                stats["skipped"] += 1

            doc_id = upsert_media_document(
                cur=cur,
                source_id=source_id,
                source_url=entry["url"],
                title=entry["title"],
                original_format="video",
                report_type="concall",
                published_at=entry["date"],
                dry_run=dry_run,
            )
            if doc_id or dry_run:
                stats["upserted"] += 1
                _log(f"  Upserted doc: {entry['title'][:60]!r}")

        time.sleep(REQUEST_DELAY_S)

    return stats


def process_soundbytes(
    sess: Session,
    cur,
    source_id: int,
    audio_dir: Path,
    dry_run: bool,
) -> dict:
    """Scrape and download sound byte MP3s."""
    _log(f"Fetching sound byte page: {SOUNDBYTE_URL}")
    html = fetch_page_html(sess, SOUNDBYTE_URL)
    if not html:
        _log("[ERROR] Could not fetch sound byte page")
        return {"found": 0, "downloaded": 0, "skipped": 0, "upserted": 0}

    entries = extract_soundbyte_entries(html)
    _log(f"Found {len(entries)} sound byte(s) (deduped)")

    stats = {"found": len(entries), "downloaded": 0, "skipped": 0, "upserted": 0}

    for entry in entries:
        _log(f"  Audio: {entry['filename']} | {entry['title']}")
        dest = audio_dir / entry["filename"]

        downloaded = stream_download(
            sess=sess,
            url=entry["url"],
            dest_path=dest,
            referer=SOUNDBYTE_URL,
            dry_run=dry_run,
        )

        if downloaded:
            if dest.exists() and dest.stat().st_size > 0:
                stats["downloaded"] += 1
            else:
                stats["skipped"] += 1

            doc_id = upsert_media_document(
                cur=cur,
                source_id=source_id,
                source_url=entry["url"],
                title=entry["title"],
                original_format="audio",
                report_type="sound_byte",
                published_at=entry["date"],
                dry_run=dry_run,
            )
            if doc_id or dry_run:
                stats["upserted"] += 1
                _log(f"  Upserted doc: {entry['title'][:60]!r}")

        time.sleep(REQUEST_DELAY_S)

    return stats


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download Goldilocks con-call videos and sound bytes"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be downloaded/inserted without writing anything",
    )
    parser.add_argument(
        "--base-dir",
        default="/home/ubuntu/jip-data-engine/data/goldilocks",
        help="Root directory for downloaded files",
    )
    args = parser.parse_args()

    dry_run: bool = args.dry_run
    base_dir = Path(args.base_dir)
    video_dir = base_dir / "video"
    audio_dir = base_dir / "audio"

    _log(f"=== Goldilocks Media Downloader | dry_run={dry_run} ===")
    _log(f"Video dir: {video_dir}")
    _log(f"Audio dir: {audio_dir}")

    if not dry_run:
        video_dir.mkdir(parents=True, exist_ok=True)
        audio_dir.mkdir(parents=True, exist_ok=True)

    # Credentials
    email = os.environ.get("GOLDILOCKS_EMAIL", "")
    password = os.environ.get("GOLDILOCKS_PASSWORD", "")
    if not email or not password:
        _log("[ERROR] GOLDILOCKS_EMAIL / GOLDILOCKS_PASSWORD not set in environment")
        sys.exit(1)

    # DB connection
    conn = get_db_conn()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        source_id = ensure_qual_source(cur)
        conn.commit()
        _log(f"Qual source id: {source_id}")

        # Authenticate
        sess = build_session(email, password)
        time.sleep(REQUEST_DELAY_S)

        # Download con-calls
        concall_stats = process_concalls(sess, cur, source_id, video_dir, dry_run)
        if not dry_run:
            conn.commit()

        # Download sound bytes
        soundbyte_stats = process_soundbytes(sess, cur, source_id, audio_dir, dry_run)
        if not dry_run:
            conn.commit()

        # Summary
        _log("=== Download Summary ===")
        _log(
            f"Con-calls: found={concall_stats['found']} "
            f"downloaded={concall_stats['downloaded']} "
            f"skipped={concall_stats['skipped']} "
            f"upserted={concall_stats['upserted']}"
        )
        _log(
            f"Sound bytes: found={soundbyte_stats['found']} "
            f"downloaded={soundbyte_stats['downloaded']} "
            f"skipped={soundbyte_stats['skipped']} "
            f"upserted={soundbyte_stats['upserted']}"
        )
        total = concall_stats["downloaded"] + soundbyte_stats["downloaded"]
        _log(f"Total files downloaded: {total}")

    except Exception as exc:
        conn.rollback()
        _log(f"[ERROR] Unhandled exception: {exc}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
