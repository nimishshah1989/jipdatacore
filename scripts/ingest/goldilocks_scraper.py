"""Goldilocks Research scraper — requests.Session() based, standalone psycopg2.

Authenticates against goldilocksresearch.com (PHP session auth), scrapes all
research pages, downloads PDFs/audio, and stores content in de_qual_documents
plus Goldilocks-specific structured tables.

Usage:
    python scripts/ingest/goldilocks_scraper.py --mode daily
    python scripts/ingest/goldilocks_scraper.py --mode historical
    python scripts/ingest/goldilocks_scraper.py --mode daily --dry-run
    python scripts/ingest/goldilocks_scraper.py --mode historical --data-dir /tmp/goldilocks

Modes:
    daily       — scrape items from last 2 days only
    historical  — backfill, max 20 items per run to stay under rate limit

Flags:
    --dry-run   — list what would be downloaded/inserted; no writes to disk or DB
    --data-dir  — override default /home/ubuntu/jip-data-engine/data/goldilocks
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
from urllib.parse import urljoin

# ---------------------------------------------------------------------------
# Bootstrap: load .env from repo root so DATABASE_URL_SYNC is available
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
# Third-party imports (available on EC2 host)
# ---------------------------------------------------------------------------
try:
    import psycopg2
    import psycopg2.extras
except ImportError as exc:  # pragma: no cover
    print(f"[ERROR] psycopg2 not available: {exc}", flush=True)
    sys.exit(1)

try:
    import requests
    from requests import Session
except ImportError as exc:  # pragma: no cover
    print(f"[ERROR] requests not available: {exc}", flush=True)
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError as exc:  # pragma: no cover
    print(f"[ERROR] beautifulsoup4 not available: {exc}", flush=True)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://www.goldilocksresearch.com"
LOGIN_URL = f"{BASE_URL}/cus_signin.php"

PAGES = [
    {
        "url": f"{BASE_URL}/cus_dashboard.php",
        "name": "india_reports",
        "description": "India reports (Trend Friend, Stock Bullet, Fortnightly, Sector Trends, Big Picture)",
    },
    {
        "url": f"{BASE_URL}/market_snippets.php",
        "name": "market_snippets",
        "description": "Intraday market snippets",
    },
    {
        "url": f"{BASE_URL}/q_a_gautam.php",
        "name": "qa_gautam",
        "description": "Q&A content with Gautam",
    },
    {
        "url": f"{BASE_URL}/monthly_con_call.php",
        "name": "monthly_concall",
        "description": "Monthly concall recordings",
    },
    {
        "url": f"{BASE_URL}/video_update.php",
        "name": "video_updates",
        "description": "Video updates",
    },
    {
        "url": f"{BASE_URL}/sound_byte.php",
        "name": "sound_bytes",
        "description": "Audio sound bytes",
    },
    {
        "url": f"{BASE_URL}/cus_dashboard_us.php",
        "name": "usa_reports",
        "description": "USA reports",
    },
]

REAL_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

REQUEST_DELAY_S = 3  # seconds between page fetches (anti-flagging)
HISTORICAL_BATCH_LIMIT = 20  # max items per historical run


# ---------------------------------------------------------------------------
# Timestamp helper
# ---------------------------------------------------------------------------
def ts() -> str:
    """Return current IST timestamp string for print logs."""
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_db_conn() -> "psycopg2.connection":
    """Return a psycopg2 connection from DATABASE_URL_SYNC."""
    url = os.environ.get("DATABASE_URL_SYNC") or os.environ.get("DATABASE_URL", "")
    # Normalise: strip async driver prefix
    if url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
    elif url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql+psycopg2://", "postgresql://", 1)
    if not url:
        raise RuntimeError("DATABASE_URL_SYNC / DATABASE_URL not set in environment")
    return psycopg2.connect(url)


def ensure_goldilocks_tables(cur: "psycopg2.cursor") -> None:
    """CREATE TABLE IF NOT EXISTS for Goldilocks-specific structured tables."""

    cur.execute("""
        CREATE TABLE IF NOT EXISTS de_goldilocks_market_view (
            report_date             DATE        PRIMARY KEY,
            nifty_close             NUMERIC(18,4),
            nifty_support_1         NUMERIC(18,4),
            nifty_support_2         NUMERIC(18,4),
            nifty_resistance_1      NUMERIC(18,4),
            nifty_resistance_2      NUMERIC(18,4),
            bank_nifty_close        NUMERIC(18,4),
            bank_nifty_support_1    NUMERIC(18,4),
            bank_nifty_support_2    NUMERIC(18,4),
            bank_nifty_resistance_1 NUMERIC(18,4),
            bank_nifty_resistance_2 NUMERIC(18,4),
            trend_direction         VARCHAR(20),
            trend_strength          INTEGER CHECK (trend_strength BETWEEN 1 AND 5),
            headline                TEXT,
            overall_view            TEXT,
            created_at              TIMESTAMPTZ DEFAULT NOW(),
            updated_at              TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS de_goldilocks_sector_view (
            report_date DATE        NOT NULL,
            sector      VARCHAR(100) NOT NULL,
            trend       VARCHAR(20),
            outlook     TEXT,
            rank        INTEGER,
            created_at  TIMESTAMPTZ DEFAULT NOW(),
            updated_at  TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (report_date, sector)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS de_goldilocks_stock_ideas (
            id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
            published_date  DATE,
            symbol          VARCHAR(20),
            company_name    VARCHAR(200),
            idea_type       VARCHAR(50),
            entry_price     NUMERIC(18,4),
            target_price    NUMERIC(18,4),
            stop_loss       NUMERIC(18,4),
            timeframe       VARCHAR(50),
            rationale       TEXT,
            status          VARCHAR(20) DEFAULT 'active',
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        )
    """)


def ensure_qual_source(cur: "psycopg2.cursor") -> int:
    """Upsert de_qual_sources row for Goldilocks Research. Returns source id."""
    cur.execute("""
        INSERT INTO de_qual_sources (source_name, source_type, feed_url, is_active)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (source_name) DO UPDATE
            SET feed_url = EXCLUDED.feed_url, is_active = EXCLUDED.is_active,
                updated_at = NOW()
        RETURNING id
    """, ("Goldilocks Research", "report", BASE_URL, True))
    row = cur.fetchone()
    return row[0]


# ---------------------------------------------------------------------------
# Content hashing
# ---------------------------------------------------------------------------
def compute_content_hash(text: str) -> str:
    """SHA-256 hex digest of the given text."""
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()


def is_duplicate(cur: "psycopg2.cursor", source_id: int, content_hash: str) -> bool:
    """Return True if (source_id, content_hash) already exists in de_qual_documents."""
    cur.execute(
        "SELECT 1 FROM de_qual_documents WHERE source_id = %s AND content_hash = %s LIMIT 1",
        (source_id, content_hash),
    )
    return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# HTTP session authentication
# ---------------------------------------------------------------------------
def build_session(email: str, password: str) -> Session:
    """Create a requests.Session(), set browser User-Agent, and log in."""
    sess = requests.Session()
    sess.headers.update({"User-Agent": REAL_UA})

    _log(f"Logging in to {LOGIN_URL} as {email}")
    resp = sess.post(
        LOGIN_URL,
        data={"Email": email, "Password": password},
        timeout=30,
        allow_redirects=True,
    )
    resp.raise_for_status()

    # Heuristic: check we are not sitting on the login page (auth failed)
    if "cus_signin" in resp.url and "dashboard" not in resp.url:
        lower_body = resp.text.lower()
        if "invalid" in lower_body or "incorrect" in lower_body or "error" in lower_body:
            raise RuntimeError(
                f"Goldilocks login failed — check credentials. "
                f"Response URL: {resp.url}"
            )

    _log(f"Login response URL: {resp.url} (status {resp.status_code})")
    return sess


# ---------------------------------------------------------------------------
# Page fetching + parsing
# ---------------------------------------------------------------------------
def fetch_page(sess: Session, url: str) -> Optional[BeautifulSoup]:
    """Fetch a page and return its BeautifulSoup tree, or None on error."""
    try:
        resp = sess.get(url, timeout=30)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as exc:
        _log(f"[WARN] Failed to fetch {url}: {exc}")
        return None


def parse_date_text(text: str) -> Optional[datetime]:
    """Attempt to parse a date string in several common formats. Returns UTC datetime."""
    text = text.strip()
    formats = [
        "%d %B %Y",      # 15 March 2024
        "%d-%b-%Y",      # 15-Mar-2024
        "%d/%m/%Y",      # 15/03/2024
        "%Y-%m-%d",      # 2024-03-15
        "%B %d, %Y",     # March 15, 2024
        "%d %b %Y",      # 15 Mar 2024
        "%d-%m-%Y",      # 15-03-2024
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def extract_items_from_soup(soup: BeautifulSoup, page_url: str) -> list[dict]:
    """
    Generic extractor: walks the BeautifulSoup tree looking for
    report-like blocks (rows, cards, list items) and extracts:
        title, date_text, body_html, body_text, pdf_links, audio_links,
        video_links, item_url
    Returns a list of item dicts.
    """
    items = []

    # Candidate container selectors — cover typical PHP portal table/div layouts
    containers = (
        soup.find_all("tr")
        or soup.find_all("div", class_=lambda c: c and any(
            k in c for k in ("row", "item", "report", "card", "entry", "post")
        ))
        or soup.find_all("li")
    )

    for container in containers:
        # Skip tiny structural rows (headers, separators)
        text_content = container.get_text(separator=" ", strip=True)
        if len(text_content) < 10:
            continue

        # ---- Title -----------------------------------------------------------
        title = None
        for tag in ("h1", "h2", "h3", "h4", "strong", "b"):
            el = container.find(tag)
            if el:
                title = el.get_text(strip=True)
                break
        if not title:
            # fall back: first anchor text
            a = container.find("a")
            if a:
                title = a.get_text(strip=True)

        # ---- Date ------------------------------------------------------------
        date_text = None
        for tag in ("time", "span", "td", "div", "p"):
            for el in container.find_all(tag):
                t = el.get_text(strip=True)
                # crude date heuristic: contains 4-digit year
                if any(str(y) in t for y in range(2018, 2030)) and len(t) < 40:
                    date_text = t
                    break
            if date_text:
                break

        # ---- PDF links -------------------------------------------------------
        pdf_links = []
        for a in container.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf") or "pdf" in href.lower():
                pdf_links.append(urljoin(page_url, href))

        # ---- Audio links -----------------------------------------------------
        audio_links = []
        for a in container.find_all("a", href=True):
            href = a["href"]
            if any(href.lower().endswith(ext) for ext in (".mp3", ".wav", ".ogg", ".m4a")):
                audio_links.append(urljoin(page_url, href))
        # also <audio> tags
        for audio_tag in container.find_all("audio"):
            src = audio_tag.get("src")
            if src:
                audio_links.append(urljoin(page_url, src))
            for source in audio_tag.find_all("source", src=True):
                audio_links.append(urljoin(page_url, source["src"]))

        # ---- Video links (URL only, never download) --------------------------
        video_links = []
        for a in container.find_all("a", href=True):
            href = a["href"]
            if any(
                domain in href
                for domain in ("youtube.com", "youtu.be", "vimeo.com")
            ) or any(href.lower().endswith(ext) for ext in (".mp4", ".webm")):
                video_links.append(href)
        for iframe in container.find_all("iframe", src=True):
            video_links.append(iframe["src"])

        # ---- Item URL --------------------------------------------------------
        item_url = None
        a = container.find("a", href=True)
        if a:
            item_url = urljoin(page_url, a["href"])

        # ---- Body text -------------------------------------------------------
        body_text = text_content
        body_html = str(container)

        items.append({
            "title": title or "(no title)",
            "date_text": date_text,
            "body_text": body_text,
            "body_html": body_html,
            "pdf_links": pdf_links,
            "audio_links": audio_links,
            "video_links": video_links,
            "item_url": item_url or page_url,
        })

    return items


# ---------------------------------------------------------------------------
# File downloading
# ---------------------------------------------------------------------------
def download_file(sess: Session, url: str, dest_path: Path, dry_run: bool) -> bool:
    """
    Download a binary file to dest_path.
    Returns True on success (or dry-run simulated success), False on error.
    """
    if dry_run:
        _log(f"  [DRY-RUN] Would download: {url} → {dest_path}")
        return True
    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        resp = sess.get(url, timeout=60, stream=True)
        resp.raise_for_status()
        with open(dest_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=65536):
                fh.write(chunk)
        _log(f"  Downloaded: {dest_path.name} ({dest_path.stat().st_size:,} bytes)")
        return True
    except Exception as exc:
        _log(f"  [WARN] Download failed {url}: {exc}")
        return False


# ---------------------------------------------------------------------------
# Audio transcription hook (placeholder)
# ---------------------------------------------------------------------------
def transcribe_audio(path: Path) -> str:
    """
    Transcribe an audio file and return text.

    TODO: Implement with OpenAI Whisper API or local whisper model:
        import openai
        client = openai.OpenAI()
        with open(path, "rb") as f:
            result = client.audio.transcriptions.create(model="whisper-1", file=f)
        return result.text
    """
    return ""


# ---------------------------------------------------------------------------
# DB insert helpers
# ---------------------------------------------------------------------------
def insert_qual_document(
    cur: "psycopg2.cursor",
    source_id: int,
    content_hash: str,
    source_url: str,
    title: str,
    raw_text: str,
    original_format: str,
    published_at: Optional[datetime],
    audio_url: Optional[str] = None,
    dry_run: bool = False,
) -> Optional[str]:
    """
    Insert a row into de_qual_documents.
    Returns the UUID of the inserted row, or None if skipped/dry-run.
    """
    if dry_run:
        _log(f"  [DRY-RUN] Would insert doc: {title[:80]!r} format={original_format}")
        return None

    doc_id = str(uuid.uuid4())
    cur.execute("""
        INSERT INTO de_qual_documents (
            id, source_id, content_hash, source_url, published_at,
            title, original_format, raw_text, audio_url, processing_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_id, content_hash) DO NOTHING
        RETURNING id
    """, (
        doc_id,
        source_id,
        content_hash,
        source_url[:2000] if source_url else None,
        published_at,
        title[:500] if title else None,
        original_format,
        raw_text or None,
        audio_url or None,
        "pending",
    ))
    result = cur.fetchone()
    return result[0] if result else None


# ---------------------------------------------------------------------------
# Per-page scrape + ingest logic
# ---------------------------------------------------------------------------
def scrape_and_ingest_page(
    sess: Session,
    page: dict,
    cur: "psycopg2.cursor",
    source_id: int,
    data_dir: Path,
    mode: str,
    dry_run: bool,
    ingested_count: list[int],
) -> int:
    """
    Fetch one portal page, extract all items, download attachments,
    insert into de_qual_documents.

    Returns the number of new items ingested.
    """
    page_url = page["url"]
    page_name = page["name"]
    _log(f"Scraping page: {page_name} ({page_url})")

    soup = fetch_page(sess, page_url)
    if soup is None:
        _log(f"  [SKIP] Could not fetch {page_name}")
        return 0

    items = extract_items_from_soup(soup, page_url)
    _log(f"  Found {len(items)} candidate items on {page_name}")

    new_this_page = 0
    limit = HISTORICAL_BATCH_LIMIT if mode == "historical" else None

    for item in items:
        # Enforce historical batch limit across all pages
        if limit is not None and ingested_count[0] >= limit:
            _log(f"  [STOP] Historical batch limit ({limit}) reached")
            break

        title = item["title"]
        body_text = item["body_text"]
        date_text = item["date_text"]
        pdf_links = item["pdf_links"]
        audio_links = item["audio_links"]
        video_links = item["video_links"]
        item_url = item["item_url"]

        # Determine published_at
        published_at: Optional[datetime] = None
        if date_text:
            published_at = parse_date_text(date_text)

        # Daily mode: skip items older than 2 days
        if mode == "daily" and published_at is not None:
            age_days = (datetime.now(tz=timezone.utc) - published_at).days
            if age_days > 2:
                continue

        # ---- Handle PDF attachments ----------------------------------------
        for pdf_url in pdf_links:
            filename = pdf_url.split("/")[-1].split("?")[0] or "report.pdf"
            dest = data_dir / "pdfs" / filename
            content_hash = compute_content_hash(pdf_url)

            if is_duplicate(cur, source_id, content_hash):
                continue

            download_file(sess, pdf_url, dest, dry_run)
            raw_text_val = body_text if not pdf_links else f"[PDF: {filename}]\n{body_text}"

            doc_id = insert_qual_document(
                cur=cur,
                source_id=source_id,
                content_hash=content_hash,
                source_url=pdf_url,
                title=title,
                raw_text=raw_text_val,
                original_format="pdf",
                published_at=published_at,
                dry_run=dry_run,
            )
            if doc_id or dry_run:
                new_this_page += 1
                ingested_count[0] += 1
                _log(f"  + PDF: {title[:60]!r} | {filename}")

            time.sleep(REQUEST_DELAY_S)

        # ---- Handle audio attachments --------------------------------------
        for audio_url in audio_links:
            filename = audio_url.split("/")[-1].split("?")[0] or "audio.mp3"
            dest = data_dir / "audio" / filename
            content_hash = compute_content_hash(audio_url)

            if is_duplicate(cur, source_id, content_hash):
                continue

            downloaded = download_file(sess, audio_url, dest, dry_run)
            transcript = ""
            if downloaded and not dry_run and dest.exists():
                transcript = transcribe_audio(dest)

            doc_id = insert_qual_document(
                cur=cur,
                source_id=source_id,
                content_hash=content_hash,
                source_url=audio_url,
                title=title,
                raw_text=transcript or body_text,
                original_format="audio",
                published_at=published_at,
                audio_url=audio_url,
                dry_run=dry_run,
            )
            if doc_id or dry_run:
                new_this_page += 1
                ingested_count[0] += 1
                _log(f"  + Audio: {title[:60]!r} | {filename}")

            time.sleep(REQUEST_DELAY_S)

        # ---- Handle video links (store URL only, no download) ---------------
        for video_url in video_links:
            content_hash = compute_content_hash(video_url)

            if is_duplicate(cur, source_id, content_hash):
                continue

            doc_id = insert_qual_document(
                cur=cur,
                source_id=source_id,
                content_hash=content_hash,
                source_url=video_url,
                title=title,
                raw_text=body_text,
                original_format="video",
                published_at=published_at,
                dry_run=dry_run,
            )
            if doc_id or dry_run:
                new_this_page += 1
                ingested_count[0] += 1
                _log(f"  + Video: {title[:60]!r}")

        # ---- HTML fallback: no attachments, store page body ----------------
        if not pdf_links and not audio_links and not video_links:
            content_hash = compute_content_hash(f"{item_url}:{body_text[:500]}")

            if is_duplicate(cur, source_id, content_hash):
                continue

            doc_id = insert_qual_document(
                cur=cur,
                source_id=source_id,
                content_hash=content_hash,
                source_url=item_url,
                title=title,
                raw_text=body_text,
                original_format="html",
                published_at=published_at,
                dry_run=dry_run,
            )
            if doc_id or dry_run:
                new_this_page += 1
                ingested_count[0] += 1
                _log(f"  + HTML: {title[:60]!r}")

        time.sleep(REQUEST_DELAY_S)

    return new_this_page


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Goldilocks Research scraper — requests.Session() approach"
    )
    parser.add_argument(
        "--mode",
        choices=["daily", "historical"],
        default="daily",
        help="daily = last 2 days; historical = backfill up to 20 items/run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be downloaded/inserted without writing anything",
    )
    parser.add_argument(
        "--data-dir",
        default="/home/ubuntu/jip-data-engine/data/goldilocks",
        help="Root directory for downloaded files (pdfs/, audio/ subdirs)",
    )
    args = parser.parse_args()

    dry_run: bool = args.dry_run
    mode: str = args.mode
    data_dir = Path(args.data_dir)

    _log(f"=== Goldilocks Scraper starting | mode={mode} dry_run={dry_run} ===")
    _log(f"Data dir: {data_dir}")

    # Credentials from environment
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
        # Ensure Goldilocks-specific tables exist
        ensure_goldilocks_tables(cur)
        conn.commit()
        _log("Goldilocks tables ensured")

        # Ensure qualitative source row
        source_id = ensure_qual_source(cur)
        conn.commit()
        _log(f"Qual source id: {source_id}")

        # Authenticate
        sess = build_session(email, password)
        time.sleep(REQUEST_DELAY_S)

        # Shared counter for historical batch limit (mutable via list)
        ingested_count = [0]

        total_new = 0
        for page in PAGES:
            new_items = scrape_and_ingest_page(
                sess=sess,
                page=page,
                cur=cur,
                source_id=source_id,
                data_dir=data_dir,
                mode=mode,
                dry_run=dry_run,
                ingested_count=ingested_count,
            )
            total_new += new_items

            if not dry_run:
                conn.commit()

            # Historical: stop once batch limit hit
            if mode == "historical" and ingested_count[0] >= HISTORICAL_BATCH_LIMIT:
                _log(f"Historical batch limit ({HISTORICAL_BATCH_LIMIT}) reached; stopping")
                break

            time.sleep(REQUEST_DELAY_S)

        _log(f"=== Done | total new items: {total_new} ===")

    except Exception as exc:
        conn.rollback()
        _log(f"[ERROR] Unhandled exception: {exc}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
