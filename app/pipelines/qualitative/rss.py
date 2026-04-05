"""RSS feed polling pipeline.

Polls configured feeds every 30 minutes. SHA-256 content_hash dedup,
per-document advisory lock to prevent concurrent insertion.

Feeds: RBI, SEBI, Economic Times, Business Standard, Fed, Mint.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any, Optional

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.models.qualitative import DeQualDocuments, DeQualSources
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.qualitative.deduplication import (
    acquire_document_advisory_lock,
    compute_content_hash,
    is_exact_duplicate,
    release_document_advisory_lock,
)

logger = get_logger(__name__)

# RSS feed definitions: (source_name, feed_url, source_type)
RSS_FEEDS: list[tuple[str, str, str]] = [
    ("RBI", "https://www.rbi.org.in/rss/RBINotifications.xml", "article"),
    ("SEBI", "https://www.sebi.gov.in/sebi_data/rss/sebipress.xml", "article"),
    ("Economic Times Markets", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms", "article"),
    ("Business Standard Markets", "https://www.business-standard.com/rss/markets-106.rss", "article"),
    ("Federal Reserve", "https://www.federalreserve.gov/feeds/press_all.xml", "article"),
    ("Mint Markets", "https://www.livemint.com/rss/markets", "article"),
]

_FETCH_TIMEOUT = 30  # seconds
_MAX_ENTRIES_PER_FEED = 50


class RssPipeline(BasePipeline):
    """Poll RSS feeds and ingest new documents.

    Designed to run every 30 minutes. Uses SHA-256 dedup and advisory locks
    to prevent duplicate ingestion under concurrent runs.
    """

    pipeline_name = "qualitative_rss"
    requires_trading_day = False  # RSS should run on all days

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Poll all configured RSS feeds and ingest new articles."""
        rows_processed = 0
        rows_failed = 0

        for source_name, feed_url, source_type in RSS_FEEDS:
            try:
                source_id = await self._ensure_source(session, source_name, source_type, feed_url)
                entries = await self._fetch_feed(feed_url)

                for entry in entries[:_MAX_ENTRIES_PER_FEED]:
                    try:
                        inserted = await self._process_entry(
                            session=session,
                            source_id=source_id,
                            entry=entry,
                        )
                        if inserted:
                            rows_processed += 1
                    except Exception as exc:
                        logger.warning(
                            "rss_entry_failed",
                            source=source_name,
                            error=str(exc),
                        )
                        rows_failed += 1

            except Exception as exc:
                logger.error(
                    "rss_feed_failed",
                    source=source_name,
                    feed_url=feed_url,
                    error=str(exc),
                )
                rows_failed += 1

        logger.info(
            "rss_pipeline_complete",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            business_date=business_date.isoformat(),
        )
        return ExecutionResult(rows_processed=rows_processed, rows_failed=rows_failed)

    async def _ensure_source(
        self,
        session: AsyncSession,
        source_name: str,
        source_type: str,
        feed_url: str,
    ) -> int:
        """Upsert source record and return source_id."""
        stmt = pg_insert(DeQualSources).values(
            source_name=source_name,
            source_type=source_type,
            feed_url=feed_url,
            is_active=True,
        ).on_conflict_do_update(
            index_elements=["source_name"],
            set_={"feed_url": feed_url, "is_active": True},
        ).returning(DeQualSources.id)

        result = await session.execute(stmt)
        await session.flush()
        return result.scalar_one()

    async def _fetch_feed(self, feed_url: str) -> list[dict[str, Any]]:
        """Fetch and parse an RSS/Atom feed.

        Returns a list of entry dicts with keys: title, link, summary, published.
        """
        import httpx
        import xml.etree.ElementTree as ET

        async with httpx.AsyncClient(timeout=_FETCH_TIMEOUT, follow_redirects=True) as client:
            response = await client.get(feed_url)
            response.raise_for_status()

        root = ET.fromstring(response.text)
        entries: list[dict[str, Any]] = []

        # Handle RSS 2.0
        for item in root.findall(".//item"):
            entry = {
                "title": _xml_text(item, "title"),
                "link": _xml_text(item, "link"),
                "summary": _xml_text(item, "description"),
                "published": _xml_text(item, "pubDate"),
            }
            entries.append(entry)

        # Handle Atom
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for item in root.findall(".//atom:entry", ns):
            link_el = item.find("atom:link", ns)
            entry = {
                "title": _xml_text(item, "atom:title", ns),
                "link": link_el.get("href") if link_el is not None else None,
                "summary": _xml_text(item, "atom:summary", ns),
                "published": _xml_text(item, "atom:published", ns),
            }
            entries.append(entry)

        return entries

    async def _process_entry(
        self,
        session: AsyncSession,
        source_id: int,
        entry: dict[str, Any],
    ) -> bool:
        """Process a single RSS entry.

        Returns True if a new document was inserted, False if skipped.
        """
        title = entry.get("title") or ""
        summary = entry.get("summary") or ""
        link = entry.get("link")
        published_str = entry.get("published")

        # Compute content hash from title + summary + link
        content = f"{title}\n{summary}\n{link or ''}"
        content_hash = compute_content_hash(content)

        # Advisory lock per document
        lock_acquired = await acquire_document_advisory_lock(session, content_hash)
        if not lock_acquired:
            logger.debug("rss_entry_lock_contention", content_hash=content_hash[:16])
            return False

        try:
            # Exact dedup check
            if await is_exact_duplicate(session, source_id=source_id, content_hash=content_hash):
                logger.debug("rss_entry_exact_dup", content_hash=content_hash[:16])
                return False

            # Parse published timestamp
            published_at: Optional[datetime] = None
            if published_str:
                try:
                    from email.utils import parsedate_to_datetime

                    published_at = parsedate_to_datetime(published_str).replace(
                        tzinfo=timezone.utc
                    )
                except Exception:
                    pass

            # Insert document
            doc_id = uuid.uuid4()
            stmt = pg_insert(DeQualDocuments).values(
                id=doc_id,
                source_id=source_id,
                content_hash=content_hash,
                source_url=link,
                published_at=published_at,
                title=title[:500] if title else None,
                original_format="html",
                raw_text=summary or None,
                processing_status="pending",
            ).on_conflict_do_nothing(
                constraint="uq_qual_doc_source_hash",
            )

            await session.execute(stmt)
            await session.flush()

            logger.info(
                "rss_entry_inserted",
                document_id=str(doc_id),
                source_id=source_id,
                title=title[:80],
            )
            return True

        finally:
            await release_document_advisory_lock(session, content_hash)


def _xml_text(
    element: Any,
    tag: str,
    ns: Optional[dict[str, str]] = None,
) -> Optional[str]:
    """Extract text from an XML element's child tag."""
    child = element.find(tag, ns) if ns else element.find(tag)
    if child is None or child.text is None:
        return None
    return child.text.strip() or None
