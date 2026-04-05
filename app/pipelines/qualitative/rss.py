"""RSS Poller — fetches market news cleanly and avoids duplication."""

import hashlib
from datetime import datetime, date
import httpx
import feedparser
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.logging import get_logger
from app.models.qualitative import DeQualDocuments, DeQualSources
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)


class RssPollingPipeline(BasePipeline):
    pipeline_name = "qualitative_rss"
    requires_trading_day = False # Runs continuously 24/7

    async def execute(self, business_date: date, session: AsyncSession, run_log: Any) -> ExecutionResult:
        # Load active RSS sources
        res = await session.execute(
            select(DeQualSources.id, DeQualSources.source_url)
            .where(DeQualSources.is_active == True, DeQualSources.source_type == 'rss')
        )
        sources = res.all()
        
        if not sources:
            logger.warning("No active RSS sources found in de_qual_sources.")
            return ExecutionResult(0, 0)

        docs_inserted = 0
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            for source_id, url in sources:
                try:
                    resp = await client.get(url)
                    resp.raise_for_status()
                    
                    feed = feedparser.parse(resp.text)
                    for entry in feed.entries:
                        title = entry.get('title', '')
                        link = entry.get('link', '')
                        published = entry.get('published', '')
                        summary = entry.get('summary', '')
                        
                        # Generate unique exact-match hash
                        content_str = f"{title}|{summary}"
                        content_hash = hashlib.sha256(content_str.encode('utf-8')).hexdigest()
                        
                        # Ensure deduplication using raw POSIX lock or query exist check
                        # For v1.8, basic select where check
                        exist_chk = await session.execute(
                            select(DeQualDocuments.id)
                            .where(DeQualDocuments.content_hash == content_hash)
                        )
                        if exist_chk.scalar_one_or_none():
                            continue
                            
                        # Insert document
                        raw_text = f"{title}\n\n{summary}"
                        new_doc = DeQualDocuments(
                            source_id=source_id,
                            document_type="text",
                            title=title,
                            original_url=link,
                            raw_text=raw_text,
                            content_hash=content_hash,
                            processing_status="pending"
                        )
                        session.add(new_doc)
                        docs_inserted += 1
                        
                except Exception as e:
                    logger.error(f"Failed to process RSS feed {url}: {e}")
                    
            await session.commit()
            
        logger.info(f"RSS polling complete. Ingested {docs_inserted} new documents.")
        return ExecutionResult(docs_inserted, 0)
