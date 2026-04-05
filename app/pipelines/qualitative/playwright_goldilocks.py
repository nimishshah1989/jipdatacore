"""Goldilocks Research scraper using Playwright.

Authenticates with Goldilocks credentials, scrapes research reports,
and ingests them as qualitative documents.
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

_GOLDILOCKS_BASE_URL = "https://www.goldilocksresearch.com"
_GOLDILOCKS_LOGIN_URL = f"{_GOLDILOCKS_BASE_URL}/login"
_GOLDILOCKS_REPORTS_URL = f"{_GOLDILOCKS_BASE_URL}/research"
_PLAYWRIGHT_TIMEOUT_MS = 30_000
_SOURCE_NAME = "Goldilocks Research"


class GoldilocksScrapingError(Exception):
    """Raised when Goldilocks scraping fails."""

    pass


class GoldilocksScraperPipeline(BasePipeline):
    """Scrape Goldilocks Research reports via Playwright.

    Authenticates via browser automation, collects report links,
    and ingests new research as qualitative documents.
    """

    pipeline_name = "qualitative_goldilocks"
    requires_trading_day = False

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Scrape and ingest Goldilocks Research reports."""
        from app.config import get_settings

        settings = get_settings()

        if not settings.goldilocks_email or not settings.goldilocks_password:
            logger.warning(
                "goldilocks_credentials_missing",
                pipeline=self.pipeline_name,
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        source_id = await self._ensure_source(session)
        rows_processed = 0
        rows_failed = 0

        try:
            reports = await self._scrape_reports(
                email=settings.goldilocks_email,
                password=settings.goldilocks_password,
            )

            for report in reports:
                try:
                    inserted = await self._ingest_report(
                        session=session,
                        source_id=source_id,
                        report=report,
                    )
                    if inserted:
                        rows_processed += 1
                except Exception as exc:
                    logger.warning(
                        "goldilocks_report_ingest_failed",
                        error=str(exc),
                        title=report.get("title", "")[:80],
                    )
                    rows_failed += 1

        except GoldilocksScrapingError as exc:
            logger.error("goldilocks_scraping_failed", error=str(exc))
            rows_failed += 1

        return ExecutionResult(rows_processed=rows_processed, rows_failed=rows_failed)

    async def _ensure_source(self, session: AsyncSession) -> int:
        """Upsert the Goldilocks source record."""
        stmt = pg_insert(DeQualSources).values(
            source_name=_SOURCE_NAME,
            source_type="report",
            feed_url=_GOLDILOCKS_REPORTS_URL,
            is_active=True,
        ).on_conflict_do_update(
            index_elements=["source_name"],
            set_={"feed_url": _GOLDILOCKS_REPORTS_URL, "is_active": True},
        ).returning(DeQualSources.id)

        result = await session.execute(stmt)
        await session.flush()
        return result.scalar_one()

    async def _scrape_reports(
        self,
        email: str,
        password: str,
    ) -> list[dict[str, Any]]:
        """Use Playwright to authenticate and collect research report metadata.

        Returns a list of report dicts with: title, url, published_at, summary.
        """
        try:
            from playwright.async_api import async_playwright  # type: ignore[import]
        except ImportError as exc:
            raise GoldilocksScrapingError(
                "playwright not installed. Run: pip install playwright && playwright install chromium"
            ) from exc

        reports: list[dict[str, Any]] = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            try:
                # Navigate to login
                await page.goto(_GOLDILOCKS_LOGIN_URL, timeout=_PLAYWRIGHT_TIMEOUT_MS)
                await page.wait_for_load_state("networkidle", timeout=_PLAYWRIGHT_TIMEOUT_MS)

                # Fill credentials
                await page.fill("input[type='email']", email)
                await page.fill("input[type='password']", password)
                await page.click("button[type='submit']")
                await page.wait_for_load_state("networkidle", timeout=_PLAYWRIGHT_TIMEOUT_MS)

                # Check login success
                if "login" in page.url.lower():
                    raise GoldilocksScrapingError("Login failed — check credentials")

                logger.info("goldilocks_login_success")

                # Navigate to research listing
                await page.goto(_GOLDILOCKS_REPORTS_URL, timeout=_PLAYWRIGHT_TIMEOUT_MS)
                await page.wait_for_load_state("networkidle", timeout=_PLAYWRIGHT_TIMEOUT_MS)

                # Extract report links — adapt selectors to actual site structure
                report_elements = await page.query_selector_all("article.report, .research-item, .report-card")

                for el in report_elements:
                    try:
                        title_el = await el.query_selector("h2, h3, .title")
                        title = await title_el.inner_text() if title_el else None

                        link_el = await el.query_selector("a")
                        href = await link_el.get_attribute("href") if link_el else None
                        if href and not href.startswith("http"):
                            href = f"{_GOLDILOCKS_BASE_URL}{href}"

                        summary_el = await el.query_selector("p, .summary, .excerpt")
                        summary = await summary_el.inner_text() if summary_el else None

                        date_el = await el.query_selector("time, .date, .published")
                        date_str = await date_el.get_attribute("datetime") if date_el else None

                        if href:
                            reports.append(
                                {
                                    "title": title,
                                    "url": href,
                                    "summary": summary,
                                    "published_str": date_str,
                                }
                            )
                    except Exception as exc:
                        logger.debug("goldilocks_element_parse_failed", error=str(exc))
                        continue

                logger.info("goldilocks_reports_found", count=len(reports))

            finally:
                await context.close()
                await browser.close()

        return reports

    async def _ingest_report(
        self,
        session: AsyncSession,
        source_id: int,
        report: dict[str, Any],
    ) -> bool:
        """Ingest a single Goldilocks report. Returns True if new, False if duplicate."""
        title = report.get("title") or ""
        url = report.get("url") or ""
        summary = report.get("summary") or ""

        content = f"{title}\n{summary}\n{url}"
        content_hash = compute_content_hash(content)

        lock_acquired = await acquire_document_advisory_lock(session, content_hash)
        if not lock_acquired:
            return False

        try:
            if await is_exact_duplicate(session, source_id=source_id, content_hash=content_hash):
                return False

            # Parse published date
            published_at: Optional[datetime] = None
            pub_str = report.get("published_str")
            if pub_str:
                try:
                    published_at = datetime.fromisoformat(pub_str).replace(tzinfo=timezone.utc)
                except Exception:
                    pass

            doc_id = uuid.uuid4()
            stmt = pg_insert(DeQualDocuments).values(
                id=doc_id,
                source_id=source_id,
                content_hash=content_hash,
                source_url=url or None,
                published_at=published_at,
                title=title[:500] if title else None,
                original_format="html",
                raw_text=summary or None,
                processing_status="pending",
            ).on_conflict_do_nothing(constraint="uq_qual_doc_source_hash")

            await session.execute(stmt)
            await session.flush()

            logger.info(
                "goldilocks_report_inserted",
                document_id=str(doc_id),
                title=title[:80],
            )
            return True

        finally:
            await release_document_advisory_lock(session, content_hash)
