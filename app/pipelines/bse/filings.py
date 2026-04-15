"""BSE corporate filings pipeline — announcements, corp actions, result calendar.

Bulk-by-date fetch strategy: 3 API calls total (not per-scripcode).
"""

from __future__ import annotations

import hashlib
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from bse import BSE
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.bse import DeBseAnnouncements, DeBseCorpActions, DeBseResultCalendar
from app.models.instruments import DeInstrument
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

ANNOUNCEMENTS_PAGE_SIZE = 50


def _sha256(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def _parse_bse_date(s: str | None) -> date | None:
    if not s or not s.strip():
        return None
    s = s.strip()
    for fmt in ("%d %b %Y", "%Y%m%d", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_bse_datetime(s: str | None) -> datetime | None:
    if not s or not s.strip():
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%d %b %Y %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone(timedelta(hours=5, minutes=30)))
        except ValueError:
            continue
    return None


def _classify_action(purpose: str) -> str:
    p = (purpose or "").lower().strip()
    if "dividend" in p or "income distribution" in p:
        return "dividend"
    if "split" in p or "sub-division" in p:
        return "split"
    if "bonus" in p:
        return "bonus"
    if "buy back" in p or "buyback" in p:
        return "buyback"
    if "rights" in p:
        return "rights"
    if "demerger" in p or "scheme of arrangement" in p:
        return "demerger"
    return "other"


def _extract_amount(purpose: str) -> Decimal | None:
    m = re.search(r"Rs\.?\s*(\d+(?:\.\d+)?)", purpose or "")
    if m:
        try:
            return Decimal(m.group(1))
        except InvalidOperation:
            return None
    return None


class BseFilingsPipeline(BasePipeline):
    pipeline_name = "bse_filings"
    requires_trading_day = False
    exchange = "BSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info("bse_filings_start", business_date=business_date.isoformat())

        scripcode_map = await self._load_scripcode_map(session)

        if len(scripcode_map) < 100:
            logger.info("bse_filings_backfill_scripcodes", existing=len(scripcode_map))
            scripcode_map = await self._backfill_scripcodes(session, scripcode_map)

        bse = BSE(download_folder="/tmp/bse_pipeline")

        total_processed = 0
        total_failed = 0

        ann_ok, ann_fail = await self._ingest_announcements(bse, session, scripcode_map, business_date)
        total_processed += ann_ok
        total_failed += ann_fail

        ca_ok, ca_fail = await self._ingest_corp_actions(bse, session, scripcode_map, business_date)
        total_processed += ca_ok
        total_failed += ca_fail

        rc_ok, rc_fail = await self._ingest_result_calendar(bse, session, scripcode_map, business_date)
        total_processed += rc_ok
        total_failed += rc_fail

        bse.exit()

        logger.info(
            "bse_filings_complete",
            rows_processed=total_processed,
            rows_failed=total_failed,
            business_date=business_date.isoformat(),
        )
        return ExecutionResult(rows_processed=total_processed, rows_failed=total_failed)

    async def _load_scripcode_map(self, session: AsyncSession) -> dict[str, uuid.UUID]:
        result = await session.execute(
            select(DeInstrument.bse_scripcode, DeInstrument.id).where(
                DeInstrument.bse_scripcode.isnot(None),
                DeInstrument.is_active == True,  # noqa: E712
            )
        )
        return {row.bse_scripcode: row.id for row in result}

    async def _backfill_scripcodes(
        self,
        session: AsyncSession,
        existing_map: dict[str, uuid.UUID],
    ) -> dict[str, uuid.UUID]:
        bse = BSE(download_folder="/tmp/bse_pipeline")

        isin_to_id: dict[str, uuid.UUID] = {}
        result = await session.execute(
            select(DeInstrument.isin, DeInstrument.id).where(
                DeInstrument.isin.isnot(None),
                DeInstrument.is_active == True,  # noqa: E712
            )
        )
        for row in result:
            isin_to_id[row.isin] = row.id

        mapped = 0
        for group in ("A", "B", "T", "X", "Z", "M", "P"):
            try:
                securities = bse.listSecurities(group=group, segment="Equity", status="Active")
            except Exception as exc:
                logger.warning("bse_list_securities_failed", group=group, error=str(exc))
                continue

            for sec in securities:
                sc = str(sec.get("SCRIP_CD", "")).strip()
                isin = (sec.get("ISIN_NUMBER") or "").strip()
                if not sc or sc in existing_map:
                    continue
                inst_id = isin_to_id.get(isin)
                if not inst_id:
                    continue

                await session.execute(
                    text(
                        "UPDATE de_instrument SET bse_scripcode = :sc "
                        "WHERE id = :id AND bse_scripcode IS NULL"
                    ),
                    {"sc": sc, "id": inst_id},
                )
                existing_map[sc] = inst_id
                mapped += 1

        await session.flush()
        bse.exit()
        logger.info("bse_scripcode_backfill_done", mapped=mapped, total=len(existing_map))
        return existing_map

    async def _ingest_announcements(
        self,
        bse: BSE,
        session: AsyncSession,
        scripcode_map: dict[str, uuid.UUID],
        business_date: date,
    ) -> tuple[int, int]:
        all_rows: list[dict[str, Any]] = []
        unmapped = 0
        page = 1
        total_count = None

        while True:
            try:
                data = bse.announcements(page_no=page)
            except Exception as exc:
                logger.warning("bse_announcements_fetch_error", page=page, error=str(exc))
                break

            if not data or "Table" not in data:
                break

            items = data["Table"]
            if not items:
                break

            if total_count is None and "Table1" in data and data["Table1"]:
                total_count = data["Table1"][0].get("ROWCNT", 0)

            for item in items:
                sc = str(item.get("SCRIP_CD", "")).strip()
                inst_id = scripcode_map.get(sc)
                if not inst_id:
                    unmapped += 1
                    continue

                headline = (item.get("HEADLINE") or item.get("NEWSSUB") or "").strip()
                if not headline:
                    continue

                dt_str = item.get("DT_TM") or item.get("NEWS_DT") or ""
                ann_dt = _parse_bse_datetime(dt_str)
                if not ann_dt:
                    continue

                dedup = _sha256(sc, dt_str, headline)

                attachment = None
                xml_name = item.get("XML_NAME") or item.get("ATTACHMENTNAME")
                if xml_name:
                    attachment = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{xml_name}.pdf"

                all_rows.append({
                    "instrument_id": inst_id,
                    "scripcode": sc,
                    "announcement_dt": ann_dt,
                    "headline": headline[:5000],
                    "category": (item.get("CATEGORYNAME") or "")[:100] or None,
                    "subcategory": (item.get("SUBCATNAME") or "")[:100] or None,
                    "description": (item.get("NEWSSUB") or "")[:5000] or None,
                    "attachment_url": attachment,
                    "dedup_hash": dedup,
                })

            fetched_so_far = page * ANNOUNCEMENTS_PAGE_SIZE
            if total_count and fetched_so_far >= total_count:
                break
            if len(items) < ANNOUNCEMENTS_PAGE_SIZE:
                break
            page += 1

        inserted = 0
        if all_rows:
            stmt = pg_insert(DeBseAnnouncements).values(all_rows)
            stmt = stmt.on_conflict_do_nothing(index_elements=["dedup_hash"])
            result = await session.execute(stmt)
            inserted = result.rowcount or 0

        logger.info(
            "bse_announcements_done",
            fetched=len(all_rows),
            inserted=inserted,
            unmapped=unmapped,
            pages=page,
        )
        return inserted, unmapped

    async def _ingest_corp_actions(
        self,
        bse: BSE,
        session: AsyncSession,
        scripcode_map: dict[str, uuid.UUID],
        business_date: date,
    ) -> tuple[int, int]:
        from_dt = datetime.combine(business_date - timedelta(days=30), datetime.min.time())
        to_dt = datetime.combine(business_date + timedelta(days=60), datetime.max.time())

        try:
            actions = bse.actions(from_date=from_dt, to_date=to_dt)
        except Exception as exc:
            logger.error("bse_actions_fetch_error", error=str(exc))
            return 0, 0

        all_rows: list[dict[str, Any]] = []
        unmapped = 0

        for act in actions:
            sc = str(act.get("scrip_code", "")).strip()
            inst_id = scripcode_map.get(sc)
            if not inst_id:
                unmapped += 1
                continue

            purpose = (act.get("Purpose") or "").strip()
            action_type = _classify_action(purpose)
            ex_date = _parse_bse_date(act.get("Ex_date"))
            record_date = _parse_bse_date(act.get("RD_Date"))

            dedup = _sha256(sc, str(ex_date or ""), action_type)

            all_rows.append({
                "instrument_id": inst_id,
                "scripcode": sc,
                "action_type": action_type,
                "ex_date": ex_date,
                "record_date": record_date,
                "announced_at": None,
                "purpose_code": None,
                "ratio": None,
                "amount_per_share": _extract_amount(purpose),
                "description": purpose[:2000] if purpose else None,
                "dedup_hash": dedup,
            })

        inserted = 0
        if all_rows:
            stmt = pg_insert(DeBseCorpActions).values(all_rows)
            stmt = stmt.on_conflict_do_nothing(index_elements=["dedup_hash"])
            result = await session.execute(stmt)
            inserted = result.rowcount or 0

        logger.info(
            "bse_corp_actions_done",
            fetched=len(all_rows),
            inserted=inserted,
            unmapped=unmapped,
        )
        return inserted, unmapped

    async def _ingest_result_calendar(
        self,
        bse: BSE,
        session: AsyncSession,
        scripcode_map: dict[str, uuid.UUID],
        business_date: date,
    ) -> tuple[int, int]:
        from_dt = datetime.combine(business_date - timedelta(days=7), datetime.min.time())
        to_dt = datetime.combine(business_date + timedelta(days=30), datetime.max.time())

        try:
            calendar = bse.resultCalendar(from_date=from_dt, to_date=to_dt)
        except Exception as exc:
            logger.error("bse_result_calendar_fetch_error", error=str(exc))
            return 0, 0

        all_rows: list[dict[str, Any]] = []
        unmapped = 0

        for entry in calendar:
            sc = str(entry.get("scrip_Code", "")).strip()
            inst_id = scripcode_map.get(sc)
            if not inst_id:
                unmapped += 1
                continue

            result_date = _parse_bse_date(entry.get("meeting_date"))
            if not result_date:
                continue

            dedup = _sha256(sc, str(result_date))

            all_rows.append({
                "instrument_id": inst_id,
                "scripcode": sc,
                "result_date": result_date,
                "period": None,
                "announced_at": None,
                "dedup_hash": dedup,
            })

        inserted = 0
        if all_rows:
            stmt = pg_insert(DeBseResultCalendar).values(all_rows)
            stmt = stmt.on_conflict_do_nothing(index_elements=["dedup_hash"])
            result = await session.execute(stmt)
            inserted = result.rowcount or 0

        logger.info(
            "bse_result_calendar_done",
            fetched=len(all_rows),
            inserted=inserted,
            unmapped=unmapped,
        )
        return inserted, unmapped
