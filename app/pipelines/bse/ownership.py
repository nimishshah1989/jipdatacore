"""BSE ownership pipeline — shareholding, pledge, insider trades, SAST.

Per-scripcode design: loads active scripcodes from de_instrument, calls 4 BSE
API endpoints per scripcode with asyncio.Semaphore(5) for rate limiting.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import date
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.bse import (
    DeBseInsiderTrades,
    DeBsePledgeHistory,
    DeBseSastDisclosures,
    DeBseShareholding,
)
from app.models.instruments import DeInstrument
from app.models.pipeline import DePipelineLog
from app.pipelines.bse.parsers import (
    parse_insider_trades,
    parse_pledge,
    parse_sast,
    parse_shareholding,
)
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

BSE_API_BASE = "https://api.bseindia.com/BseIndiaAPI/api"
BSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.bseindia.com/",
    "Accept": "application/json",
}
MAX_CONCURRENCY = 5
MAX_RETRIES = 3
BACKOFF_BASE = 2.0
BATCH_SIZE = 200


class BseOwnershipPipeline(BasePipeline):
    pipeline_name = "bse_ownership"
    requires_trading_day = False
    exchange = "BSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info("bse_ownership_start", business_date=business_date.isoformat())

        scripcode_map = await self._load_scripcode_map(session)
        if not scripcode_map:
            logger.error("bse_ownership_no_scripcodes")
            return ExecutionResult(rows_processed=0, rows_failed=1)

        logger.info("bse_ownership_scripcodes_loaded", count=len(scripcode_map))

        semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        total_processed = 0
        total_failed = 0

        sh_rows: list[dict[str, Any]] = []
        pledge_rows: list[dict[str, Any]] = []
        insider_rows: list[dict[str, Any]] = []
        sast_rows: list[dict[str, Any]] = []

        async with httpx.AsyncClient(
            headers=BSE_HEADERS,
            timeout=httpx.Timeout(20.0, connect=10.0),
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            follow_redirects=False,
        ) as client:
            items = list(scripcode_map.items())
            for batch_start in range(0, len(items), BATCH_SIZE):
                batch = items[batch_start : batch_start + BATCH_SIZE]
                tasks = [
                    self._fetch_scripcode(client, semaphore, sc, inst_id)
                    for sc, inst_id in batch
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        total_failed += 1
                        continue
                    s, p, i, sa_rows = result
                    sh_rows.extend(s)
                    pledge_rows.extend(p)
                    insider_rows.extend(i)
                    sast_rows.extend(sa_rows)

                logger.info(
                    "bse_ownership_batch_done",
                    batch_start=batch_start,
                    batch_size=len(batch),
                    sh=len(sh_rows),
                    pledge=len(pledge_rows),
                    insider=len(insider_rows),
                    sast=len(sast_rows),
                )

        sh_ok = await self._upsert_shareholding(session, sh_rows)
        pl_ok = await self._insert_deduped(session, DeBsePledgeHistory, pledge_rows)
        ins_ok = await self._insert_deduped(session, DeBseInsiderTrades, insider_rows)
        sast_ok = await self._insert_deduped(session, DeBseSastDisclosures, sast_rows)

        total_processed = sh_ok + pl_ok + ins_ok + sast_ok

        logger.info(
            "bse_ownership_complete",
            shareholding=sh_ok,
            pledge=pl_ok,
            insider=ins_ok,
            sast=sast_ok,
            total=total_processed,
            failed=total_failed,
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

    async def _fetch_scripcode(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        scripcode: str,
        instrument_id: uuid.UUID,
    ) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
        sh_rows: list[dict[str, Any]] = []
        pledge_rows: list[dict[str, Any]] = []
        insider_rows: list[dict[str, Any]] = []
        sast_rows: list[dict[str, Any]] = []

        endpoints = [
            ("CorpShareHoldingPattern_New", "shareholding"),
            ("Shrholdpledge", "pledge"),
            ("Cinsidertrading", "insider"),
            ("CorpSASTData", "sast"),
        ]

        for endpoint, kind in endpoints:
            async with semaphore:
                data = await self._fetch_with_retry(
                    client, f"{BSE_API_BASE}/{endpoint}/w?scripcode={scripcode}", scripcode, kind
                )

            if data is None:
                continue

            if kind == "shareholding":
                parsed = parse_shareholding(data if isinstance(data, list) else [data], scripcode)
                for row in parsed:
                    row["instrument_id"] = instrument_id
                sh_rows.extend(parsed)
            elif kind == "pledge":
                parsed = parse_pledge(data if isinstance(data, list) else [data], scripcode)
                for row in parsed:
                    row["instrument_id"] = instrument_id
                pledge_rows.extend(parsed)
            elif kind == "insider":
                parsed = parse_insider_trades(data if isinstance(data, list) else [data], scripcode)
                for row in parsed:
                    row["instrument_id"] = instrument_id
                insider_rows.extend(parsed)
            elif kind == "sast":
                parsed = parse_sast(data if isinstance(data, list) else [data], scripcode)
                for row in parsed:
                    row["instrument_id"] = instrument_id
                sast_rows.extend(parsed)

        return sh_rows, pledge_rows, insider_rows, sast_rows

    async def _fetch_with_retry(
        self,
        client: httpx.AsyncClient,
        url: str,
        scripcode: str,
        kind: str,
    ) -> list[dict[str, Any]] | None:
        for attempt in range(MAX_RETRIES):
            try:
                resp = await client.get(url)
                if resp.status_code == 404:
                    return None
                if resp.status_code == 302:
                    return None
                resp.raise_for_status()
                data = resp.json()
                if not data:
                    return None
                return data if isinstance(data, list) else [data]
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code < 500:
                    logger.debug(
                        "bse_ownership_client_error",
                        scripcode=scripcode,
                        kind=kind,
                        status=exc.response.status_code,
                    )
                    return None
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(BACKOFF_BASE ** (attempt + 1))
                    continue
                logger.warning(
                    "bse_ownership_server_error",
                    scripcode=scripcode,
                    kind=kind,
                    status=exc.response.status_code,
                )
                return None
            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(BACKOFF_BASE ** (attempt + 1))
                    continue
                logger.warning(
                    "bse_ownership_timeout",
                    scripcode=scripcode,
                    kind=kind,
                    error=str(exc),
                )
                return None
            except Exception as exc:
                logger.warning(
                    "bse_ownership_parse_error",
                    scripcode=scripcode,
                    kind=kind,
                    error=str(exc),
                )
                return None
        return None

    async def _upsert_shareholding(
        self,
        session: AsyncSession,
        rows: list[dict[str, Any]],
    ) -> int:
        if not rows:
            return 0
        inserted = 0
        for chunk_start in range(0, len(rows), 500):
            chunk = rows[chunk_start : chunk_start + 500]
            stmt = pg_insert(DeBseShareholding).values(chunk)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_bse_sh_inst_qtr",
                set_={
                    "promoter_pct": stmt.excluded.promoter_pct,
                    "promoter_pledged_pct": stmt.excluded.promoter_pledged_pct,
                    "public_pct": stmt.excluded.public_pct,
                    "fii_pct": stmt.excluded.fii_pct,
                    "dii_pct": stmt.excluded.dii_pct,
                    "insurance_pct": stmt.excluded.insurance_pct,
                    "mutual_funds_pct": stmt.excluded.mutual_funds_pct,
                    "retail_pct": stmt.excluded.retail_pct,
                    "body_corporate_pct": stmt.excluded.body_corporate_pct,
                    "total_shareholders": stmt.excluded.total_shareholders,
                    "raw_json": stmt.excluded.raw_json,
                },
            )
            result = await session.execute(stmt)
            inserted += result.rowcount or 0
        return inserted

    async def _insert_deduped(
        self,
        session: AsyncSession,
        model: type,
        rows: list[dict[str, Any]],
    ) -> int:
        if not rows:
            return 0

        unique_key = None
        if model is DeBsePledgeHistory:
            unique_key = "uq_bse_pledge_inst_dt"
        else:
            unique_key = "dedup_hash"

        inserted = 0
        for chunk_start in range(0, len(rows), 500):
            chunk = rows[chunk_start : chunk_start + 500]
            stmt = pg_insert(model).values(chunk)
            if unique_key == "dedup_hash":
                stmt = stmt.on_conflict_do_nothing(index_elements=["dedup_hash"])
            else:
                stmt = stmt.on_conflict_do_nothing(constraint=unique_key)
            result = await session.execute(stmt)
            inserted += result.rowcount or 0
        return inserted
