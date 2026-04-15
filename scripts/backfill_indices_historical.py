"""GAP-03: Historical backfill for indices with insufficient data in de_index_prices.

Identifies indices in de_index_master with < 250 days of data (or < 2000 for
critical sectoral indices) and backfills from niftyindices.com historical API.

Sources (in priority order):
  1. fie2 / mfpulse sister DBs — checked and found inaccessible/empty (see
     reports/index_source_inventory_2026-04-15.md)
  2. niftyindices.com POST API — used here
  3. NSE bhav-copy archive — fallback for indices not on niftyindices.com

Usage:
    python scripts/backfill_indices_historical.py [--workers 5] [--start 2016-01-01]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import httpx
import structlog
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv

load_dotenv()

from app.models.instruments import DeIndexMaster
from app.models.prices import DeIndexPrices

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# niftyindices.com API
# ---------------------------------------------------------------------------
NIFTY_HIST_OHLCV_URL = (
    "https://niftyindices.com/Backpage.aspx/getHistoricaldatatabletoString"
)
NIFTY_HIST_PEPB_URL = (
    "https://niftyindices.com/Backpage.aspx/getpepbHistoricaldataDBtoString"
)
NIFTY_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/json; charset=UTF-8",
    "Origin": "https://niftyindices.com",
    "Referer": "https://niftyindices.com/reports/historical-data",
}

CHUNK_DAYS = 40
REQUEST_DELAY = 0.35

CRITICAL_SECTORAL = {
    "NIFTY PHARMA", "NIFTY REALTY", "NIFTY PVT BANK",
    "NIFTY OIL AND GAS", "NIFTY HEALTHCARE", "NIFTY CONSR DURBL",
    "NIFTY FIN SERVICE",
}

API_NAME_OVERRIDES = {
    "NIFTY INTERNET": "NIFTY INDIA INTERNET",
    "NIFTY RAILWAYSPSU": "NIFTY INDIA RAILWAYS PSU",
}

SKIP_INDICES: set[str] = set()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _safe_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == "" or value == "-":
        return None
    try:
        return Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError, TypeError):
        return None


def _parse_hist_date(date_str: str) -> Optional[date]:
    for fmt in ("%d %b %Y", "%d-%b-%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except (ValueError, AttributeError):
            continue
    return None


def _date_chunks(start: date, end: date) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=CHUNK_DAYS - 1), end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def _format_date(d: date) -> str:
    return d.strftime("%d-%b-%Y")


# ---------------------------------------------------------------------------
# API fetch
# ---------------------------------------------------------------------------
async def _fetch_json_post(
    client: httpx.AsyncClient, url: str, index_name: str, start: date, end: date
) -> list[dict[str, Any]]:
    payload = {
        "cinfo": json.dumps({
            "name": index_name,
            "startDate": _format_date(start),
            "endDate": _format_date(end),
            "indexName": index_name,
        })
    }
    resp = await client.post(url, json=payload, headers=NIFTY_HEADERS, timeout=30.0)
    resp.raise_for_status()
    raw = resp.json().get("d")
    if not raw or raw in ("null", "[]", ""):
        return []
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []


# ---------------------------------------------------------------------------
# Identify gaps
# ---------------------------------------------------------------------------
async def get_indices_needing_backfill(
    session: AsyncSession,
) -> list[dict[str, Any]]:
    result = await session.execute(text("""
        SELECT m.index_code, m.index_name, COALESCE(p.cnt, 0) as cnt,
               p.min_date, p.max_date
        FROM de_index_master m
        LEFT JOIN (
            SELECT index_code, COUNT(*) as cnt, MIN(date) as min_date,
                   MAX(date) as max_date
            FROM de_index_prices GROUP BY index_code
        ) p ON m.index_code = p.index_code
        ORDER BY cnt ASC
    """))
    rows = result.fetchall()
    needs = []
    for r in rows:
        index_code, index_name, cnt, min_date, max_date = r
        if index_code in SKIP_INDICES:
            continue
        threshold = 2000 if index_code in CRITICAL_SECTORAL else 250
        if cnt < threshold:
            needs.append({
                "index_code": index_code,
                "index_name": index_name,
                "current_count": cnt,
                "min_date": min_date,
                "max_date": max_date,
                "threshold": threshold,
            })
    return needs


# ---------------------------------------------------------------------------
# Single-index worker
# ---------------------------------------------------------------------------
_progress_lock = asyncio.Lock()
_progress = {"done": 0, "total": 0}


async def _backfill_one(
    sem: asyncio.Semaphore,
    session_factory: async_sessionmaker,
    index_code: str,
    api_name: str,
    chunks: list[tuple[date, date]],
) -> tuple[str, int]:
    global _progress

    async with sem:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            ohlcv_by_date: dict[date, dict[str, Any]] = {}
            consecutive_fails = 0
            for cs, ce in chunks:
                if consecutive_fails >= 200:
                    break
                try:
                    records = await _fetch_json_post(
                        client, NIFTY_HIST_OHLCV_URL, api_name, cs, ce
                    )
                    if not records:
                        consecutive_fails += 1
                        await asyncio.sleep(REQUEST_DELAY)
                        continue
                    consecutive_fails = 0
                    for rec in records:
                        d = _parse_hist_date(rec.get("HistoricalDate", ""))
                        if d:
                            ohlcv_by_date[d] = {
                                "open": _safe_decimal(rec.get("OPEN")),
                                "high": _safe_decimal(rec.get("HIGH")),
                                "low": _safe_decimal(rec.get("LOW")),
                                "close": _safe_decimal(rec.get("CLOSE")),
                            }
                except Exception as e:
                    logger.warning("ohlcv_chunk_error", index=index_code, error=str(e))
                    consecutive_fails += 1
                await asyncio.sleep(REQUEST_DELAY)

            pepb_by_date: dict[date, dict[str, Any]] = {}
            if ohlcv_by_date:
                consecutive_fails = 0
                for cs, ce in chunks:
                    if consecutive_fails >= 200:
                        break
                    try:
                        records = await _fetch_json_post(
                            client, NIFTY_HIST_PEPB_URL, api_name, cs, ce
                        )
                        if not records:
                            consecutive_fails += 1
                            await asyncio.sleep(REQUEST_DELAY)
                            continue
                        consecutive_fails = 0
                        for rec in records:
                            d = _parse_hist_date(rec.get("DATE", ""))
                            if d:
                                pepb_by_date[d] = {
                                    "pe_ratio": _safe_decimal(rec.get("pe")),
                                    "pb_ratio": _safe_decimal(rec.get("pb")),
                                    "div_yield": _safe_decimal(rec.get("divYield")),
                                }
                    except Exception:
                        consecutive_fails += 1
                    await asyncio.sleep(REQUEST_DELAY)

        if not ohlcv_by_date:
            async with _progress_lock:
                _progress["done"] += 1
            logger.warning(
                "no_data_from_niftyindices",
                index=index_code,
                progress=f"{_progress['done']}/{_progress['total']}",
            )
            return index_code, 0

        all_dates = sorted(ohlcv_by_date.keys())
        rows: list[dict[str, Any]] = []
        for d in all_dates:
            ohlcv = ohlcv_by_date[d]
            pepb = pepb_by_date.get(d, {})
            rows.append({
                "date": d,
                "index_code": index_code,
                "open": ohlcv.get("open"),
                "high": ohlcv.get("high"),
                "low": ohlcv.get("low"),
                "close": ohlcv.get("close"),
                "pe_ratio": pepb.get("pe_ratio"),
                "pb_ratio": pepb.get("pb_ratio"),
                "div_yield": pepb.get("div_yield"),
            })

        async with session_factory() as session:
            async with session.begin():
                for i in range(0, len(rows), 1000):
                    batch = rows[i : i + 1000]
                    stmt = pg_insert(DeIndexPrices).values(batch)
                    stmt = stmt.on_conflict_do_nothing(
                        index_elements=["date", "index_code"],
                    )
                    await session.execute(stmt)

        async with _progress_lock:
            _progress["done"] += 1

        logger.info(
            "backfilled",
            index=index_code,
            rows=len(rows),
            dates=f"{all_dates[0]}→{all_dates[-1]}",
            progress=f"{_progress['done']}/{_progress['total']}",
        )
        return index_code, len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main(workers: int = 5, start_date_str: str = "2016-01-01") -> None:
    global _progress

    database_url = os.environ["DATABASE_URL"]
    engine = create_async_engine(
        database_url,
        pool_size=workers + 2,
        max_overflow=5,
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        needs = await get_indices_needing_backfill(session)

    if not needs:
        logger.info("all_indices_have_sufficient_data")
        await engine.dispose()
        return

    logger.info("indices_needing_backfill", count=len(needs))
    for n in needs:
        logger.info(
            "gap",
            index=n["index_code"],
            current=n["current_count"],
            threshold=n["threshold"],
        )

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = date.today() - timedelta(days=1)
    chunks = _date_chunks(start_date, end_date)

    _progress["total"] = len(needs)
    _progress["done"] = 0

    logger.info(
        "backfill_start",
        indices=len(needs),
        workers=workers,
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        chunks_per_index=len(chunks),
    )

    sem = asyncio.Semaphore(workers)
    tasks = [
        _backfill_one(
            sem, session_factory, n["index_code"],
            API_NAME_OVERRIDES.get(n["index_code"], n["index_code"]),
            chunks,
        )
        for n in needs
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    total_rows = 0
    successes = 0
    failures = []
    for r in results:
        if isinstance(r, Exception):
            logger.error("worker_exception", error=str(r))
            failures.append(str(r))
        else:
            code, rows = r
            total_rows += rows
            if rows > 0:
                successes += 1
            else:
                failures.append(code)

    logger.info(
        "backfill_complete",
        total_rows=total_rows,
        indices_with_data=successes,
        indices_failed=len(failures),
    )

    if failures:
        logger.warning("failed_indices", indices=failures)

    # Verification
    async with session_factory() as session:
        skip_list = list(SKIP_INDICES) if SKIP_INDICES else ["__none__"]
        result = await session.execute(text("""
            SELECT COUNT(*) FROM de_index_master
            WHERE index_code NOT IN (
                SELECT index_code FROM de_index_prices
                GROUP BY index_code HAVING COUNT(*) >= 250
            )
            AND index_code != ALL(:skip)
        """), {"skip": skip_list})
        remaining = result.scalar()
        logger.info("verification", indices_below_250=remaining)

    await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GAP-03: Index historical backfill")
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--start", type=str, default="2016-01-01")
    args = parser.parse_args()

    asyncio.run(main(workers=args.workers, start_date_str=args.start))
