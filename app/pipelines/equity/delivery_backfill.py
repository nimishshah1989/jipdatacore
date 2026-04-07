"""Delivery data backfill — extracts delivery_vol and delivery_pct from
NSE sec_bhavdata_full CSV files (which already contain DELIV_QTY, DELIV_PER).

Updates de_equity_ohlcv rows that have NULL delivery_pct.

Usage:
    python -m app.pipelines.equity.delivery_backfill [--workers 10]
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import httpx
import structlog
from sqlalchemy import text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

try:
    from app.config import get_settings
    DATABASE_URL = get_settings().database_url
except Exception:
    import os
    from dotenv import load_dotenv
    load_dotenv()
    DATABASE_URL = os.environ["DATABASE_URL"]

from app.models.prices import DeEquityOhlcv

logger = structlog.get_logger(__name__)

# sec_bhavdata_full is available ~2010 to present
BHAV_URL = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv"
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

REQUEST_DELAY = 0.5


def _safe_int(val: str) -> Optional[int]:
    try:
        return int(Decimal(val.strip().replace(",", "")))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _safe_decimal(val: str) -> Optional[Decimal]:
    try:
        return Decimal(val.strip().replace(",", ""))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _parse_bhav_delivery(content: str) -> dict[str, tuple[Optional[int], Optional[Decimal]]]:
    """Parse sec_bhavdata_full CSV and extract delivery data.

    Returns dict of symbol → (delivery_qty, delivery_pct) for EQ series only.
    """
    lines = content.strip().splitlines()
    if not lines:
        return {}

    headers = [h.strip().upper() for h in lines[0].split(",")]
    hmap = {h: i for i, h in enumerate(headers)}

    sym_idx = hmap.get("SYMBOL")
    ser_idx = hmap.get("SERIES")
    dq_idx = hmap.get("DELIV_QTY")
    dp_idx = hmap.get("DELIV_PER")

    if sym_idx is None or dq_idx is None or dp_idx is None:
        return {}

    result: dict[str, tuple[Optional[int], Optional[Decimal]]] = {}
    for line in lines[1:]:
        cols = [c.strip() for c in line.split(",")]
        if len(cols) <= max(sym_idx, ser_idx or 0, dq_idx, dp_idx):
            continue

        series = cols[ser_idx].strip().upper() if ser_idx is not None else "EQ"
        if series != "EQ":
            continue

        symbol = cols[sym_idx].strip().upper()
        if not symbol:
            continue

        delivery_qty = _safe_int(cols[dq_idx])
        delivery_pct = _safe_decimal(cols[dp_idx])
        result[symbol] = (delivery_qty, delivery_pct)

    return result


async def _get_dates_needing_delivery(sf: async_sessionmaker) -> list[date]:
    """Find distinct dates with NULL delivery_pct, weekdays only, from 2010+."""
    async with sf() as session:
        result = await session.execute(
            text("""
                SELECT DISTINCT date FROM de_equity_ohlcv
                WHERE delivery_pct IS NULL
                  AND date >= '2020-01-01'
                  AND extract(dow FROM date) NOT IN (0, 6)
                ORDER BY date
            """)
        )
        return [row[0] for row in result.fetchall()]


async def _load_symbol_to_instrument(sf: async_sessionmaker) -> dict[str, Any]:
    """Load symbol → instrument_id from de_instrument."""
    from app.models.instruments import DeInstrument
    from sqlalchemy import select

    async with sf() as session:
        result = await session.execute(
            select(DeInstrument.current_symbol, DeInstrument.id)
        )
        return {row[0].upper(): row[1] for row in result.fetchall()}


async def _process_date(
    sem: asyncio.Semaphore,
    sf: async_sessionmaker,
    target_date: date,
    symbol_map: dict[str, Any],
    progress: dict[str, int],
) -> None:
    async with sem:
        date_str = target_date.strftime("%d%m%Y")
        url = BHAV_URL.format(date_str=date_str)

        try:
            async with httpx.AsyncClient(
                headers=NSE_HEADERS, timeout=30.0, follow_redirects=True
            ) as client:
                resp = await client.get(url)
                if resp.status_code == 404:
                    progress["skipped"] += 1
                    return
                resp.raise_for_status()
        except Exception as exc:
            progress["failed"] += 1
            if progress["failed"] <= 5:
                logger.warning("download_error", date=target_date.isoformat(), error=str(exc))
            return

        delivery_data = _parse_bhav_delivery(resp.text)
        if not delivery_data:
            progress["skipped"] += 1
            return

        rows_updated = 0
        async with sf() as session:
            async with session.begin():
                for symbol, (dq, dp) in delivery_data.items():
                    instrument_id = symbol_map.get(symbol)
                    if instrument_id is None:
                        continue

                    vals: dict[str, Any] = {}
                    if dq is not None:
                        vals["delivery_vol"] = dq
                    if dp is not None:
                        vals["delivery_pct"] = dp
                    if not vals:
                        continue

                    result = await session.execute(
                        update(DeEquityOhlcv)
                        .where(
                            DeEquityOhlcv.date == target_date,
                            DeEquityOhlcv.instrument_id == instrument_id,
                        )
                        .values(**vals)
                    )
                    if result.rowcount > 0:
                        rows_updated += 1

        progress["done"] += 1
        progress["rows"] += rows_updated
        await asyncio.sleep(REQUEST_DELAY)


async def main(workers: int = 10) -> None:
    engine = create_async_engine(
        DATABASE_URL,
        pool_size=workers + 2,
        max_overflow=5,
        pool_pre_ping=True,
    )
    sf = async_sessionmaker(engine, expire_on_commit=False)

    dates = await _get_dates_needing_delivery(sf)
    logger.info("delivery_backfill_start", dates=len(dates), workers=workers)

    if not dates:
        logger.info("nothing_to_do")
        await engine.dispose()
        return

    logger.info("range", from_date=dates[0].isoformat(), to_date=dates[-1].isoformat())

    symbol_map = await _load_symbol_to_instrument(sf)
    logger.info("symbols_loaded", count=len(symbol_map))

    progress = {"done": 0, "skipped": 0, "failed": 0, "rows": 0, "total": len(dates)}

    async def log_progress():
        while progress["done"] + progress["skipped"] + progress["failed"] < progress["total"]:
            await asyncio.sleep(30)
            processed = progress["done"] + progress["skipped"] + progress["failed"]
            logger.info(
                "progress",
                done=progress["done"],
                skipped=progress["skipped"],
                failed=progress["failed"],
                rows=progress["rows"],
                pct=round(processed / progress["total"] * 100, 1),
            )

    progress_task = asyncio.create_task(log_progress())

    sem = asyncio.Semaphore(workers)
    tasks = [_process_date(sem, sf, d, symbol_map, progress) for d in dates]
    await asyncio.gather(*tasks, return_exceptions=True)

    progress_task.cancel()

    logger.info(
        "delivery_backfill_complete",
        dates_done=progress["done"],
        dates_skipped=progress["skipped"],
        dates_failed=progress["failed"],
        rows_updated=progress["rows"],
    )
    await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delivery data backfill from sec_bhavdata_full")
    parser.add_argument("--workers", type=int, default=10, help="Concurrent workers")
    args = parser.parse_args()
    asyncio.run(main(workers=args.workers))
