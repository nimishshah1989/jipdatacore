"""One-time backfill: seed de_index_master and load 10 years of historical
OHLCV + PE/PB/DivYield into de_index_prices.

Sources:
  - NSE allIndices API  → index master (135 indices, properly categorised)
  - niftyindices.com    → historical OHLCV + PE/PB (POST API, ~50-day chunks)

Usage (from project root):
    python -m app.pipelines.indices.index_backfill [--years 10] [--workers 10]
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import httpx
import structlog
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

try:
    from app.config import get_settings
    _settings = get_settings()
    DATABASE_URL = _settings.database_url
except Exception:
    import os
    from dotenv import load_dotenv
    load_dotenv()
    DATABASE_URL = os.environ["DATABASE_URL"]

from app.models.instruments import DeIndexMaster
from app.models.prices import DeIndexPrices

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# NSE allIndices API
# ---------------------------------------------------------------------------
NSE_BASE = "https://www.nseindia.com"
NSE_ALL_INDICES_URL = f"{NSE_BASE}/api/allIndices"
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

# ---------------------------------------------------------------------------
# niftyindices.com historical API
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
REQUEST_DELAY = 0.3  # seconds between requests per worker

# ---------------------------------------------------------------------------
# Index categorisation
# ---------------------------------------------------------------------------
_BROAD_SYMBOLS = {
    "NIFTY 50", "NIFTY NEXT 50", "NIFTY 100", "NIFTY 200", "NIFTY 500",
    "NIFTY MIDCAP 50", "NIFTY MIDCAP 100", "NIFTY MIDCAP 150",
    "NIFTY SMLCAP 50", "NIFTY SMLCAP 100", "NIFTY SMLCAP 250",
    "NIFTY MIDSML 400", "NIFTY500 MULTICAP", "NIFTY LARGEMID250",
    "NIFTY TOTAL MKT", "NIFTY MICROCAP250", "NIFTY500 LMS EQL",
    "NIFTY FPI 150", "NIFTY MID SELECT",
}

_SECTORAL_SYMBOLS = {
    "NIFTY AUTO", "NIFTY BANK", "NIFTY FIN SERVICE", "NIFTY FINSRV25 50",
    "NIFTY FMCG", "NIFTY IT", "NIFTY MEDIA", "NIFTY METAL", "NIFTY PHARMA",
    "NIFTY PSU BANK", "NIFTY PVT BANK", "NIFTY REALTY", "NIFTY HEALTHCARE",
    "NIFTY CONSR DURBL", "NIFTY OIL AND GAS", "NIFTY MIDSML HLTH",
    "NIFTY FINSEREXBNK", "NIFTY MS FIN SERV", "NIFTY MS IT TELCM",
    "NIFTY CHEMICALS", "NIFTY500 HEALTH", "NIFTY CAPITAL MKT",
}

_STRATEGY_SYMBOLS = {
    "NIFTY DIV OPPS 50", "NIFTY GROWSECT 15", "NIFTY100 QUALTY30",
    "NIFTY50 VALUE 20", "NIFTY50 TR 2X LEV", "NIFTY50 PR 2X LEV",
    "NIFTY50 TR 1X INV", "NIFTY50 PR 1X INV", "NIFTY50 DIV POINT",
    "NIFTY ALPHA 50", "NIFTY50 EQL WGT", "NIFTY100 EQL WGT",
    "NIFTY100 LOWVOL30", "NIFTY200 QUALTY30", "NIFTY ALPHALOWVOL",
    "NIFTY200MOMENTM30", "NIFTY M150 QLTY50", "NIFTY200 ALPHA 30",
    "NIFTYM150MOMNTM50", "NIFTY500MOMENTM50", "NIFTYMS400 MQ 100",
    "NIFTYSML250MQ 100", "NIFTY TOP 10 EW", "NIFTY AQL 30",
    "NIFTY AQLV 30", "NIFTY HIGHBETA 50", "NIFTY LOW VOL 50",
    "NIFTY QLTY LV 30", "NIFTY SML250 Q50", "NIFTY TOP 15 EW",
    "NIFTY100 ALPHA 30", "NIFTY200 VALUE 30", "NIFTY500 EW",
    "NIFTY MULTI MQ 50", "NIFTY500 VALUE 50", "NIFTY TOP 20 EW",
    "NIFTY500 QLTY50", "NIFTY500 LOWVOL50", "NIFTY500 MQVLV50",
    "NIFTY50 USD", "NIFTY500 FLEXICAP", "NIFTY TMMQ 50",
}

_SKIP_SYMBOLS = {
    "INDIA VIX",
    "NIFTY GS 8 13YR", "NIFTY GS 10YR", "NIFTY GS 10YR CLN",
    "NIFTY GS 4 8YR", "NIFTY GS 11 15YR", "NIFTY GS 15YRPLUS",
    "NIFTY GS COMPSITE",
    "BHARATBOND-APR30", "BHARATBOND-APR31", "BHARATBOND-APR32",
    "BHARATBOND-APR33",
}


def _categorise_index(symbol: str) -> str:
    if symbol in _BROAD_SYMBOLS:
        return "broad"
    if symbol in _SECTORAL_SYMBOLS:
        return "sectoral"
    if symbol in _STRATEGY_SYMBOLS:
        return "strategy"
    return "thematic"


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
    try:
        return datetime.strptime(date_str.strip(), "%d %b %Y").date()
    except (ValueError, AttributeError):
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
# API fetch helpers
# ---------------------------------------------------------------------------
async def _fetch_json_post(
    client: httpx.AsyncClient, url: str, index_name: str, start: date, end: date
) -> list[dict[str, Any]]:
    """POST to niftyindices.com and parse the double-encoded JSON response."""
    payload = {
        "cinfo": json.dumps({
            "name": index_name,
            "startDate": _format_date(start),
            "endDate": _format_date(end),
            "indexName": index_name,
        })
    }
    resp = await client.post(url, json=payload, headers=NIFTY_HEADERS)
    resp.raise_for_status()
    raw = resp.json().get("d")
    if not raw or raw in ("null", "[]", ""):
        return []
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []


# ---------------------------------------------------------------------------
# Seed index master
# ---------------------------------------------------------------------------
async def seed_index_master(session: AsyncSession) -> dict[str, str]:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        await client.get(NSE_BASE, headers=NSE_HEADERS)
        resp = await client.get(NSE_ALL_INDICES_URL, headers=NSE_HEADERS)
        resp.raise_for_status()

    data = resp.json().get("data", [])
    index_map: dict[str, str] = {}
    rows: list[dict[str, Any]] = []

    for rec in data:
        symbol = (rec.get("indexSymbol") or rec.get("index", "")).strip()
        full_name = (rec.get("index") or symbol).strip()
        if not symbol or symbol in _SKIP_SYMBOLS:
            continue
        category = _categorise_index(symbol)
        index_map[symbol] = full_name
        rows.append({"index_code": symbol, "index_name": full_name, "category": category})

    if rows:
        stmt = pg_insert(DeIndexMaster).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["index_code"],
            set_={"index_name": stmt.excluded.index_name, "category": stmt.excluded.category},
        )
        await session.execute(stmt)

    logger.info("index_master_seeded", total=len(rows))
    return index_map


# ---------------------------------------------------------------------------
# Single-index worker (runs concurrently)
# ---------------------------------------------------------------------------
_done_counter = 0
_done_lock = asyncio.Lock()


async def _backfill_one_index(
    sem: asyncio.Semaphore,
    session_factory: async_sessionmaker,
    symbol: str,
    full_name: str,
    chunks: list[tuple[date, date]],
    total_indices: int,
) -> tuple[str, int]:
    """Fetch OHLCV + PE/PB for one index and upsert. Runs under semaphore."""
    global _done_counter

    async with sem:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            # --- OHLCV ---
            ohlcv_by_date: dict[date, dict[str, Any]] = {}
            consecutive_fails = 0
            for cs, ce in chunks:
                if consecutive_fails >= 5:
                    break
                try:
                    records = await _fetch_json_post(client, NIFTY_HIST_OHLCV_URL, full_name, cs, ce)
                    if not records:
                        consecutive_fails += 1
                        await asyncio.sleep(REQUEST_DELAY)
                        continue
                    consecutive_fails = 0
                    for rec in records:
                        d = _parse_hist_date(rec.get("HistoricalDate", ""))
                        if d and d.weekday() < 5:
                            ohlcv_by_date[d] = {
                                "open": _safe_decimal(rec.get("OPEN")),
                                "high": _safe_decimal(rec.get("HIGH")),
                                "low": _safe_decimal(rec.get("LOW")),
                                "close": _safe_decimal(rec.get("CLOSE")),
                            }
                except Exception:
                    consecutive_fails += 1
                await asyncio.sleep(REQUEST_DELAY)

            if not ohlcv_by_date:
                async with _done_lock:
                    _done_counter += 1
                logger.info("no_data", index=symbol, progress=f"{_done_counter}/{total_indices}")
                return symbol, 0

            # --- PE/PB (fast bail on failure) ---
            pepb_by_date: dict[date, dict[str, Any]] = {}
            consecutive_fails = 0
            for cs, ce in chunks:
                if consecutive_fails >= 3:
                    break
                try:
                    records = await _fetch_json_post(client, NIFTY_HIST_PEPB_URL, full_name, cs, ce)
                    if not records:
                        consecutive_fails += 1
                        await asyncio.sleep(REQUEST_DELAY)
                        continue
                    consecutive_fails = 0
                    for rec in records:
                        d = _parse_hist_date(rec.get("DATE", ""))
                        if d and d.weekday() < 5:
                            pepb_by_date[d] = {
                                "pe_ratio": _safe_decimal(rec.get("pe")),
                                "pb_ratio": _safe_decimal(rec.get("pb")),
                                "div_yield": _safe_decimal(rec.get("divYield")),
                            }
                except Exception:
                    consecutive_fails += 1
                await asyncio.sleep(REQUEST_DELAY)

        # --- Upsert into DB (own session + transaction) ---
        all_dates = sorted(ohlcv_by_date.keys())
        rows: list[dict[str, Any]] = []
        for d in all_dates:
            ohlcv = ohlcv_by_date[d]
            pepb = pepb_by_date.get(d, {})
            rows.append({
                "date": d,
                "index_code": symbol,
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
                # Upsert in batches of 1000
                for i in range(0, len(rows), 1000):
                    batch = rows[i : i + 1000]
                    stmt = pg_insert(DeIndexPrices).values(batch)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["date", "index_code"],
                        set_={
                            "open": stmt.excluded.open,
                            "high": stmt.excluded.high,
                            "low": stmt.excluded.low,
                            "close": stmt.excluded.close,
                            "pe_ratio": stmt.excluded.pe_ratio,
                            "pb_ratio": stmt.excluded.pb_ratio,
                            "div_yield": stmt.excluded.div_yield,
                        },
                    )
                    await session.execute(stmt)

        async with _done_lock:
            _done_counter += 1

        logger.info(
            "done",
            index=symbol,
            rows=len(rows),
            dates=f"{all_dates[0]}→{all_dates[-1]}",
            progress=f"{_done_counter}/{total_indices}",
            pepb="yes" if pepb_by_date else "no",
        )
        return symbol, len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main(years: int = 10, workers: int = 10) -> None:
    engine = create_async_engine(
        DATABASE_URL,
        pool_size=workers + 2,
        max_overflow=5,
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # Step 1: Seed master
    async with session_factory() as session:
        async with session.begin():
            index_map = await seed_index_master(session)
    logger.info("master_seeded", count=len(index_map))

    # Step 2: Parallel backfill
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=years * 365)
    chunks = _date_chunks(start_date, end_date)

    logger.info(
        "backfill_start",
        indices=len(index_map),
        workers=workers,
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        chunks_per_index=len(chunks),
    )

    sem = asyncio.Semaphore(workers)
    tasks = [
        _backfill_one_index(sem, session_factory, symbol, name, chunks, len(index_map))
        for symbol, name in index_map.items()
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    total_rows = 0
    successes = 0
    for r in results:
        if isinstance(r, Exception):
            logger.error("worker_exception", error=str(r))
        else:
            _, rows = r
            total_rows += rows
            if rows > 0:
                successes += 1

    logger.info("backfill_complete", total_rows=total_rows, indices_with_data=successes)
    await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NSE Index Backfill")
    parser.add_argument("--years", type=int, default=10, help="Years of history")
    parser.add_argument("--workers", type=int, default=10, help="Concurrent workers")
    args = parser.parse_args()

    asyncio.run(main(years=args.years, workers=args.workers))
