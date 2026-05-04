"""Atlas-M0 -- one-shot loader for NIFTY MICROCAP 250 constituents.

The general fetch_constituents_and_sectors.py script:
  1. doesn't have NIFTY MICROCAP 250 in its slug map, and
  2. crashes on the partial unique constraint `uix_index_constituent_active`
     when re-running against indices that already have an active row.

This script is narrow: download the Microcap 250 CSV, look up each symbol in
de_instrument, and insert into de_index_constituents -- closing any existing
active row for the same (index_code, instrument_id) first so the partial
unique constraint is satisfied.

Usage (inside the data-engine container):
    python /app/scripts/atlas_m0_fetch_microcap250.py
"""
from __future__ import annotations

import asyncio
import csv
import io
import os
import sys
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import httpx
from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from app.config import get_settings
    DATABASE_URL = get_settings().database_url
except Exception:  # pragma: no cover -- container path
    from dotenv import load_dotenv
    load_dotenv()
    DATABASE_URL = os.environ["DATABASE_URL"]

from app.models.instruments import DeIndexConstituents, DeInstrument

INDEX_CODE = "NIFTY MICROCAP250"
INDEX_NAME = "NIFTY MICROCAP 250"
SLUG_CANDIDATES = [
    # niftyindices.com primary
    "https://www.niftyindices.com/IndexConstituent/ind_niftymicrocap250list.csv",
    "https://www.niftyindices.com/IndexConstituent/ind_niftymicrocap250_list.csv",
    # nsearchives.nseindia.com fallback
    "https://nsearchives.nseindia.com/content/indices/ind_niftymicrocap250list.csv",
    "https://nsearchives.nseindia.com/content/indices/ind_niftymicrocap250_list.csv",
]


def _safe_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == "" or value == "-":
        return None
    try:
        return Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError, TypeError):
        return None


async def _fetch_csv() -> list[dict[str, str]]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept": "text/csv,*/*",
    }
    async with httpx.AsyncClient(headers=headers, timeout=30, follow_redirects=True) as client:
        for url in SLUG_CANDIDATES:
            try:
                r = await client.get(url)
                r.raise_for_status()
                if r.text.strip().lower().startswith(("<html", "<!doctype")):
                    continue
                reader = csv.DictReader(io.StringIO(r.text))
                rows = [dict(row) for row in reader]
                if rows:
                    print(f"[microcap250] fetched {len(rows)} rows from {url}", flush=True)
                    return rows
            except Exception as exc:
                print(f"[microcap250] {url} -> {exc}", flush=True)
                continue
    raise RuntimeError("All URL candidates failed for NIFTY MICROCAP 250")


async def _load_symbol_map(session) -> dict[str, Any]:
    result = await session.execute(
        select(DeInstrument.current_symbol, DeInstrument.id).where(
            DeInstrument.is_active == True  # noqa: E712
        )
    )
    return {(row[0] or "").upper(): row[1] for row in result if row[0]}


async def main() -> int:
    rows = await _fetch_csv()

    engine = create_async_engine(DATABASE_URL)
    Session = async_sessionmaker(engine, expire_on_commit=False)

    today = date.today()
    yesterday = today - timedelta(days=1)

    async with Session() as session:
        async with session.begin():
            symbol_map = await _load_symbol_map(session)

            new_pairs: list[dict[str, Any]] = []
            unmatched: list[str] = []
            for row in rows:
                symbol = (row.get("Symbol") or row.get("symbol") or "").strip().upper()
                if not symbol:
                    continue
                instrument_id = symbol_map.get(symbol)
                if instrument_id is None:
                    unmatched.append(symbol)
                    continue
                weight = _safe_decimal(row.get("Weight(%)") or row.get("Weightage"))
                new_pairs.append({
                    "index_code": INDEX_CODE,
                    "instrument_id": instrument_id,
                    "effective_from": today,
                    "weight_pct": weight,
                })

            print(
                f"[microcap250] matched={len(new_pairs)} unmatched={len(unmatched)}",
                flush=True,
            )
            if unmatched:
                print(f"[microcap250] sample unmatched: {unmatched[:10]}", flush=True)

            if not new_pairs:
                print("[microcap250] nothing to insert -- aborting", flush=True)
                return 0

            # Close any existing active rows for this index that aren't in the new set
            # (avoids violating uix_index_constituent_active when re-running).
            new_instrument_ids = {p["instrument_id"] for p in new_pairs}
            await session.execute(
                update(DeIndexConstituents)
                .where(DeIndexConstituents.index_code == INDEX_CODE)
                .where(DeIndexConstituents.effective_to.is_(None))
                .where(~DeIndexConstituents.instrument_id.in_(new_instrument_ids))
                .values(effective_to=yesterday)
            )

            # Skip instruments that already have an active row for this index
            # (no-op for them); insert only the new ones.
            existing_active = await session.execute(
                select(DeIndexConstituents.instrument_id).where(
                    DeIndexConstituents.index_code == INDEX_CODE,
                    DeIndexConstituents.effective_to.is_(None),
                )
            )
            already_active = {r[0] for r in existing_active}

            to_insert = [p for p in new_pairs if p["instrument_id"] not in already_active]
            print(
                f"[microcap250] active_already={len(already_active)} to_insert={len(to_insert)}",
                flush=True,
            )

            if to_insert:
                stmt = pg_insert(DeIndexConstituents).values(to_insert)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["index_code", "instrument_id", "effective_from"],
                    set_={"weight_pct": stmt.excluded.weight_pct},
                )
                await session.execute(stmt)

        # Verify
        count = await session.execute(
            text("SELECT COUNT(*) FROM de_index_constituents "
                 "WHERE index_code = :ic AND effective_to IS NULL"),
            {"ic": INDEX_CODE},
        )
        print(f"[microcap250] active constituents now: {count.scalar()}", flush=True)

    await engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
