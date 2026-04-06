"""Fetch ALL NSE index constituent CSVs, insert into de_index_constituents,
extract Industry for de_instrument, and build sector mapping from index membership.

Two URL sources tried per index (fallback order):
  1. niftyindices.com  — https://www.niftyindices.com/IndexConstituent/{slug}.csv
  2. nsearchives.nseindia.com — https://nsearchives.nseindia.com/content/indices/{slug}.csv

Usage (inside Docker):
    python scripts/fetch_constituents_and_sectors.py
"""

from __future__ import annotations

import asyncio
import csv
import io
import os
import sys
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import httpx
import structlog

# --- Bootstrap: ensure app is importable ---
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

try:
    from app.config import get_settings
    DATABASE_URL = get_settings().database_url
except Exception:
    from dotenv import load_dotenv
    load_dotenv()
    DATABASE_URL = os.environ["DATABASE_URL"]

from app.models.instruments import DeIndexConstituents, DeIndexMaster, DeInstrument

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# URL sources (tried in order)
# ---------------------------------------------------------------------------
NIFTYINDICES_BASE = "https://www.niftyindices.com/IndexConstituent"
NSEARCHIVES_BASE = "https://nsearchives.nseindia.com/content/indices"

HEADERS_NIFTYINDICES = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

HEADERS_NSEARCHIVES = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.nseindia.com/",
}

# ---------------------------------------------------------------------------
# Index → CSV slug mapping
# Key = index_code as stored in de_index_master
# Value = (niftyindices_slug, nsearchives_slug)
#   - either can be None if unknown for that source
# ---------------------------------------------------------------------------
INDEX_CSV_SLUGS: dict[str, tuple[Optional[str], Optional[str]]] = {
    # Broad-market
    "NIFTY 50": ("ind_nifty50list", "ind_nifty50list"),
    "NIFTY NEXT 50": ("ind_niftynext50list", "ind_niftynext50list"),
    "NIFTY 100": ("ind_nifty100list", "ind_nifty100list"),
    "NIFTY 200": ("ind_nifty200list", "ind_nifty200list"),
    "NIFTY 500": ("ind_nifty500list", "ind_nifty500list"),
    "NIFTY MIDCAP 50": ("ind_niftymidcap50list", "ind_niftymidcap50list"),
    "NIFTY MIDCAP 100": ("ind_niftymidcap100list", "ind_niftymidcap100list"),
    "NIFTY MIDCAP 150": ("ind_niftymidcap150list", "ind_niftymidcap150list"),
    "NIFTY SMLCAP 50": ("ind_niftysmallcap50list", "ind_niftysmallcap50list"),
    "NIFTY SMLCAP 100": ("ind_niftysmallcap100list", "ind_niftysmallcap100list"),
    "NIFTY SMLCAP 250": ("ind_niftysmallcap250list", "ind_niftysmallcap250list"),
    "NIFTY MIDSML 400": ("ind_niftymidsmallcap400list", "ind_niftymidsmallcap400list"),
    "NIFTY TOTAL MKT": ("ind_niftytotalmarketlist", "ind_niftytotalmarketlist"),
    "NIFTY500 MULTICAP": ("ind_nifty500multicap502525list", "ind_nifty500multicap502525list"),
    "NIFTY LARGEMID250": ("ind_niftylargemidcap250list", "ind_niftylargemidcap250list"),
    "NIFTY MID SELECT": ("ind_niftymidcapselectlist", "ind_niftymidcapselectlist"),
    # Sectoral
    "NIFTY AUTO": ("ind_niftyautolist", "ind_niftyautolist"),
    "NIFTY BANK": ("ind_niftybanklist", "ind_niftybanklist"),
    "NIFTY FIN SERVICE": ("ind_niftyfinancelist", "ind_niftyfinancialserviceslist"),
    "NIFTY FMCG": ("ind_niftyfmcglist", "ind_niftyfmcglist"),
    "NIFTY IT": ("ind_niftyitlist", "ind_niftyitlist"),
    "NIFTY MEDIA": ("ind_niftymedialist", "ind_niftymedialist"),
    "NIFTY METAL": ("ind_niftymetallist", "ind_niftymetallist"),
    "NIFTY PHARMA": ("ind_niftypharmalist", "ind_niftypharmalist"),
    "NIFTY PSU BANK": ("ind_niftypsubanksList", "ind_niftypsubanklist"),
    "NIFTY PVT BANK": ("ind_niftyprivatebanklist", "ind_niftyprivatebanklist"),
    "NIFTY REALTY": ("ind_niftyrealtylist", "ind_niftyrealtylist"),
    "NIFTY HEALTHCARE": ("ind_niftyhaborhealthcarelist", "ind_niftyhealthcarelist"),
    "NIFTY ENERGY": ("ind_niftyenergylist", "ind_niftyenergylist"),
    "NIFTY INFRA": ("ind_niftyinfrastructurelist", "ind_niftyinfrastructurelist"),
    "NIFTY COMMODITIES": ("ind_niftycommoditieslist", "ind_niftycommoditieslist"),
    "NIFTY CONSUMPTION": ("ind_niftyconsumptionlist", "ind_niftyconsumptionlist"),
    "NIFTY CPSE": ("ind_niftycpselist", "ind_niftycpselist"),
    "NIFTY MNC": ("ind_niftymnclist", "ind_niftymnclist"),
    "NIFTY PSE": ("ind_niftypselist", "ind_niftypselist"),
    "NIFTY CONSR DURBL": ("ind_niftyconsumerdurableslist", "ind_niftyconsumerdurableslist"),
    "NIFTY OIL AND GAS": ("ind_niftyoilgaslist", "ind_niftyoilandgaslist"),
    "NIFTY SERV SECTOR": ("ind_niftyservicessectorlist", "ind_niftyservicessectorlist"),
    "NIFTY IND DEFENCE": ("ind_niftyindiadefencelist", "ind_niftyindiadefencelist"),
    "NIFTY CAPITAL MKT": (None, "ind_niftycapitalmarketslist"),
    "NIFTY CHEMICALS": (None, "ind_niftychemicalslist"),
    # Strategy / Factor
    "NIFTY DIV OPPS 50": ("ind_niftydividendopportunities50list", "ind_niftydividendopportunities50list"),
    "NIFTY ALPHA 50": ("ind_niftyalpha50list", "ind_niftyalpha50list"),
    "NIFTY50 EQL WGT": ("ind_nifty50equalweightlist", "ind_nifty50equalweightlist"),
    "NIFTY100 EQL WGT": ("ind_nifty100equalweightlist", "ind_nifty100equalweightlist"),
    "NIFTY100 LOWVOL30": ("ind_nifty100lowvolatility30list", "ind_nifty100lowvolatility30list"),
    "NIFTY200 QUALTY30": ("ind_nifty200quality30list", "ind_nifty200quality30list"),
    "NIFTY200MOMENTM30": ("ind_nifty200momentum30list", "ind_nifty200momentum30list"),
    "NIFTY HIGHBETA 50": ("ind_niftyhighbeta50list", "ind_niftyhighbeta50list"),
    "NIFTY LOW VOL 50": ("ind_niftylowvolatility50list", "ind_niftylowvolatility50list"),
}

# ---------------------------------------------------------------------------
# Sector mapping from index membership (priority order — first match wins)
# ---------------------------------------------------------------------------
CORE_SECTORS: list[tuple[str, str]] = [
    ("NIFTY IT", "IT"),
    ("NIFTY PHARMA", "Pharma"),
    ("NIFTY HEALTHCARE", "Healthcare"),
    ("NIFTY BANK", "Banking"),
    ("NIFTY PSU BANK", "Banking"),
    ("NIFTY PVT BANK", "Banking"),
    ("NIFTY AUTO", "Automobile"),
    ("NIFTY METAL", "Metal"),
    ("NIFTY REALTY", "Realty"),
    ("NIFTY MEDIA", "Media"),
    ("NIFTY FMCG", "FMCG"),
    ("NIFTY OIL AND GAS", "Oil & Gas"),
    ("NIFTY CHEMICALS", "Chemicals"),
    ("NIFTY CONSR DURBL", "Consumer Durables"),
    ("NIFTY CAPITAL MKT", "Capital Markets"),
    ("NIFTY ENERGY", "Energy"),
    ("NIFTY INFRA", "Infrastructure"),
    ("NIFTY FIN SERVICE", "Financial Services"),
    ("NIFTY COMMODITIES", "Commodities"),
    ("NIFTY IND DEFENCE", "Defence"),
]

# Indices whose CSVs have Industry column (broad indices)
INDUSTRY_SOURCE_INDICES = {"NIFTY 500", "NIFTY MIDSML 400", "NIFTY TOTAL MKT"}


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


async def _try_fetch_csv(
    client: httpx.AsyncClient,
    index_code: str,
    niftyindices_slug: Optional[str],
    nsearchives_slug: Optional[str],
) -> Optional[list[dict[str, str]]]:
    """Try downloading constituent CSV from both sources. Return parsed rows or None."""
    urls_to_try: list[tuple[str, dict[str, str]]] = []

    if niftyindices_slug:
        urls_to_try.append((
            f"{NIFTYINDICES_BASE}/{niftyindices_slug}.csv",
            HEADERS_NIFTYINDICES,
        ))
    if nsearchives_slug:
        urls_to_try.append((
            f"{NSEARCHIVES_BASE}/{nsearchives_slug}.csv",
            HEADERS_NSEARCHIVES,
        ))

    for url, headers in urls_to_try:
        try:
            resp = await client.get(url, headers=headers)
            if resp.status_code == 200 and len(resp.text) > 50:
                # Verify it looks like CSV
                reader = csv.DictReader(io.StringIO(resp.text))
                rows = list(reader)
                if rows and any("Symbol" in r for r in rows[:1]):
                    logger.info("csv_downloaded", index=index_code, url=url, rows=len(rows))
                    return rows
        except Exception as e:
            logger.debug("csv_fetch_failed", index=index_code, url=url, error=str(e))

    return None


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------
async def run() -> None:
    engine = create_async_engine(
        DATABASE_URL,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    today = date.today()

    # Stats
    csvs_downloaded = 0
    csvs_failed: list[str] = []
    constituents_inserted = 0
    industry_updated = 0
    sector_mapped = 0

    async with session_factory() as session:
        async with session.begin():
            # --- Load symbol → instrument_id map ---
            result = await session.execute(
                select(DeInstrument.current_symbol, DeInstrument.id)
            )
            symbol_map = {row[0]: str(row[1]) for row in result.fetchall()}
            logger.info("instruments_loaded", count=len(symbol_map))

            # --- Load known index codes ---
            result = await session.execute(select(DeIndexMaster.index_code))
            known_codes = {row[0] for row in result.fetchall()}
            logger.info("index_master_loaded", count=len(known_codes))

            # --- Fetch and ingest all CSVs ---
            async with httpx.AsyncClient(
                timeout=30.0, follow_redirects=True
            ) as client:
                # Warm up NSE cookies
                try:
                    await client.get(
                        "https://www.nseindia.com",
                        headers=HEADERS_NSEARCHIVES,
                    )
                except Exception:
                    pass

                for index_code, (ni_slug, nsa_slug) in INDEX_CSV_SLUGS.items():
                    if index_code not in known_codes:
                        logger.debug("index_not_in_master", index_code=index_code)
                        continue

                    rows = await _try_fetch_csv(client, index_code, ni_slug, nsa_slug)
                    if rows is None:
                        csvs_failed.append(index_code)
                        continue
                    csvs_downloaded += 1

                    # --- 1b: Upsert into de_index_constituents ---
                    batch: list[dict[str, Any]] = []
                    for row in rows:
                        symbol = (row.get("Symbol") or "").strip()
                        if not symbol or symbol not in symbol_map:
                            continue
                        weight = _safe_decimal(
                            row.get("Weight(%)", row.get("Weightage", ""))
                        )
                        batch.append({
                            "index_code": index_code,
                            "instrument_id": symbol_map[symbol],
                            "effective_from": today,
                            "weight_pct": weight,
                        })

                    if batch:
                        stmt = pg_insert(DeIndexConstituents).values(batch)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=[
                                "index_code", "instrument_id", "effective_from"
                            ],
                            set_={"weight_pct": stmt.excluded.weight_pct},
                        )
                        await session.execute(stmt)
                        await session.flush()
                        constituents_inserted += len(batch)

                    # --- 1c: Extract Industry from broad-market CSVs ---
                    if index_code in INDUSTRY_SOURCE_INDICES:
                        for row in rows:
                            symbol = (row.get("Symbol") or "").strip()
                            industry = (row.get("Industry") or "").strip()
                            if symbol and industry and symbol in symbol_map:
                                result2 = await session.execute(
                                    update(DeInstrument)
                                    .where(DeInstrument.current_symbol == symbol)
                                    .where(DeInstrument.industry.is_(None))
                                    .values(industry=industry)
                                )
                                if result2.rowcount > 0:
                                    industry_updated += result2.rowcount

                    logger.info(
                        "index_processed",
                        index_code=index_code,
                        constituents=len(batch),
                    )

                    # Rate limit
                    await asyncio.sleep(0.5)

            # --- Step 2: Sector mapping from index membership ---
            logger.info("sector_mapping_start")

            for index_code, sector_name in CORE_SECTORS:
                if index_code not in known_codes:
                    continue

                # Find all instrument_ids in this index (today's snapshot)
                result3 = await session.execute(
                    select(DeIndexConstituents.instrument_id).where(
                        DeIndexConstituents.index_code == index_code,
                        DeIndexConstituents.effective_from == today,
                    )
                )
                instrument_ids = [str(row[0]) for row in result3.fetchall()]

                if not instrument_ids:
                    continue

                # Update sector where NULL
                result4 = await session.execute(
                    update(DeInstrument)
                    .where(DeInstrument.id.in_(instrument_ids))
                    .where(DeInstrument.sector.is_(None))
                    .values(sector=sector_name)
                )
                if result4.rowcount > 0:
                    sector_mapped += result4.rowcount
                    logger.info(
                        "sector_assigned",
                        index_code=index_code,
                        sector=sector_name,
                        count=result4.rowcount,
                    )

    # --- Step 3: Report ---
    async with session_factory() as session:
        # Sector distribution
        result_dist = await session.execute(text(
            "SELECT sector, COUNT(*) FROM de_instrument "
            "WHERE sector IS NOT NULL GROUP BY sector ORDER BY COUNT(*) DESC"
        ))
        sector_dist = result_dist.fetchall()

        # Stocks without sector
        result_no_sector = await session.execute(text(
            "SELECT COUNT(*) FROM de_instrument WHERE sector IS NULL AND is_active = true"
        ))
        no_sector_count = result_no_sector.scalar()

        # Stocks without industry
        result_no_industry = await session.execute(text(
            "SELECT COUNT(*) FROM de_instrument WHERE industry IS NULL AND is_active = true"
        ))
        no_industry_count = result_no_industry.scalar()

    print("\n" + "=" * 70)
    print("INDEX CONSTITUENTS & SECTOR MAPPING — REPORT")
    print("=" * 70)
    print(f"CSVs successfully downloaded:  {csvs_downloaded}")
    print(f"CSVs failed:                   {len(csvs_failed)}")
    if csvs_failed:
        print(f"  Failed indices: {', '.join(csvs_failed)}")
    print(f"Index constituents upserted:   {constituents_inserted}")
    print(f"Industry values updated:       {industry_updated}")
    print(f"Stocks sector-mapped:          {sector_mapped}")
    print(f"Active stocks without sector:  {no_sector_count}")
    print(f"Active stocks without industry:{no_industry_count}")
    print()
    print("SECTOR DISTRIBUTION:")
    print("-" * 40)
    for sector, count in sector_dist:
        print(f"  {sector:<30s} {count:>5}")
    print("=" * 70)

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run())
