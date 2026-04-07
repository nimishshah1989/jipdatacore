"""Index constituents pipeline — fetches current constituents from niftyindices.com.

Records current snapshot into de_index_constituents with effective_from = today.
Constituent CSVs are available at:
    https://www.niftyindices.com/IndexConstituent/ind_{slug}list.csv

Usage:
    python -m app.pipelines.indices.index_constituents
"""

from __future__ import annotations

import asyncio
import csv
import io
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import httpx
import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.models.instruments import DeIndexConstituents, DeIndexMaster, DeInstrument
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = structlog.get_logger(__name__)

NIFTY_BASE = "https://www.niftyindices.com/IndexConstituent"
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

# Mapping: index_code (from de_index_master) → CSV filename slug on niftyindices.com
# Only indices that have constituent CSVs are mapped here.
INDEX_CONSTITUENT_SLUGS: dict[str, str] = {
    "NIFTY 50": "ind_nifty50list",
    "NIFTY NEXT 50": "ind_niftynext50list",
    "NIFTY 100": "ind_nifty100list",
    "NIFTY 200": "ind_nifty200list",
    "NIFTY 500": "ind_nifty500list",
    "NIFTY MIDCAP 50": "ind_niftymidcap50list",
    "NIFTY MIDCAP 100": "ind_niftymidcap100list",
    "NIFTY MIDCAP 150": "ind_niftymidcap150list",
    "NIFTY SMLCAP 50": "ind_niftysmallcap50list",
    "NIFTY SMLCAP 100": "ind_niftysmallcap100list",
    "NIFTY SMLCAP 250": "ind_niftysmallcap250list",
    "NIFTY MIDSML 400": "ind_niftymidsmallcap400list",
    "NIFTY BANK": "ind_niftybanklist",
    "NIFTY AUTO": "ind_niftyautolist",
    "NIFTY FIN SERVICE": "ind_niftyfinancelist",
    "NIFTY FMCG": "ind_niftyfmcglist",
    "NIFTY IT": "ind_niftyitlist",
    "NIFTY MEDIA": "ind_niftymedialist",
    "NIFTY METAL": "ind_niftymetallist",
    "NIFTY PHARMA": "ind_niftypharmalist",
    "NIFTY PSU BANK": "ind_niftypsubanksList",
    "NIFTY PVT BANK": "ind_niftyprivatebanklist",
    "NIFTY REALTY": "ind_niftyrealtylist",
    "NIFTY HEALTHCARE": "ind_niftyhaborhealthcarelist",
    "NIFTY ENERGY": "ind_niftyenergylist",
    "NIFTY INFRA": "ind_niftyinfrastructurelist",
    "NIFTY COMMODITIES": "ind_niftycommoditieslist",
    "NIFTY CONSUMPTION": "ind_niftyconsumptionlist",
    "NIFTY CPSE": "ind_niftycpselist",
    "NIFTY MNC": "ind_niftymnclist",
    "NIFTY PSE": "ind_niftypselist",
    "NIFTY DIV OPPS 50": "ind_niftydividendopportunities50list",
    "NIFTY ALPHA 50": "ind_niftyalpha50list",
    "NIFTY50 EQL WGT": "ind_nifty50equalweightlist",
    "NIFTY100 EQL WGT": "ind_nifty100equalweightlist",
    "NIFTY100 LOWVOL30": "ind_nifty100lowvolatility30list",
    "NIFTY200 QUALTY30": "ind_nifty200quality30list",
    "NIFTY200MOMENTM30": "ind_nifty200momentum30list",
    "NIFTY HIGHBETA 50": "ind_niftyhighbeta50list",
    "NIFTY LOW VOL 50": "ind_niftylowvolatility50list",
    "NIFTY IND DEFENCE": "ind_niftyindiadefencelist",
    "NIFTY TOTAL MKT": "ind_niftytotalmarketlist",
    "NIFTY500 MULTICAP": "ind_nifty500multicap502525list",
    "NIFTY LARGEMID250": "ind_niftylargemidcap250list",
    "NIFTY MID SELECT": "ind_niftymidcapselectlist",
    "NIFTY OIL AND GAS": "ind_niftyoilgaslist",
    "NIFTY CONSR DURBL": "ind_niftyconsumerdurableslist",
    "NIFTY SERV SECTOR": "ind_niftyservicessectorlist",
}


def _safe_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == "" or value == "-":
        return None
    try:
        return Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError, TypeError):
        return None


async def _fetch_constituent_csv(
    client: httpx.AsyncClient,
    slug: str,
) -> list[dict[str, str]]:
    """Download a constituent CSV from niftyindices.com and parse it."""
    url = f"{NIFTY_BASE}/{slug}.csv"
    resp = await client.get(url, headers=REQUEST_HEADERS)
    resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text))
    return list(reader)


async def _load_instrument_symbols(session: AsyncSession) -> dict[str, str]:
    """Return dict mapping symbol → instrument_id (as string UUID)."""
    result = await session.execute(
        select(DeInstrument.current_symbol, DeInstrument.id)
    )
    return {row[0]: str(row[1]) for row in result.fetchall()}


class IndexConstituentsPipeline(BasePipeline):
    """Fetch current index constituents from niftyindices.com CSVs.

    Records snapshot into de_index_constituents with effective_from = business_date.
    Only processes indices that have known CSV slugs.
    Matches constituent symbols to de_instrument records.
    """

    pipeline_name = "index_constituents"
    requires_trading_day = False  # Can run any day
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info("index_constituents_start", business_date=business_date.isoformat())

        # Load instrument symbol → ID mapping
        symbol_map = await _load_instrument_symbols(session)

        # Load known index codes from master
        result = await session.execute(select(DeIndexMaster.index_code))
        known_codes = {row[0] for row in result.fetchall()}

        rows_processed = 0
        rows_failed = 0

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            for index_code, slug in INDEX_CONSTITUENT_SLUGS.items():
                if index_code not in known_codes:
                    logger.debug("index_not_in_master", index_code=index_code)
                    continue

                try:
                    csv_rows = await _fetch_constituent_csv(client, slug)
                except httpx.HTTPStatusError as e:
                    logger.warning(
                        "constituent_csv_failed",
                        index_code=index_code,
                        slug=slug,
                        status=e.response.status_code,
                    )
                    rows_failed += 1
                    continue
                except Exception as e:
                    logger.warning(
                        "constituent_csv_error",
                        index_code=index_code,
                        error=str(e),
                    )
                    rows_failed += 1
                    continue

                batch: list[dict[str, Any]] = []
                for row in csv_rows:
                    symbol = (row.get("Symbol") or "").strip()
                    if not symbol or symbol not in symbol_map:
                        continue

                    weight = _safe_decimal(row.get("Weight(%)", row.get("Weightage", "")))
                    batch.append({
                        "index_code": index_code,
                        "instrument_id": symbol_map[symbol],
                        "effective_from": business_date,
                        "weight_pct": weight,
                    })

                if batch:
                    stmt = pg_insert(DeIndexConstituents).values(batch)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["index_code", "instrument_id", "effective_from"],
                        set_={
                            "weight_pct": stmt.excluded.weight_pct,
                        },
                    )
                    await session.execute(stmt)
                    await session.flush()
                    rows_processed += len(batch)

                logger.info(
                    "constituents_loaded",
                    index_code=index_code,
                    constituents=len(batch),
                )

                # Rate limiting
                await asyncio.sleep(0.5)

        logger.info(
            "index_constituents_complete",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
        return ExecutionResult(rows_processed=rows_processed, rows_failed=rows_failed)


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------
async def main() -> None:
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    engine = create_async_engine(
        get_settings().database_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    pipeline = IndexConstituentsPipeline()
    async with async_session() as session:
        async with session.begin():
            result = await pipeline.execute(
                business_date=date.today(),
                session=session,
                run_log=DePipelineLog(
                    pipeline_name="index_constituents",
                    business_date=date.today(),
                    run_number=1,
                    status="running",
                    started_at=datetime.utcnow(),
                ),
            )
            print(f"Done: {result.rows_processed} constituents, {result.rows_failed} failed")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
