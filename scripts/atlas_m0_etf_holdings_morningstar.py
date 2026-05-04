"""Atlas-M0 -- ETF holdings via Morningstar Ticker endpoint (XML).

Earlier diagnostic confirmed:
  - URL pattern: https://api.morningstar.com/v2/service/mf/{SERVICE}/{IdType}/{Identifier}?accesscode={ACCESS}
  - id_type 'Ticker' WORKS for Indian ETFs (NIFTYBEES returns 200 with holdings)
  - id_type 'ISIN' WORKS too (INF204KB14I2 -> NIFTYBEES holdings)
  - id_type 'FundId' / 'SecId' return 404 -- they don't exist on this service
  - Response is XML, not JSON. Format: <response><data><api><FHV2-Holdings>
                                            <HoldingDetail>...</HoldingDetail>
                                            ...

This script iterates de_etf_master rows with country='IN' (61 ETFs), calls
Morningstar with id_type='Ticker' (stripping the .NS suffix), parses the
XML, extracts each HoldingDetail's ISIN, weight, and security name, resolves
ISIN -> de_instrument.id, and upserts de_etf_holdings.

ETFs that 404 (Morningstar doesn't cover them) are documented as accepted
limitations.

Service / universe / access (per architect, 2026-05-04):
    SERVICE  = fq9mxhk7xeb20f3b
    UNIVERSE = q3zv6b817mp4fz0f
    ACCESS   = ftijxp6pf11ezmizn19otbz18ghq2iu4

Usage (inside data-engine container):
    python /app/scripts/atlas_m0_etf_holdings_morningstar.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import xml.etree.ElementTree as ET
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional

import httpx
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from app.config import get_settings
    DATABASE_URL = get_settings().database_url
except Exception:
    from dotenv import load_dotenv
    load_dotenv()
    DATABASE_URL = os.environ["DATABASE_URL"]

from app.models.etf import DeEtfMaster
from app.models.holdings import DeEtfHoldings
from app.models.instruments import DeInstrument

SERVICE = "fq9mxhk7xeb20f3b"
ACCESS = os.environ.get(
    "MORNINGSTAR_ACCESS_CODE", "ftijxp6pf11ezmizn19otbz18ghq2iu4"
)
DATAPOINTS = "Holdings"
RATE_PER_SEC = 5
TIMEOUT = 30.0


def _safe_decimal(s: Optional[str]) -> Optional[Decimal]:
    if not s:
        return None
    try:
        return Decimal(s.strip())
    except (InvalidOperation, AttributeError):
        return None


def parse_holdings_xml(body: str) -> tuple[Optional[date], list[dict]]:
    """Parse Morningstar FHV2-Holdings XML.

    Returns (as_of_date, [{isin, name, weight (decimal fraction)}, ...]).
    Weight in the response is a percentage (e.g. 3.16534 = 3.16534 %); we
    convert to a 0..1 fraction for Numeric(8,6) storage.
    """
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return None, []

    # Many Morningstar responses include a top-level HoldingDate; if absent,
    # fall back to the first HoldingDetail's FirstBoughtDate or None (caller
    # will use today's date).
    hd_text = None
    for elem in root.iter():
        if elem.tag.endswith("HoldingDate"):
            hd_text = (elem.text or "").strip()
            break

    as_of = None
    if hd_text:
        try:
            as_of = date.fromisoformat(hd_text[:10])
        except ValueError:
            pass

    holdings: list[dict] = []
    for hd in root.iter():
        if not hd.tag.endswith("HoldingDetail"):
            continue
        d: dict[str, Optional[str]] = {}
        for child in hd:
            tag = child.tag.split("}")[-1]  # strip namespace if any
            d[tag] = (child.text or "").strip() if child.text else None

        isin = d.get("ISIN")
        name = d.get("Name")
        weight_pct = _safe_decimal(d.get("Weighting"))
        if weight_pct is None or not isin:
            continue
        if weight_pct < 0:
            continue
        weight = (weight_pct * Decimal("0.01")).quantize(Decimal("0.000001"))
        if weight > Decimal("1"):
            weight = Decimal("1")
        holdings.append({"isin": isin, "name": name, "weight": weight})

    return as_of, holdings


async def _fetch_one(client: httpx.AsyncClient, ticker: str) -> Optional[str]:
    """Fetch holdings XML for a single ticker. Returns body or None on 404."""
    # Strip .NS suffix used by yfinance/de_etf_master
    short = ticker.upper().replace(".NS", "").replace(".BO", "").strip()
    url = (
        f"https://api.morningstar.com/v2/service/mf/{SERVICE}/Ticker/{short}"
    )
    try:
        r = await client.get(url, params={
            "accesscode": ACCESS, "datapoints": DATAPOINTS,
        })
    except (httpx.HTTPError, httpx.TimeoutException) as exc:
        print(f"  [{ticker}] ERROR: {exc}", flush=True)
        return None
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        print(f"  [{ticker}] HTTP {r.status_code}: {r.text[:200]}", flush=True)
        return None
    return r.text


async def main() -> int:
    engine = create_async_engine(DATABASE_URL)
    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as session:
        etfs = (await session.execute(
            select(DeEtfMaster.ticker, DeEtfMaster.name).where(
                DeEtfMaster.is_active == True,  # noqa: E712
                DeEtfMaster.country == "IN",
            )
        )).all()

        # ISIN -> instrument_id map for resolution
        isin_rows = (await session.execute(
            select(DeInstrument.isin, DeInstrument.id).where(
                DeInstrument.isin.isnot(None)
            )
        )).all()
        isin_to_iid = {(r[0] or "").upper(): r[1] for r in isin_rows if r[0]}

    print(f"Indian ETFs in de_etf_master: {len(etfs)}")
    print(f"de_instrument ISIN map size:  {len(isin_to_iid)}")

    today = date.today()
    matched = 0
    not_found = []
    parse_failed = []
    no_resolvable = []
    rows_inserted_total = 0

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        for i, (ticker, etf_name) in enumerate(etfs):
            # Throttle ~5 req/s
            if i and i % RATE_PER_SEC == 0:
                await asyncio.sleep(1.0)

            body = await _fetch_one(client, ticker)
            if body is None:
                not_found.append(ticker)
                continue

            as_of, holdings = parse_holdings_xml(body)
            if not holdings:
                parse_failed.append(ticker)
                continue
            effective = as_of or today

            # Resolve ISINs and prepare upsert rows
            upsert_rows = []
            unresolved = 0
            for h in holdings:
                instrument_id = isin_to_iid.get(h["isin"].upper())
                if instrument_id is None:
                    unresolved += 1
                    continue
                upsert_rows.append({
                    "ticker": ticker,
                    "instrument_id": instrument_id,
                    "as_of_date": effective,
                    "weight": h["weight"],
                    "last_disclosed_date": today,
                })

            if not upsert_rows:
                no_resolvable.append((ticker, len(holdings), unresolved))
                continue

            async with Session() as session:
                async with session.begin():
                    stmt = pg_insert(DeEtfHoldings).values(upsert_rows)
                    stmt = stmt.on_conflict_do_update(
                        constraint="pk_de_etf_holdings",
                        set_={
                            "weight": stmt.excluded.weight,
                            "last_disclosed_date": stmt.excluded.last_disclosed_date,
                        },
                    )
                    await session.execute(stmt)
            rows_inserted_total += len(upsert_rows)
            matched += 1
            print(f"  [{i+1}/{len(etfs)}] {ticker}: holdings={len(holdings)} "
                  f"resolved={len(upsert_rows)} unresolved={unresolved} "
                  f"as_of={effective}", flush=True)

    # Verify
    async with Session() as session:
        n_distinct = (await session.execute(text(
            "SELECT COUNT(DISTINCT ticker) FROM de_etf_holdings"))).scalar()
        n_rows = (await session.execute(text(
            "SELECT COUNT(*) FROM de_etf_holdings"))).scalar()
    await engine.dispose()

    print("\n=== Morningstar ETF holdings -- summary ===")
    print(f"Indian ETFs probed:           {len(etfs)}")
    print(f"  matched + upserted:         {matched}")
    print(f"  Morningstar 404 (no data):  {len(not_found)}")
    print(f"  XML parse / 0 holdings:     {len(parse_failed)}")
    print(f"  no ISIN resolved:           {len(no_resolvable)}")
    print(f"\nRows upserted this run:       {rows_inserted_total}")
    print(f"de_etf_holdings totals: distinct_etfs={n_distinct} rows={n_rows}")
    if not_found[:10]:
        print(f"\nSample 404 (Morningstar doesn't cover): {not_found[:10]}")
    if no_resolvable[:5]:
        print(f"Sample 'no ISIN resolved': {no_resolvable[:5]}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
