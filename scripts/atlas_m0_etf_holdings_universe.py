"""Atlas-M0 -- ETF holdings via Morningstar universe endpoint (single XML call).

Architecturally correct approach (per architect, 2026-05-04): the universe
endpoint returns the ENTIRE universe in one response, with each fund's
metadata + holdings inline. We don't need per-ticker calls or a country
filter -- the universe is already curated to this Morningstar service's
coverage of Indian funds.

Endpoint:
  https://api.morningstar.com/v2/service/mf/{SERVICE}/universeid/{UNIVERSE}
  ?accesscode={ACCESS}&datapoints=Name,Ticker,CategoryName,InvestmentType,Holdings

Response shape (XML, confirmed via diagnostic):
  <response>
    <status><code>0</code><message>OK</message></status>
    <data _idtype="mstarid" _id="F00001EGG8">
      <api _id="{SERVICE}">
        <FHV2-Holdings>
          <HoldingDetail>
            <Name>BSE Ltd</Name>
            <ISIN>INE118H01025</ISIN>
            <Weighting>3.16534</Weighting>
            ...
          </HoldingDetail>
          ...
        </FHV2-Holdings>
        <Name>...</Name>
        <Ticker>...</Ticker>
        <CategoryName>...</CategoryName>
        <InvestmentType>...</InvestmentType>
      </api>
    </data>
    ...

For each <data> element identified as an ETF (CategoryName / InvestmentType
contains 'ETF'), we:
  1. Extract its mstar_id, ticker, holdings list
  2. Map ticker (with .NS variants) to a de_etf_master.ticker row
  3. Resolve each holding's ISIN to de_instrument.id
  4. Upsert into de_etf_holdings

Funds without a matching de_etf_master row are reported as 'no_master_match'
(may need to be added to de_etf_master in a separate step).

Usage (inside the data-engine container):
    python /app/scripts/atlas_m0_etf_holdings_universe.py
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
UNIVERSE = "q3zv6b817mp4fz0f"
ACCESS = os.environ.get(
    "MORNINGSTAR_ACCESS_CODE", "ftijxp6pf11ezmizn19otbz18ghq2iu4"
)
DATAPOINTS = "Name,Ticker,CategoryName,InvestmentType,Holdings,HoldingDate"

UNIVERSE_URL = (
    f"https://api.morningstar.com/v2/service/mf/{SERVICE}/universeid/{UNIVERSE}"
)


def _decimal(s: Optional[str]) -> Optional[Decimal]:
    if not s:
        return None
    try:
        return Decimal(s.strip())
    except (InvalidOperation, AttributeError):
        return None


def _is_etf(category: Optional[str], investment_type: Optional[str]) -> bool:
    """Heuristic: any of these strings appearing in category/type means ETF."""
    blob = " ".join(filter(None, [category or "", investment_type or ""])).upper()
    return "ETF" in blob or "EXCHANGE TRADED" in blob or blob == "ETF"


def _local(tag: str) -> str:
    """Strip XML namespace if present."""
    return tag.split("}", 1)[-1]


def parse_universe(xml_text: str):
    """Yield (mstar_id, ticker, name, category, investment_type, as_of_date,
    holdings) for each <data> element in the universe response.
    """
    root = ET.fromstring(xml_text)
    for data_elem in root.iter():
        if _local(data_elem.tag) != "data":
            continue

        mstar_id = data_elem.attrib.get("_id") or ""
        # Many fields live under <api>/<...> — collect them by local name
        fund_name = ticker = category = inv_type = None
        holding_date_str = None
        holdings: list[dict] = []

        for elem in data_elem.iter():
            tag = _local(elem.tag)
            if tag == "Name" and fund_name is None and elem.text:
                fund_name = elem.text.strip()
            elif tag == "Ticker" and ticker is None and elem.text:
                ticker = elem.text.strip()
            elif tag == "CategoryName" and elem.text:
                category = elem.text.strip()
            elif tag == "InvestmentType" and elem.text:
                inv_type = elem.text.strip()
            elif tag == "HoldingDate" and holding_date_str is None and elem.text:
                holding_date_str = elem.text.strip()
            elif tag == "HoldingDetail":
                h: dict[str, Optional[str]] = {}
                for child in elem:
                    h[_local(child.tag)] = (child.text or "").strip() if child.text else None
                isin = h.get("ISIN")
                weight_pct = _decimal(h.get("Weighting"))
                if not isin or weight_pct is None or weight_pct < 0:
                    continue
                weight = (weight_pct * Decimal("0.01")).quantize(Decimal("0.000001"))
                if weight > Decimal("1"):
                    weight = Decimal("1")
                holdings.append({
                    "isin": isin.upper(),
                    "name": h.get("Name"),
                    "weight": weight,
                })

        as_of = None
        if holding_date_str:
            try:
                as_of = date.fromisoformat(holding_date_str[:10])
            except ValueError:
                pass

        yield (mstar_id, ticker, fund_name, category, inv_type, as_of, holdings)


async def main() -> int:
    print(f"Fetching universe: {UNIVERSE_URL}")
    async with httpx.AsyncClient(timeout=600.0) as client:
        r = await client.get(UNIVERSE_URL, params={
            "accesscode": ACCESS, "datapoints": DATAPOINTS,
        })
    print(f"HTTP {r.status_code}  size={len(r.text):,} chars")
    if r.status_code != 200:
        print(f"Body: {r.text[:600]}")
        return 1

    # Pre-load lookup maps from DB
    engine = create_async_engine(DATABASE_URL)
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        master_rows = (await session.execute(
            select(DeEtfMaster.ticker, DeEtfMaster.name).where(
                DeEtfMaster.is_active == True  # noqa: E712
            )
        )).all()
        # Build a ticker_lookup keyed by upper-case ticker AND ticker-without-.NS
        ticker_lookup: dict[str, str] = {}
        for t, _ in master_rows:
            if not t:
                continue
            ticker_lookup[t.upper()] = t
            ticker_lookup[t.upper().replace(".NS", "").replace(".BO", "")] = t

        isin_rows = (await session.execute(
            select(DeInstrument.isin, DeInstrument.id).where(
                DeInstrument.isin.isnot(None)
            )
        )).all()
        isin_to_iid = {(r[0] or "").upper(): r[1] for r in isin_rows if r[0]}

    print(f"de_etf_master ticker map: {len(master_rows)} rows ({len(ticker_lookup)} keys)")
    print(f"de_instrument ISIN map:   {len(isin_to_iid)}")

    today = date.today()
    funds_total = etfs_total = etfs_matched = etfs_unmatched = etfs_no_holdings = 0
    rows_inserted = 0
    sample_unmatched: list[tuple[str, str, str]] = []

    print("\nParsing universe...")
    async with Session() as session:
        async with session.begin():
            for mstar_id, fund_ticker, fund_name, category, inv_type, as_of, holdings in \
                    parse_universe(r.text):
                funds_total += 1
                if not _is_etf(category, inv_type):
                    continue
                etfs_total += 1

                # Resolve fund_ticker to a de_etf_master.ticker
                resolved = None
                if fund_ticker:
                    key = fund_ticker.upper()
                    resolved = ticker_lookup.get(key) or ticker_lookup.get(
                        key.replace(".NS", "").replace(".BO", "")
                    )
                if resolved is None and fund_ticker:
                    # Try with .NS suffix appended (Indian ETFs in master)
                    resolved = ticker_lookup.get(fund_ticker.upper() + ".NS")

                if resolved is None:
                    etfs_unmatched += 1
                    if len(sample_unmatched) < 10:
                        sample_unmatched.append((mstar_id, fund_ticker or "", fund_name or ""))
                    continue
                if not holdings:
                    etfs_no_holdings += 1
                    continue

                effective = as_of or today
                upsert_rows = []
                for h in holdings:
                    instrument_id = isin_to_iid.get(h["isin"])
                    if instrument_id is None:
                        continue
                    upsert_rows.append({
                        "ticker": resolved,
                        "instrument_id": instrument_id,
                        "as_of_date": effective,
                        "weight": h["weight"],
                        "last_disclosed_date": today,
                    })
                if not upsert_rows:
                    etfs_no_holdings += 1
                    continue

                stmt = pg_insert(DeEtfHoldings).values(upsert_rows)
                stmt = stmt.on_conflict_do_update(
                    constraint="pk_de_etf_holdings",
                    set_={
                        "weight": stmt.excluded.weight,
                        "last_disclosed_date": stmt.excluded.last_disclosed_date,
                    },
                )
                await session.execute(stmt)
                rows_inserted += len(upsert_rows)
                etfs_matched += 1

    async with Session() as session:
        n_distinct = (await session.execute(text(
            "SELECT COUNT(DISTINCT ticker) FROM de_etf_holdings"))).scalar()
        n_rows = (await session.execute(text(
            "SELECT COUNT(*) FROM de_etf_holdings"))).scalar()
    await engine.dispose()

    print("\n=== Universe-based ETF holdings -- summary ===")
    print(f"Funds in universe response:   {funds_total}")
    print(f"  ETFs (category/type):       {etfs_total}")
    print(f"    matched + upserted:       {etfs_matched}")
    print(f"    no de_etf_master match:   {etfs_unmatched}")
    print(f"    parsed but 0 resolvable:  {etfs_no_holdings}")
    print(f"\nRows upserted this run:       {rows_inserted}")
    print(f"de_etf_holdings totals: distinct_etfs={n_distinct} rows={n_rows}")
    if sample_unmatched:
        print("\nSample 'no master match' (consider adding to de_etf_master):")
        for m, t, n in sample_unmatched:
            print(f"  mstar_id={m}  ticker={t}  name={n[:60]}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
