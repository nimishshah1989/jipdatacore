"""Atlas-M0 -- COMPREHENSIVE universe ingest from Morningstar.

The universe q3zv6b817mp4fz0f covers ~4,133 India funds (OE + ETF, primary
share) per the Morningstar API Center. Today our de_mf_master only has 985
active rows -- the existing fund_master_refresh job has been pulling a
narrow subset and we never reconciled.

This script does the full sweep in one call:

  1. GET /v2/service/mf/{SERVICE}/universeid/{UNIVERSE}?accesscode=...&datapoints=...
     Single response (~50-100 MB XML).
  2. For every <data> element, parse:
        mstar_id, fund_name, ticker, ISIN, broad_category, category_name,
        purchase_mode, inception_date, expense_ratio, holding_date, holdings[]
  3. UPSERT de_mf_master (insert missing rows, refresh existing).
  4. For ETFs (CategoryName / InvestmentType contains 'ETF'):
        - look up de_etf_master.ticker by name/ticker
        - upsert de_etf_holdings (ticker, instrument_id, as_of_date, weight)
  5. For MFs (everything else):
        - upsert de_mf_holdings (mstar_id, as_of_date, isin, ...)

Idempotent. NAVs are NOT touched -- AMFI remains the daily NAV source.

Usage (inside the data-engine container):
    python /app/scripts/atlas_m0_universe_ingest.py
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
from app.models.holdings import DeEtfHoldings, DeMfHoldings
from app.models.instruments import DeInstrument, DeMfMaster

SERVICE = "fq9mxhk7xeb20f3b"
UNIVERSE = "q3zv6b817mp4fz0f"
ACCESS = os.environ.get(
    "MORNINGSTAR_ACCESS_CODE", "ftijxp6pf11ezmizn19otbz18ghq2iu4"
)
DATAPOINTS = ",".join([
    "Name", "Ticker", "ISIN", "InceptionDate", "NetExpenseRatio",
    "CategoryName", "BroadCategoryGroup", "InvestmentType", "PurchaseMode",
    "Holdings", "HoldingDate",
])

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


def _int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    try:
        return int(s.strip())
    except (ValueError, AttributeError):
        return None


def _date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except (ValueError, AttributeError):
        return None


def _local(tag: str) -> str:
    return tag.split("}", 1)[-1]


def _is_etf(category: Optional[str], investment_type: Optional[str]) -> bool:
    blob = " ".join(filter(None, [category or "", investment_type or ""])).upper()
    return "ETF" in blob or "EXCHANGE TRADED" in blob


def parse_universe(xml_text: str):
    """Yield one dict per fund."""
    root = ET.fromstring(xml_text)
    for data_elem in root.iter():
        if _local(data_elem.tag) != "data":
            continue
        mstar_id = data_elem.attrib.get("_id") or ""
        if not mstar_id:
            continue
        f: dict[str, Optional[str]] = {
            "mstar_id": mstar_id,
            "Name": None, "Ticker": None, "ISIN": None,
            "InceptionDate": None, "NetExpenseRatio": None,
            "CategoryName": None, "BroadCategoryGroup": None,
            "InvestmentType": None, "PurchaseMode": None,
            "HoldingDate": None,
        }
        holdings: list[dict] = []

        for elem in data_elem.iter():
            tag = _local(elem.tag)
            text_v = (elem.text or "").strip() if elem.text else None
            if tag in f and f[tag] is None and text_v:
                f[tag] = text_v
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
                    "weight_pct": weight_pct,
                    "shares_held": _int(h.get("NumberOfShare")),
                    "market_value": _decimal(h.get("MarketValue")),
                    "sector_code": h.get("SectorId") or h.get("GlobalSectorId"),
                })

        f["holdings"] = holdings
        yield f


async def upsert_mf_master(session, fund: dict) -> None:
    is_etf = _is_etf(fund.get("CategoryName"), fund.get("InvestmentType"))
    values = {
        "mstar_id": fund["mstar_id"],
        "fund_name": fund.get("Name") or fund["mstar_id"],
        "isin": fund.get("ISIN"),
        "category_name": fund.get("CategoryName"),
        "broad_category": fund.get("BroadCategoryGroup"),
        "is_etf": is_etf,
        "is_index_fund": False,  # cannot infer; leave default
        "is_active": True,
        "inception_date": _date(fund.get("InceptionDate")),
        "expense_ratio": _decimal(fund.get("NetExpenseRatio")),
        "purchase_mode": _int(fund.get("PurchaseMode")),
    }
    stmt = pg_insert(DeMfMaster).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["mstar_id"],
        set_={
            "fund_name": stmt.excluded.fund_name,
            "isin": stmt.excluded.isin,
            "category_name": stmt.excluded.category_name,
            "broad_category": stmt.excluded.broad_category,
            "is_etf": stmt.excluded.is_etf,
            "is_active": stmt.excluded.is_active,
            "inception_date": stmt.excluded.inception_date,
            "expense_ratio": stmt.excluded.expense_ratio,
            "purchase_mode": stmt.excluded.purchase_mode,
        },
    )
    await session.execute(stmt)


async def main() -> int:
    print(f"Fetching universe: {UNIVERSE_URL}")
    print(f"  service={SERVICE}  universe={UNIVERSE}  datapoints={DATAPOINTS}")
    async with httpx.AsyncClient(timeout=900.0) as client:
        r = await client.get(UNIVERSE_URL, params={
            "accesscode": ACCESS, "datapoints": DATAPOINTS,
        })
    print(f"HTTP {r.status_code}  size={len(r.text):,} chars")
    if r.status_code != 200:
        print(f"Body: {r.text[:600]}")
        return 1

    engine = create_async_engine(DATABASE_URL)
    Session = async_sessionmaker(engine, expire_on_commit=False)

    # Pre-load lookup maps
    async with Session() as session:
        master_rows = (await session.execute(
            select(DeEtfMaster.ticker, DeEtfMaster.name).where(
                DeEtfMaster.is_active == True  # noqa: E712
            )
        )).all()
        ticker_lookup: dict[str, str] = {}
        for t, _ in master_rows:
            if t:
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
    n_funds = n_etfs = n_mfs = 0
    n_master_upserted = 0
    n_etf_holdings_inserted = n_mf_holdings_inserted = 0
    n_etf_no_master = n_etf_no_holdings = 0
    n_mf_no_holdings = 0
    sample_etf_unmatched: list[tuple] = []

    print("\nParsing + ingesting...")
    async with Session() as session:
        async with session.begin():
            for fund in parse_universe(r.text):
                n_funds += 1

                # 1. Refresh de_mf_master row
                await upsert_mf_master(session, fund)
                n_master_upserted += 1

                category = fund.get("CategoryName")
                inv_type = fund.get("InvestmentType")
                is_etf = _is_etf(category, inv_type)
                holdings = fund.get("holdings", [])

                if is_etf:
                    n_etfs += 1
                    if not holdings:
                        n_etf_no_holdings += 1
                        continue
                    # Map mstar_id -> de_etf_master.ticker via Ticker field
                    ticker_in = (fund.get("Ticker") or "").upper()
                    resolved = (ticker_lookup.get(ticker_in)
                                or ticker_lookup.get(ticker_in.replace(".NS", "").replace(".BO", ""))
                                or ticker_lookup.get(ticker_in + ".NS"))
                    if resolved is None:
                        n_etf_no_master += 1
                        if len(sample_etf_unmatched) < 10:
                            sample_etf_unmatched.append((fund["mstar_id"], ticker_in,
                                                         (fund.get("Name") or "")[:60]))
                        continue
                    effective = _date(fund.get("HoldingDate")) or today
                    rows = []
                    for h in holdings:
                        iid = isin_to_iid.get(h["isin"])
                        if iid is None:
                            continue
                        rows.append({
                            "ticker": resolved, "instrument_id": iid,
                            "as_of_date": effective, "weight": h["weight"],
                            "last_disclosed_date": today,
                        })
                    if rows:
                        stmt = pg_insert(DeEtfHoldings).values(rows)
                        stmt = stmt.on_conflict_do_update(
                            constraint="pk_de_etf_holdings",
                            set_={"weight": stmt.excluded.weight,
                                  "last_disclosed_date": stmt.excluded.last_disclosed_date},
                        )
                        await session.execute(stmt)
                        n_etf_holdings_inserted += len(rows)
                else:
                    n_mfs += 1
                    if not holdings:
                        n_mf_no_holdings += 1
                        continue
                    effective = _date(fund.get("HoldingDate")) or today
                    rows = []
                    for h in holdings:
                        iid = isin_to_iid.get(h["isin"])
                        rows.append({
                            "mstar_id": fund["mstar_id"],
                            "as_of_date": effective,
                            "isin": h["isin"],
                            "holding_name": h.get("name"),
                            "instrument_id": iid,
                            "weight_pct": h.get("weight_pct"),
                            "shares_held": h.get("shares_held"),
                            "market_value": h.get("market_value"),
                            "sector_code": h.get("sector_code"),
                            "is_mapped": iid is not None,
                        })
                    if rows:
                        with_isin = [r for r in rows if r["isin"]]
                        if with_isin:
                            stmt = pg_insert(DeMfHoldings).values(with_isin)
                            stmt = stmt.on_conflict_do_update(
                                constraint="uq_mf_holdings",
                                set_={
                                    "holding_name": stmt.excluded.holding_name,
                                    "weight_pct": stmt.excluded.weight_pct,
                                    "shares_held": stmt.excluded.shares_held,
                                    "market_value": stmt.excluded.market_value,
                                    "sector_code": stmt.excluded.sector_code,
                                    "instrument_id": stmt.excluded.instrument_id,
                                    "is_mapped": stmt.excluded.is_mapped,
                                },
                            )
                            await session.execute(stmt)
                            n_mf_holdings_inserted += len(with_isin)

    async with Session() as session:
        n_master = (await session.execute(text(
            "SELECT COUNT(*) FROM de_mf_master WHERE is_active"))).scalar()
        n_master_etf = (await session.execute(text(
            "SELECT COUNT(*) FROM de_mf_master WHERE is_active AND is_etf"))).scalar()
        n_master_eq = (await session.execute(text(
            "SELECT COUNT(*) FROM de_mf_master WHERE is_active AND broad_category ILIKE '%Equity%'"))).scalar()
        n_etf_h = (await session.execute(text(
            "SELECT COUNT(DISTINCT ticker) FROM de_etf_holdings"))).scalar()
        n_mf_h = (await session.execute(text(
            "SELECT COUNT(DISTINCT mstar_id) FROM de_mf_holdings"))).scalar()
    await engine.dispose()

    print("\n=== Universe ingest -- summary ===")
    print(f"Funds in universe response:   {n_funds}")
    print(f"  ETFs:                       {n_etfs}")
    print(f"    holdings upserted (rows): {n_etf_holdings_inserted}")
    print(f"    no de_etf_master match:   {n_etf_no_master}")
    print(f"    no holdings parsed:       {n_etf_no_holdings}")
    print(f"  MFs:                        {n_mfs}")
    print(f"    holdings upserted (rows): {n_mf_holdings_inserted}")
    print(f"    no holdings parsed:       {n_mf_no_holdings}")
    print(f"\nDB state after ingest:")
    print(f"  de_mf_master active:        {n_master}")
    print(f"    of which ETF:             {n_master_etf}")
    print(f"    of which Equity (broad):  {n_master_eq}")
    print(f"  de_etf_holdings funds:      {n_etf_h}")
    print(f"  de_mf_holdings funds:       {n_mf_h}")
    if sample_etf_unmatched:
        print("\nSample ETFs not matched to de_etf_master (consider adding):")
        for m, t, n in sample_etf_unmatched:
            print(f"  mstar_id={m}  ticker={t}  name={n}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
