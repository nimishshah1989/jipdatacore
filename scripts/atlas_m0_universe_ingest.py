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

SERVICE_MASTER = "x6d9w6xxu0hmhrr4"      # atlas_fund_master service
SERVICE_HOLDINGS = "fq9mxhk7xeb20f3b"    # holdings service
UNIVERSE = "q3zv6b817mp4fz0f"            # India OE+ETF, 4,133 funds
ACCESS = os.environ.get(
    "MORNINGSTAR_ACCESS_CODE", "ftijxp6pf11ezmizn19otbz18ghq2iu4"
)

MASTER_DATAPOINTS = ",".join([
    "Name", "Ticker", "ISIN", "InceptionDate", "NetExpenseRatio",
    "CategoryName", "BroadCategoryGroup", "InvestmentType", "PurchaseMode",
    "Benchmark", "ManagerName", "TotalNetAssets",
])
HOLDINGS_DATAPOINTS = "Holdings,HoldingDate"

MASTER_URL = (
    f"https://api.morningstar.com/v2/service/mf/{SERVICE_MASTER}/universeid/{UNIVERSE}"
)
HOLDINGS_URL = (
    f"https://api.morningstar.com/v2/service/mf/{SERVICE_HOLDINGS}/universeid/{UNIVERSE}"
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


def parse_master(xml_text: str) -> dict[str, dict]:
    """Parse the fund-master XML response into {mstar_id: {field: value, ...}}.

    The master service x6d9w6xxu0hmhrr4 uses FSCBI-/ARF-/FB-/FNA-/FM- tag
    prefixes, NOT the bare Name/CategoryName tags returned by the holdings
    service. We map the prefixed tags onto canonical keys so the rest of
    the script can stay tag-agnostic.

    A response can include multiple <data> elements for the same ISIN
    (primary + secondary share class -- different mstar_ids). We key by
    mstar_id and keep the FIRST one seen (usually the primary share).
    """
    # Map source tag -> canonical key. Source tags are matched on local name
    # so XML namespaces are stripped; the FSCBI-/ARF- prefixes are part of
    # the tag itself, not a namespace.
    TAG_MAP = {
        "FSCBI-FundName": "Name",
        "FSCBI-FundStandardName": "Name",   # fallback if FundName missing
        "FSCBI-ISIN": "ISIN",
        "FSCBI-AMFICode": "AMFICode",
        "FSCBI-BroadCategoryGroup": "BroadCategoryGroup",
        "FSCBI-AggregatedCategoryName": "CategoryName",
        "FSCBI-InceptionDate": "InceptionDate",
        "FSCBI-MStarID": "MStarID",
        "ARF-NetExpenseRatio": "NetExpenseRatio",
        "FB-PrimaryIndexName": "Benchmark",
        "FNA-FundNetAssets": "TotalNetAssets",
        # Holdings service style fallbacks (in case some funds expose them)
        "Name": "Name",
        "Ticker": "Ticker",
        "ISIN": "ISIN",
        "InceptionDate": "InceptionDate",
        "NetExpenseRatio": "NetExpenseRatio",
        "CategoryName": "CategoryName",
        "BroadCategoryGroup": "BroadCategoryGroup",
        "InvestmentType": "InvestmentType",
        "PurchaseMode": "PurchaseMode",
    }
    root = ET.fromstring(xml_text)
    by_id: dict[str, dict] = {}
    for data_elem in root.iter():
        if _local(data_elem.tag) != "data":
            continue
        mstar_id = data_elem.attrib.get("_id") or ""
        if not mstar_id or mstar_id in by_id:
            continue  # keep first occurrence
        f: dict[str, Optional[str]] = {
            "mstar_id": mstar_id,
            "Name": None, "Ticker": None, "ISIN": None,
            "AMFICode": None, "InceptionDate": None,
            "NetExpenseRatio": None, "CategoryName": None,
            "BroadCategoryGroup": None, "InvestmentType": None,
            "PurchaseMode": None, "Benchmark": None,
            "TotalNetAssets": None, "MStarID": None,
        }
        for elem in data_elem.iter():
            tag = _local(elem.tag)
            target = TAG_MAP.get(tag)
            if target and f.get(target) is None and elem.text:
                f[target] = elem.text.strip()
        by_id[mstar_id] = f
    return by_id


def _looks_like_etf(name: Optional[str]) -> bool:
    """Heuristic ETF detection by fund name.

    Excludes FoFs aggressively -- Morningstar's compressed names sometimes
    drop spaces, so 'ETFFoFRegGr' has to be caught alongside 'ETF Fund of
    Fund'. We collapse spaces before matching the FoF substring.
    """
    if not name:
        return False
    n = name.upper()
    n_compact = n.replace(" ", "")
    if "FUND OF FUND" in n or "ETFFOF" in n_compact or "FOFREGGR" in n_compact \
            or "FOFDIRGR" in n_compact:
        return False
    if " FOF" in n or n.endswith("FOF"):
        return False
    return ("ETF" in n) or ("BEES" in n)


def _is_keep(fund: dict) -> bool:
    """Architect filter (2026-05-04): keep only ETFs + regular equity growth MFs.

    - ETFs always kept (regardless of category, since they're a separate class).
    - For mutual funds: broad_category must be Equity; name must not mark
      Direct plan, IDCW/Dividend variant, Segregated, FoF, or Index fund.
    """
    name = (fund.get("Name") or "")
    name_up = name.upper()

    if "FUND OF FUND" in name_up or " FOF" in name_up or name_up.endswith("FOF"):
        return False

    if _looks_like_etf(name):
        return True

    if (fund.get("BroadCategoryGroup") or "") != "Equity":
        return False
    if any(x in name_up for x in ("DIRECT", "IDCW", "DIVIDEND",
                                  "SEGREGATED", "INDEX")):
        return False
    return True


def parse_holdings(xml_text: str) -> dict[str, dict]:
    """Parse the holdings XML response into {mstar_id: {holding_date, holdings[]}}."""
    root = ET.fromstring(xml_text)
    by_id: dict[str, dict] = {}
    for data_elem in root.iter():
        if _local(data_elem.tag) != "data":
            continue
        mstar_id = data_elem.attrib.get("_id") or ""
        if not mstar_id:
            continue
        holding_date = None
        holdings: list[dict] = []
        for elem in data_elem.iter():
            tag = _local(elem.tag)
            if tag == "HoldingDate" and holding_date is None and elem.text:
                holding_date = elem.text.strip()
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
                    # Clamp to 99.9999 -- de_mf_holdings.weight_pct is NUMERIC(6,4)
                    # and Morningstar occasionally reports >100% for fund-of-fund
                    # accounting (e.g. ICICI Pru Nifty EV & New Age Automtv ETF
                    # showed 100.38892 in the universe response).
                    "weight_pct": min(weight_pct, Decimal("99.9999")),
                    "shares_held": _int(h.get("NumberOfShare")),
                    "market_value": _decimal(h.get("MarketValue")),
                    "sector_code": h.get("SectorId") or h.get("GlobalSectorId"),
                })
        by_id[mstar_id] = {"HoldingDate": holding_date, "holdings": holdings}
    return by_id


async def upsert_mf_master(session, fund: dict) -> None:
    is_etf = _looks_like_etf(fund.get("Name"))
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
    print(f"Universe: {UNIVERSE}")
    async with httpx.AsyncClient(timeout=900.0) as client:
        print(f"\n[1/2] Fetching MASTER from service {SERVICE_MASTER}")
        rm = await client.get(MASTER_URL, params={
            "accesscode": ACCESS, "datapoints": MASTER_DATAPOINTS,
        })
        print(f"  HTTP {rm.status_code}  size={len(rm.text):,} chars")
        if rm.status_code != 200:
            print(f"  Body: {rm.text[:400]}")
            return 1

        print(f"\n[2/2] Fetching HOLDINGS from service {SERVICE_HOLDINGS}")
        rh = await client.get(HOLDINGS_URL, params={
            "accesscode": ACCESS, "datapoints": HOLDINGS_DATAPOINTS,
        })
        print(f"  HTTP {rh.status_code}  size={len(rh.text):,} chars")
        if rh.status_code != 200:
            print(f"  Body: {rh.text[:400]}")
            return 1

    print("\nParsing both responses...")
    master_by_id = parse_master(rm.text)
    holdings_by_id = parse_holdings(rh.text)
    print(f"  master entries:   {len(master_by_id)}")
    print(f"  holdings entries: {len(holdings_by_id)}")

    # Merge + filter: only architect-approved funds (ETFs + regular equity growth MFs)
    keep_ids: set[str] = set()
    skip_reasons: dict[str, int] = {"not_equity": 0, "direct_or_idcw": 0,
                                    "fof": 0, "index": 0, "kept_etf": 0,
                                    "kept_mf": 0}
    for mid in (set(master_by_id) | set(holdings_by_id)):
        m = master_by_id.get(mid)
        if not m:
            continue
        if _is_keep(m):
            keep_ids.add(mid)
            if _looks_like_etf(m.get("Name")):
                skip_reasons["kept_etf"] += 1
            else:
                skip_reasons["kept_mf"] += 1
    print(f"\nFilter result: keeping {len(keep_ids)} funds out of {len(master_by_id)}")
    print(f"  ETFs kept:                {skip_reasons['kept_etf']}")
    print(f"  Regular Equity MFs kept:  {skip_reasons['kept_mf']}")

    def iter_funds():
        for mid in keep_ids:
            m = master_by_id.get(mid, {"mstar_id": mid})
            h = holdings_by_id.get(mid, {"HoldingDate": None, "holdings": []})
            yield m, h

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

    # ---- Cleanup pass: delete pre-existing master rows not in the filtered set
    # (legacy data from older / unfiltered runs). Cascades to de_mf_holdings.
    print("\nCleanup: dropping de_mf_master rows outside the keep set...")
    async with Session() as session:
        async with session.begin():
            keep_list = list(keep_ids)
            # Use temporary table to handle large IN() lists efficiently
            await session.execute(text(
                "CREATE TEMP TABLE _keep_ids (mstar_id text PRIMARY KEY) ON COMMIT DROP"
            ))
            for i in range(0, len(keep_list), 5000):
                batch = keep_list[i:i+5000]
                await session.execute(
                    text("INSERT INTO _keep_ids VALUES " +
                         ",".join(f"(:m{j})" for j in range(len(batch)))),
                    {f"m{j}": v for j, v in enumerate(batch)},
                )
            res = await session.execute(text(
                "DELETE FROM de_mf_master "
                "WHERE mstar_id NOT IN (SELECT mstar_id FROM _keep_ids)"
            ))
            print(f"  deleted {res.rowcount} legacy/non-matching de_mf_master rows "
                  f"(de_mf_holdings cascaded)")

    print("\nIngesting...")
    n_skipped_errors = 0
    error_samples: list[tuple] = []
    async with Session() as session:
        async with session.begin():
            for master, hold in iter_funds():
                n_funds += 1
                # Primary classifier: name pattern. Master service doesn't
                # return InvestmentType, so the substring-on-name approach is
                # the only reliable signal in production.
                is_etf = _looks_like_etf(master.get("Name"))
                holdings = hold.get("holdings") or []
                holding_date = hold.get("HoldingDate")
                try:
                    # Savepoint per fund -- a single bad row (e.g. weight_pct
                    # overflow on a fund-of-fund) doesn't roll back all 4,133.
                    async with session.begin_nested():
                        await upsert_mf_master(session, master)
                        n_master_upserted += 1

                        if is_etf:
                            n_etfs += 1
                            if not holdings:
                                n_etf_no_holdings += 1
                                continue
                            effective = _date(holding_date) or today

                            # 1. Always write to de_mf_holdings keyed by mstar_id.
                            #    This is the durable home for ETF holdings -- most
                            #    Morningstar ETFs aren't in de_etf_master so the
                            #    FK-strict de_etf_holdings can't accept them.
                            mf_rows = []
                            for h in holdings:
                                iid = isin_to_iid.get(h["isin"])
                                mf_rows.append({
                                    "mstar_id": master["mstar_id"],
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
                            with_isin = [r for r in mf_rows if r["isin"]]
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

                            # 2. ALSO try to mirror into de_etf_holdings if we
                            #    can map this Morningstar fund to a row in
                            #    de_etf_master (best-effort; most won't match
                            #    because the master service doesn't return Ticker).
                            ticker_in = (master.get("Ticker") or "").upper()
                            resolved = (ticker_lookup.get(ticker_in)
                                        or ticker_lookup.get(ticker_in.replace(".NS", "").replace(".BO", ""))
                                        or ticker_lookup.get(ticker_in + ".NS"))
                            if resolved is None:
                                n_etf_no_master += 1
                                if len(sample_etf_unmatched) < 10:
                                    sample_etf_unmatched.append((master["mstar_id"], ticker_in,
                                                                 (master.get("Name") or "")[:60]))
                                continue
                            etf_rows = []
                            for h in holdings:
                                iid = isin_to_iid.get(h["isin"])
                                if iid is None:
                                    continue
                                etf_rows.append({
                                    "ticker": resolved, "instrument_id": iid,
                                    "as_of_date": effective, "weight": h["weight"],
                                    "last_disclosed_date": today,
                                })
                            if etf_rows:
                                stmt = pg_insert(DeEtfHoldings).values(etf_rows)
                                stmt = stmt.on_conflict_do_update(
                                    constraint="pk_de_etf_holdings",
                                    set_={"weight": stmt.excluded.weight,
                                          "last_disclosed_date": stmt.excluded.last_disclosed_date},
                                )
                                await session.execute(stmt)
                                n_etf_holdings_inserted += len(etf_rows)
                        else:
                            n_mfs += 1
                            if not holdings:
                                n_mf_no_holdings += 1
                                continue
                            effective = _date(holding_date) or today
                            rows = []
                            for h in holdings:
                                iid = isin_to_iid.get(h["isin"])
                                rows.append({
                                    "mstar_id": master["mstar_id"],
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
                except Exception as exc:
                    n_skipped_errors += 1
                    if len(error_samples) < 10:
                        error_samples.append((master.get("mstar_id"), str(exc)[:200]))

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

    print(f"\n  funds skipped on error:    {n_skipped_errors}")
    if error_samples:
        print("  Error samples (first 10):")
        for mid, err in error_samples:
            print(f"    {mid}: {err}")
    print("\n=== Universe ingest -- summary ===")
    print(f"Funds in universe response:   {n_funds}")
    print(f"  ETFs:                       {n_etfs}")
    print(f"    holdings upserted (rows): {n_etf_holdings_inserted}")
    print(f"    no de_etf_master match:   {n_etf_no_master}")
    print(f"    no holdings parsed:       {n_etf_no_holdings}")
    print(f"  MFs:                        {n_mfs}")
    print(f"    holdings upserted (rows): {n_mf_holdings_inserted}")
    print(f"    no holdings parsed:       {n_mf_no_holdings}")
    print("\nDB state after ingest:")
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
