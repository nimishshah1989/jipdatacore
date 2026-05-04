"""Atlas-M0 fallback -- populate de_etf_holdings via index-constituent proxy.

Morningstar's MF service (fq9mxhk7xeb20f3b / universe q3zv6b817mp4fz0f) returns
404 for Indian ETFs -- confirmed by smoke-testing both id_type='Ticker' (US
ETFs from de_etf_master) and id_type='FundId' with mstar_id (Indian ETFs from
de_mf_master.is_etf=True). The service evidently covers MF schemes only.

Pragmatic fallback per architect direction (2026-05-04): for ETFs that track
a Nifty index we already have constituents for, the holdings ARE the index
constituents (with weights). This populates de_etf_holdings cheaply for
~80 % of Indian ETFs (broad-market trackers + sector trackers).

Mapping rule: case-insensitive substring match on de_etf_master.name vs
de_index_master.index_name. Manual overrides in TICKER_OVERRIDES handle the
common short-name vs long-name mismatches.

For thematic / actively-managed ETFs that don't track a known index, this
script logs them as 'no_index_match' -- those are the ~20 % that need a
proper holdings source (AMFI monthly portfolio fetch is the next step,
deferred to Atlas-M1 follow-up).

Usage (inside data-engine container):
    python /app/scripts/atlas_m0_etf_holdings_from_index.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid
from datetime import date
from decimal import Decimal
from typing import Optional

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
from app.models.instruments import DeIndexConstituents, DeIndexMaster

# Manual short-name -> de_index_master.index_code overrides for the common
# ETF naming patterns. Keys are lower-cased substrings of de_etf_master.name.
TICKER_OVERRIDES: dict[str, str] = {
    "nifty 50": "NIFTY 50",
    "nifty50": "NIFTY 50",
    "niftybees": "NIFTY 50",
    "nifty bank": "NIFTY BANK",
    "bank nifty": "NIFTY BANK",
    "bankbees": "NIFTY BANK",
    "nifty next 50": "NIFTY NEXT 50",
    "junior nifty": "NIFTY NEXT 50",
    "juniorbees": "NIFTY NEXT 50",
    "nifty 100": "NIFTY 100",
    "nifty 200": "NIFTY 200",
    "nifty 500": "NIFTY 500",
    "nifty500": "NIFTY 500",
    "nifty midcap 150": "NIFTY MIDCAP 150",
    "nifty midcap150": "NIFTY MIDCAP 150",
    "nifty midcap 50": "NIFTY MIDCAP 50",
    "nifty smallcap 50": "NIFTY SMLCAP 50",
    "nifty smallcap 100": "NIFTY SMLCAP 100",
    "nifty smallcap 250": "NIFTY SMLCAP 250",
    "nifty microcap 250": "NIFTY MICROCAP250",
    "nifty largemid 250": "NIFTY LARGEMID250",
    "nifty it": "NIFTY IT",
    "nifty pharma": "NIFTY PHARMA",
    "nifty auto": "NIFTY AUTO",
    "nifty fmcg": "NIFTY FMCG",
    "nifty metal": "NIFTY METAL",
    "nifty realty": "NIFTY REALTY",
    "nifty psu bank": "NIFTY PSU BANK",
    "nifty private bank": "NIFTY PVT BANK",
    "nifty pvt bank": "NIFTY PVT BANK",
    "nifty financial services": "NIFTY FIN SERVICE",
    "nifty fin services": "NIFTY FIN SERVICE",
    "nifty consumption": "NIFTY CONSUMPTION",
    "nifty infrastructure": "NIFTY INFRA",
    "nifty infra": "NIFTY INFRA",
    "nifty energy": "NIFTY ENERGY",
    "nifty oil and gas": "NIFTY OIL AND GAS",
    "nifty oil & gas": "NIFTY OIL AND GAS",
    "nifty cpse": "NIFTY CPSE",
    "nifty mnc": "NIFTY MNC",
    "nifty alpha 50": "NIFTY ALPHA 50",
    "nifty low volatility 50": "NIFTY LOW VOL 50",
    "nifty100 quality 30": "NIFTY100 QUALTY30",
    "nifty200 momentum 30": "NIFTY200MOMENTM30",
    "nifty 50 equal weight": "NIFTY50 EQL WGT",
    "nifty50 equal weight": "NIFTY50 EQL WGT",
    "nifty 100 equal weight": "NIFTY100 EQL WGT",
    "nifty100 equal weight": "NIFTY100 EQL WGT",
    "nifty 100 low volatility": "NIFTY100 LOWVOL30",
    "nifty 200 quality": "NIFTY200 QUALTY30",
    "nifty dividend opportunities": "NIFTY DIV OPPS 50",
    "nifty india defence": "NIFTY IND DEFENCE",
    "nifty india manufacturing": "NIFTY INDIA MFG",
    "nifty india digital": "NIFTY IND DIGITAL",
    "nifty health": "NIFTY HEALTHCARE",
    "nifty healthcare": "NIFTY HEALTHCARE",

    # ---- Compact ticker-form keys (match against ETF ticker) ----
    # AMC ETF naming convention: "<theme>BEES" / "<theme>ETF" / etc.
    "psubnk": "NIFTY PSU BANK",
    "psubnkbees": "NIFTY PSU BANK",
    "cpseetf": "NIFTY CPSE",
    "cpse": "NIFTY CPSE",
    "infrabees": "NIFTY INFRA",
    "infra": "NIFTY INFRA",
    "pharmabees": "NIFTY PHARMA",
    "phar": "NIFTY PHARMA",
    "healthbees": "NIFTY HEALTHCARE",
    "healthi": "NIFTY HEALTHCARE",
    "healthcare": "NIFTY HEALTHCARE",
    "divopp": "NIFTY DIV OPPS 50",
    "divoppbees": "NIFTY DIV OPPS 50",
    "consumbees": "NIFTY CONSUMPTION",
    "consum": "NIFTY CONSUMPTION",
    "itbees": "NIFTY IT",
    "moitbees": "NIFTY IT",
    "autobees": "NIFTY AUTO",
    "psbk": "NIFTY PSU BANK",
    "fmcg": "NIFTY FMCG",
    "metal": "NIFTY METAL",
    "energy": "NIFTY ENERGY",
    "realty": "NIFTY REALTY",
    "media": "NIFTY MEDIA",
    "oilngas": "NIFTY OIL AND GAS",
    "mnc": "NIFTY MNC",
    "midcap": "NIFTY MIDCAP 150",
    "mom100": "NIFTY200MOMENTM30",
    "alpha50": "NIFTY ALPHA 50",
    "lowvol": "NIFTY LOW VOL 50",
    "qualty": "NIFTY100 QUALTY30",
    "100ewb": "NIFTY100 EQL WGT",
    "50ewb": "NIFTY50 EQL WGT",
    "next50": "NIFTY NEXT 50",
    "midqual": "NIFTY M150 QLTY50",
    "smallqual": "NIFTY SML250 Q50",
    "highbeta": "NIFTY HIGHBETA 50",
    "tatagrp": "NIFTY TATA 25 CAP",
    "manufact": "NIFTY INDIA MFG",
    "defence": "NIFTY IND DEFENCE",
    "indigital": "NIFTY IND DIGITAL",
    "tourism": "NIFTY IND TOURISM",
    "rural": "NIFTY RURAL",
    "ev": "NIFTY EV",
    "value": "NIFTY200 VALUE 30",
}


def _resolve_index(
    etf_name: str,
    etf_ticker: str,
    index_name_to_code: dict[str, str],
) -> Optional[str]:
    """Return de_index_master.index_code for the index this ETF tracks.

    Match priority:
      1. Substring match against the **ticker** (e.g. BANKBEES -> NIFTY BANK).
         Tickers are short and unambiguous; tried first.
      2. Substring match against the **name** via TICKER_OVERRIDES.
      3. Substring match against the name vs known full index_names.
    """
    if not etf_name and not etf_ticker:
        return None
    name_low = (etf_name or "").lower()
    # Strip the .NS suffix and any whitespace before matching the ticker.
    ticker_low = (etf_ticker or "").lower().replace(".ns", "").strip()

    for key, code in TICKER_OVERRIDES.items():
        if key in ticker_low or key in name_low:
            return code

    for index_name in sorted(index_name_to_code.keys(), key=len, reverse=True):
        if index_name and len(index_name) > 5 and index_name.lower() in name_low:
            return index_name_to_code[index_name]
    return None


async def main() -> int:
    engine = create_async_engine(DATABASE_URL)
    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as session:
        async with session.begin():
            # Filter to Indian ETFs -- US ETFs (SPY/QQQ/VTI/etc.) live in
            # de_etf_master too but legitimately can't be matched to Nifty
            # indices. Restrict by country so the denominator is honest.
            etfs = (await session.execute(
                select(DeEtfMaster.ticker, DeEtfMaster.name).where(
                    DeEtfMaster.is_active == True,  # noqa: E712
                    DeEtfMaster.country == "IN",
                )
            )).all()
            indices = (await session.execute(
                select(DeIndexMaster.index_code, DeIndexMaster.index_name)
            )).all()
            index_name_to_code = {n: c for c, n in indices if n}

            # Pre-fetch all active constituents grouped by index_code
            all_consts = (await session.execute(
                select(
                    DeIndexConstituents.index_code,
                    DeIndexConstituents.instrument_id,
                    DeIndexConstituents.weight_pct,
                ).where(DeIndexConstituents.effective_to.is_(None))
            )).all()
            consts_by_index: dict[str, list[tuple[uuid.UUID, Optional[Decimal]]]] = {}
            for ic, iid, w in all_consts:
                consts_by_index.setdefault(ic, []).append((iid, w))

            today = date.today()
            matched_etfs: list[tuple[str, str, int]] = []
            unmatched_etfs: list[str] = []
            no_consts_etfs: list[tuple[str, str]] = []
            total_rows_inserted = 0

            for ticker, etf_name in etfs:
                if not etf_name:
                    unmatched_etfs.append(ticker)
                    continue
                index_code = _resolve_index(etf_name, ticker, index_name_to_code)
                if not index_code:
                    unmatched_etfs.append(ticker)
                    continue
                consts = consts_by_index.get(index_code, [])
                if not consts:
                    no_consts_etfs.append((ticker, index_code))
                    continue

                n_consts = len(consts)
                # Equal-weight if no per-constituent weight is recorded
                rows: list[dict] = []
                for instrument_id, w in consts:
                    if w is not None:
                        weight = Decimal(w) / Decimal(100)
                    else:
                        weight = Decimal(1) / Decimal(n_consts)
                    weight = weight.quantize(Decimal("0.000001"))
                    if weight <= 0:
                        continue
                    rows.append({
                        "ticker": ticker,
                        "instrument_id": instrument_id,
                        "as_of_date": today,
                        "weight": weight,
                        "last_disclosed_date": today,
                    })

                if not rows:
                    no_consts_etfs.append((ticker, index_code))
                    continue

                # Idempotent upsert
                stmt = pg_insert(DeEtfHoldings).values(rows)
                stmt = stmt.on_conflict_do_update(
                    constraint="pk_de_etf_holdings",
                    set_={
                        "weight": stmt.excluded.weight,
                        "last_disclosed_date": stmt.excluded.last_disclosed_date,
                    },
                )
                await session.execute(stmt)
                total_rows_inserted += len(rows)
                matched_etfs.append((ticker, index_code, len(rows)))

        # Verify
        n_distinct_etfs = (await session.execute(text(
            "SELECT COUNT(DISTINCT ticker) FROM de_etf_holdings"
        ))).scalar()
        n_total_rows = (await session.execute(text(
            "SELECT COUNT(*) FROM de_etf_holdings"
        ))).scalar()

    print("\n=== ETF holdings via index proxy ===")
    print(f"ETFs in de_etf_master:       {len(etfs)}")
    print(f"  matched to index:          {len(matched_etfs)}")
    print(f"  no index match:            {len(unmatched_etfs)}")
    print(f"  index has 0 constituents:  {len(no_consts_etfs)}")
    print(f"\nRows upserted this run:      {total_rows_inserted}")
    print(f"de_etf_holdings totals: distinct_etfs={n_distinct_etfs} rows={n_total_rows}")
    if unmatched_etfs[:10]:
        print(f"\nSample unmatched (need AMFI portfolio source): {unmatched_etfs[:10]}")
    if no_consts_etfs[:5]:
        print(f"Sample 'index empty' (constituents not loaded): {no_consts_etfs[:5]}")

    await engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
