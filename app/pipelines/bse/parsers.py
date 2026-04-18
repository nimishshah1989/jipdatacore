"""Parsers for BSE ownership endpoints (shareholding, pledge, insider, SAST).

Each parser takes the raw JSON response (list of dicts) from the BSE API
and returns a list of normalized dicts ready for DB insert.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


def _sha256(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def _safe_decimal(val: Any) -> Decimal | None:
    if val is None or val == "" or val == "-":
        return None
    try:
        return Decimal(str(val).strip().replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _safe_int(val: Any) -> int | None:
    if val is None or val == "" or val == "-":
        return None
    try:
        return int(str(val).strip().replace(",", ""))
    except (ValueError, TypeError):
        return None


def _parse_date(s: str | None) -> date | None:
    if not s or not s.strip():
        return None
    s = s.strip()
    for fmt in ("%d %b %Y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def parse_shareholding(raw: list[dict[str, Any]], scripcode: str) -> list[dict[str, Any]]:
    """Parse CorpShareHoldingPattern_New response.

    BSE returns a list of quarterly snapshots per scripcode. Each item has
    fields like SHPDate, PromoterPer, PublicPer, FIIPer, DIIPer, etc.
    """
    rows: list[dict[str, Any]] = []
    for item in raw:
        quarter_end = _parse_date(
            item.get("SHPDate") or item.get("shpDate") or item.get("quarter_end")
        )
        if not quarter_end:
            continue

        rows.append({
            "scripcode": scripcode,
            "quarter_end": quarter_end,
            "promoter_pct": _safe_decimal(
                item.get("PromoterPer") or item.get("promoterPer")
            ),
            "promoter_pledged_pct": _safe_decimal(
                item.get("PromoterPledgedPer") or item.get("promoterPledgedPer")
            ),
            "public_pct": _safe_decimal(
                item.get("PublicPer") or item.get("publicPer")
            ),
            "fii_pct": _safe_decimal(
                item.get("FIIPer") or item.get("fiiPer") or item.get("FPIPer")
            ),
            "dii_pct": _safe_decimal(
                item.get("DIIPer") or item.get("diiPer")
            ),
            "insurance_pct": _safe_decimal(
                item.get("InsurancePer") or item.get("insurancePer")
            ),
            "mutual_funds_pct": _safe_decimal(
                item.get("MFPer") or item.get("mfPer") or item.get("MutualFundPer")
            ),
            "retail_pct": _safe_decimal(
                item.get("RetailPer") or item.get("retailPer") or item.get("IndividualPer")
            ),
            "body_corporate_pct": _safe_decimal(
                item.get("BodyCorpPer") or item.get("bodyCorpPer")
            ),
            "total_shareholders": _safe_int(
                item.get("TotalShareholders") or item.get("totalShareholders")
            ),
            "raw_json": item,
        })
    return rows


def parse_pledge(raw: list[dict[str, Any]], scripcode: str) -> list[dict[str, Any]]:
    """Parse Shrholdpledge response.

    BSE returns pledge history rows with date, holding qty, pledged qty, pct.
    """
    rows: list[dict[str, Any]] = []
    for item in raw:
        as_of = _parse_date(
            item.get("Date") or item.get("date") or item.get("PLEDGEDATE")
        )
        if not as_of:
            continue

        rows.append({
            "as_of_date": as_of,
            "promoter_holding_qty": _safe_int(
                item.get("PromoterHolding") or item.get("promoterHolding")
                or item.get("PROMOTER_HOLDING")
            ),
            "promoter_pledged_qty": _safe_int(
                item.get("PromoterPledged") or item.get("promoterPledged")
                or item.get("PROMOTER_PLEDGED")
            ),
            "pledged_pct": _safe_decimal(
                item.get("PledgedPer") or item.get("pledgedPer")
                or item.get("PLEDGED_PER")
            ),
            "total_shares": _safe_int(
                item.get("TotalShares") or item.get("totalShares")
                or item.get("TOTAL_SHARES")
            ),
        })
    return rows


def parse_insider_trades(raw: list[dict[str, Any]], scripcode: str) -> list[dict[str, Any]]:
    """Parse Cinsidertrading response.

    BSE returns insider trade filings (SEBI PIT Form C/D).
    """
    rows: list[dict[str, Any]] = []
    for item in raw:
        filer = (
            item.get("PERSONNAME") or item.get("personName") or item.get("AcqName") or ""
        ).strip()
        category = (
            item.get("CATEGORY") or item.get("category") or item.get("PERSONCATEGORY") or ""
        ).strip()[:50]
        txn_type = _classify_insider_txn(
            item.get("ACQMODE") or item.get("acqMode") or item.get("TDPTTRANSACTIONTYPE") or ""
        )
        qty = _safe_int(
            item.get("SECACQ") or item.get("secAcq") or item.get("AFTERACQ_SALEOFSHARES")
        )
        value = _safe_decimal(
            item.get("TDPTVALUE") or item.get("tdptValue") or item.get("VALUE")
        )
        txn_date = _parse_date(
            item.get("ACQUISITIONFROMDATE") or item.get("acqfromDt")
            or item.get("TDPTTRANSACTIONDATE")
        )
        acq_mode = (
            item.get("ACQMODE") or item.get("acqMode") or ""
        ).strip()[:50]
        intim_date = _parse_date(
            item.get("INTIMATEDDT") or item.get("intimDt") or item.get("IntimationDate")
        )

        dedup = _sha256(
            scripcode, filer, txn_type, str(txn_date or ""), str(qty or ""),
        )

        if value and value > 0:
            value = value / Decimal("10000000")

        rows.append({
            "filer_name": filer[:200] or None,
            "filer_category": category or None,
            "transaction_type": txn_type,
            "qty": qty,
            "value_cr": value,
            "transaction_date": txn_date,
            "acquisition_mode": acq_mode or None,
            "intimation_date": intim_date,
            "dedup_hash": dedup,
        })
    return rows


def _classify_insider_txn(mode: str) -> str:
    m = (mode or "").lower().strip()
    if "buy" in m or "purchase" in m or "acquisition" in m:
        return "Buy"
    if "sell" in m or "sale" in m or "disposal" in m:
        return "Sell"
    if "pledge" in m and "revoke" not in m:
        return "Pledge"
    if "revoke" in m or "invocation" in m:
        return "Revoke"
    return m[:20] if m else "Other"


def parse_sast(raw: list[dict[str, Any]], scripcode: str) -> list[dict[str, Any]]:
    """Parse CorpSASTData response.

    BSE returns SAST (Substantial Acquisition of Shares and Takeover) disclosures.
    """
    rows: list[dict[str, Any]] = []
    for item in raw:
        acquirer = (
            item.get("ACQUIRERNAME") or item.get("acquirerName") or item.get("AcqName") or ""
        ).strip()
        acq_type = (
            item.get("ACQUIRERTYPE") or item.get("acquirerType") or item.get("PERSONCATEGORY") or ""
        ).strip()[:50]
        pre = _safe_decimal(
            item.get("PREHOLDING") or item.get("preHolding") or item.get("BEFOREACQ")
        )
        post = _safe_decimal(
            item.get("POSTHOLDING") or item.get("postHolding") or item.get("AFTERACQ")
        )
        delta = None
        if pre is not None and post is not None:
            delta = post - pre
        txn_date = _parse_date(
            item.get("TRANSACTIONDATE") or item.get("transDate") or item.get("TDPTTRANSACTIONDATE")
        )
        disc_date = _parse_date(
            item.get("DISCLOSUREDATE") or item.get("disclosureDate") or item.get("INTIMATEDDT")
        )
        regulation = (
            item.get("REGULATION") or item.get("regulation") or item.get("REGNAME") or ""
        ).strip()[:50]

        dedup = _sha256(
            scripcode, acquirer, str(txn_date or ""), str(post or ""),
        )

        rows.append({
            "acquirer_name": acquirer[:300] or None,
            "acquirer_type": acq_type or None,
            "pre_holding_pct": pre,
            "post_holding_pct": post,
            "delta_pct": delta,
            "transaction_date": txn_date,
            "disclosure_date": disc_date,
            "regulation": regulation or None,
            "dedup_hash": dedup,
        })
    return rows
