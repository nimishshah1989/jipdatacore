"""Screener.in snapshot metric extraction — ported from theta-india screener_enricher.py.

Parses the top-ratios section of a Screener.in company HTML page to extract
valuation metrics: market cap, PE, PB, book value, dividend yield, ROCE, ROE,
face value, 52-week high/low, sector/industry.
"""

import re
from typing import Optional

from app.logging import get_logger

logger = get_logger(__name__)


def _parse_indian_number(text: str) -> Optional[float]:
    """Parse Indian-formatted numbers: '19,26,475', '52.8', '₹ 413', '0.39 %'."""
    if not text:
        return None
    cleaned = text.replace("\u20b9", "").replace(",", "").replace("%", "").replace("Cr.", "").strip()
    if not cleaned or cleaned == "--":
        return None
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def parse_screener_html(html: str) -> dict:
    """Extract all snapshot metrics from the top-ratios section of Screener.in HTML."""
    result: dict = {}

    ratios_section = re.search(r'id="top-ratios">(.*?)</ul>', html, re.DOTALL)
    if not ratios_section:
        return result

    items = re.findall(r'<li.*?>(.*?)</li>', ratios_section.group(1), re.DOTALL)

    for item in items:
        name_match = re.search(r'<span class="name">(.*?)</span>', item, re.DOTALL)
        if not name_match:
            continue
        name = re.sub(r'<.*?>', '', name_match.group(1)).strip()

        numbers = re.findall(r'<span class="number">([^<]+)</span>', item)
        value_match = re.search(r'class="nowrap[^"]*">(.*?)</span>', item, re.DOTALL)
        full_value = ""
        if value_match:
            full_value = re.sub(r'<.*?>', '', value_match.group(1)).strip()
            full_value = re.sub(r'\s+', ' ', full_value)

        if name == "Market Cap":
            if numbers:
                result["market_cap_cr"] = _parse_indian_number(numbers[0])

        elif name == "Current Price":
            if numbers:
                result["current_price"] = _parse_indian_number(numbers[0])

        elif name == "High / Low":
            if len(numbers) >= 2:
                result["high_52w"] = _parse_indian_number(numbers[0])
                result["low_52w"] = _parse_indian_number(numbers[1])
            elif len(numbers) == 1:
                result["high_52w"] = _parse_indian_number(numbers[0])

        elif name == "Stock P/E":
            result["pe_ratio"] = _parse_indian_number(full_value)

        elif name == "Book Value":
            bv = _parse_indian_number(full_value)
            if bv and bv > 0:
                result["book_value"] = bv
                if result.get("current_price") and result["current_price"] > 0:
                    result["pb_ratio"] = round(result["current_price"] / bv, 4)

        elif name == "Dividend Yield":
            result["dividend_yield_pct"] = _parse_indian_number(full_value)

        elif name == "ROCE":
            result["roce_pct"] = _parse_indian_number(full_value)

        elif name == "ROE":
            result["roe_pct"] = _parse_indian_number(full_value)

        elif name == "Face Value":
            result["face_value"] = _parse_indian_number(full_value)

        elif name == "PEG Ratio":
            result["peg_ratio"] = _parse_indian_number(full_value)

        elif name == "EV/EBITDA" or name == "EV / EBITDA":
            result["ev_ebitda"] = _parse_indian_number(full_value)

    if (
        "pb_ratio" not in result
        and result.get("current_price")
        and result.get("book_value")
        and result["book_value"] > 0
    ):
        result["pb_ratio"] = round(result["current_price"] / result["book_value"], 4)

    return result
