"""AMFI NAV file fetcher and parser."""

import csv
import io
from datetime import datetime
from typing import Dict, Any, List

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger

logger = get_logger(__name__)

AMFI_URL = "https://www.amfiindia.com/spages/NAVAll.txt"


async def fetch_amfi_nav() -> str:
    """Download the current live NAV file from AMFI."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Standard AMFI response is raw text separated by '\r\n'
        resp = await client.get(AMFI_URL)
        resp.raise_for_status()
        return resp.text


def parse_amfi_nav(content: str) -> List[Dict[str, Any]]:
    """Parse the AMFI text file format.
    
    Expected format: Semicolon delimited.
    Header: Scheme Code;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date
    Example row: 120503;INF205K01UP5;-;Aditya Birla Sun Life Frontline Equity Fund;579.2;05-Apr-2026
    """
    lines = content.splitlines()
    parsed_records = []
    
    # AMFI file often has blank lines or section headers (e.g. "Open Ended Schemes (Equity Scheme - Large Cap Fund)")
    # Valid lines always start with a numeric Scheme Code.
    
    for line in lines:
        line = line.strip()
        if not line or not line[0].isdigit():
            continue
            
        parts = line.split(";")
        if len(parts) >= 6:
            try:
                amfi_code = parts[0].strip()
                isin = parts[1].strip()
                fund_name = parts[3].strip()
                nav_str = parts[4].strip()
                date_str = parts[5].strip()
                
                # Check for N.A. (not available)
                if not nav_str or nav_str.upper() == "N.A.":
                    continue
                    
                nav = float(nav_str)
                nav_date = datetime.strptime(date_str, "%d-%b-%Y").date()
                
                parsed_records.append({
                    "amfi_code": amfi_code,
                    "isin": isin if isin and isin != "-" else None,
                    "fund_name": fund_name,
                    "nav": nav,
                    "nav_date": nav_date
                })
            except ValueError:
                # Handle edge cases where date or NAV parsing fails
                continue
                
    return parsed_records
