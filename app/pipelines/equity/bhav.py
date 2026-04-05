"""BHAV parser — supports pre-2010, standard, and UDiFF formats."""

import csv
import io
import zipfile
from datetime import date
from io import BytesIO
from typing import Any, Dict

import httpx

from app.logging import get_logger

logger = get_logger(__name__)


def detect_bhav_format(header_row: list[str]) -> str:
    """Detect NSE BHAV file format from headers."""
    headers = [h.strip().upper() for h in header_row]
    if "TRDSTSIND" in headers or "BISELL" in headers:
        return "udiff"
    elif "TOTTRDQTY" in headers and "ISIN" in headers:
        return "standard"
    elif "TOTTRDQTY" in headers:
        return "pre-2010"
    return "unknown"


async def download_bhav(target_date: date) -> tuple[str, bytes]:
    """Download the correct BHAV copy based on date.
    Returns (format_type, decompressed_csv_bytes).
    """
    dd = target_date.strftime("%d")
    mm = target_date.strftime("%m")
    yyyy = target_date.strftime("%Y")
    mmm = target_date.strftime("%b").upper()
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
        "Authority": "archives.nseindia.com",
    }
    
    async with httpx.AsyncClient() as client:
        # Standard format check
        url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{dd}{mm}{yyyy}.csv"
        resp = await client.get(url, headers=headers)
        
        if resp.status_code == 200:
            return "csv", resp.content
            
        # Pre-2010 / Legacy zip format check
        url = f"https://archives.nseindia.com/content/historical/EQUITIES/{yyyy}/{mmm}/eq_{dd}{mm}{yyyy}_csv.zip"
        resp = await client.get(url, headers=headers)
        
        if resp.status_code == 200:
            with zipfile.ZipFile(BytesIO(resp.content)) as z:
                first_file = z.namelist()[0]
                with z.open(first_file) as f:
                    return "zip", f.read()
                    
        raise ValueError(f"No BHAV copy found for {target_date} on NSE.")


def parse_bhav_content(content: bytes) -> list[Dict[str, Any]]:
    """Parse raw bytes into unified dictionary structure."""
    text = content.decode('utf-8', errors='replace')
    reader = csv.reader(io.StringIO(text))
    
    try:
        header = next(reader)
    except StopIteration:
        return []
        
    format_type = detect_bhav_format(header)
    parsed_rows = []
    
    # We want to normalize the output dict to:
    # symbol, series, open, high, low, close, volume, trades
    # Map the indices based on the format
    if format_type == "udiff":
        sym_idx = header.index("TckrSymb")
        series_idx = header.index("SctySrs")
        o_idx = header.index("OpnPric")
        h_idx = header.index("HghPric")
        l_idx = header.index("LwPric")
        c_idx = header.index("ClsPric")
        v_idx = header.index("TtlTradgVol")
        t_idx = header.index("TotNoOfTrds")
    else:  # standard or pre-2010
        try:
            sym_idx = header.index("SYMBOL")
            series_idx = header.index("SERIES")
            o_idx = header.index("OPEN_PRICE")
            h_idx = header.index("HIGH_PRICE")
            l_idx = header.index("LOW_PRICE")
            c_idx = header.index("CLOSE_PRICE")
            v_idx = header.index("TOTTRDQTY")
            t_idx = header.index("TOTALTRADES") if "TOTALTRADES" in header else -1
        except ValueError as e:
            logger.error(f"Missing expected header in {format_type} format: {e}")
            return []

    for row in reader:
        if len(row) < max(sym_idx, c_idx, v_idx) + 1:
            continue
            
        series = row[series_idx].strip()
        if series not in ("EQ", "BE", "SM"):  # Target equity series only
            continue
            
        try:
            parsed_rows.append({
                "symbol": row[sym_idx].strip(),
                "series": series,
                "open": float(row[o_idx]),
                "high": float(row[h_idx]),
                "low": float(row[l_idx]),
                "close": float(row[c_idx]),
                "volume": int(row[v_idx]) if row[v_idx].strip() else 0,
                "trades": int(row[t_idx]) if t_idx >= 0 and row[t_idx].strip() else 0,
            })
        except ValueError:
            continue
            
    return parsed_rows
