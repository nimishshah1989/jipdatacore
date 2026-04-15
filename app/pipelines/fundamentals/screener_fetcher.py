"""Screener.in HTML fetcher — ported from theta-india/india_alpha/fetchers/screener_fetcher.py.

Fetches company pages from screener.in and parses HTML tables for financial data.
Authentication: requires SCREENER_SESSION_COOKIE from a logged-in browser session.
Rate limit: ~1 request per 1.2 seconds (free tier).
"""

import re
from calendar import monthrange
from typing import Optional

import httpx

from app.logging import get_logger

logger = get_logger(__name__)

SCREENER_BASE = "https://www.screener.in"

MONTH_MAP = {
    "Jan": "01", "Feb": "02", "Mar": "03",
    "Apr": "04", "May": "05", "Jun": "06",
    "Jul": "07", "Aug": "08", "Sep": "09",
    "Oct": "10", "Nov": "11", "Dec": "12",
}


def parse_screener_date(date_str: str) -> Optional[str]:
    """Convert 'Mar 2024' -> '2024-03-31', 'TTM' -> None."""
    if not date_str or date_str.strip() == "TTM":
        return None
    try:
        parts = str(date_str).strip().split()
        if len(parts) == 2:
            month_abbr = parts[0][:3]
            year = int(parts[1])
            if month_abbr in MONTH_MAP:
                month_num = int(MONTH_MAP[month_abbr])
                last_day = monthrange(year, month_num)[1]
                return f"{year}-{MONTH_MAP[month_abbr]}-{last_day:02d}"
    except Exception:
        pass
    return None


def safe_float(val) -> Optional[float]:
    """Convert screener value to float, handling commas and % signs."""
    if val is None or val == "" or val == "--" or val == "\u2014":
        return None
    try:
        cleaned = str(val).replace(",", "").replace("%", "").strip()
        return float(cleaned) if cleaned else None
    except (ValueError, TypeError):
        return None


def _parse_html_table(table_html: str) -> dict:
    """Parse a Screener.in HTML data-table into {headers: [...], rows: {name: [values]}}."""
    result: dict = {"headers": [], "rows": {}}

    thead = re.search(r'<thead>(.*?)</thead>', table_html, re.DOTALL)
    if thead:
        headers = re.findall(r'<th[^>]*>(.*?)</th>', thead.group(1), re.DOTALL)
        result["headers"] = [re.sub(r'<.*?>', '', h).strip() for h in headers]

    tbody = re.search(r'<tbody>(.*?)</tbody>', table_html, re.DOTALL)
    if not tbody:
        return result

    for row_match in re.finditer(r'<tr[^>]*>(.*?)</tr>', tbody.group(1), re.DOTALL):
        row_html = row_match.group(1)
        cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row_html, re.DOTALL)
        if not cells:
            continue
        row_name = re.sub(r'<[^>]+>', '', cells[0], flags=re.DOTALL)
        row_name = re.sub(r'&nbsp;', ' ', row_name).strip().rstrip('+').strip()
        values = [re.sub(r'<[^>]+>', '', c, flags=re.DOTALL).strip() for c in cells[1:]]
        if row_name:
            result["rows"][row_name] = values

    return result


async def fetch_company_html(
    client: httpx.AsyncClient,
    ticker: str,
) -> Optional[str]:
    """Fetch one Screener.in company HTML page. Returns HTML or None."""
    url = f"{SCREENER_BASE}/company/{ticker}/"
    try:
        resp = await client.get(url)
        if resp.status_code == 200:
            return resp.text
        if resp.status_code == 403:
            logger.warning("screener_auth_failed", ticker=ticker)
        else:
            logger.debug("screener_page_failed", ticker=ticker, status=resp.status_code)
        return None
    except Exception as e:
        logger.debug("screener_request_failed", ticker=ticker, error=str(e)[:80])
        return None


def _find_section_table(html: str, section_name: str) -> Optional[str]:
    """Find the data-table HTML for a named section."""
    section_patterns = {
        "quarters": r'id="quarters".*?(<table[^>]*class="data-table[^"]*".*?</table>)',
        "profit_loss": r'id="profit-loss".*?(<table[^>]*class="data-table[^"]*".*?</table>)',
        "balance_sheet": r'id="balance-sheet".*?(<table[^>]*class="data-table[^"]*".*?</table>)',
        "cash_flow": r'id="cash-flow".*?(<table[^>]*class="data-table[^"]*".*?</table>)',
        "ratios": r'id="ratios".*?(<table[^>]*class="data-table[^"]*".*?</table>)',
        "shareholding": r'id="shareholding".*?(<table[^>]*class="data-table[^"]*".*?</table>)',
    }
    pattern = section_patterns.get(section_name)
    if not pattern:
        return None

    match = re.search(pattern, html, re.DOTALL)
    if match:
        return match.group(1)

    heading_patterns = {
        "quarters": "Quarterly Results",
        "profit_loss": "Profit &amp; Loss",
        "balance_sheet": "Balance Sheet",
        "cash_flow": "Cash Flows",
        "ratios": "Ratios",
        "shareholding": "Shareholding Pattern",
    }
    heading = heading_patterns.get(section_name, "")
    if heading:
        fallback = re.search(
            heading + r'.*?(<table[^>]*class="data-table[^"]*".*?</table>)',
            html, re.DOTALL,
        )
        if fallback:
            return fallback.group(1)
    return None


_SHAREHOLDING_ROW_MAP = {
    "promoters": "promoter_pct",
    "promoter": "promoter_pct",
    "promoters & promoter group": "promoter_pct",
    "fiis": "fii_pct",
    "fii": "fii_pct",
    "foreign institutional investors": "fii_pct",
    "diis": "dii_pct",
    "dii": "dii_pct",
    "domestic institutional investors": "dii_pct",
    "public": "public_pct",
    "government": "govt_pct",
    "others": "others_pct",
}


def extract_shareholding(html: str) -> dict:
    """Extract latest quarter shareholding percentages. Returns flat dict."""
    result: dict = {}
    sh_table = _find_section_table(html, "shareholding")
    if not sh_table:
        return result

    sh_data = _parse_html_table(sh_table)
    headers = sh_data["headers"]
    if len(headers) < 2:
        return result

    last_col = len(headers) - 1
    for row_name, values in sh_data["rows"].items():
        clean_name = row_name.replace("\xa0", "").replace("&nbsp;", "").strip().lower()
        field = _SHAREHOLDING_ROW_MAP.get(clean_name)
        if not field:
            continue
        adj_idx = last_col - 1
        if 0 <= adj_idx < len(values):
            result[field] = safe_float(values[adj_idx])

    return result


def extract_ratios_latest(html: str) -> dict:
    """Extract latest-period ratios from the Ratios section table."""
    result: dict = {}
    ratios_table = _find_section_table(html, "ratios")
    if not ratios_table:
        return result

    ratios_data = _parse_html_table(ratios_table)
    headers = ratios_data["headers"]
    if len(headers) < 2:
        return result

    last_col = len(headers) - 1

    def _get(row_name: str) -> Optional[float]:
        vals = ratios_data["rows"].get(row_name, [])
        adj = last_col - 1
        if 0 <= adj < len(vals):
            return safe_float(vals[adj])
        return None

    result["debtor_days"] = _get("Debtor Days")
    result["roce_pct"] = _get("ROCE %")
    result["roe_pct"] = _get("ROE %")
    return result


def extract_pl_growth(html: str) -> dict:
    """Extract YoY revenue/profit growth from last two annual periods."""
    result: dict = {}
    pl_table = _find_section_table(html, "profit_loss")
    if not pl_table:
        return result

    pl_data = _parse_html_table(pl_table)
    headers = pl_data["headers"]
    if len(headers) < 3:
        return result

    def _get_val(row_name: str, idx: int) -> Optional[float]:
        vals = pl_data["rows"].get(row_name, [])
        adj = idx - 1
        if 0 <= adj < len(vals):
            return safe_float(vals[adj])
        return None

    dated_indices = []
    for i, h in enumerate(headers):
        if i == 0:
            continue
        if parse_screener_date(h):
            dated_indices.append(i)

    if len(dated_indices) >= 2:
        curr = dated_indices[-1]
        prev = dated_indices[-2]

        rev_curr = _get_val("Sales", curr) or _get_val("Revenue", curr)
        rev_prev = _get_val("Sales", prev) or _get_val("Revenue", prev)
        if rev_curr and rev_prev and rev_prev != 0:
            result["revenue_growth_yoy_pct"] = round(
                ((rev_curr - rev_prev) / abs(rev_prev)) * 100, 4
            )

        pat_curr = _get_val("Net Profit", curr)
        pat_prev = _get_val("Net Profit", prev)
        if pat_curr and pat_prev and pat_prev != 0:
            result["profit_growth_yoy_pct"] = round(
                ((pat_curr - pat_prev) / abs(pat_prev)) * 100, 4
            )

    ttm_idx = None
    for i, h in enumerate(headers):
        if h.strip() == "TTM":
            ttm_idx = i
            break

    if ttm_idx:
        eps = _get_val("EPS in Rs", ttm_idx)
        if eps is not None:
            result["eps_ttm"] = eps

        revenue = _get_val("Sales", ttm_idx) or _get_val("Revenue", ttm_idx)
        pat = _get_val("Net Profit", ttm_idx)
        opm = _get_val("OPM %", ttm_idx)
        if opm is not None:
            result["operating_margin_pct"] = opm
        if revenue and pat and revenue != 0:
            result["net_margin_pct"] = round((pat / revenue) * 100, 4)

    return result


def extract_balance_sheet_latest(html: str) -> dict:
    """Extract latest balance sheet ratios."""
    result: dict = {}
    bs_table = _find_section_table(html, "balance_sheet")
    if not bs_table:
        return result

    bs_data = _parse_html_table(bs_table)
    headers = bs_data["headers"]
    if len(headers) < 2:
        return result

    last_col = len(headers) - 1

    def _get(row_name: str) -> Optional[float]:
        vals = bs_data["rows"].get(row_name, [])
        adj = last_col - 1
        if 0 <= adj < len(vals):
            return safe_float(vals[adj])
        return None

    borrowings = _get("Borrowings")
    equity_capital = _get("Equity Capital") or 0
    reserves = _get("Reserves") or 0
    net_worth = equity_capital + reserves

    if net_worth and net_worth > 0 and borrowings is not None:
        result["debt_to_equity"] = round(borrowings / net_worth, 4)

    return result


def build_http_client(session_cookie: str) -> httpx.AsyncClient:
    """Create an httpx client configured for Screener.in requests."""
    return httpx.AsyncClient(
        timeout=20,
        headers={
            "Cookie": f"sessionid={session_cookie}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Referer": "https://www.screener.in/",
        },
        follow_redirects=True,
    )
