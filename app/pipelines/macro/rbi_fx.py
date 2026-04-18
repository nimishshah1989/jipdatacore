"""RBI / FBIL INR reference exchange rates pipeline.

Primary source: Yahoo Finance (USDINR=X, EURINR=X, GBPINR=X, JPYINR=X) — reliable,
broad historical coverage, no anti-bot issues.
Secondary: FBIL JSON API (https://fbil.org.in/ReferenceRatePublishApi/PublishApi/getReferenceRate).
Tertiary: FBIL archive HTML page (USD-INR-Reference-Rate-Archives.aspx).

Captures the four core INR reference rates published daily (post 13:30 IST on
working days): USD/INR, EUR/INR, GBP/INR, JPY/INR.
"""

from __future__ import annotations

import asyncio
import re
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.computed import DeRbiFxRate
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

FBIL_JSON_API_URL = (
    "https://fbil.org.in/ReferenceRatePublishApi/PublishApi/getReferenceRate"
)
FBIL_ARCHIVE_HTML_URL = (
    "https://www.fbil.org.in/Archives/USD-INR-Reference-Rate-Archives.aspx"
)

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.fbil.org.in/",
    "Origin": "https://www.fbil.org.in",
    "Content-Type": "application/json",
}

# Map FBIL API currency tokens -> canonical currency_pair values used in DB.
# FBIL publishes USD, EUR, GBP as rate-per-1-unit and JPY as rate-per-100-units.
SUPPORTED_CURRENCIES: dict[str, str] = {
    "USD": "USD/INR",
    "EUR": "EUR/INR",
    "GBP": "GBP/INR",
    "JPY": "JPY/INR",
}


def _safe_decimal(value: Any) -> Decimal | None:
    """Convert a value to Decimal safely (tolerant of commas / whitespace)."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned in ("", "-", "N/A"):
            return None
        return Decimal(cleaned)
    except Exception:
        return None


def _normalise_currency_token(token: Any) -> str | None:
    """Extract a 3-letter ISO currency code from a raw FBIL label."""
    if token is None:
        return None
    raw = str(token).strip().upper()
    # FBIL labels often look like "INR/USD" or "USD" or "USD (100)".
    match = re.search(r"\b(USD|EUR|GBP|JPY)\b", raw)
    return match.group(1) if match else None


def _parse_fbil_json(
    payload: Any,
    business_date: date,
) -> list[dict[str, Any]]:
    """Parse the FBIL JSON reference-rate response into DB row dicts."""
    rows: list[dict[str, Any]] = []

    # FBIL response shape varies; it is usually a list of records, or a dict
    # with a "TT_DATA" / "data" key wrapping a list.
    records: list[Any]
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict):
        for key in ("TT_DATA", "data", "Data", "result", "Table"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                records = candidate
                break
        else:
            records = []
    else:
        records = []

    for record in records:
        if not isinstance(record, dict):
            continue

        ccy_token = (
            record.get("CCY_PAIR")
            or record.get("currency")
            or record.get("Currency")
            or record.get("PAIR")
            or record.get("pair")
        )
        ccy = _normalise_currency_token(ccy_token)
        if ccy is None or ccy not in SUPPORTED_CURRENCIES:
            continue

        rate_val = (
            record.get("RATE")
            or record.get("rate")
            or record.get("referenceRate")
            or record.get("Reference_Rate")
        )
        rate = _safe_decimal(rate_val)
        if rate is None:
            continue

        rows.append(
            {
                "rate_date": business_date,
                "currency_pair": SUPPORTED_CURRENCIES[ccy],
                "reference_rate": rate,
                "source": "FBIL",
            }
        )

    return rows


def _parse_fbil_archive_html(
    html: str,
    business_date: date,
) -> list[dict[str, Any]]:
    """Best-effort parser for the FBIL archive HTML page.

    The archive page publishes rows like:
        <tr><td>DD/MM/YYYY</td><td>USD</td><td>83.1234</td>...</tr>
    We scan for the first block matching the requested date and pull
    USD/EUR/GBP/JPY values.
    """
    rows: list[dict[str, Any]] = []
    date_str = business_date.strftime("%d/%m/%Y")

    # Extract all table rows as (cell1, cell2, ...) tuples.
    row_pattern = re.compile(
        r"<tr[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL
    )
    cell_pattern = re.compile(
        r"<t[dh][^>]*>(.*?)</t[dh]>", re.IGNORECASE | re.DOTALL
    )
    tag_strip = re.compile(r"<[^>]+>")

    for row_html in row_pattern.findall(html):
        cells = [
            tag_strip.sub("", c).strip()
            for c in cell_pattern.findall(row_html)
        ]
        if not cells:
            continue
        if date_str not in cells[0]:
            continue

        # Look across remaining cells for (currency, rate) adjacency.
        for i in range(len(cells) - 1):
            ccy = _normalise_currency_token(cells[i])
            if ccy is None or ccy not in SUPPORTED_CURRENCIES:
                continue
            rate = _safe_decimal(cells[i + 1])
            if rate is None:
                continue
            rows.append(
                {
                    "rate_date": business_date,
                    "currency_pair": SUPPORTED_CURRENCIES[ccy],
                    "reference_rate": rate,
                    "source": "FBIL",
                }
            )

    # Deduplicate on currency_pair — keep first occurrence.
    seen: set[str] = set()
    unique_rows: list[dict[str, Any]] = []
    for r in rows:
        if r["currency_pair"] in seen:
            continue
        seen.add(r["currency_pair"])
        unique_rows.append(r)
    return unique_rows


YFINANCE_TICKERS: dict[str, str] = {
    "USDINR=X": "USD/INR",
    "EURINR=X": "EUR/INR",
    "GBPINR=X": "GBP/INR",
    "JPYINR=X": "JPY/INR",
}


def _fetch_from_yfinance_sync(business_date: date) -> list[dict[str, Any]]:
    """Synchronous yfinance fetch — runs in a thread via asyncio.to_thread.

    Uses Ticker.history() (not yf.download) to avoid MultiIndex column
    issues in newer yfinance/pandas versions. JPY/INR from yfinance is
    rate-per-1-JPY; FBIL publishes rate-per-100-JPY, so we scale by 100.
    """
    import yfinance as yf  # noqa: PLC0415

    rows: list[dict[str, Any]] = []
    start = business_date - timedelta(days=5)
    end = business_date + timedelta(days=1)

    for ticker, pair in YFINANCE_TICKERS.items():
        try:
            t = yf.Ticker(ticker)
            df = t.history(start=start.isoformat(), end=end.isoformat())
            if df is None or df.empty:
                continue
            # Find exact date or closest prior
            matching = df[df.index.date == business_date]
            if not matching.empty:
                raw = float(matching["Close"].iloc[-1])
            else:
                raw = float(df["Close"].iloc[-1])
            if pair == "JPY/INR":
                raw *= 100
            rows.append(
                {
                    "rate_date": business_date,
                    "currency_pair": pair,
                    "reference_rate": Decimal(str(round(raw, 4))),
                    "source": "YFINANCE",
                }
            )
        except Exception as exc:
            logger.warning(
                "rbi_fx_yfinance_ticker_failed",
                ticker=ticker,
                error=str(exc),
                business_date=business_date.isoformat(),
            )
    return rows


async def _fetch_from_yfinance(business_date: date) -> list[dict[str, Any]]:
    """Async wrapper over synchronous yfinance call."""
    return await asyncio.to_thread(_fetch_from_yfinance_sync, business_date)


async def _fetch_from_fbil_json(
    client: httpx.AsyncClient,
    business_date: date,
) -> Any:
    """POST the business date to the FBIL JSON reference-rate endpoint."""
    body = {"date": business_date.strftime("%d/%m/%Y")}
    response = await client.post(
        FBIL_JSON_API_URL,
        json=body,
        headers=BROWSER_HEADERS,
        timeout=20.0,
    )
    response.raise_for_status()
    return response.json()


async def _fetch_from_fbil_archive(
    client: httpx.AsyncClient,
) -> str:
    """Fetch the FBIL USD/INR archive HTML page as a fallback."""
    response = await client.get(
        FBIL_ARCHIVE_HTML_URL,
        headers=BROWSER_HEADERS,
        timeout=30.0,
    )
    response.raise_for_status()
    return response.text


async def upsert_rbi_fx_rates(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    """Upsert FX rate rows into de_rbi_fx_rate.

    Returns (rows_processed, rows_failed).
    """
    if not rows:
        return 0, 0

    stmt = pg_insert(DeRbiFxRate).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["rate_date", "currency_pair"],
        set_={
            "reference_rate": stmt.excluded.reference_rate,
            "source": stmt.excluded.source,
        },
    )
    await session.execute(stmt)
    return len(rows), 0


class RbiFxRatesPipeline(BasePipeline):
    """Fetches daily RBI / FBIL INR reference exchange rates.

    Primary source: FBIL JSON reference-rate API.
    Fallback on error: FBIL archive HTML page.
    Captures USD/INR, EUR/INR, GBP/INR, JPY/INR.
    Trigger: After FBIL publishes (13:30 IST on working days).
    """

    pipeline_name = "rbi_fx_rates"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info(
            "rbi_fx_execute_start",
            business_date=business_date.isoformat(),
        )

        rows: list[dict[str, Any]] = []
        source_used = "YFINANCE"

        # ---- PRIMARY: yfinance (reliable, wide historical coverage) ----
        try:
            rows = await _fetch_from_yfinance(business_date)
            if rows:
                logger.info(
                    "rbi_fx_yfinance_success",
                    parsed_rows=len(rows),
                    business_date=business_date.isoformat(),
                )
        except Exception as yf_exc:
            logger.warning(
                "rbi_fx_yfinance_failed_trying_fbil",
                error=str(yf_exc),
                business_date=business_date.isoformat(),
            )

        # ---- SECONDARY: FBIL JSON API ----
        if not rows:
            async with httpx.AsyncClient() as client:
                try:
                    payload = await _fetch_from_fbil_json(client, business_date)
                    rows = _parse_fbil_json(payload, business_date)
                    if rows:
                        source_used = "FBIL_JSON"
                        logger.info(
                            "rbi_fx_fbil_json_success",
                            parsed_rows=len(rows),
                            business_date=business_date.isoformat(),
                        )
                except Exception as json_exc:
                    logger.warning(
                        "rbi_fx_fbil_json_failed_trying_archive",
                        error=str(json_exc),
                        business_date=business_date.isoformat(),
                    )

                # ---- TERTIARY: FBIL archive HTML ----
                if not rows:
                    try:
                        html = await _fetch_from_fbil_archive(client)
                        rows = _parse_fbil_archive_html(html, business_date)
                        if rows:
                            source_used = "FBIL_HTML"
                            logger.info(
                                "rbi_fx_fbil_html_fallback_success",
                                parsed_rows=len(rows),
                                business_date=business_date.isoformat(),
                            )
                    except Exception as html_exc:
                        logger.warning(
                            "rbi_fx_fbil_html_fallback_failed",
                            error=str(html_exc),
                            business_date=business_date.isoformat(),
                        )

        if not rows:
            logger.warning(
                "rbi_fx_no_rows_all_sources_failed",
                business_date=business_date.isoformat(),
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        rows_processed, rows_failed = await upsert_rbi_fx_rates(session, rows)

        logger.info(
            "rbi_fx_upserted",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            source=source_used,
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
