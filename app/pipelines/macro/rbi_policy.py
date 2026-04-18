"""RBI policy rates pipeline — repo, reverse repo, MSF, bank rate, CRR, SLR.

Low-frequency data: changes only at MPC meetings (~8 times/year). The pipeline
scrapes the RBI current-rates HTML page and performs an idempotent insert with
ON CONFLICT DO NOTHING — so repeated daily runs preserve the first observation
date for each rate_type.

Primary source:
    https://www.rbi.org.in/Scripts/BS_NSDPDisplay.aspx?param=4

Because the page exposes only the current (latest) value, effective_date is
best-effort: the parser tries to pull a date from the page; otherwise it falls
back to today. Since ON CONFLICT DO NOTHING is used, first-seen wins.

Design notes:
    - DBIE scraping is notoriously flaky, so parse failures MUST NOT crash
      the daily run. We log WARNING and return an empty ExecutionResult.
    - A SECONDARY static-seed fallback is stubbed below as a TODO; it can be
      enabled if the primary page is restructured or goes offline for long.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.computed import DeRbiPolicyRate
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

RBI_CURRENT_RATES_URL = (
    "https://www.rbi.org.in/Scripts/BS_NSDPDisplay.aspx?param=4"
)

RBI_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Map label strings found in the RBI HTML to our canonical rate_type codes.
# Labels are matched case-insensitively after whitespace normalisation.
RATE_LABEL_MAP: dict[str, str] = {
    "policy repo rate": "REPO",
    "repo rate": "REPO",
    "reverse repo rate": "REVERSE_REPO",
    "fixed reverse repo rate": "REVERSE_REPO",
    "marginal standing facility rate": "MSF",
    "msf rate": "MSF",
    "bank rate": "BANK_RATE",
    "cash reserve ratio": "CRR",
    "crr": "CRR",
    "statutory liquidity ratio": "SLR",
    "slr": "SLR",
}

# TODO (SECONDARY seed fallback): if primary parse yields zero rows for an
# extended period, seed from a hand-maintained historical dict here, e.g.:
#
# HISTORICAL_SEED: dict[date, dict[str, Decimal]] = {
#     date(2024, 10, 9): {
#         "REPO": Decimal("6.5000"),
#         "REVERSE_REPO": Decimal("3.3500"),
#         "MSF": Decimal("6.7500"),
#         "BANK_RATE": Decimal("6.7500"),
#         "CRR": Decimal("4.5000"),
#         "SLR": Decimal("18.0000"),
#     },
#     # ... additional MPC meeting dates
# }
#
# Not enabled in the default run path. Callers can opt in by wiring it into
# execute() below.


def _safe_decimal(value: Any) -> Decimal | None:
    """Convert scraped text to Decimal. Strip '%', commas, whitespace."""
    if value is None:
        return None
    try:
        cleaned = (
            str(value)
            .replace("%", "")
            .replace(",", "")
            .replace("\xa0", " ")
            .strip()
        )
        if cleaned in ("", "-", "N/A", "NA"):
            return None
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def _normalise_label(text: str) -> str:
    """Lowercase + collapse whitespace + strip trailing colons."""
    return re.sub(r"\s+", " ", text or "").strip().rstrip(":").lower()


def _try_parse_date(text: str) -> date | None:
    """Best-effort date parser for RBI page 'as on' / 'w.e.f.' strings."""
    if not text:
        return None
    text = text.strip()
    for fmt in (
        "%d-%b-%Y", "%d %b %Y", "%d %B %Y", "%d/%m/%Y", "%Y-%m-%d",
        "%b %d, %Y", "%B %d, %Y",
    ):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _parse_rbi_html(html: str, fallback_date: date) -> list[dict[str, Any]]:
    """Extract rate rows from RBI current-rates HTML.

    The page layout is an HTML table. Rather than depend on BeautifulSoup we
    use forgiving regex sweeps — the page is simple and this makes the module
    dependency-light. A tr containing "<td>label</td>...<td>value%</td>" is
    the target pattern.

    Returns a list of dicts ready for upsert.
    """
    rows: list[dict[str, Any]] = []

    # Attempt to pull a "w.e.f." / "as on" date from the page header area.
    parsed_effective_date: date | None = None
    date_match = re.search(
        r"(?:w\.?e\.?f\.?|as on|with effect from)[^0-9A-Za-z]*"
        r"([0-9]{1,2}[\- /][A-Za-z]{3,9}[\- /][0-9]{2,4})",
        html,
        flags=re.IGNORECASE,
    )
    if date_match:
        parsed_effective_date = _try_parse_date(date_match.group(1))
    effective_date = parsed_effective_date or fallback_date

    # Extract <tr>...</tr> blocks, then pull <td> cells inside each.
    tr_pattern = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
    td_pattern = re.compile(r"<t[dh]\b[^>]*>(.*?)</t[dh]>", re.IGNORECASE | re.DOTALL)
    tag_strip = re.compile(r"<[^>]+>")

    seen_rate_types: set[str] = set()

    for tr_match in tr_pattern.finditer(html):
        tr_inner = tr_match.group(1)
        cells = [
            tag_strip.sub("", c).replace("&nbsp;", " ").strip()
            for c in td_pattern.findall(tr_inner)
        ]
        if len(cells) < 2:
            continue

        label_norm = _normalise_label(cells[0])
        rate_type = RATE_LABEL_MAP.get(label_norm)
        if rate_type is None:
            # Also try partial contains-match for robustness
            for key, code in RATE_LABEL_MAP.items():
                if key in label_norm:
                    rate_type = code
                    break
        if rate_type is None or rate_type in seen_rate_types:
            continue

        # The numeric value is usually in cell[1], but could be cell[-1].
        rate_pct: Decimal | None = None
        for candidate in (cells[1], cells[-1]):
            rate_pct = _safe_decimal(candidate)
            if rate_pct is not None:
                break
        if rate_pct is None:
            continue

        seen_rate_types.add(rate_type)
        rows.append(
            {
                "effective_date": effective_date,
                "rate_type": rate_type,
                "rate_pct": rate_pct,
                "source": "RBI",
            }
        )

    return rows


async def _fetch_rbi_page(client: httpx.AsyncClient) -> str:
    response = await client.get(
        RBI_CURRENT_RATES_URL, headers=RBI_HEADERS, timeout=30.0
    )
    response.raise_for_status()
    return response.text


async def upsert_rbi_policy_rates(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    """Insert-only upsert: ON CONFLICT DO NOTHING preserves first observation.

    Returns (rows_processed, rows_failed).
    """
    if not rows:
        return 0, 0

    stmt = pg_insert(DeRbiPolicyRate).values(rows)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["effective_date", "rate_type"],
    )
    await session.execute(stmt)
    return len(rows), 0


class RbiPolicyRatesPipeline(BasePipeline):
    """Scrapes the RBI current-rates page for the six policy rates.

    Source: https://www.rbi.org.in/Scripts/BS_NSDPDisplay.aspx?param=4
    Cadence: can run daily; rows only change on MPC decisions.
    Conflict strategy: ON CONFLICT DO NOTHING (historical preserved).
    Failure mode: parse/network errors logged at WARNING, return 0/0 —
    must NOT crash the daily orchestrator.
    """

    pipeline_name = "rbi_policy_rates"
    requires_trading_day = False
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info(
            "rbi_policy_execute_start",
            business_date=business_date.isoformat(),
        )

        html: str | None = None
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                html = await _fetch_rbi_page(client)
        except Exception as exc:
            logger.warning(
                "rbi_policy_fetch_failed",
                error=str(exc),
                business_date=business_date.isoformat(),
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        try:
            rows = _parse_rbi_html(html, fallback_date=business_date)
        except Exception as exc:
            logger.warning(
                "rbi_policy_parse_failed",
                error=str(exc),
                business_date=business_date.isoformat(),
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        if not rows:
            logger.warning(
                "rbi_policy_no_rows_parsed",
                business_date=business_date.isoformat(),
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        rows_processed, rows_failed = await upsert_rbi_policy_rates(session, rows)

        logger.info(
            "rbi_policy_upserted",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            business_date=business_date.isoformat(),
            rate_types=[r["rate_type"] for r in rows],
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
