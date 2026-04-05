"""NSE corporate actions ingestion pipeline.

Fetches corporate actions (splits, bonuses, dividends, rights) from the
NSE corporateActions API and inserts into de_corporate_actions.

NSE API requires session cookie for authentication — uses httpx with
cookie jar from a pre-seeded NSE homepage visit.
"""

from __future__ import annotations


import asyncio
import uuid
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from app.logging import get_logger
from app.models.instruments import DeInstrument
from app.models.pipeline import DePipelineLog
from app.models.prices import DeCorporateActions
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.validation import AnomalyRecord
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)

# NSE Corporate Actions API
NSE_CORP_ACTIONS_URL = (
    "https://www.nseindia.com/api/corporates-corporateActions"
    "?index=equities&from_date={from_date}&to_date={to_date}"
)
NSE_HOMEPAGE_URL = "https://www.nseindia.com"

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.nseindia.com/",
    "X-Requested-With": "XMLHttpRequest",
}

DOWNLOAD_RETRIES = 3
RETRY_BACKOFF_BASE = 2.0

# Split ratio validation bounds
MIN_VALID_RATIO = Decimal("0.01")   # 1:100 (adj_factor = 0.01)
MAX_VALID_RATIO = Decimal("100")    # 100:1 (adj_factor = 100)
ANOMALY_RATIO_THRESHOLD = Decimal("100")  # >100x flagged as anomaly


def compute_adjustment_factor(
    ratio_from: Decimal,
    ratio_to: Decimal,
    action_type: str,
) -> Decimal:
    """Compute the adjustment factor for a corporate action.

    For splits and bonuses, adj_factor = ratio_from / ratio_to.

    Examples:
    - Stock split 1:10 → ratio_from=1, ratio_to=10, adj_factor=0.1
    - Bonus 1:1 → ratio_from=1, ratio_to=2, adj_factor=0.5
    - Rights issue 1:3 → ratio_from=1, ratio_to=3, adj_factor=0.333...

    Args:
        ratio_from: Numerator of the ratio (typically 1).
        ratio_to: Denominator of the ratio.
        action_type: Type of corporate action.

    Returns:
        Decimal adjustment factor (ratio_from / ratio_to).

    Raises:
        ValueError: If ratio_to is zero.
    """
    if ratio_to == Decimal("0"):
        raise ValueError(f"ratio_to cannot be zero for {action_type}")
    return ratio_from / ratio_to


def parse_nse_corporate_actions(
    raw_data: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Parse NSE corporateActions API response into normalized action dicts.

    Each returned dict has keys matching de_corporate_actions columns:
    symbol, ex_date, action_type, dividend_type, ratio_from, ratio_to,
    cash_value, adj_factor, notes.

    Args:
        raw_data: List of raw action dicts from NSE API.

    Returns:
        List of normalized action dicts.
    """
    actions: list[dict[str, Any]] = []

    for item in raw_data:
        symbol = (item.get("symbol") or "").strip().upper()
        if not symbol:
            continue

        # Parse ex-date
        ex_date: date | None = None
        ex_date_str = item.get("exDate") or item.get("ex_date") or ""
        if ex_date_str:
            from datetime import datetime
            for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
                try:
                    ex_date = datetime.strptime(ex_date_str.strip(), fmt).date()
                    break
                except ValueError:
                    continue

        if ex_date is None:
            logger.warning("corp_action_no_exdate", symbol=symbol, raw=str(item)[:100])
            continue

        # Determine action type
        purpose = (item.get("purpose") or item.get("subject") or "").strip().lower()
        action_type, dividend_type = _classify_action(purpose)

        # Parse ratios and cash values
        ratio_from: Decimal | None = None
        ratio_to: Decimal | None = None
        cash_value: Decimal | None = None
        adj_factor: Decimal | None = None

        remarks = item.get("remarks") or item.get("purpose") or ""

        if action_type in ("split", "bonus", "rights"):
            ratio_from, ratio_to = _parse_ratio(purpose)
            if ratio_from is not None and ratio_to is not None:
                try:
                    adj_factor = compute_adjustment_factor(ratio_from, ratio_to, action_type)
                except ValueError as exc:
                    logger.warning(
                        "corp_action_adj_factor_error",
                        symbol=symbol,
                        error=str(exc),
                    )

        elif action_type == "dividend":
            div_val_str = item.get("divPerShare") or item.get("div_per_share") or ""
            if div_val_str:
                try:
                    cash_value = Decimal(str(div_val_str).strip())
                except InvalidOperation:
                    pass

        actions.append(
            {
                "symbol": symbol,
                "ex_date": ex_date,
                "action_type": action_type,
                "dividend_type": dividend_type,
                "ratio_from": ratio_from,
                "ratio_to": ratio_to,
                "cash_value": cash_value,
                "adj_factor": adj_factor,
                "notes": str(remarks)[:500] if remarks else None,
            }
        )

    return actions


def _classify_action(purpose: str) -> tuple[str, str | None]:
    """Classify an action type and dividend_type from purpose string.

    Args:
        purpose: Lowercased purpose/subject string from NSE API.

    Returns:
        Tuple of (action_type, dividend_type).
    """
    purpose_lower = purpose.lower()

    if "split" in purpose_lower or "sub-division" in purpose_lower:
        return "split", None
    elif "bonus" in purpose_lower:
        return "bonus", None
    elif "rights" in purpose_lower:
        return "rights", None
    elif "merger" in purpose_lower or "amalgam" in purpose_lower:
        return "merger", None
    elif "buyback" in purpose_lower or "buy-back" in purpose_lower:
        return "buyback", None
    elif "demerger" in purpose_lower or "de-merger" in purpose_lower:
        return "demerger", None
    elif "dividend" in purpose_lower or "div" in purpose_lower:
        div_type: str | None = None
        if "interim" in purpose_lower:
            div_type = "interim"
        elif "final" in purpose_lower:
            div_type = "final"
        elif "special" in purpose_lower:
            div_type = "special"
        else:
            div_type = "final"  # default
        return "dividend", div_type
    else:
        return "other", None


def _parse_ratio(purpose: str) -> tuple[Decimal | None, Decimal | None]:
    """Extract ratio_from:ratio_to from a purpose string.

    Looks for patterns like "1:10", "1:2", "3:4" etc.

    Args:
        purpose: The purpose or remarks string.

    Returns:
        Tuple of (ratio_from, ratio_to) or (None, None) if not found.
    """
    import re
    match = re.search(r"(\d+(?:\.\d+)?)\s*:\s*(\d+(?:\.\d+)?)", purpose)
    if match:
        try:
            ratio_from = Decimal(match.group(1))
            ratio_to = Decimal(match.group(2))
            return ratio_from, ratio_to
        except InvalidOperation:
            pass
    return None, None


async def _get_nse_session_cookie(client: httpx.AsyncClient) -> None:
    """Pre-seed NSE session cookie by visiting the homepage.

    NSE API requires a valid session cookie obtained from the homepage.
    The cookie is automatically stored in the httpx client's cookie jar.

    Args:
        client: httpx async client (cookie jar is mutated in place).
    """
    try:
        response = await client.get(NSE_HOMEPAGE_URL, follow_redirects=True)
        response.raise_for_status()
        logger.debug("nse_session_cookie_seeded", cookies=list(client.cookies.keys()))
    except httpx.HTTPError as exc:
        logger.warning("nse_session_cookie_failed", error=str(exc))


async def _fetch_corporate_actions(
    client: httpx.AsyncClient,
    from_date: date,
    to_date: date,
) -> list[dict[str, Any]]:
    """Fetch corporate actions from NSE API with retry.

    Args:
        client: httpx async client (must have NSE session cookie).
        from_date: Start date for actions lookup.
        to_date: End date for actions lookup.

    Returns:
        Raw list of action dicts from NSE API.
    """
    url = NSE_CORP_ACTIONS_URL.format(
        from_date=from_date.strftime("%d-%m-%Y"),
        to_date=to_date.strftime("%d-%m-%Y"),
    )

    last_exc: Exception | None = None
    for attempt in range(DOWNLOAD_RETRIES):
        try:
            response = await client.get(url, follow_redirects=True)
            response.raise_for_status()
            data = response.json()
            # API may return list directly or under a key
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get("data", data.get("corporateActions", []))
            return []
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as exc:
            last_exc = exc
            wait_secs = RETRY_BACKOFF_BASE ** attempt
            logger.warning(
                "corp_actions_download_retry",
                attempt=attempt + 1,
                wait_secs=wait_secs,
                error=str(exc),
            )
            if attempt < DOWNLOAD_RETRIES - 1:
                await asyncio.sleep(wait_secs)

    raise last_exc or RuntimeError("Failed to fetch NSE corporate actions")


class CorporateActionsPipeline(BasePipeline):
    """NSE corporate actions ingestion pipeline.

    Fetches actions for business_date ± 30 days window to capture
    recently announced and upcoming ex-dates.

    Validates split ratios and flags extreme adjustments as anomalies.
    """

    pipeline_name = "equity_corporate_actions"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Fetch and upsert corporate actions."""
        from datetime import timedelta

        logger.info("corp_actions_execute_start", business_date=business_date.isoformat())

        # Fetch window: 30 days before and after business_date
        from_date = business_date - timedelta(days=30)
        to_date = business_date + timedelta(days=30)

        async with httpx.AsyncClient(headers=NSE_HEADERS, timeout=60.0) as client:
            # Seed session cookie
            await _get_nse_session_cookie(client)
            raw_data = await _fetch_corporate_actions(client, from_date, to_date)

        logger.info(
            "corp_actions_fetched",
            count=len(raw_data),
            from_date=from_date.isoformat(),
            to_date=to_date.isoformat(),
        )

        actions = parse_nse_corporate_actions(raw_data)

        # Load symbol → instrument_id map
        symbol_to_id = await _load_symbol_map(session)

        rows_processed = 0
        rows_failed = 0
        anomaly_symbols: list[str] = []

        insert_rows: list[dict[str, Any]] = []

        for action in actions:
            symbol = action["symbol"]
            instrument_id = symbol_to_id.get(symbol)
            if instrument_id is None:
                logger.warning("corp_action_unknown_symbol", symbol=symbol)
                rows_failed += 1
                continue

            # Validate split ratio if applicable
            ratio_from = action.get("ratio_from")
            ratio_to = action.get("ratio_to")
            if ratio_from is not None and ratio_to is not None:
                try:
                    adj = compute_adjustment_factor(ratio_from, ratio_to, action["action_type"])
                    if adj > ANOMALY_RATIO_THRESHOLD or adj < (Decimal("1") / ANOMALY_RATIO_THRESHOLD):
                        anomaly_symbols.append(symbol)
                        logger.warning(
                            "corp_action_extreme_ratio",
                            symbol=symbol,
                            ratio_from=str(ratio_from),
                            ratio_to=str(ratio_to),
                            adj_factor=str(adj),
                        )
                except ValueError:
                    pass

            insert_rows.append(
                {
                    "id": uuid.uuid4(),
                    "instrument_id": instrument_id,
                    "ex_date": action["ex_date"],
                    "action_type": action["action_type"],
                    "dividend_type": action.get("dividend_type"),
                    "ratio_from": action.get("ratio_from"),
                    "ratio_to": action.get("ratio_to"),
                    "cash_value": action.get("cash_value"),
                    "adj_factor": action.get("adj_factor"),
                    "notes": action.get("notes"),
                }
            )

        if insert_rows:
            stmt = pg_insert(DeCorporateActions).values(insert_rows)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_corporate_actions",
                set_={
                    "ratio_from": stmt.excluded.ratio_from,
                    "ratio_to": stmt.excluded.ratio_to,
                    "cash_value": stmt.excluded.cash_value,
                    "adj_factor": stmt.excluded.adj_factor,
                    "notes": stmt.excluded.notes,
                },
            )
            await session.execute(stmt)
            rows_processed = len(insert_rows)

        logger.info(
            "corp_actions_execute_complete",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            anomaly_count=len(anomaly_symbols),
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )

    async def validate(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> list[AnomalyRecord]:
        """Validate corporate actions for extreme ratios.

        Checks for actions on business_date with adj_factor > 100 or < 0.01.
        """
        from sqlalchemy import and_

        anomalies: list[AnomalyRecord] = []

        # Find extreme adj_factors
        result = await session.execute(
            select(
                DeCorporateActions.instrument_id,
                DeCorporateActions.adj_factor,
                DeCorporateActions.action_type,
            ).where(
                and_(
                    DeCorporateActions.ex_date == business_date,
                    DeCorporateActions.adj_factor.isnot(None),
                )
            )
        )

        for row in result:
            adj = row.adj_factor
            if adj is None:
                continue
            if adj > ANOMALY_RATIO_THRESHOLD or adj < (Decimal("1") / ANOMALY_RATIO_THRESHOLD):
                anomalies.append(
                    AnomalyRecord(
                        entity_type="equity",
                        anomaly_type="invalid_ratio",
                        severity="high",
                        instrument_id=row.instrument_id,
                        expected_range="0.01 to 100.0",
                        actual_value=str(adj),
                    )
                )

        return anomalies


async def _load_symbol_map(session: AsyncSession) -> dict[str, uuid.UUID]:
    """Load current_symbol → instrument_id mapping from de_instrument.

    Returns:
        Dict mapping uppercase symbol to UUID.
    """
    result = await session.execute(
        select(DeInstrument.current_symbol, DeInstrument.id).where(
            DeInstrument.is_active == True,  # noqa: E712
        )
    )
    return {row.current_symbol.upper(): row.id for row in result}
