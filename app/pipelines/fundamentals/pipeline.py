"""Equity fundamentals pipeline — weekly Screener.in scrape into de_equity_fundamentals."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.fundamentals import DeEquityFundamentals
from app.models.instruments import DeInstrument
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.fundamentals.screener_enricher import parse_screener_html
from app.pipelines.fundamentals.screener_fetcher import (
    build_http_client,
    extract_balance_sheet_latest,
    extract_pl_growth,
    extract_shareholding,
    fetch_company_html,
)

logger = get_logger(__name__)

DELAY_SECONDS = 1.2
MAX_CONSECUTIVE_FAILURES = 5


def _to_decimal(val, precision: int = 4) -> Optional[Decimal]:
    if val is None:
        return None
    try:
        return round(Decimal(str(val)), precision)
    except (InvalidOperation, ValueError, TypeError):
        return None


class FundamentalsPipeline(BasePipeline):
    pipeline_name = "equity_fundamentals"
    requires_trading_day = False
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        session_cookie = os.environ.get("SCREENER_SESSION_COOKIE", "")
        if not session_cookie:
            logger.error("no_screener_cookie", msg="SCREENER_SESSION_COOKIE not set")
            return ExecutionResult(rows_processed=0, rows_failed=0)

        instruments = await self._load_universe(session)
        logger.info("fundamentals_universe", count=len(instruments))

        if not instruments:
            return ExecutionResult(rows_processed=0, rows_failed=0)

        rows_ok = 0
        rows_fail = 0
        consecutive_failures = 0

        async with build_http_client(session_cookie) as client:
            for i, inst in enumerate(instruments):
                symbol = inst["symbol"]
                instrument_id = inst["id"]

                html = await fetch_company_html(client, symbol)

                if html is None:
                    consecutive_failures += 1
                    rows_fail += 1
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        logger.error(
                            "screener_cookie_expired",
                            msg=f"{MAX_CONSECUTIVE_FAILURES} consecutive failures — aborting",
                            processed=i + 1,
                            success=rows_ok,
                        )
                        break
                    await asyncio.sleep(DELAY_SECONDS)
                    continue

                consecutive_failures = 0

                try:
                    row = self._build_row(html, instrument_id, business_date)
                    if row:
                        await self._upsert(session, row)
                        rows_ok += 1
                    else:
                        rows_fail += 1
                except Exception as e:
                    logger.debug("fundamentals_store_failed", symbol=symbol, error=str(e)[:100])
                    rows_fail += 1

                await asyncio.sleep(DELAY_SECONDS)

                if (i + 1) % 50 == 0:
                    await session.commit()
                    logger.info(
                        "fundamentals_progress",
                        processed=i + 1,
                        total=len(instruments),
                        success=rows_ok,
                        failed=rows_fail,
                    )

        await session.commit()
        return ExecutionResult(rows_processed=rows_ok, rows_failed=rows_fail)

    async def _load_universe(self, session: AsyncSession) -> list[dict]:
        result = await session.execute(
            select(DeInstrument.id, DeInstrument.current_symbol)
            .where(
                DeInstrument.is_active.is_(True),
                DeInstrument.exchange == "NSE",
            )
            .order_by(DeInstrument.current_symbol)
        )
        return [{"id": row.id, "symbol": row.current_symbol} for row in result.all()]

    def _build_row(self, html: str, instrument_id: uuid.UUID, as_of_date: date) -> Optional[dict]:
        snapshot = parse_screener_html(html)
        if not snapshot:
            return None

        shareholding = extract_shareholding(html)
        pl_growth = extract_pl_growth(html)
        bs_ratios = extract_balance_sheet_latest(html)

        return {
            "instrument_id": instrument_id,
            "as_of_date": as_of_date,
            "market_cap_cr": _to_decimal(snapshot.get("market_cap_cr"), 2),
            "pe_ratio": _to_decimal(snapshot.get("pe_ratio")),
            "pb_ratio": _to_decimal(snapshot.get("pb_ratio")),
            "peg_ratio": _to_decimal(snapshot.get("peg_ratio")),
            "ev_ebitda": _to_decimal(snapshot.get("ev_ebitda")),
            "roe_pct": _to_decimal(snapshot.get("roe_pct")),
            "roce_pct": _to_decimal(snapshot.get("roce_pct")),
            "operating_margin_pct": _to_decimal(pl_growth.get("operating_margin_pct")),
            "net_margin_pct": _to_decimal(pl_growth.get("net_margin_pct")),
            "debt_to_equity": _to_decimal(bs_ratios.get("debt_to_equity")),
            "interest_coverage": None,
            "current_ratio": None,
            "eps_ttm": _to_decimal(pl_growth.get("eps_ttm")),
            "book_value": _to_decimal(snapshot.get("book_value")),
            "face_value": _to_decimal(snapshot.get("face_value"), 2),
            "dividend_per_share": None,
            "dividend_yield_pct": _to_decimal(snapshot.get("dividend_yield_pct")),
            "promoter_holding_pct": _to_decimal(shareholding.get("promoter_pct"), 2),
            "pledged_pct": None,
            "fii_holding_pct": _to_decimal(shareholding.get("fii_pct"), 2),
            "dii_holding_pct": _to_decimal(shareholding.get("dii_pct"), 2),
            "revenue_growth_yoy_pct": _to_decimal(pl_growth.get("revenue_growth_yoy_pct")),
            "profit_growth_yoy_pct": _to_decimal(pl_growth.get("profit_growth_yoy_pct")),
            "high_52w": _to_decimal(snapshot.get("high_52w")),
            "low_52w": _to_decimal(snapshot.get("low_52w")),
            "source": "screener",
        }

    async def _upsert(self, session: AsyncSession, row: dict) -> None:
        stmt = pg_insert(DeEquityFundamentals).values(**row)
        update_cols = {
            c: stmt.excluded[c]
            for c in row
            if c not in ("instrument_id", "as_of_date", "created_at")
        }
        stmt = stmt.on_conflict_do_update(
            constraint=DeEquityFundamentals.__table__.primary_key,
            set_=update_cols,
        )
        await session.execute(stmt)
