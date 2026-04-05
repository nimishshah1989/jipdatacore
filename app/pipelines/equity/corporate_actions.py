"""Corporate Actions Pipeline — fetches dividends, splits, and bonuses from NSE."""

import json
from datetime import date, timedelta
from typing import Any
from uuid import UUID

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.logging import get_logger
from app.models.instruments import DeInstrument
from app.models.prices import DeCorporateActions
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)


class CorporateActionsPipeline(BasePipeline):
    pipeline_name = "equity_corporate_actions"
    requires_trading_day = True

    async def _fetch_nse_corporate_actions(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        """Fetch corp actions from NSE JSON API."""
        # NSE APIs require a session cookie first
        base_url = "https://www.nseindia.com"
        api_url = f"{base_url}/api/corporateActions?index=equities&from_date={from_date.strftime('%d-%m-%Y')}&to_date={to_date.strftime('%d-%m-%Y')}"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "*/*",
            "Authority": "www.nseindia.com",
        }
        
        async with httpx.AsyncClient() as client:
            # 1. Hit homepage to get cookies
            await client.get(base_url, headers=headers)
            # 2. Hit actual API
            resp = await client.get(api_url, headers=headers)
            resp.raise_for_status()
            return resp.json()

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        # Fetch actions spanning [-2 days, +5 days] for lookahead/catchup
        from_date = business_date - timedelta(days=2)
        to_date = business_date + timedelta(days=5)
        
        raw_actions = await self._fetch_nse_corporate_actions(from_date, to_date)
        rows_processed = 0
        rows_failed = 0
        
        if not raw_actions:
            return ExecutionResult(0, 0)
            
        # Get active instruments map
        ins_result = await session.execute(
            select(DeInstrument.current_symbol, DeInstrument.id).where(DeInstrument.exchange == "NSE")
        )
        symbol_map: dict[str, UUID] = {row[0]: row[1] for row in ins_result.all()}
        
        records_to_upsert = []

        for item in raw_actions:
            symbol = item.get("symbol", "").strip()
            if symbol not in symbol_map:
                continue
                
            purpose = item.get("purpose", "").lower()
            ex_date_str = item.get("exDate")
            if not ex_date_str or ex_date_str == "-":
                continue
                
            from datetime import datetime
            ex_date = datetime.strptime(ex_date_str, "%d-%b-%Y").date()
            
            # Map NSE purpose string to our Action Type schema
            action_type = "other"
            dividend_type = "none"
            cash_value = None
            ratio_from = None
            ratio_to = None
            
            if "dividend" in purpose:
                action_type = "dividend"
                dividend_type = "interim" if "interim" in purpose else "special" if "special" in purpose else "final"
                # Extraction heuristic: "Interim Dividend - Rs 5 Per Share"
                import re
                match = re.search(r'rs\s*([\d\.]+)', purpose)
                if match:
                    cash_value = match.group(1)
            elif "split" in purpose or "sub-division" in purpose:
                action_type = "split"
                # "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 2/- Per Share"
                # ratio_from=1, ratio_to=5 (1 old gives 5 new)
                match = re.search(r'from rs\s*([\d\.]+)[\w\s/-]+to rs\s*([\d\.]+)', purpose)
                if match:
                    old_fv = float(match.group(1))
                    new_fv = float(match.group(2))
                    if new_fv > 0:
                        ratio_from = 1.0
                        ratio_to = float(old_fv / new_fv)
            elif "bonus" in purpose:
                action_type = "bonus"
                # "Bonus 1:1" -> ratio_from=1, ratio_to=2
                match = re.search(r'(\d+)[:/](\d+)', purpose)
                if match:
                    bonus_shares = float(match.group(1))
                    held_shares = float(match.group(2))
                    ratio_from = held_shares
                    ratio_to = held_shares + bonus_shares

            records_to_upsert.append({
                "instrument_id": symbol_map[symbol],
                "ex_date": ex_date,
                "action_type": action_type,
                "dividend_type": dividend_type,
                "cash_value": cash_value,
                "ratio_from": ratio_from,
                "ratio_to": ratio_to,
                "notes": item.get("purpose"),
            })
            rows_processed += 1
            
        if records_to_upsert:
            stmt = insert(DeCorporateActions).values(records_to_upsert)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_corporate_actions",
                set_={
                    "ratio_from": stmt.excluded.ratio_from,
                    "ratio_to": stmt.excluded.ratio_to,
                    "cash_value": stmt.excluded.cash_value,
                    "notes": stmt.excluded.notes,
                    "updated_at": sa.func.now(),
                }
            )
            await session.execute(stmt)
            await session.commit()

        logger.info(f"Corp actions complete. Processed {rows_processed} actions.")
        return ExecutionResult(rows_processed=rows_processed, rows_failed=rows_failed)
