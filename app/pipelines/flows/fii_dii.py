"""FII / DII Institutional Flows Pipeline."""

import json
from datetime import date
from typing import Dict, Any

import httpx
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.models.flows import DeInstitutionalFlows
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)


class InstitutionalFlowsPipeline(BasePipeline):
    pipeline_name = "equity_fii_dii"
    requires_trading_day = True

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        # The primary source: NSE fiidiiTradeReact API
        url = "https://www.nseindia.com/api/fiidiiTradeReact"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.",
            "Accept": "application/json",
            "Authority": "www.nseindia.com",
            # Referer often critical for NSE anti-bot
            "Referer": "https://www.nseindia.com/reports/fii-dii"
        }
        
        async with httpx.AsyncClient() as client:
            # Grab session cookie
            await client.get("https://www.nseindia.com", headers=headers)
            resp = await client.get(url, headers=headers)
            
            if resp.status_code == 403:
                logger.error("403 Forbidden received from NSE. Wait for manual SEBI fallback logic.")
                # TODO: In v1.9, implement 403 fallback to SEBI CSV
                return ExecutionResult(0, 1)
                
            resp.raise_for_status()
            data = resp.json()
            
        records_to_insert = []
        # JSON typically has keys like: {"date": "05-Apr-2026", "FIICash": {"buy": 1000, "sell": ...}} etc
        # Our target entity requires extracting 'fii' or 'dii' mapped to 'equity'
        
        for item in data:
            category = item.get("category", "")
            if "FII" in category.upper():
                investor_type = "fii"
            elif "DII" in category.upper():
                investor_type = "dii"
            else:
                continue
                
            try:
                buy_val = float(item.get("buyValue", 0))
                sell_val = float(item.get("sellValue", 0))
                net_val = float(item.get("netValue", 0))
            except ValueError:
                continue

            records_to_insert.append({
                "date": business_date,
                "investor_type": investor_type,
                "market_type": "equity",
                "buy_value": buy_val,
                "sell_value": sell_val,
                "net_value": net_val,
                "pipeline_run_id": run_log.id
            })

        if not records_to_insert:
            return ExecutionResult(0, 0)

        stmt = insert(DeInstitutionalFlows).values(records_to_insert)
        stmt = stmt.on_conflict_do_update(
            index_elements=["date", "investor_type", "market_type"],
            set_={
                "buy_value": stmt.excluded.buy_value,
                "sell_value": stmt.excluded.sell_value,
                "net_value": stmt.excluded.net_value,
                "pipeline_run_id": stmt.excluded.pipeline_run_id,
            }
        )
        await session.execute(stmt)
        await session.commit()
        
        return ExecutionResult(len(records_to_insert), 0)
