"""T+1 Delivery Pipeline — updates OHLCV with delivery volume/pct."""

import csv
import io
from datetime import date
from typing import Dict, Any

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.instruments import DeInstrument
from app.models.pipeline import DePipelineLog
from app.models.prices import DeEquityOhlcv
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)


class DeliveryPipeline(BasePipeline):
    pipeline_name = "equity_delivery"
    requires_trading_day = True

    async def _fetch_delivery_data(self, target_date: date) -> str:
        """Download NSE delivery file."""
        dd = target_date.strftime("%d")
        mm = target_date.strftime("%m")
        yyyy = target_date.strftime("%Y")
        
        url = f"https://archives.nseindia.com/archives/equities/mto/MTO_{dd}{mm}{yyyy}.DAT"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Authority": "archives.nseindia.com",
        }
        
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            return resp.text

    def _parse_delivery_file(self, content: str) -> dict[str, dict[str, Any]]:
        """Parse the specific MTO format.
        Line format: Record Type,Sr No,Name of Security,Series,Quantity Traded,Deliverable Quantity(gross across client level),% of Deliverable Quantity to Traded Quantity
        """
        lines = content.splitlines()
        parsed = {}
        for line in lines:
            if not line.strip() or line.startswith("10") or line.startswith("Record"):
                continue  # Skip comments/headers

            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 7 and parts[3] == "EQ":
                try:
                    symbol = parts[2]
                    deliv_qty = int(parts[5])
                    deliv_pct = float(parts[6])
                    parsed[symbol] = {
                        "delivery_vol": deliv_qty,
                        "delivery_pct": deliv_pct
                    }
                except ValueError:
                    continue
        return parsed

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        try:
            content = await self._fetch_delivery_data(business_date)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning(f"Delivery file not found for {business_date}. Skipping.")
                return ExecutionResult(0, 0)
            raise
            
        delivery_data = self._parse_delivery_file(content)
        if not delivery_data:
            return ExecutionResult(0, 0)

        # Get instruments to map symbol to UUID
        ins_result = await session.execute(
            select(DeInstrument.current_symbol, DeInstrument.id)
            .where(DeInstrument.exchange == "NSE")
        )
        symbol_map = {row[0]: row[1] for row in ins_result.all()}

        rows_processed = 0
        rows_failed = 0

        # Batch update logic
        for symbol, metrics in delivery_data.items():
            inst_id = symbol_map.get(symbol)
            if not inst_id:
                continue
                
            stmt = (
                update(DeEquityOhlcv)
                .where(
                    DeEquityOhlcv.date == business_date,
                    DeEquityOhlcv.instrument_id == inst_id
                )
                .values(
                    delivery_vol=metrics["delivery_vol"],
                    delivery_pct=metrics["delivery_pct"],
                    updated_at=sa.func.now(),
                )
            )
            result = await session.execute(stmt)
            if result.rowcount > 0:
                rows_processed += 1
            else:
                rows_failed += 1

        await session.commit()
        logger.info(f"Delivery pipeline complete. Updated {rows_processed} rows.")
        return ExecutionResult(rows_processed, rows_failed)
