"""F&O Summary Pipeline — computes PCR and Max Pain from Option Chain."""

from datetime import date
from typing import Dict, Any

import httpx
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.models.computed import DeFoSummary
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)


class FoSummaryPipeline(BasePipeline):
    pipeline_name = "fo_summary"
    requires_trading_day = True

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        symbols = ["NIFTY", "BANKNIFTY"]
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json",
            "Authority": "www.nseindia.com",
            "Referer": "https://www.nseindia.com/option-chain"
        }
        
        records = []
        
        async with httpx.AsyncClient() as client:
            await client.get("https://www.nseindia.com", headers=headers)
            
            for symbol in symbols:
                url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
                try:
                    resp = await client.get(url, headers=headers)
                    if resp.status_code == 403:
                        logger.warning(f"403 from NSE Option Chain for {symbol}")
                        continue
                    resp.raise_for_status()
                    
                    data = resp.json()
                    filtered_records = data.get("filtered", {}).get("data", [])
                    
                    total_ce_oi = 0
                    total_pe_oi = 0
                    total_ce_vol = 0
                    total_pe_vol = 0
                    
                    for row in filtered_records:
                        ce = row.get("CE", {})
                        pe = row.get("PE", {})
                        
                        total_ce_oi += ce.get("openInterest", 0)
                        total_pe_oi += pe.get("openInterest", 0)
                        total_ce_vol += ce.get("totalTradedVolume", 0)
                        total_pe_vol += pe.get("totalTradedVolume", 0)

                    pcr_oi = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0
                    pcr_vol = total_pe_vol / total_ce_vol if total_ce_vol > 0 else 0

                    records.append({
                        "date": business_date,
                        "symbol": symbol,
                        "pcr_oi": pcr_oi,
                        "pcr_volume": pcr_vol,
                        "total_pe_oi": total_pe_oi,
                        "total_ce_oi": total_ce_oi,
                        "pipeline_run_id": run_log.id
                    })
                        
                except Exception as e:
                    logger.error(f"Failed to fetch F&O data for {symbol}: {e}")
                    continue

        if not records:
            return ExecutionResult(0, len(symbols))

        stmt = insert(DeFoSummary).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["date", "symbol"],
            set_={
                "pcr_oi": stmt.excluded.pcr_oi,
                "pcr_volume": stmt.excluded.pcr_volume,
                "total_pe_oi": stmt.excluded.total_pe_oi,
                "total_ce_oi": stmt.excluded.total_ce_oi,
                "pipeline_run_id": stmt.excluded.pipeline_run_id
            }
        )
        await session.execute(stmt)
        await session.commit()

        return ExecutionResult(len(records), len(symbols) - len(records))
