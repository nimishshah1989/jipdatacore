"""Master Refresh Pipeline — updates de_instrument from NSE equity listing."""

import csv
import io
import time
from datetime import date
from typing import TextIO
from uuid import uuid4

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.instruments import DeInstrument, DeSymbolHistory
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)


class MasterRefreshPipeline(BasePipeline):
    pipeline_name = "equity_master_refresh"
    requires_trading_day = True

    async def _fetch_nse_master(self) -> str:
        """Download the NSE Equities listing CSV."""
        # The SEC_BHAVDATA file or the master active securities file
        # We will use the NSE market status / active equities URL
        url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            return resp.text

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        csv_data = await self._fetch_nse_master()
        reader = csv.DictReader(io.StringIO(csv_data))
        
        # Load existing active instruments to track suspensions/delistings
        result = await session.execute(
            select(DeInstrument).where(
                DeInstrument.exchange == "NSE",
                DeInstrument.is_active == True,
            )
        )
        existing_instruments = {ins.current_symbol: ins for ins in result.scalars().all()}
        
        rows_processed = 0
        rows_failed = 0
        active_symbols = set()

        for row in reader:
            # Expected columns: SYMBOL, NAME OF COMPANY, SERIES, DATE OF LISTING, FACE VALUE, ISIN NUMBER
            symbol = row.get("SYMBOL", "").strip()
            series = row.get("SERIES", "").strip()
            if not symbol or series != "EQ":
                continue
                
            active_symbols.add(symbol)
            rows_processed += 1
            
            company_name = row.get("NAME OF COMPANY", "").strip()
            isin = row.get("ISIN NUMBER", "").strip()
            listing_date_str = row.get("DATE OF LISTING", "").strip()
            
            try:
                listing_date = None
                if listing_date_str:
                    from datetime import datetime
                    listing_date = datetime.strptime(listing_date_str, "%d-%b-%Y").date()
            except ValueError:
                listing_date = None

            if symbol in existing_instruments:
                # Update ISIN or name if changed (not common, but possible)
                ins = existing_instruments[symbol]
                if ins.isin != isin or ins.company_name != company_name:
                    ins.isin = isin
                    ins.company_name = company_name
                    session.add(ins)
            else:
                # Check if it exists under old symbol (Symbol History logic handles runtime changes)
                # But for a raw INSERT ON CONFLICT DO NOTHING approach:
                new_ins = DeInstrument(
                    current_symbol=symbol,
                    isin=isin,
                    company_name=company_name,
                    exchange="NSE",
                    series="EQ",
                    is_active=True,
                    is_tradeable=True,
                    listing_date=listing_date,
                )
                session.add(new_ins)

        # Mark instruments no longer present as suspended (if they were previously active)
        suspended_count = 0
        for current_symbol, ins in existing_instruments.items():
            if current_symbol not in active_symbols:
                ins.is_tradeable = False
                ins.is_suspended = True
                ins.suspended_from = business_date
                session.add(ins)
                suspended_count += 1

        await session.commit()
        
        logger.info(f"Master refresh complete. Processed {rows_processed} EQ series. Marked {suspended_count} as suspended.")
        return ExecutionResult(rows_processed=rows_processed, rows_failed=rows_failed)
