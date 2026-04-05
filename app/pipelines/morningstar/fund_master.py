"""Morningstar Fund Master synchronization."""

import asyncio
from typing import Dict, Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.instruments import DeMfMaster, DeMfLifecycle
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.morningstar.client import MorningstarClient

logger = get_logger(__name__)


class MorningstarFundMasterPipeline(BasePipeline):
    pipeline_name = "morningstar_master_refresh"
    requires_trading_day = False  # Runs weekly on Sunday

    async def execute(
        self,
        business_date: Any,  # The date we run on
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        # Load active equity funds
        query = select(DeMfMaster).where(
            DeMfMaster.is_active == True,
            DeMfMaster.broad_category.ilike("%equity%")
        )
        result = await session.execute(query)
        target_funds = result.scalars().all()
        
        logger.info(f"Targeting {len(target_funds)} active equity funds for Morningstar refresh.")
        
        client = MorningstarClient()
        rows_processed = 0
        rows_failed = 0
        
        try:
            # Note: sequential loop to respect rate limit, or chunked asyncio.gather with semaphores
            sem = asyncio.Semaphore(5)  # 5 concurrent requests max
            
            async def fetch_and_update(fund: DeMfMaster):
                nonlocal rows_processed, rows_failed
                async with sem:
                    data = await client.get_fund_details(fund.mstar_id)
                    if not data:
                        # 404 or empty handles
                        rows_failed += 1
                        # Wait 30 days before marking inactive, handled out of band
                        return
                        
                    # Compare and trigger lifecycle events if needed
                    # e.g., if Category changes -> write lifecycle log
                    new_category = data.get("CategoryName")
                    if new_category and fund.category_name != new_category:
                        lc = DeMfLifecycle(
                            mstar_id=fund.mstar_id,
                            event_type="category_change",
                            event_date=business_date,
                            old_value=fund.category_name,
                            new_value=new_category
                        )
                        session.add(lc)
                        fund.category_name = new_category
                        
                    # Apply simple metadata updates
                    fund.broad_category = data.get("BroadCategoryGroup", fund.broad_category)
                    
                    try:
                        expense = float(data.get("NetExpenseRatio", 0) or 0)
                        if expense > 0:
                            fund.expense_ratio = expense
                    except ValueError:
                        pass
                        
                    fund.primary_benchmark = data.get("Benchmark", fund.primary_benchmark)
                    # For a true scale app, save Manager and AUM to mf_master, etc.
                    
                    rows_processed += 1

            tasks = [fetch_and_update(fund) for fund in target_funds]
            await asyncio.gather(*tasks)
            
            await session.commit()
            
        finally:
            await client.close()
            
        return ExecutionResult(rows_processed, rows_failed)
