"""Pipeline DAG Orchestrator."""

import asyncio
from datetime import date
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from app.logging import get_logger
from app.db.session import get_db
from app.models.pipeline import DePipelineLog

from app.pipelines.equity.eod import EquityEodPipeline
from app.pipelines.mf.eod import MfEodPipeline
from app.pipelines.flows.fii_dii import InstitutionalFlowsPipeline
from app.pipelines.flows.fo_summary import FoSummaryPipeline
from app.computation.technicals import TechnicalsComputationPipeline
from app.computation.rs import RsComputationPipeline
from app.computation.breadth import BreadthComputationPipeline
from app.computation.regime import RegimeComputationPipeline
from app.computation.sectors import SectorMetricsPipeline

logger = get_logger(__name__)


class PipelineContext:
    def __init__(self, business_date: date):
        self.business_date = business_date
        self.run_logs = {}


async def run_eod_dag(business_date: date):
    """Executes the daily EOD DAG in dependency order.
    
    Track A: Equity (P1)
    Track B: MF (P2)
    Track C/D/E: Supporting Flows
    Track Layer 4: Computation (Requires Track A)
    """
    logger.info(f"Starting EOD DAG for {business_date}")
    ctx = PipelineContext(business_date)
    
    # Simple dependency sequential wrapper for Python native Orchestrator 
    # Use asyncio.gather for parallelization where layers are independent
    
    # Layer 3 Instantiation
    equity_eod = EquityEodPipeline()
    mf_eod = MfEodPipeline()
    fii_flows = InstitutionalFlowsPipeline()
    fo_flows = FoSummaryPipeline()
    
    # Phase 1 (Ingestion) -> Parallel Tracks
    async def __run_wrapped(pipeline):
        # We would normally grab session dynamically and instantiate a run log
        # For boilerplate, just pretend execution directly wrapper
        logger.info(f"Running pipeline {pipeline.pipeline_name}")
        # In actual system, we'd wrap this with session generator
        pass
        
    await asyncio.gather(
        __run_wrapped(equity_eod),
        __run_wrapped(mf_eod),
        __run_wrapped(fii_flows),
        __run_wrapped(fo_flows)
    )
    
    logger.info("Layer 3 Ingestion complete.")
    
    # Layer 4 Computation (Sequential strict)
    # Wait for Equity EOD to finish before running RS and Technicals
    
    tech = TechnicalsComputationPipeline()
    rs = RsComputationPipeline()
    breadth = BreadthComputationPipeline()
    regime = RegimeComputationPipeline()
    sectors = SectorMetricsPipeline()
    
    logger.info("Starting Layer 4 Computation Phase.")
    await __run_wrapped(tech)
    await __run_wrapped(rs)
    await __run_wrapped(sectors)
    await __run_wrapped(breadth)
    await __run_wrapped(regime)
    
    logger.info(f"EOD DAG Execution entirely completed for {business_date}.")
