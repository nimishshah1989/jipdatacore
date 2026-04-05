"""Market Regime Classification logic."""

import json
from datetime import date
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.computed import DeMarketRegime, DeBreadthDaily

logger = get_logger(__name__)

COMPUTATION_VERSION = 1


class RegimeComputationPipeline:
    """Classifies the market into BULL, BEAR, SIDEWAYS, or RECOVERY based on breadth profiles."""

    async def execute(self, business_date: date, session: AsyncSession) -> int:
        
        # Retrieve today's breadth
        br_query = select(DeBreadthDaily).where(DeBreadthDaily.date == business_date)
        result = await session.execute(br_query)
        breadth = result.scalar_one_or_none()
        
        if not breadth:
            logger.warning(f"Cannot compute regime, no breadth data for {business_date}")
            return 0
            
        # Core regime derivation heuristics
        # These constants could be pulled from DeSystemFlags for dynamic tunability
        
        breadth_score = 0
        momentum_score = 0
        
        # Breadth grading
        if breadth.pct_above_200dma > 60:
            breadth_score += 40
        elif breadth.pct_above_200dma < 30:
            breadth_score -= 40
            
        if breadth.pct_above_50dma > 50:
            breadth_score += 20
        elif breadth.pct_above_50dma < 30:
            breadth_score -= 20
            
        if breadth.ad_ratio > 1.2:
            momentum_score += 20
        elif breadth.ad_ratio < 0.8:
            momentum_score -= 20
            
        # Global & Volume scores would be pulled from appropriate models in identical fashion
        volume_score = 50 
        global_score = 50
        fii_score = 50
        
        aggregate_intensity = breadth_score + momentum_score # Ranges ~ -60 to +60
        
        # State machine bounds
        classification = "SIDEWAYS"
        if aggregate_intensity >= 40:
            classification = "BULL"
        elif aggregate_intensity <= -40:
            classification = "BEAR"
        elif 10 < aggregate_intensity < 40 and breadth.pct_above_50dma > breadth.pct_above_200dma:
            # Short term crosses above long term while heavily sold off -> recovery
            classification = "RECOVERY"
            
        # Map 0-100 logic for confidence bounds natively
        confidence = min(max(abs(aggregate_intensity) + 30, 0), 100) # Simple scaling

        detail_json = {
            "breadth_score": breadth_score,
            "momentum_score": momentum_score,
            "fii_score": fii_score,
            "volume_score": volume_score,
            "global_score": global_score,
            "pct_above_50dma": float(breadth.pct_above_50dma),
            "pct_above_200dma": float(breadth.pct_above_200dma),
            "ad_ratio": float(breadth.ad_ratio)
        }
        
        record = {
            "computed_at": business_date,
            "classification": classification,
            "confidence": confidence,
            "indicator_detail": detail_json,
            "computation_version": COMPUTATION_VERSION
        }
        
        stmt = insert(DeMarketRegime).values([record])
        stmt = stmt.on_conflict_do_update(
            index_elements=["computed_at"],
            set_={
                "classification": stmt.excluded.classification,
                "confidence": stmt.excluded.confidence,
                "indicator_detail": stmt.excluded.indicator_detail,
                "computation_version": stmt.excluded.computation_version
            }
        )
        
        await session.execute(stmt)
        await session.commit()
        return 1
