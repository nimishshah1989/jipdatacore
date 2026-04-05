"""Fund Derived Metrics pipeline."""

import pandas as pd
from datetime import date
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.mf_derived import DeMfDerivedDaily

logger = get_logger(__name__)


class FundDerivedMetricsPipeline:
    """Computes daily Fund metrics (derived RS, NAV RS, Manager Alpha)."""

    async def execute(self, business_date: date, session: AsyncSession) -> int:
        """
        Holdings x Stock Metrics algorithm:
        We multiply the weight_pct in de_mf_holdings by the corresponding de_rs_scores
        for each stock the fund holds, to derive what the fund's RS SHOULD be based on its portfolio.
        """
        
        # We need the most recent holdings per fund.
        # Then join against today's RS scores.
        query = text("""
            WITH latest_holdings AS (
                SELECT DISTINCT ON (mstar_id, instrument_id)
                    mstar_id, instrument_id, weight_pct
                FROM de_mf_holdings
                WHERE report_date <= :b_date AND instrument_id IS NOT NULL
                ORDER BY mstar_id, instrument_id, report_date DESC
            ),
            fund_coverage AS (
                SELECT mstar_id, SUM(weight_pct) as coverage_pct
                FROM latest_holdings
                GROUP BY mstar_id
            ),
            today_rs AS (
                SELECT instrument_id, rs_composite
                FROM de_rs_scores
                WHERE date = :b_date AND entity_type = 'equity' AND vs_benchmark = 'NIFTY 50'
            ),
            derived AS (
                SELECT 
                    lh.mstar_id,
                    SUM(lh.weight_pct * rs.rs_composite / 100.0) as derived_rs_composite
                FROM latest_holdings lh
                JOIN today_rs rs ON lh.instrument_id = rs.instrument_id
                GROUP BY lh.mstar_id
            )
            SELECT 
                d.mstar_id, 
                d.derived_rs_composite, 
                fc.coverage_pct,
                -- Grab actual NAV RS as well to compute Alpha
                nav_rs.rs_composite as nav_rs_composite
            FROM derived d
            JOIN fund_coverage fc ON d.mstar_id = fc.mstar_id
            LEFT JOIN de_rs_scores nav_rs 
              ON d.mstar_id = nav_rs.instrument_id::text 
              AND nav_rs.date = :b_date 
              AND nav_rs.entity_type = 'mf_category'
        """)
        
        result = await session.execute(query, {"b_date": business_date})
        records = []
        
        for row in result.fetchall():
            derived_rs = float(row.derived_rs_composite) if row.derived_rs_composite else 0.0
            nav_rs = float(row.nav_rs_composite) if row.nav_rs_composite else 0.0
            
            manager_alpha = nav_rs - derived_rs if nav_rs else None
            
            records.append({
                "nav_date": business_date,
                "mstar_id": row.mstar_id,
                "derived_rs_composite": derived_rs,
                "nav_rs_composite": nav_rs if nav_rs else None,
                "manager_alpha": manager_alpha,
                "coverage_pct": float(row.coverage_pct)
            })
            
        if not records:
            return 0
            
        stmt = insert(DeMfDerivedDaily).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["nav_date", "mstar_id"],
            set_={
                "derived_rs_composite": stmt.excluded.derived_rs_composite,
                "nav_rs_composite": stmt.excluded.nav_rs_composite,
                "manager_alpha": stmt.excluded.manager_alpha,
                "coverage_pct": stmt.excluded.coverage_pct
            }
        )
        
        await session.execute(stmt)
        await session.commit()
        return len(records)
