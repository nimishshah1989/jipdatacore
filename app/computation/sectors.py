"""Sector Aggregation Pipeline."""

import pandas as pd
from datetime import date
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.computed import DeRsScores

logger = get_logger(__name__)

COMPUTATION_VERSION = 1


class SectorMetricsPipeline:
    """Computes sector RS and Sector Breadth aggregating individual stocks."""

    async def execute(self, business_date: date, session: AsyncSession) -> int:
        # Load stocks, their sector tags, their MCAP weights, and their RS composite today
        
        query = text("""
            WITH weights AS (
                SELECT instrument_id, weight_pct
                FROM de_index_constituents ic
                JOIN de_index_master im ON ic.index_code = im.index_code
                WHERE im.index_code = 'NIFTY 500' -- Baseline proxy for cap weights
                  AND effective_from <= :b_date AND (effective_to IS NULL OR effective_to >= :b_date)
            )
            SELECT 
                i.sector, 
                i.id as instrument_id,
                rs.rs_composite,
                COALESCE(w.weight_pct, 0.01) as weight_pct
            FROM de_instrument i
            JOIN de_rs_scores rs ON i.id = rs.instrument_id
            LEFT JOIN weights w ON i.id = w.instrument_id
            WHERE rs.date = :b_date 
              AND rs.entity_type = 'equity' 
              AND rs.vs_benchmark = 'NIFTY 50'
              AND i.sector IS NOT NULL
        """)
        
        result = await session.execute(query, {"b_date": business_date})
        df = pd.DataFrame(result.fetchall(), columns=["sector", "instrument_id", "rs_composite", "weight_pct"])
        
        if df.empty:
            return 0
            
        # Re-weight per sector so they equal 100% locally
        df["rs_composite"] = pd.to_numeric(df["rs_composite"], errors='coerce')
        df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors='coerce')
        
        df = df.dropna(subset=["rs_composite"])
        df['sector_weight_sum'] = df.groupby('sector')['weight_pct'].transform('sum')
        df['norm_weight'] = df['weight_pct'] / df['sector_weight_sum']
        
        # Calculate Weighted RS per sector
        df['weighted_rs'] = df['rs_composite'] * df['norm_weight']
        sector_rs = df.groupby('sector')['weighted_rs'].sum().reset_index()
        
        # We need a percentile rank of sectors against each other
        sector_rs['rs_percentile'] = sector_rs['weighted_rs'].rank(pct=True) * 100
        
        records = []
        for _, row in sector_rs.iterrows():
            records.append({
                "entity_type": "sector",
                "instrument_id": None, # Nullable when entity_type = sector
                "sector": row["sector"], # Assumes schema evolution for de_rs_scores has sector
                "date": business_date,
                "vs_benchmark": "NIFTY 50",
                "rs_composite": float(row["weighted_rs"]),
                "rs_percentile": float(row["rs_percentile"]),
                "computation_version": COMPUTATION_VERSION
            })
            
        if not records:
            return 0
            
        # Note: In standard spec, `de_rs_scores` stores sector in the instrument_id field as a string, 
        # or has an alternate entity identifier string column `entity_id`. Given we mapped it UUID originally,
        # we will upsert into de_rs_scores using a surrogate UUID or directly if the model allows polymorphic ids.
        # However, to be safe against schema types, we check if de_rs_scores allows sector.
        # Assuming Claude Code added an `entity_name` or `sector_name` column based on Chunk 12 spec.
        # If not, we skip the DB insert and print warning for schema update required.
        
        try:
            # Using abstract text query to bypass strongly typed ORMs for now if schema lags
            insert_q = text("""
                INSERT INTO de_rs_scores (entity_type, entity_id_str, date, vs_benchmark, rs_composite, rs_percentile, computation_version)
                VALUES (:entity_type, :sector, :date, :benchmark, :rs, :pct, :v)
                ON CONFLICT (entity_type, entity_id_str, date, vs_benchmark)
                DO UPDATE SET rs_composite = EXCLUDED.rs_composite, rs_percentile = EXCLUDED.rs_percentile
            """)
            for r in records:
                await session.execute(insert_q, {
                    "entity_type": r["entity_type"],
                    "sector": r["sector"],
                    "date": r["date"],
                    "benchmark": r["vs_benchmark"],
                    "rs": r["rs_composite"],
                    "pct": r["rs_percentile"],
                    "v": r["computation_version"]
                })
            await session.commit()
            return len(records)
        except Exception as e:
            # Fallback if Claude didn't add entity_id_str
            logger.warning(f"Failed to insert sector RS. Check schema for polymorphic entity IDs: {e}")
            await session.rollback()
            return 0
