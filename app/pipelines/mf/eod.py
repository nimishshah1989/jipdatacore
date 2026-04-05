"""MF EOD Pipeline — orchestrates the daily NAV ingestion."""

import hashlib
from datetime import date
from typing import Dict, Any
from uuid import UUID

from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog, DeSourceFiles
from app.models.prices import DeMfNavDaily
from app.models.instruments import DeMfMaster
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.validation import AnomalyRecord
from app.pipelines.mf.amfi import fetch_amfi_nav, parse_amfi_nav

logger = get_logger(__name__)


class MfEodPipeline(BasePipeline):
    pipeline_name = "mf_eod"
    requires_trading_day = True

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        # Step 1: Download from AMFI
        raw_text = await fetch_amfi_nav()
        file_hash = hashlib.sha256(raw_text.encode('utf-8')).hexdigest()
        
        # Check idempotency
        existing_file = await session.execute(
            select(DeSourceFiles).where(DeSourceFiles.file_hash == file_hash)
        )
        if existing_file.scalar_one_or_none():
            logger.info("AMFI NAV file already ingested today (hash match). Skipping.")
            return ExecutionResult(0, 0)
            
        parsed_records = parse_amfi_nav(raw_text)
        if len(parsed_records) < 1000:
            raise ValueError(f"AMFI file has abnormally low row count: {len(parsed_records)}")
            
        # Register source file
        source_file = DeSourceFiles(
            file_name=f"amfi_navAll_{business_date.strftime('%Y%m%d')}.txt",
            file_hash=file_hash,
            pipeline_name=self.pipeline_name,
            record_count=len(parsed_records)
        )
        session.add(source_file)
        await session.flush()

        # Step 2: Fetch Target Universe from de_mf_master
        # Target: Is Active AND is an Equity fund AND has 'Growth' or 'Accumulated' AND 'Regular' in the name
        # The logic strictly limits to ~450-550 funds per the V2 architecture spec.
        universe_query = select(DeMfMaster.amfi_code, DeMfMaster.mstar_id).where(
            DeMfMaster.is_active == True,
            DeMfMaster.broad_category.ilike("%equity%"),
            select(DeMfMaster.fund_name).contains("Regular"),
            (select(DeMfMaster.fund_name).contains("Growth") | select(DeMfMaster.fund_name).contains("Accumulated"))
        )
        
        mapping_result = await session.execute(universe_query)
        # Create map from AMFI code to Morningstar ID
        target_map = {row[0]: row[1] for row in mapping_result.all() if row[0]}
        
        # Step 3: Filter and Insert
        records_to_insert = []
        rows_failed = 0
        
        for record in parsed_records:
            # We enforce exact business_date match. AMFI typically updates around 9PM.
            # If the date doesn't match the required business date, we drop it to avoid corrupting history.
            if record["nav_date"] != business_date:
                continue
                
            amfi_code = record["amfi_code"]
            if amfi_code not in target_map:
                continue
                
            mstar_id = target_map[amfi_code]
            nav_value = record["nav"]
            
            records_to_insert.append({
                "nav_date": business_date,
                "mstar_id": mstar_id,
                "nav": nav_value,
                "nav_adj": nav_value,  # For growth funds, adj equals nav implicitly
                "data_status": "raw",
                "source_file_id": source_file.id,
                "pipeline_run_id": run_log.id
            })

        if not records_to_insert:
            logger.warning(f"No targeted mutual funds matched for {business_date}.")
            return ExecutionResult(0, 0, source_file.id)
            
        # Batch Upsert
        chunk_size = 1000
        for i in range(0, len(records_to_insert), chunk_size):
            chunk = records_to_insert[i:i + chunk_size]
            stmt = insert(DeMfNavDaily).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=['nav_date', 'mstar_id'],  # PK constraints
                set_={
                    "nav": stmt.excluded.nav,
                    "nav_adj": stmt.excluded.nav_adj,
                    "pipeline_run_id": stmt.excluded.pipeline_run_id,
                    "updated_at": select(db.func.now())
                }
            )
            await session.execute(stmt)
            
        await session.commit()
        return ExecutionResult(len(records_to_insert), rows_failed, source_file.id)

    async def validate(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> list[AnomalyRecord]:
        """Validate newly inserted MF NAV rows."""
        anomalies = []
        
        new_rows = await session.execute(
            select(DeMfNavDaily).where(
                and_(
                    DeMfNavDaily.nav_date == business_date,
                    DeMfNavDaily.pipeline_run_id == run_log.id
                )
            )
        )
        
        for row in new_rows.scalars().all():
            is_critical = False
            is_warning = False
            
            if row.nav <= 0:
                anomalies.append(AnomalyRecord(
                    entity_type="mf",
                    mstar_id=row.mstar_id,
                    anomaly_type="negative_value",
                    severity="critical",
                    actual_value=str(row.nav)
                ))
                is_critical = True
                
            # If we had prev day NAV, we could check for >15% swing here. (Skipped exact 1-day math to avoid heavy DB join here; handled in returns.py)
            
            row.data_status = "quarantined" if is_critical else "validated"
            session.add(row)
            
        await session.commit()
        return anomalies
