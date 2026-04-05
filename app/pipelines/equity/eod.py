"""EOD Equity Pipeline."""

import hashlib
from datetime import date
from typing import Any
from uuid import UUID

from sqlalchemy import select, func, and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.instruments import DeInstrument
from app.models.pipeline import DePipelineLog, DeSourceFiles
from app.models.prices import DeEquityOhlcv
from app.pipelines.equity.bhav import download_bhav, parse_bhav_content
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.validation import AnomalyRecord

logger = get_logger(__name__)


class EquityEodPipeline(BasePipeline):
    pipeline_name = "equity_eod"
    requires_trading_day = True

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        # Step 1: Download BHAV
        _, raw_bytes = await download_bhav(business_date)
        file_hash = hashlib.sha256(raw_bytes).hexdigest()
        
        # Step 2: Idempotency check via file hash
        existing_file = await session.execute(
            select(DeSourceFiles).where(DeSourceFiles.file_hash == file_hash)
        )
        if existing_file.scalar_one_or_none():
            logger.info(f"BHAV for {business_date} already ingested (hash match). Skipping.")
            return ExecutionResult(0, 0)
            
        # Parse data
        parsed_data = parse_bhav_content(raw_bytes)
        if len(parsed_data) < 500:
            raise ValueError(f"BHAV file has only {len(parsed_data)} rows. Corrupt download.")
            
        # Register file
        source_file = DeSourceFiles(
            file_name=f"bhav_{business_date.strftime('%Y%m%d')}.csv",
            file_hash=file_hash,
            pipeline_name=self.pipeline_name,
            record_count=len(parsed_data)
        )
        session.add(source_file)
        await session.flush()

        # Build symbol map
        ins_result = await session.execute(
            select(DeInstrument.current_symbol, DeInstrument.id)
            .where(DeInstrument.exchange == "NSE")
        )
        symbol_map: dict[str, UUID] = {row[0]: row[1] for row in ins_result.all()}
        
        records_to_insert = []
        rows_failed = 0
        
        for row in parsed_data:
            symbol = row["symbol"]
            inst_id = symbol_map.get(symbol)
            if not inst_id:
                # Typically newly listed same-day, or name change without master refresh
                rows_failed += 1
                continue
                
            records_to_insert.append({
                "date": business_date,
                "instrument_id": inst_id,
                "symbol": symbol,
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
                "trades": row["trades"],
                # Initial insert assumes no adjustments; close_adj will be updated by recompute worker if corp actions exist
                "close_adj": row["close"],
                "open_adj": row["open"],
                "high_adj": row["high"],
                "low_adj": row["low"],
                "volume_adj": row["volume"],
                "source_file_id": source_file.id,
                "pipeline_run_id": run_log.id,
                "data_status": "raw"
            })
            
        if records_to_insert:
            # Batch insert with ON CONFLICT DO UPDATE
            # Note: We bucket into chunks of 1000 to avoid statement size limits
            chunk_size = 1000
            for i in range(0, len(records_to_insert), chunk_size):
                chunk = records_to_insert[i:i + chunk_size]
                stmt = insert(DeEquityOhlcv).values(chunk)
                stmt = stmt.on_conflict_do_update(
                    constraint="pk_de_equity_ohlcv", # Assuming constraint named correctly in Alembic
                    set_={
                        "open": stmt.excluded.open,
                        "high": stmt.excluded.high,
                        "low": stmt.excluded.low,
                        "close": stmt.excluded.close,
                        "volume": stmt.excluded.volume,
                        "trades": stmt.excluded.trades,
                        "pipeline_run_id": stmt.excluded.pipeline_run_id,
                        "updated_at": func.now()
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
        """Post-insert validation."""
        anomalies = []
        
        # 1. Fetch newly inserted rows
        new_rows = await session.execute(
            select(DeEquityOhlcv).where(
                and_(
                    DeEquityOhlcv.date == business_date,
                    DeEquityOhlcv.pipeline_run_id == run_log.id
                )
            )
        )
        
        for row in new_rows.scalars().all():
            is_critical = False
            # Zero/Negative checks
            if row.close <= 0 or row.open <= 0 or row.high <= 0 or row.low <= 0:
                anomalies.append(AnomalyRecord(
                    entity_type="equity",
                    instrument_id=row.instrument_id,
                    anomaly_type="negative_value",
                    severity="critical",
                    actual_value=str(row.close)
                ))
                is_critical = True
                
            if row.high < row.low:
                anomalies.append(AnomalyRecord(
                    entity_type="equity",
                    instrument_id=row.instrument_id,
                    anomaly_type="invalid_ratio",
                    severity="critical",
                    actual_value=f"H:{row.high} M:{row.low}"
                ))
                is_critical = True

            # Update row status based on validation result
            row.data_status = "quarantined" if is_critical else "validated"
            session.add(row)
            
        await session.commit()
        return anomalies
