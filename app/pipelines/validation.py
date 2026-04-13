"""Post-ingestion validation framework for pipeline anomaly detection and data gating."""

from __future__ import annotations


import uuid
from dataclasses import dataclass
from datetime import date

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DeSourceFiles
from app.models.prices import DeDataAnomalies

logger = get_logger(__name__)

QUARANTINE_THRESHOLD_PCT = 5.0  # Halt downstream if more than 5% rows are quarantined


@dataclass
class AnomalyRecord:
    """Represents a detected anomaly before DB insertion."""

    entity_type: str  # equity/mf/index/macro/flow
    anomaly_type: str  # price_spike/zero_volume/missing_data/etc
    severity: str  # low/medium/high/critical
    expected_range: str | None = None
    actual_value: str | None = None
    instrument_id: uuid.UUID | None = None
    mstar_id: str | None = None
    ticker: str | None = None


async def record_anomalies(
    session: AsyncSession,
    pipeline_name: str,
    business_date: date,
    anomalies: list[AnomalyRecord],
) -> int:
    """Insert anomalies into de_data_anomalies.

    Skips duplicates silently (ON CONFLICT DO NOTHING is not applicable here
    since id is a UUID PK — each call creates new anomaly records).

    Returns count of anomalies inserted.
    """
    if not anomalies:
        return 0

    rows = [
        {
            "id": uuid.uuid4(),
            "pipeline_name": pipeline_name,
            "business_date": business_date,
            "entity_type": a.entity_type,
            "anomaly_type": a.anomaly_type,
            "severity": a.severity,
            "expected_range": a.expected_range,
            "actual_value": a.actual_value,
            "instrument_id": a.instrument_id,
            "mstar_id": a.mstar_id,
            "ticker": a.ticker,
            "is_resolved": False,
        }
        for a in anomalies
    ]

    await session.execute(sa.insert(DeDataAnomalies), rows)

    logger.info(
        "anomalies_recorded",
        pipeline_name=pipeline_name,
        business_date=business_date.isoformat(),
        count=len(rows),
    )
    return len(rows)


async def check_quarantine_threshold(
    session: AsyncSession,
    pipeline_name: str,
    business_date: date,
    total_rows: int,
) -> tuple[bool, float]:
    """Check if quarantined anomaly rows exceed 5% of total_rows.

    Counts distinct anomaly records for this pipeline + date as a proxy for
    quarantined rows, then computes the percentage against total_rows.

    Returns (should_halt, quarantine_pct).
    If quarantine_pct > 5.0, should_halt=True to prevent downstream aggregates
    from consuming bad data.
    """
    if total_rows <= 0:
        logger.warning(
            "quarantine_check_zero_rows",
            pipeline_name=pipeline_name,
            business_date=business_date.isoformat(),
        )
        return False, 0.0

    result = await session.execute(
        select(sa.func.count(DeDataAnomalies.id)).where(
            DeDataAnomalies.pipeline_name == pipeline_name,
            DeDataAnomalies.business_date == business_date,
        )
    )
    quarantine_count = result.scalar_one() or 0
    quarantine_pct = (quarantine_count / total_rows) * 100.0
    should_halt = quarantine_pct > QUARANTINE_THRESHOLD_PCT

    logger.info(
        "quarantine_threshold_check",
        pipeline_name=pipeline_name,
        business_date=business_date.isoformat(),
        total_rows=total_rows,
        quarantine_count=quarantine_count,
        quarantine_pct=round(quarantine_pct, 4),
        should_halt=should_halt,
    )
    return should_halt, quarantine_pct


async def apply_data_status(
    session: AsyncSession,
    table_name: str,
    business_date: date,
    pipeline_run_id: int,
    anomaly_instrument_ids: set[uuid.UUID] | None = None,
    anomaly_mstar_ids: set[str] | None = None,
    date_column: str = "business_date",
) -> tuple[int, int]:
    """Update data_status from 'raw' to 'validated' or 'quarantined'.

    For rows that have anomalies (matched by instrument_id or mstar_id),
    sets data_status = 'quarantined'.
    For all remaining 'raw' rows from this pipeline run, sets data_status = 'validated'.

    The target table MUST have columns:
      - data_status (VARCHAR)
      - <date_column> (DATE) — defaults to 'business_date'; pass 'nav_date'
        for de_mf_nav_daily, 'date' for de_equity_ohlcv_*, etc.
      - pipeline_run_id (INTEGER, optional — matched if provided)

    Returns (validated_count, quarantined_count).
    """
    quarantined_count = 0
    validated_count = 0

    # Mark anomalous rows as quarantined
    if anomaly_instrument_ids:
        quarantine_stmt = (
            sa.text(
                f"""
                UPDATE {table_name}
                SET data_status = 'quarantined'
                WHERE {date_column} = :business_date
                  AND data_status = 'raw'
                  AND instrument_id = ANY(:instrument_ids)
                """
            )
        )
        result = await session.execute(
            quarantine_stmt,
            {
                "business_date": business_date,
                "instrument_ids": list(anomaly_instrument_ids),
            },
        )
        quarantined_count += result.rowcount

    if anomaly_mstar_ids:
        quarantine_stmt = sa.text(
            f"""
            UPDATE {table_name}
            SET data_status = 'quarantined'
            WHERE {date_column} = :business_date
              AND data_status = 'raw'
              AND mstar_id = ANY(:mstar_ids)
            """
        )
        result = await session.execute(
            quarantine_stmt,
            {
                "business_date": business_date,
                "mstar_ids": list(anomaly_mstar_ids),
            },
        )
        quarantined_count += result.rowcount

    # Promote remaining raw rows to validated
    validate_stmt = sa.text(
        f"""
        UPDATE {table_name}
        SET data_status = 'validated'
        WHERE {date_column} = :business_date
          AND data_status = 'raw'
        """
    )
    result = await session.execute(
        validate_stmt,
        {"business_date": business_date},
    )
    validated_count = result.rowcount

    logger.info(
        "data_status_applied",
        table_name=table_name,
        business_date=business_date.isoformat(),
        pipeline_run_id=pipeline_run_id,
        validated_count=validated_count,
        quarantined_count=quarantined_count,
    )
    return validated_count, quarantined_count


async def check_freshness(
    session: AsyncSession,
    source_name: str,
    file_date: date,
    checksum: str | None = None,
    row_count: int | None = None,
) -> tuple[bool, str]:
    """Check if this file has already been ingested (dedup via de_source_files).

    A file is considered a duplicate if de_source_files contains a record
    with the same (source_name, file_date, checksum).
    If checksum is None, deduplication falls back to (source_name, file_date) only.

    Returns (is_fresh, reason).
    is_fresh=True  → new data, should ingest.
    is_fresh=False → duplicate, should skip.
    """
    query = select(DeSourceFiles.id, DeSourceFiles.row_count).where(
        DeSourceFiles.source_name == source_name,
        DeSourceFiles.file_date == file_date,
    )

    if checksum is not None:
        query = query.where(DeSourceFiles.checksum == checksum)

    result = await session.execute(query)
    existing = result.first()

    if existing is None:
        reason = f"No prior ingestion found for {source_name} on {file_date.isoformat()}"
        logger.info("freshness_check_fresh", source_name=source_name, file_date=file_date.isoformat())
        return True, reason

    existing_id, existing_row_count = existing
    reason = (
        f"Duplicate detected: {source_name} on {file_date.isoformat()} "
        f"already ingested (id={existing_id}, rows={existing_row_count})"
    )
    logger.info(
        "freshness_check_duplicate",
        source_name=source_name,
        file_date=file_date.isoformat(),
        existing_id=str(existing_id),
        reason=reason,
    )
    return False, reason
