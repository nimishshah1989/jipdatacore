"""
Admin API endpoints — require admin JWT claim.

GET  /api/v1/admin/pipeline/status        — Pipeline run statuses
GET  /api/v1/admin/anomalies              — Data anomalies list
POST /api/v1/admin/anomalies/{id}/resolve — Resolve an anomaly
POST /api/v1/admin/data/override          — Manual data override (system flag)
POST /api/v1/admin/pipeline/replay        — Trigger pipeline replay
POST /api/v1/admin/system/flag            — Set/unset a system flag
"""

import uuid
from datetime import date, datetime, timezone
from typing import Annotated, Optional

import sqlalchemy as sa
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import PaginationParams, get_admin_user, get_db
from app.logging import get_logger
from app.middleware.response import (
    EnvelopeResponse,
    PaginationMeta,
    ResponseMeta,
    build_envelope,
    envelope_headers,
)
from app.models.pipeline import DePipelineLog, DeSystemFlags
from app.models.prices import DeDataAnomalies

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


# ---- Request schemas ----


class ResolveAnomalyRequest(BaseModel):
    resolution_note: Optional[str] = Field(default=None, max_length=2000)


class DataOverrideRequest(BaseModel):
    flag_key: str = Field(..., max_length=50, description="de_system_flags key to set")
    value: bool = Field(..., description="True to enable, False to disable")
    reason: Optional[str] = Field(default=None, max_length=1000)


class PipelineReplayRequest(BaseModel):
    pipeline_name: str = Field(..., max_length=100, description="Pipeline name to replay")
    business_date: date = Field(..., description="Business date to replay")
    reason: Optional[str] = Field(default=None, max_length=500)


class SystemFlagRequest(BaseModel):
    key: str = Field(..., max_length=50, description="Flag key")
    value: bool = Field(..., description="Flag value")
    reason: Optional[str] = Field(default=None, max_length=1000)


# ---- Endpoints ----


@router.get(
    "/pipeline/status",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Pipeline run statuses",
)
async def get_pipeline_status(
    response: Response,
    admin: Annotated[dict, Depends(get_admin_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    pagination: Annotated[PaginationParams, Depends()],
    pipeline_name: Optional[str] = Query(default=None, description="Filter by pipeline name"),
    run_status: Optional[str] = Query(
        default=None,
        description="Filter by status: pending, running, success, partial, failed, skipped",
    ),
    business_date: Optional[date] = Query(default=None, description="Filter by business date"),
) -> EnvelopeResponse:
    """Return pipeline run logs. Admin only."""
    filters: list = []
    if pipeline_name:
        filters.append(DePipelineLog.pipeline_name == pipeline_name)
    if run_status:
        filters.append(DePipelineLog.status == run_status)
    if business_date:
        filters.append(DePipelineLog.business_date == business_date)

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DePipelineLog).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DePipelineLog)
        .where(*filters)
        .order_by(DePipelineLog.created_at.desc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        {
            "id": r.id,
            "pipeline_name": r.pipeline_name,
            "business_date": str(r.business_date) if r.business_date else None,
            "run_number": r.run_number,
            "status": r.status,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            "rows_processed": r.rows_processed,
            "rows_failed": r.rows_failed,
            "error_detail": r.error_detail,
        }
        for r in rows
    ]

    meta = ResponseMeta()
    pag = PaginationMeta(
        page=pagination.page,
        page_size=pagination.page_size,
        total_count=total,
        has_next=(pagination.offset + pagination.page_size) < total,
    )
    envelope = build_envelope(data=data, meta=meta, pagination=pag)
    response.headers.update(envelope_headers(meta))
    return envelope


@router.get(
    "/anomalies",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="List data anomalies",
)
async def get_anomalies(
    response: Response,
    admin: Annotated[dict, Depends(get_admin_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    pagination: Annotated[PaginationParams, Depends()],
    severity: Optional[str] = Query(
        default=None, description="Filter by severity: low, medium, high, critical"
    ),
    entity_type: Optional[str] = Query(
        default=None, description="Filter by entity type: equity, mf, index, macro, flow"
    ),
    is_resolved: Optional[bool] = Query(default=False, description="Include resolved anomalies"),
    pipeline_name: Optional[str] = Query(default=None, description="Filter by pipeline"),
) -> EnvelopeResponse:
    """Return data anomalies detected by pipelines. Admin only."""
    filters: list = []
    if severity:
        filters.append(DeDataAnomalies.severity == severity)
    if entity_type:
        filters.append(DeDataAnomalies.entity_type == entity_type)
    if is_resolved is not None:
        filters.append(DeDataAnomalies.is_resolved.is_(is_resolved))
    if pipeline_name:
        filters.append(DeDataAnomalies.pipeline_name == pipeline_name)

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeDataAnomalies).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeDataAnomalies)
        .where(*filters)
        .order_by(DeDataAnomalies.detected_at.desc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        {
            "id": str(r.id),
            "pipeline_name": r.pipeline_name,
            "business_date": str(r.business_date) if r.business_date else None,
            "entity_type": r.entity_type,
            "anomaly_type": r.anomaly_type,
            "severity": r.severity,
            "expected_range": r.expected_range,
            "actual_value": r.actual_value,
            "is_resolved": r.is_resolved,
            "resolved_by": r.resolved_by,
            "resolved_at": r.resolved_at.isoformat() if r.resolved_at else None,
            "resolution_note": r.resolution_note,
            "detected_at": r.detected_at.isoformat() if r.detected_at else None,
        }
        for r in rows
    ]

    meta = ResponseMeta()
    pag = PaginationMeta(
        page=pagination.page,
        page_size=pagination.page_size,
        total_count=total,
        has_next=(pagination.offset + pagination.page_size) < total,
    )
    envelope = build_envelope(data=data, meta=meta, pagination=pag)
    response.headers.update(envelope_headers(meta))
    return envelope


@router.post(
    "/anomalies/{anomaly_id}/resolve",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Mark a data anomaly as resolved",
)
async def resolve_anomaly(
    anomaly_id: uuid.UUID,
    body: ResolveAnomalyRequest,
    response: Response,
    admin: Annotated[dict, Depends(get_admin_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> EnvelopeResponse:
    """Resolve a data anomaly. Admin only."""
    result = await db.execute(
        sa.select(DeDataAnomalies).where(DeDataAnomalies.id == anomaly_id)
    )
    anomaly = result.scalar_one_or_none()
    if anomaly is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Anomaly {anomaly_id} not found",
        )

    if anomaly.is_resolved:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Anomaly is already resolved",
        )

    actor = admin.get("sub", "unknown")
    now = datetime.now(tz=timezone.utc)

    async with db.begin():
        await db.execute(
            sa.update(DeDataAnomalies)
            .where(DeDataAnomalies.id == anomaly_id)
            .values(
                is_resolved=True,
                resolved_by=actor,
                resolved_at=now,
                resolution_note=body.resolution_note,
            )
        )

    logger.info("anomaly_resolved", anomaly_id=str(anomaly_id), resolved_by=actor)

    meta = ResponseMeta()
    envelope = build_envelope(
        data={"anomaly_id": str(anomaly_id), "resolved": True, "resolved_by": actor},
        meta=meta,
    )
    response.headers.update(envelope_headers(meta))
    return envelope


@router.post(
    "/data/override",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Set a data override system flag",
)
async def data_override(
    body: DataOverrideRequest,
    response: Response,
    admin: Annotated[dict, Depends(get_admin_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> EnvelopeResponse:
    """Set or update a system flag for data override. Admin only."""
    actor = admin.get("sub", "unknown")
    now = datetime.now(tz=timezone.utc)

    async with db.begin():
        # Upsert system flag
        existing = await db.execute(
            sa.select(DeSystemFlags).where(DeSystemFlags.key == body.flag_key)
        )
        flag = existing.scalar_one_or_none()

        if flag is None:
            db.add(
                DeSystemFlags(
                    key=body.flag_key,
                    value=body.value,
                    updated_by=actor,
                    updated_at=now,
                    reason=body.reason,
                )
            )
        else:
            await db.execute(
                sa.update(DeSystemFlags)
                .where(DeSystemFlags.key == body.flag_key)
                .values(value=body.value, updated_by=actor, updated_at=now, reason=body.reason)
            )

    logger.info(
        "data_override_set",
        flag_key=body.flag_key,
        value=body.value,
        actor=actor,
    )

    meta = ResponseMeta()
    envelope = build_envelope(
        data={"flag_key": body.flag_key, "value": body.value, "updated_by": actor},
        meta=meta,
    )
    response.headers.update(envelope_headers(meta))
    return envelope


@router.post(
    "/pipeline/replay",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Queue a pipeline replay",
)
async def pipeline_replay(
    body: PipelineReplayRequest,
    response: Response,
    admin: Annotated[dict, Depends(get_admin_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> EnvelopeResponse:
    """
    Queue a pipeline replay by creating a new log entry with 'pending' status.
    The actual replay is executed by the pipeline scheduler.
    Admin only.
    """
    actor = admin.get("sub", "unknown")

    # Check max run_number for this pipeline+date
    run_num_result = await db.execute(
        sa.select(sa.func.coalesce(sa.func.max(DePipelineLog.run_number), 0)).where(
            DePipelineLog.pipeline_name == body.pipeline_name,
            DePipelineLog.business_date == body.business_date,
        )
    )
    next_run = (run_num_result.scalar_one() or 0) + 1

    new_log = DePipelineLog(
        pipeline_name=body.pipeline_name,
        business_date=body.business_date,
        run_number=next_run,
        status="pending",
        error_detail=f"Replay requested by {actor}" + (f": {body.reason}" if body.reason else ""),
    )

    async with db.begin():
        db.add(new_log)

    logger.info(
        "pipeline_replay_queued",
        pipeline=body.pipeline_name,
        date=str(body.business_date),
        run_number=next_run,
        actor=actor,
    )

    meta = ResponseMeta()
    envelope = build_envelope(
        data={
            "pipeline_name": body.pipeline_name,
            "business_date": str(body.business_date),
            "run_number": next_run,
            "status": "pending",
        },
        meta=meta,
    )
    response.headers.update(envelope_headers(meta))
    return envelope


@router.post(
    "/system/flag",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Set or unset a system flag",
)
async def set_system_flag(
    body: SystemFlagRequest,
    response: Response,
    admin: Annotated[dict, Depends(get_admin_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> EnvelopeResponse:
    """Set or update a system-wide flag. Admin only."""
    actor = admin.get("sub", "unknown")
    now = datetime.now(tz=timezone.utc)

    async with db.begin():
        existing = await db.execute(
            sa.select(DeSystemFlags).where(DeSystemFlags.key == body.key)
        )
        flag = existing.scalar_one_or_none()

        if flag is None:
            db.add(
                DeSystemFlags(
                    key=body.key,
                    value=body.value,
                    updated_by=actor,
                    updated_at=now,
                    reason=body.reason,
                )
            )
        else:
            await db.execute(
                sa.update(DeSystemFlags)
                .where(DeSystemFlags.key == body.key)
                .values(value=body.value, updated_by=actor, updated_at=now, reason=body.reason)
            )

    logger.info("system_flag_set", key=body.key, value=body.value, actor=actor)

    meta = ResponseMeta()
    envelope = build_envelope(
        data={"key": body.key, "value": body.value, "updated_by": actor},
        meta=meta,
    )
    response.headers.update(envelope_headers(meta))
    return envelope
