"""
Qualitative data API endpoints.

POST /api/v1/qualitative/upload   — Upload a document for processing
GET  /api/v1/qualitative/search   — Search qualitative extracts
GET  /api/v1/qualitative/recent   — Recent qualitative documents
"""

import uuid
from datetime import datetime
from typing import Annotated, List, Optional

import sqlalchemy as sa
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import PaginationParams, get_current_user, get_db, get_redis
from app.logging import get_logger
from app.middleware.response import (
    EnvelopeResponse,
    PaginationMeta,
    ResponseMeta,
    build_envelope,
    envelope_headers,
)
from app.models.qualitative import DeQualDocuments, DeQualExtracts, DeQualSources
from app.services.redis_service import RedisService

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/qualitative", tags=["qualitative"])

_VALID_FORMATS = {"pdf", "audio", "video", "html", "text", "docx", "xlsx"}
_VALID_SOURCE_TYPES = {"podcast", "report", "interview", "webinar", "article", "social", "internal"}


# ---- Request schemas ----


class UploadRequest(BaseModel):
    source_id: int = Field(..., description="ID from de_qual_sources")
    title: Optional[str] = Field(default=None, max_length=500)
    source_url: Optional[str] = Field(default=None, description="URL of the document")
    original_format: str = Field(
        ..., description="Format: pdf, audio, video, html, text, docx, xlsx"
    )
    raw_text: Optional[str] = Field(default=None, description="Raw text content")
    tags: Optional[List[str]] = Field(default=None, description="Tags for the document")
    published_at: Optional[datetime] = Field(default=None, description="Publication datetime (IST)")


# ---- Endpoints ----


@router.post(
    "/upload",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Upload a qualitative document for processing",
)
async def upload_document(
    body: UploadRequest,
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> EnvelopeResponse:
    """Ingest a qualitative document and queue it for Claude processing."""
    if body.original_format not in _VALID_FORMATS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"original_format must be one of: {', '.join(sorted(_VALID_FORMATS))}",
        )

    # Verify source exists
    src_result = await db.execute(
        sa.select(DeQualSources).where(DeQualSources.id == body.source_id)
    )
    if src_result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source {body.source_id} not found",
        )

    doc = DeQualDocuments(
        id=uuid.uuid4(),
        source_id=body.source_id,
        title=body.title,
        source_url=body.source_url,
        original_format=body.original_format,
        raw_text=body.raw_text,
        tags=body.tags,
        published_at=body.published_at,
        processing_status="pending",
    )

    async with db.begin():
        db.add(doc)

    logger.info("qualitative_document_uploaded", doc_id=str(doc.id), source_id=body.source_id)

    meta = ResponseMeta()
    envelope = build_envelope(
        data={"document_id": str(doc.id), "processing_status": "pending"},
        meta=meta,
    )
    response.headers.update(envelope_headers(meta))
    return envelope


@router.get(
    "/search",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Search qualitative extracts",
)
async def search_qualitative(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    pagination: Annotated[PaginationParams, Depends()],
    q: Optional[str] = Query(default=None, description="Full-text search in view_text"),
    asset_class: Optional[str] = Query(
        default=None,
        description="Filter by asset class: equity, mf, bond, commodity, currency, macro, real_estate, other",
    ),
    direction: Optional[str] = Query(
        default=None,
        description="Filter by direction: bullish, bearish, neutral, cautious",
    ),
    conviction: Optional[str] = Query(
        default=None,
        description="Filter by conviction: low, medium, high, very_high",
    ),
    entity_ref: Optional[str] = Query(default=None, description="Filter by entity reference (symbol/ticker)"),
) -> EnvelopeResponse:
    """Search qualitative extracts with optional full-text and filter params."""
    filters: list = []
    if q:
        filters.append(DeQualExtracts.view_text.ilike(f"%{q}%"))
    if asset_class:
        filters.append(DeQualExtracts.asset_class == asset_class)
    if direction:
        filters.append(DeQualExtracts.direction == direction)
    if conviction:
        filters.append(DeQualExtracts.conviction == conviction)
    if entity_ref:
        filters.append(sa.func.lower(DeQualExtracts.entity_ref) == entity_ref.lower())

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeQualExtracts).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeQualExtracts)
        .where(*filters)
        .order_by(DeQualExtracts.created_at.desc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        {
            "id": str(r.id),
            "document_id": str(r.document_id),
            "asset_class": r.asset_class,
            "entity_ref": r.entity_ref,
            "direction": r.direction,
            "timeframe": r.timeframe,
            "conviction": r.conviction,
            "view_text": r.view_text,
            "source_quote": r.source_quote,
            "quality_score": r.quality_score,
            "created_at": r.created_at.isoformat() if r.created_at else None,
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
    "/recent",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Recent qualitative documents",
)
async def get_recent_documents(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    pagination: Annotated[PaginationParams, Depends()],
    processing_status: Optional[str] = Query(
        default=None,
        description="Filter by status: pending, processing, done, failed, skipped",
    ),
    source_id: Optional[int] = Query(default=None, description="Filter by source ID"),
) -> EnvelopeResponse:
    """Return recently ingested documents, newest first."""
    filters: list = []
    if processing_status:
        filters.append(DeQualDocuments.processing_status == processing_status)
    if source_id is not None:
        filters.append(DeQualDocuments.source_id == source_id)

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeQualDocuments).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeQualDocuments)
        .where(*filters)
        .order_by(DeQualDocuments.ingested_at.desc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        {
            "id": str(r.id),
            "source_id": r.source_id,
            "title": r.title,
            "original_format": r.original_format,
            "processing_status": r.processing_status,
            "published_at": r.published_at.isoformat() if r.published_at else None,
            "ingested_at": r.ingested_at.isoformat() if r.ingested_at else None,
            "tags": r.tags,
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
