"""Qualitative layer — sources, documents, extracts, outcomes."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

import sqlalchemy as sa
from sqlalchemy import ForeignKey, Numeric, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DeQualSources(Base):
    """Qualitative data sources — podcasts, reports, interviews, etc."""

    __tablename__ = "de_qual_sources"
    __table_args__ = (
        sa.CheckConstraint(
            "source_type IN ('podcast','report','interview','webinar','article','social','internal')",
            name="chk_qual_source_type",
        ),
        UniqueConstraint("source_name", name="uq_qual_sources_name"),
    )

    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    source_name: Mapped[str] = mapped_column(sa.String(200), nullable=False)
    source_type: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    contributor_id: Mapped[Optional[int]] = mapped_column(
        sa.Integer,
        ForeignKey("de_contributors.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    feed_url: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(sa.Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeQualDocuments(Base):
    """Ingested documents from qualitative sources."""

    __tablename__ = "de_qual_documents"
    __table_args__ = (
        UniqueConstraint("source_id", "content_hash", name="uq_qual_doc_source_hash"),
        sa.CheckConstraint(
            "original_format IN ('pdf','audio','video','html','text','docx','xlsx')",
            name="chk_qual_doc_format",
        ),
        sa.CheckConstraint(
            "processing_status IN ('pending','processing','done','failed','skipped')",
            name="chk_qual_doc_status",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    source_id: Mapped[int] = mapped_column(
        sa.Integer,
        ForeignKey("de_qual_sources.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    content_hash: Mapped[Optional[str]] = mapped_column(sa.String(64), nullable=True)
    source_url: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    published_at: Mapped[Optional[datetime]] = mapped_column(
        sa.TIMESTAMP(timezone=True), nullable=True
    )
    ingested_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    title: Mapped[Optional[str]] = mapped_column(sa.String(500), nullable=True)
    original_format: Mapped[Optional[str]] = mapped_column(sa.String(10), nullable=True)
    raw_text: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    audio_url: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    audio_duration_s: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    summary: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    # vector(1536) — stored as LargeBinary in ORM; migration uses ALTER TABLE with pgvector type
    embedding: Mapped[Optional[bytes]] = mapped_column(sa.LargeBinary, nullable=True)
    tags: Mapped[Optional[List[str]]] = mapped_column(ARRAY(sa.Text), nullable=True)
    report_type: Mapped[Optional[str]] = mapped_column(sa.String(30), nullable=True)
    processing_status: Mapped[str] = mapped_column(
        sa.String(20), nullable=False, default="pending"
    )
    processing_error: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeQualExtracts(Base):
    """Structured investment views extracted from qualitative documents."""

    __tablename__ = "de_qual_extracts"
    __table_args__ = (
        sa.CheckConstraint(
            "asset_class IN ('equity','mf','bond','commodity','currency','macro','real_estate','other')",
            name="chk_qual_extract_asset_class",
        ),
        sa.CheckConstraint(
            "direction IN ('bullish','bearish','neutral','cautious')",
            name="chk_qual_extract_direction",
        ),
        sa.CheckConstraint(
            "conviction IN ('low','medium','high','very_high')",
            name="chk_qual_extract_conviction",
        ),
        sa.CheckConstraint(
            "quality_score BETWEEN 0 AND 1",
            name="chk_qual_extract_quality",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    document_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_qual_documents.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    asset_class: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    entity_ref: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    direction: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    timeframe: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    conviction: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    view_text: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    source_quote: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    quality_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2), nullable=True)
    # vector(1536) — stored as LargeBinary; migration uses pgvector type
    embedding: Mapped[Optional[bytes]] = mapped_column(sa.LargeBinary, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeQualOutcomes(Base):
    """Outcome tracking for qualitative investment extracts."""

    __tablename__ = "de_qual_outcomes"

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    extract_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_qual_extracts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    outcome_date: Mapped[Optional[sa.Date]] = mapped_column(sa.Date, nullable=True)
    was_correct: Mapped[Optional[bool]] = mapped_column(sa.Boolean, nullable=True)
    actual_move_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    entity_ref: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    recorded_by: Mapped[Optional[int]] = mapped_column(
        sa.Integer,
        ForeignKey("de_contributors.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    recorded_at: Mapped[Optional[datetime]] = mapped_column(
        sa.TIMESTAMP(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )
