"""SHA-256 exact deduplication and per-document advisory locks."""

from __future__ import annotations

import hashlib

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.qualitative import DeQualDocuments

logger = get_logger(__name__)


def compute_content_hash(content: str) -> str:
    """Compute SHA-256 hex digest of content string.

    Returns a 64-character lowercase hex string.
    """
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


async def is_exact_duplicate(
    session: AsyncSession,
    source_id: int,
    content_hash: str,
) -> bool:
    """Check if a document with the same source_id and content_hash already exists.

    Returns True if an exact duplicate is found, False otherwise.
    """
    stmt = sa.select(sa.func.count()).where(
        DeQualDocuments.source_id == source_id,
        DeQualDocuments.content_hash == content_hash,
    )
    result = await session.execute(stmt)
    count = result.scalar_one()
    return count > 0


async def acquire_document_advisory_lock(
    session: AsyncSession,
    content_hash: str,
) -> bool:
    """Acquire a per-document advisory lock keyed on content_hash.

    Uses pg_try_advisory_lock(hashtext('qual:<content_hash>')) for non-blocking acquisition.
    Returns True if lock was acquired, False if another session holds it.
    """
    key = f"qual:{content_hash}"
    result = await session.execute(
        sa.text("SELECT pg_try_advisory_lock(hashtext(:key))"),
        {"key": key},
    )
    acquired: bool = result.scalar()
    logger.debug(
        "document_advisory_lock_acquire",
        content_hash=content_hash[:16] + "...",
        acquired=acquired,
    )
    return bool(acquired)


async def release_document_advisory_lock(
    session: AsyncSession,
    content_hash: str,
) -> None:
    """Release the advisory lock for the given content_hash."""
    key = f"qual:{content_hash}"
    await session.execute(
        sa.text("SELECT pg_advisory_unlock(hashtext(:key))"),
        {"key": key},
    )
    logger.debug(
        "document_advisory_lock_release",
        content_hash=content_hash[:16] + "...",
    )
