"""OpenAI text-embedding-3-small embeddings with semantic deduplication."""

from __future__ import annotations

import math
import struct
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger

logger = get_logger(__name__)

EMBEDDING_DIM = 1536
SEMANTIC_DUP_THRESHOLD = 0.92
_EMBEDDING_MODEL = "text-embedding-3-small"

# Look back window for semantic duplicate search (hours)
_DEDUP_LOOKBACK_HOURS = 48


def cosine_similarity(v1: list[float], v2: list[float]) -> float:
    """Compute cosine similarity between two float vectors.

    Returns a value in [-1, 1].

    Raises:
        ValueError: When vectors have different lengths.
    """
    if len(v1) != len(v2):
        raise ValueError(
            f"Vector length mismatch: len(v1)={len(v1)}, len(v2)={len(v2)}"
        )

    dot = sum(a * b for a, b in zip(v1, v2))
    norm1 = math.sqrt(sum(a * a for a in v1))
    norm2 = math.sqrt(sum(b * b for b in v2))

    if norm1 == 0.0 or norm2 == 0.0:
        return 0.0

    return dot / (norm1 * norm2)


def encode_embedding(vector: list[float]) -> bytes:
    """Encode a float vector as packed float32 bytes.

    Returns bytes of length len(vector) * 4.
    """
    return struct.pack(f"{len(vector)}f", *vector)


def decode_embedding(data: bytes) -> list[float]:
    """Decode packed float32 bytes back to a float list.

    Returns a list of floats with length len(data) // 4.
    """
    n = len(data) // 4
    return list(struct.unpack(f"{n}f", data))


async def generate_embedding(text: str) -> list[float]:
    """Generate a text embedding using OpenAI text-embedding-3-small.

    Args:
        text: Input text to embed.

    Returns:
        A list of EMBEDDING_DIM (1536) float values.
    """
    from app.config import get_settings

    settings = get_settings()

    try:
        import openai  # type: ignore[import]

        client = openai.AsyncOpenAI(api_key=settings.openai_api_key)
        response = await client.embeddings.create(
            model=_EMBEDDING_MODEL,
            input=text,
        )
        embedding: list[float] = response.data[0].embedding
        logger.debug("embedding_generated", dim=len(embedding), model=_EMBEDDING_MODEL)
        return embedding
    except Exception as exc:
        logger.error("embedding_generation_failed", error=str(exc))
        raise


async def is_semantic_duplicate(
    session: AsyncSession,
    embedding: list[float],
) -> tuple[bool, Optional[str]]:
    """Check if a document is semantically duplicate against recent documents.

    Compares the given embedding against documents ingested in the past
    DEDUP_LOOKBACK_HOURS hours using cosine similarity.

    Returns:
        A tuple of (is_duplicate, matched_document_id).
        matched_document_id is None if no duplicate is found.
    """
    # Fetch recent document embeddings
    stmt = sa.text(
        """
        SELECT id::text, embedding
        FROM de_qual_documents
        WHERE embedding IS NOT NULL
          AND ingested_at >= NOW() - INTERVAL ':hours hours'
        ORDER BY ingested_at DESC
        LIMIT 1000
        """
    ).bindparams(hours=_DEDUP_LOOKBACK_HOURS)

    # Use a raw query without text interpolation in the interval
    stmt = sa.text(
        f"""
        SELECT id::text, embedding
        FROM de_qual_documents
        WHERE embedding IS NOT NULL
          AND ingested_at >= NOW() - INTERVAL '{_DEDUP_LOOKBACK_HOURS} hours'
        ORDER BY ingested_at DESC
        LIMIT 1000
        """
    )

    result = await session.execute(stmt)
    rows = result.fetchall()

    for doc_id, raw_embedding in rows:
        if raw_embedding is None:
            continue
        existing = decode_embedding(bytes(raw_embedding))
        try:
            sim = cosine_similarity(embedding, existing)
        except ValueError:
            continue

        if sim > SEMANTIC_DUP_THRESHOLD:
            logger.info(
                "semantic_duplicate_found",
                similarity=round(sim, 4),
                matched_id=doc_id,
                threshold=SEMANTIC_DUP_THRESHOLD,
            )
            return True, doc_id

    return False, None
