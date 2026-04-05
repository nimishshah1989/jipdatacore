"""Tests for embedding pipeline: cosine similarity, dedup, and dimension checks."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.pipelines.qualitative.embeddings import (
    EMBEDDING_DIM,
    SEMANTIC_DUP_THRESHOLD,
    cosine_similarity,
    decode_embedding,
    encode_embedding,
    is_semantic_duplicate,
)


class TestCosineSimilarity:
    """Tests for cosine similarity computation."""

    def test_identical_vectors_similarity_is_1(self) -> None:
        """Two identical vectors should have cosine similarity of 1.0."""
        v = [0.1, 0.2, 0.3, 0.4, 0.5]
        sim = cosine_similarity(v, v)
        assert abs(sim - 1.0) < 1e-6

    def test_opposite_vectors_similarity_is_negative_1(self) -> None:
        """Opposite vectors should have cosine similarity of -1.0."""
        v = [1.0, 0.0, 0.0]
        neg_v = [-1.0, 0.0, 0.0]
        sim = cosine_similarity(v, neg_v)
        assert abs(sim - (-1.0)) < 1e-6

    def test_orthogonal_vectors_similarity_is_0(self) -> None:
        """Orthogonal vectors should have cosine similarity of 0."""
        v1 = [1.0, 0.0, 0.0]
        v2 = [0.0, 1.0, 0.0]
        sim = cosine_similarity(v1, v2)
        assert abs(sim) < 1e-6

    def test_cosine_similarity_above_092_flagged(self) -> None:
        """Vectors with cosine similarity > 0.92 should be flagged as duplicates."""
        # Slightly perturbed version of the same vector
        base = [1.0, 0.5, 0.3] * 100
        perturbed = [v + 0.01 for v in base]
        sim = cosine_similarity(base, perturbed)
        assert sim > SEMANTIC_DUP_THRESHOLD, f"Expected sim > {SEMANTIC_DUP_THRESHOLD}, got {sim}"

    def test_cosine_similarity_below_092_accepted(self) -> None:
        """Vectors with cosine similarity < 0.92 should NOT be flagged as duplicates."""
        # Very different vectors
        v1 = [1.0] + [0.0] * 99
        v2 = [0.0] * 50 + [1.0] + [0.0] * 49
        sim = cosine_similarity(v1, v2)
        assert sim < SEMANTIC_DUP_THRESHOLD, f"Expected sim < {SEMANTIC_DUP_THRESHOLD}, got {sim}"

    def test_length_mismatch_raises(self) -> None:
        """Vectors of different lengths should raise ValueError."""
        with pytest.raises(ValueError, match="length mismatch"):
            cosine_similarity([1.0, 2.0], [1.0, 2.0, 3.0])

    def test_zero_vector_returns_zero(self) -> None:
        """Zero vector paired with any vector should return 0.0 (not divide-by-zero)."""
        v1 = [0.0, 0.0, 0.0]
        v2 = [1.0, 2.0, 3.0]
        sim = cosine_similarity(v1, v2)
        assert sim == 0.0


class TestEmbeddingEncoding:
    """Tests for binary encoding/decoding of float vectors."""

    def test_encode_decode_roundtrip(self) -> None:
        """encode then decode should return approximately the same vector."""
        original = [float(i) / 1536.0 for i in range(EMBEDDING_DIM)]
        encoded = encode_embedding(original)
        decoded = decode_embedding(encoded)

        assert len(decoded) == EMBEDDING_DIM
        for orig, dec in zip(original, decoded):
            assert abs(orig - dec) < 1e-6

    def test_embedding_dimension_1536(self) -> None:
        """Standard embedding should have exactly 1536 dimensions."""
        vector = [0.1] * EMBEDDING_DIM
        encoded = encode_embedding(vector)
        decoded = decode_embedding(encoded)
        assert len(decoded) == EMBEDDING_DIM

    def test_encoded_length_correct(self) -> None:
        """Encoded bytes should be exactly dim * 4 bytes (float32)."""
        vector = [0.5] * 10
        encoded = encode_embedding(vector)
        assert len(encoded) == 10 * 4

    def test_encode_produces_bytes(self) -> None:
        """encode_embedding should return bytes."""
        result = encode_embedding([1.0, 2.0, 3.0])
        assert isinstance(result, bytes)


class TestSemanticDeduplication:
    """Tests for semantic duplicate detection in the DB."""

    @pytest.mark.asyncio
    async def test_cosine_similarity_above_092_flagged_in_db(self) -> None:
        """Vectors with cosine sim > 0.92 against DB record should be flagged as duplicates."""
        import uuid

        # Create two nearly identical vectors
        base_vec = [1.0, 0.5, 0.3] * 512  # 1536 dimensions
        perturbed = [v + 0.001 for v in base_vec]

        existing_id = str(uuid.uuid4())
        encoded_base = encode_embedding(base_vec)

        session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(existing_id, encoded_base)]
        session.execute = AsyncMock(return_value=mock_result)

        is_dup, matched_id = await is_semantic_duplicate(session, perturbed)

        assert is_dup is True
        assert matched_id == existing_id

    @pytest.mark.asyncio
    async def test_cosine_similarity_below_092_accepted_in_db(self) -> None:
        """Vectors with cosine sim < 0.92 against DB records should NOT be flagged."""
        import uuid

        # Very different vectors (orthogonal-ish)
        new_vec = [1.0] + [0.0] * 1535
        existing_vec = [0.0] + [1.0] + [0.0] * 1534

        existing_id = str(uuid.uuid4())
        encoded_existing = encode_embedding(existing_vec)

        session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(existing_id, encoded_existing)]
        session.execute = AsyncMock(return_value=mock_result)

        is_dup, matched_id = await is_semantic_duplicate(session, new_vec)

        assert is_dup is False
        assert matched_id is None

    @pytest.mark.asyncio
    async def test_no_existing_documents_returns_not_duplicate(self) -> None:
        """Empty DB window (no docs in past 48h) should return not duplicate."""
        new_vec = [0.5] * EMBEDDING_DIM

        session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        session.execute = AsyncMock(return_value=mock_result)

        is_dup, matched_id = await is_semantic_duplicate(session, new_vec)

        assert is_dup is False
        assert matched_id is None
