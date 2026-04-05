"""Tests for RSS feed deduplication and advisory lock logic."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.pipelines.qualitative.deduplication import (
    acquire_document_advisory_lock,
    compute_content_hash,
    is_exact_duplicate,
    release_document_advisory_lock,
)


class TestSha256Dedup:
    """Tests for SHA-256 content hash deduplication."""

    def test_sha256_dedup_same_content(self) -> None:
        """Same content should always produce the same hash."""
        content = "RBI Governor announces new monetary policy measures for 2026."
        hash1 = compute_content_hash(content)
        hash2 = compute_content_hash(content)
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA-256 hex digest length

    def test_sha256_dedup_different_content(self) -> None:
        """Different content must produce different hashes."""
        content_a = "SEBI tightens F&O margin requirements effective Q2."
        content_b = "NSE extends trading hours for equity derivatives."
        hash_a = compute_content_hash(content_a)
        hash_b = compute_content_hash(content_b)
        assert hash_a != hash_b

    def test_sha256_hash_is_hex_string(self) -> None:
        """Hash should be a lowercase hex string of length 64."""
        h = compute_content_hash("some market content")
        assert isinstance(h, str)
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_sha256_unicode_content_stable(self) -> None:
        """Unicode content (Indian text) should hash consistently."""
        content = "भारतीय रिज़र्व बैंक ने ब्याज दर में बदलाव किया"
        h1 = compute_content_hash(content)
        h2 = compute_content_hash(content)
        assert h1 == h2

    @pytest.mark.asyncio
    async def test_is_exact_duplicate_returns_false_when_not_found(self) -> None:
        """is_exact_duplicate should return False when no matching row exists."""
        session = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one.return_value = 0
        session.execute = AsyncMock(return_value=mock_result)

        result = await is_exact_duplicate(session, source_id=1, content_hash="abc123")
        assert result is False

    @pytest.mark.asyncio
    async def test_is_exact_duplicate_returns_true_when_found(self) -> None:
        """is_exact_duplicate should return True when a matching row exists."""
        session = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one.return_value = 1
        session.execute = AsyncMock(return_value=mock_result)

        result = await is_exact_duplicate(session, source_id=1, content_hash="abc123")
        assert result is True


class TestAdvisoryLockRelease:
    """Tests for per-document advisory lock acquire/release lifecycle."""

    @pytest.mark.asyncio
    async def test_advisory_lock_acquired_returns_true(self) -> None:
        """acquire_document_advisory_lock should return True when lock is granted."""
        session = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = True
        session.execute = AsyncMock(return_value=mock_result)

        acquired = await acquire_document_advisory_lock(session, "deadbeef" * 8)
        assert acquired is True

    @pytest.mark.asyncio
    async def test_advisory_lock_contention_returns_false(self) -> None:
        """acquire_document_advisory_lock returns False when another session holds the lock."""
        session = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = False
        session.execute = AsyncMock(return_value=mock_result)

        acquired = await acquire_document_advisory_lock(session, "deadbeef" * 8)
        assert acquired is False

    @pytest.mark.asyncio
    async def test_advisory_lock_release_on_error(self) -> None:
        """Lock must be released in finally block even when processing raises an error."""
        session = MagicMock()

        acquire_result = MagicMock()
        acquire_result.scalar.return_value = True

        release_result = MagicMock()

        call_log: list[str] = []

        async def mock_execute(stmt, params=None):
            sql_str = str(stmt)
            if "pg_try_advisory_lock" in sql_str:
                call_log.append("acquire")
                return acquire_result
            if "pg_advisory_unlock" in sql_str:
                call_log.append("release")
                return release_result
            return MagicMock()

        session.execute = mock_execute

        content_hash = "a" * 64  # valid 64-char hash

        with pytest.raises(RuntimeError, match="simulated processing error"):
            lock_acquired = await acquire_document_advisory_lock(session, content_hash)
            try:
                assert lock_acquired is True
                raise RuntimeError("simulated processing error")
            finally:
                await release_document_advisory_lock(session, content_hash)

        assert "acquire" in call_log
        assert "release" in call_log
        assert call_log.index("release") > call_log.index("acquire")
