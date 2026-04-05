"""Tests for deduplication module: SHA-256 hashing and advisory locks."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.pipelines.qualitative.deduplication import (
    acquire_document_advisory_lock,
    compute_content_hash,
    is_exact_duplicate,
    release_document_advisory_lock,
)


class TestComputeContentHash:
    """Tests for SHA-256 content hashing."""

    def test_deterministic_output(self) -> None:
        """Same input always produces same hash."""
        h1 = compute_content_hash("Fed holds rates steady at 5.25%")
        h2 = compute_content_hash("Fed holds rates steady at 5.25%")
        assert h1 == h2

    def test_different_inputs_differ(self) -> None:
        """Different inputs must produce different hashes."""
        h1 = compute_content_hash("Bullish on Nifty 50")
        h2 = compute_content_hash("Bearish on Nifty 50")
        assert h1 != h2

    def test_hash_length_is_64(self) -> None:
        """SHA-256 hex digest must be 64 characters."""
        h = compute_content_hash("test content")
        assert len(h) == 64

    def test_empty_string_hash(self) -> None:
        """Empty string should produce a valid (but predictable) hash."""
        h = compute_content_hash("")
        assert len(h) == 64
        # SHA-256 of empty string is well-known
        assert h == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    def test_whitespace_sensitivity(self) -> None:
        """Leading/trailing whitespace changes the hash (no auto-strip)."""
        h1 = compute_content_hash("content")
        h2 = compute_content_hash("content ")
        assert h1 != h2


class TestIsExactDuplicate:
    """Tests for database duplicate checking."""

    @pytest.mark.asyncio
    async def test_returns_false_for_new_document(self) -> None:
        """New document (count=0) should return False."""
        session = MagicMock()
        result = MagicMock()
        result.scalar_one.return_value = 0
        session.execute = AsyncMock(return_value=result)

        assert await is_exact_duplicate(session, 42, "a" * 64) is False

    @pytest.mark.asyncio
    async def test_returns_true_for_existing_document(self) -> None:
        """Existing document (count>0) should return True."""
        session = MagicMock()
        result = MagicMock()
        result.scalar_one.return_value = 1
        session.execute = AsyncMock(return_value=result)

        assert await is_exact_duplicate(session, 42, "b" * 64) is True


class TestAdvisoryLock:
    """Tests for per-document advisory lock functions."""

    @pytest.mark.asyncio
    async def test_acquire_lock_returns_true_on_success(self) -> None:
        """Lock acquisition should return True when granted."""
        session = MagicMock()
        result = MagicMock()
        result.scalar.return_value = True
        session.execute = AsyncMock(return_value=result)

        acquired = await acquire_document_advisory_lock(session, "a" * 64)
        assert acquired is True

    @pytest.mark.asyncio
    async def test_acquire_lock_returns_false_when_contended(self) -> None:
        """Lock acquisition should return False when already held."""
        session = MagicMock()
        result = MagicMock()
        result.scalar.return_value = False
        session.execute = AsyncMock(return_value=result)

        acquired = await acquire_document_advisory_lock(session, "b" * 64)
        assert acquired is False

    @pytest.mark.asyncio
    async def test_release_lock_calls_execute(self) -> None:
        """release_document_advisory_lock should call session.execute."""
        session = MagicMock()
        session.execute = AsyncMock(return_value=MagicMock())

        await release_document_advisory_lock(session, "c" * 64)
        session.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_lock_key_includes_qual_prefix(self) -> None:
        """Lock key should be prefixed with 'qual:' for namespacing."""
        session = MagicMock()
        executed_params: list[dict] = []

        async def capture_execute(stmt, params=None):
            if params:
                executed_params.append(params)
            result = MagicMock()
            result.scalar.return_value = True
            return result

        session.execute = capture_execute

        content_hash = "f" * 64
        await acquire_document_advisory_lock(session, content_hash)

        assert len(executed_params) > 0
        assert executed_params[0]["key"] == f"qual:{content_hash}"
