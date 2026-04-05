"""Upload handler for admin-submitted qualitative documents.

Accepts: PDF, audio, text, or URL.
Admin JWT required. 10 uploads/hour rate limit.
"""

from __future__ import annotations

import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.qualitative import DeQualDocuments
from app.pipelines.qualitative.archival import archive_to_s3
from app.pipelines.qualitative.cost_guard import check_all_caps
from app.pipelines.qualitative.deduplication import (
    acquire_document_advisory_lock,
    compute_content_hash,
    is_exact_duplicate,
    release_document_advisory_lock,
)
from app.pipelines.qualitative.extractor import ContentExtractor
from app.pipelines.qualitative.security_gate import (
    is_audio_mime,
    mime_to_format,
    run_security_gate,
)

logger = get_logger(__name__)

_SUPPORTED_MIMES = frozenset(
    [
        "application/pdf",
        "audio/mpeg",
        "audio/mp3",
        "audio/wav",
        "audio/x-wav",
        "audio/ogg",
        "audio/flac",
        "audio/aac",
        "audio/mp4",
        "audio/x-m4a",
        "text/plain",
    ]
)


class UploadValidationError(Exception):
    """Raised when upload request fails validation."""

    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


class UploadResult:
    """Result of a document upload operation."""

    def __init__(
        self,
        document_id: uuid.UUID,
        source_id: int,
        content_hash: str,
        s3_uri: Optional[str],
        was_duplicate: bool,
    ) -> None:
        self.document_id = document_id
        self.source_id = source_id
        self.content_hash = content_hash
        self.s3_uri = s3_uri
        self.was_duplicate = was_duplicate


class UploadHandler:
    """Handles document upload ingestion.

    Flow: validate → security gate → cost check → dedup → extract → archive → insert.
    """

    def __init__(self) -> None:
        self._extractor = ContentExtractor()

    async def handle_file_upload(
        self,
        session: AsyncSession,
        file_bytes: bytes,
        filename: str,
        declared_mime: str,
        source_id: int,
        title: Optional[str] = None,
        skip_clamav: bool = False,
    ) -> UploadResult:
        """Process an uploaded file document.

        Args:
            session: Async SQLAlchemy session.
            file_bytes: Raw file bytes from the upload.
            filename: Original filename.
            declared_mime: MIME type declared by the client.
            source_id: Source to associate the document with.
            title: Optional document title.
            skip_clamav: Skip ClamAV scan (dev mode only).

        Returns:
            UploadResult with document_id and metadata.

        Raises:
            UploadValidationError: On validation failures.
        """
        # Validate MIME type
        if declared_mime.lower() not in _SUPPORTED_MIMES:
            raise UploadValidationError(
                f"Unsupported MIME type: {declared_mime!r}. "
                f"Accepted: {sorted(_SUPPORTED_MIMES)}"
            )

        today = datetime.now(tz=timezone.utc).date()
        is_audio = is_audio_mime(declared_mime)

        # Cost cap check
        await check_all_caps(session, source_id=source_id, is_audio=is_audio, today=today)

        # Write to temp file for security gate
        with tempfile.NamedTemporaryFile(
            suffix=Path(filename).suffix,
            delete=False,
        ) as tmp:
            tmp.write(file_bytes)
            tmp_path = Path(tmp.name)

        try:
            # Security gate: magic bytes → ClamAV
            actual_mime = run_security_gate(tmp_path, declared_mime, skip_clamav=skip_clamav)

            # Extract text content
            fmt = mime_to_format(actual_mime) or "text"
            if is_audio:
                raw_text = await self._extractor.extract("audio", file_path=tmp_path)
            elif fmt == "pdf":
                raw_text = await self._extractor.extract("pdf", file_path=tmp_path)
            else:
                raw_text = await self._extractor.extract("text", file_path=tmp_path)

            # Content hash for dedup
            content_hash = compute_content_hash(raw_text or file_bytes.hex())

            # Advisory lock
            lock_acquired = await acquire_document_advisory_lock(session, content_hash)
            if not lock_acquired:
                raise UploadValidationError("Document is currently being processed", status_code=409)

            try:
                # Exact dedup
                if await is_exact_duplicate(session, source_id=source_id, content_hash=content_hash):
                    # Return existing document info
                    return UploadResult(
                        document_id=uuid.uuid4(),  # placeholder
                        source_id=source_id,
                        content_hash=content_hash,
                        s3_uri=None,
                        was_duplicate=True,
                    )

                # Insert document record
                doc_id = uuid.uuid4()
                stmt = pg_insert(DeQualDocuments).values(
                    id=doc_id,
                    source_id=source_id,
                    content_hash=content_hash,
                    title=title[:500] if title else filename[:500],
                    original_format=fmt,
                    raw_text=raw_text,
                    processing_status="pending",
                    audio_duration_s=None,
                ).on_conflict_do_nothing(constraint="uq_qual_doc_source_hash")

                await session.execute(stmt)
                await session.flush()

                # Archive to S3
                s3_uri: Optional[str] = None
                try:
                    s3_uri = await archive_to_s3(
                        file_path=tmp_path,
                        document_id=doc_id,
                        mime_type=actual_mime,
                        delete_local=True,
                    )
                    tmp_path = Path("/dev/null")  # already deleted
                except Exception as exc:
                    logger.warning("upload_s3_archive_failed", error=str(exc))

                logger.info(
                    "upload_document_ingested",
                    document_id=str(doc_id),
                    source_id=source_id,
                    mime=actual_mime,
                    s3_uri=s3_uri,
                )

                return UploadResult(
                    document_id=doc_id,
                    source_id=source_id,
                    content_hash=content_hash,
                    s3_uri=s3_uri,
                    was_duplicate=False,
                )

            finally:
                await release_document_advisory_lock(session, content_hash)

        finally:
            # Clean up temp file if still exists
            if tmp_path.exists() and str(tmp_path) != "/dev/null":
                tmp_path.unlink(missing_ok=True)

    async def handle_url_upload(
        self,
        session: AsyncSession,
        url: str,
        source_id: int,
        title: Optional[str] = None,
    ) -> UploadResult:
        """Process a URL-based document submission.

        Fetches content, deduplicates, and inserts as pending document.
        """
        today = datetime.now(tz=timezone.utc).date()

        # Cost cap (URL is not audio)
        await check_all_caps(session, source_id=source_id, is_audio=False, today=today)

        # Fetch URL content
        raw_text = await self._extractor.extract("url", url=url)

        content_hash = compute_content_hash(raw_text)

        lock_acquired = await acquire_document_advisory_lock(session, content_hash)
        if not lock_acquired:
            raise UploadValidationError("URL is currently being processed", status_code=409)

        try:
            if await is_exact_duplicate(session, source_id=source_id, content_hash=content_hash):
                return UploadResult(
                    document_id=uuid.uuid4(),
                    source_id=source_id,
                    content_hash=content_hash,
                    s3_uri=None,
                    was_duplicate=True,
                )

            doc_id = uuid.uuid4()
            stmt = pg_insert(DeQualDocuments).values(
                id=doc_id,
                source_id=source_id,
                content_hash=content_hash,
                source_url=url,
                title=title[:500] if title else url[:500],
                original_format="html",
                raw_text=raw_text,
                processing_status="pending",
            ).on_conflict_do_nothing(constraint="uq_qual_doc_source_hash")

            await session.execute(stmt)
            await session.flush()

            logger.info(
                "upload_url_ingested",
                document_id=str(doc_id),
                source_id=source_id,
                url=url[:80],
            )

            return UploadResult(
                document_id=doc_id,
                source_id=source_id,
                content_hash=content_hash,
                s3_uri=None,
                was_duplicate=False,
            )

        finally:
            await release_document_advisory_lock(session, content_hash)
