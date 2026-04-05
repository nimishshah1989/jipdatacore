"""S3 archival: upload processed documents then delete local temp files."""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Optional

from app.logging import get_logger

logger = get_logger(__name__)

_S3_PREFIX = "qualitative/"


async def archive_to_s3(
    file_path: Path,
    document_id: uuid.UUID,
    mime_type: str,
    delete_local: bool = True,
) -> str:
    """Upload a file to S3 and optionally delete the local copy.

    Args:
        file_path: Local path to the file to upload.
        document_id: Document UUID used to build the S3 key.
        mime_type: MIME type of the file (used as ContentType).
        delete_local: If True, delete the local file after successful upload.

    Returns:
        S3 URI (s3://bucket/key) of the uploaded file.

    Raises:
        RuntimeError: If S3 upload fails.
    """
    from app.config import get_settings

    settings = get_settings()

    suffix = file_path.suffix or ""
    s3_key = f"{_S3_PREFIX}{document_id}{suffix}"
    bucket = settings.s3_archive_bucket

    try:
        import aioboto3  # type: ignore[import]

        session = aioboto3.Session()
        async with session.client("s3", region_name=settings.aws_region) as s3:
            with open(file_path, "rb") as f:
                await s3.put_object(
                    Bucket=bucket,
                    Key=s3_key,
                    Body=f,
                    ContentType=mime_type,
                    Metadata={"document_id": str(document_id)},
                )

        s3_uri = f"s3://{bucket}/{s3_key}"
        logger.info(
            "s3_archive_uploaded",
            document_id=str(document_id),
            s3_uri=s3_uri,
            file=file_path.name,
        )

        if delete_local:
            file_path.unlink(missing_ok=True)
            logger.info("s3_archive_local_deleted", file=file_path.name)

        return s3_uri

    except Exception as exc:
        logger.error(
            "s3_archive_failed",
            document_id=str(document_id),
            file=file_path.name,
            error=str(exc),
        )
        raise RuntimeError(f"S3 archival failed for {file_path.name}: {exc}") from exc


async def get_s3_presigned_url(
    document_id: uuid.UUID,
    suffix: str = "",
    expiry_seconds: int = 3600,
) -> Optional[str]:
    """Generate a presigned URL for a previously archived document.

    Args:
        document_id: Document UUID to build the S3 key.
        suffix: File extension (e.g. '.pdf').
        expiry_seconds: URL expiry duration in seconds.

    Returns:
        Presigned URL string, or None if object doesn't exist.
    """
    from app.config import get_settings

    settings = get_settings()

    s3_key = f"{_S3_PREFIX}{document_id}{suffix}"
    bucket = settings.s3_archive_bucket

    try:
        import aioboto3  # type: ignore[import]

        session = aioboto3.Session()
        async with session.client("s3", region_name=settings.aws_region) as s3:
            url: str = await s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": s3_key},
                ExpiresIn=expiry_seconds,
            )
        return url
    except Exception as exc:
        logger.warning(
            "s3_presigned_url_failed",
            document_id=str(document_id),
            error=str(exc),
        )
        return None
