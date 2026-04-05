"""Qualitative data ingestion pipelines — RSS, upload, extraction, embeddings."""

from app.pipelines.qualitative.rss import RssPipeline
from app.pipelines.qualitative.upload import UploadHandler
from app.pipelines.qualitative.security_gate import run_security_gate, SecurityGateError
from app.pipelines.qualitative.extractor import ContentExtractor, ExtractionError
from app.pipelines.qualitative.claude_extract import extract_views_from_text, ClaudeExtractionError
from app.pipelines.qualitative.embeddings import generate_embedding, is_semantic_duplicate
from app.pipelines.qualitative.deduplication import (
    compute_content_hash,
    is_exact_duplicate,
    acquire_document_advisory_lock,
    release_document_advisory_lock,
)
from app.pipelines.qualitative.archival import archive_to_s3
from app.pipelines.qualitative.cost_guard import check_all_caps, CostLimitExceededError

__all__ = [
    "RssPipeline",
    "UploadHandler",
    "run_security_gate",
    "SecurityGateError",
    "ContentExtractor",
    "ExtractionError",
    "extract_views_from_text",
    "ClaudeExtractionError",
    "generate_embedding",
    "is_semantic_duplicate",
    "compute_content_hash",
    "is_exact_duplicate",
    "acquire_document_advisory_lock",
    "release_document_advisory_lock",
    "archive_to_s3",
    "check_all_caps",
    "CostLimitExceededError",
]
