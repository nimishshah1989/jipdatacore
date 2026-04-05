"""
Response envelope models and helpers.

Every data endpoint wraps its payload in:
  { "data": [...], "meta": {...}, "pagination": {...} }

Response headers are duplicated for header-only clients:
  X-Data-Freshness, X-Computation-Version, X-System-Status
"""

from datetime import datetime
from enum import Enum
from typing import Any, Generic, Optional, TypeVar

from pydantic import BaseModel, Field

DataT = TypeVar("DataT")


class DataFreshness(str, Enum):
    FRESH = "fresh"
    STALE = "stale"
    PARTIAL = "partial"


class SystemStatus(str, Enum):
    NORMAL = "normal"
    DEGRADED = "degraded"


class PipelineStatus(str, Enum):
    COMPLETE = "complete"
    RUNNING = "running"
    FAILED = "failed"
    PARTIAL = "partial"


class ResponseMeta(BaseModel):
    data_freshness: DataFreshness = DataFreshness.FRESH
    last_updated_at: Optional[datetime] = None
    pipeline_status: PipelineStatus = PipelineStatus.COMPLETE
    computation_version: int = 1
    system_status: SystemStatus = SystemStatus.NORMAL


class PaginationMeta(BaseModel):
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=100, ge=1, le=10000)
    total_count: int = Field(default=0, ge=0)
    has_next: bool = False


class EnvelopeResponse(BaseModel, Generic[DataT]):
    """
    Standard response envelope for all data endpoints.

    Usage:
        return EnvelopeResponse(
            data=results,
            meta=ResponseMeta(...),
            pagination=PaginationMeta(...),
        )
    """

    data: DataT
    meta: ResponseMeta = Field(default_factory=ResponseMeta)
    pagination: Optional[PaginationMeta] = None

    model_config = {"arbitrary_types_allowed": True}


def build_envelope(
    data: Any,
    *,
    meta: Optional[ResponseMeta] = None,
    pagination: Optional[PaginationMeta] = None,
) -> EnvelopeResponse:
    """Convenience factory — creates envelope with sensible defaults."""
    return EnvelopeResponse(
        data=data,
        meta=meta or ResponseMeta(),
        pagination=pagination,
    )


def envelope_headers(meta: ResponseMeta) -> dict[str, str]:
    """
    Return the standard response headers derived from meta.
    Callers add these to the FastAPI Response object.
    """
    headers = {
        "X-Data-Freshness": meta.data_freshness.value,
        "X-Computation-Version": str(meta.computation_version),
        "X-System-Status": meta.system_status.value,
    }
    return headers
