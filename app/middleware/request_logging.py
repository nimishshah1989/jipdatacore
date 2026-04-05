"""
Request logging middleware.

Logs every request to structlog (and optionally de_request_log table).
Generates X-Request-ID header.
Captures: request_id, actor, source_ip, method, endpoint, status_code, duration_ms.
"""

import time
import uuid
from typing import Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from app.logging import get_logger

logger = get_logger(__name__)


def _get_client_ip(request: Request) -> str:
    """Extract real client IP, respecting X-Forwarded-For."""
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware that:
    1. Generates a UUID request_id and attaches it to request.state and X-Request-ID header
    2. Times the request
    3. Logs structured info after response
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id

        start_time = time.monotonic()
        response: Optional[Response] = None

        try:
            response = await call_next(request)
        except Exception as exc:
            duration_ms = int((time.monotonic() - start_time) * 1000)
            actor = self._get_actor(request)
            logger.error(
                "request_unhandled_exception",
                request_id=request_id,
                actor=actor,
                source_ip=_get_client_ip(request),
                method=request.method,
                endpoint=str(request.url.path),
                duration_ms=duration_ms,
                error=str(exc),
            )
            raise

        duration_ms = int((time.monotonic() - start_time) * 1000)
        actor = self._get_actor(request)

        logger.info(
            "request_completed",
            request_id=request_id,
            actor=actor,
            source_ip=_get_client_ip(request),
            method=request.method,
            endpoint=str(request.url.path),
            status_code=response.status_code,
            duration_ms=duration_ms,
        )

        response.headers["X-Request-ID"] = request_id
        return response

    @staticmethod
    def _get_actor(request: Request) -> str:
        """Extract actor from request state (set by auth middleware) or fallback."""
        platform = getattr(request.state, "platform", None)
        if platform:
            return platform
        return "anonymous"
