"""
Observatory UI router — serves the static dashboard HTML.

GET /observatory  — returns observatory.html (no auth required)

This is a thin wrapper so the dashboard is served from the same FastAPI
process without needing a separate static file server or nginx config change.
"""

from __future__ import annotations

import os

from fastapi import APIRouter
from fastapi.responses import FileResponse, HTMLResponse

router = APIRouter(tags=["observatory-ui"])

_STATIC_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "static")
_HTML_PATH = os.path.join(_STATIC_DIR, "observatory.html")
_AUDIT_HTML_PATH = os.path.join(_STATIC_DIR, "data-audit.html")


@router.get(
    "/observatory",
    include_in_schema=False,
    response_class=HTMLResponse,
    summary="JIP Data Observatory dashboard",
)
async def observatory_dashboard() -> FileResponse:
    """
    Serve the Data Observatory HTML dashboard.
    No auth required — public ops dashboard.
    """
    abs_path = os.path.abspath(_HTML_PATH)
    if not os.path.exists(abs_path):
        return HTMLResponse(
            content="<h1>Observatory dashboard not found</h1>",
            status_code=404,
        )
    return FileResponse(abs_path, media_type="text/html")


@router.get(
    "/data-audit",
    include_in_schema=False,
    response_class=HTMLResponse,
    summary="JIP Data Audit dashboard",
)
async def data_audit_dashboard() -> FileResponse:
    """Serve the Data Audit HTML dashboard. No auth required."""
    abs_path = os.path.abspath(_AUDIT_HTML_PATH)
    if not os.path.exists(abs_path):
        return HTMLResponse(
            content="<h1>Data audit dashboard not found</h1>",
            status_code=404,
        )
    return FileResponse(abs_path, media_type="text/html")
