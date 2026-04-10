"""Claude API structured extraction of investment views from document text.

Uses claude-sonnet-4-20250514 with tool_use for structured output.
"""

from __future__ import annotations


import asyncio
from decimal import Decimal
from typing import Any

from app.config import get_settings
from app.logging import get_logger

logger = get_logger(__name__)

QUALITY_SCORE_THRESHOLD = Decimal("0.70")
_CLAUDE_MODEL = "claude-sonnet-4-20250514"
_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 2.0  # seconds
_MAX_TOKENS = 4096

_EXTRACT_TOOL = {
    "name": "extract_market_views",
    "description": (
        "Extract structured investment views from financial text. "
        "Identify specific calls on asset classes, securities, or macro themes."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "views": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "asset_class": {
                            "type": "string",
                            "enum": [
                                "equity", "mf", "bond", "commodity",
                                "currency", "macro", "real_estate", "other",
                            ],
                        },
                        "entity_ref": {
                            "type": ["string", "null"],
                            "description": "Specific security, sector, or entity referenced",
                        },
                        "direction": {
                            "type": "string",
                            "enum": ["bullish", "bearish", "neutral", "cautious"],
                        },
                        "timeframe": {
                            "type": ["string", "null"],
                            "description": "Investment timeframe e.g. '12 months', 'Q3 FY26'",
                        },
                        "conviction": {
                            "type": "string",
                            "enum": ["low", "medium", "high", "very_high"],
                        },
                        "view_text": {
                            "type": "string",
                            "description": "One-line summary of the investment view",
                        },
                        "source_quote": {
                            "type": ["string", "null"],
                            "description": "Verbatim quote from the source supporting this view",
                        },
                        "quality_score": {
                            "type": "number",
                            "minimum": 0.0,
                            "maximum": 1.0,
                            "description": "Confidence in extraction quality (0.0-1.0)",
                        },
                    },
                    "required": [
                        "asset_class", "direction", "conviction",
                        "view_text", "quality_score",
                    ],
                },
            }
        },
        "required": ["views"],
    },
}

# ---------------------------------------------------------------------------
# Goldilocks tool definitions — importable by goldilocks_extractor.py
# These are module-level constants only; extraction logic lives in goldilocks_extractor.py
# ---------------------------------------------------------------------------
GOLDILOCKS_TREND_FRIEND_TOOL_NAME = "extract_trend_friend"
GOLDILOCKS_STOCK_IDEA_TOOL_NAME = "extract_stock_idea"
GOLDILOCKS_SECTOR_VIEWS_TOOL_NAME = "extract_sector_views"

_SYSTEM_PROMPT = (
    "You are a financial analyst assistant specializing in Indian and global markets. "
    "Extract specific, actionable investment views from the provided text. "
    "Focus on views about equities, mutual funds, bonds, commodities, currencies, "
    "and macroeconomic themes. Set quality_score based on how clearly and specifically "
    "the view is expressed (0.0 = vague, 1.0 = very precise with clear rationale)."
)


class ClaudeExtractionError(Exception):
    """Raised when Claude API extraction fails after retries."""

    def __init__(self, message: str, attempts: int) -> None:
        super().__init__(f"Claude extraction failed after {attempts} attempts: {message}")
        self.attempts = attempts


async def extract_views_from_text(
    raw_text: str,
    document_id: str,
) -> list[dict[str, Any]]:
    """Extract structured investment views from raw text using Claude.

    Args:
        raw_text: Text content to analyze.
        document_id: Document identifier for logging.

    Returns:
        List of view dicts with keys: asset_class, entity_ref, direction,
        timeframe, conviction, view_text, source_quote, quality_score.
        Returns empty list if text is empty or Claude finds no views.

    Raises:
        ClaudeExtractionError: After _MAX_RETRIES consecutive failures.
    """
    if not raw_text.strip():
        return []

    settings = get_settings()

    last_exc: Exception | None = None

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            import anthropic  # type: ignore[import]

            client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
            response = await client.messages.create(
                model=_CLAUDE_MODEL,
                max_tokens=_MAX_TOKENS,
                system=_SYSTEM_PROMPT,
                tools=[_EXTRACT_TOOL],
                tool_choice={"type": "auto"},
                messages=[
                    {
                        "role": "user",
                        "content": (
                            f"Extract investment views from the following text:\n\n{raw_text}"
                        ),
                    }
                ],
            )

            # Find the tool_use block
            for block in response.content:
                if getattr(block, "type", None) == "tool_use" and block.name == "extract_market_views":
                    views: list[dict[str, Any]] = block.input.get("views", [])
                    logger.info(
                        "claude_extraction_complete",
                        document_id=document_id,
                        views_found=len(views),
                        attempt=attempt,
                    )
                    return views

            # No tool_use block — Claude returned text only (no views found)
            logger.info(
                "claude_extraction_no_views",
                document_id=document_id,
                attempt=attempt,
            )
            return []

        except Exception as exc:
            last_exc = exc
            logger.warning(
                "claude_extraction_attempt_failed",
                document_id=document_id,
                attempt=attempt,
                error=str(exc),
            )
            if attempt < _MAX_RETRIES:
                await asyncio.sleep(_RETRY_BACKOFF_BASE ** attempt)

    raise ClaudeExtractionError(
        message=str(last_exc),
        attempts=_MAX_RETRIES,
    )
