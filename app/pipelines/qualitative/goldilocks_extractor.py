"""Goldilocks Research — LLM structured extraction for market views, sectors, stock ideas.

Uses Google Gemini for structured JSON output. Falls back to Claude if available.
All numeric values are stored as Decimal. 3 attempts with exponential backoff.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.logging import get_logger
from app.models.goldilocks import (
    DeGoldilocksMarketView,
    DeGoldilocksSectorView,
    DeGoldilocksStockIdeas,
)
from app.models.qualitative import DeQualExtracts
from app.pipelines.qualitative.claude_extract import (
    ClaudeExtractionError,
    extract_views_from_text,
)

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_GEMINI_MODEL = "gemini-2.0-flash"
_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 2.0  # seconds
_MAX_TEXT_CHARS = 100_000  # safety truncation

# ---------------------------------------------------------------------------
# Tool definitions (also importable by other modules)
# ---------------------------------------------------------------------------
TREND_FRIEND_TOOL: dict[str, Any] = {
    "name": "extract_trend_friend",
    "description": "Extract Trend Friend daily market view data",
    "input_schema": {
        "type": "object",
        "required": ["report_date", "trend_direction"],
        "properties": {
            "report_date": {"type": "string", "description": "YYYY-MM-DD"},
            "nifty_close": {"type": ["number", "null"]},
            "nifty_support_1": {"type": ["number", "null"]},
            "nifty_support_2": {"type": ["number", "null"]},
            "nifty_resistance_1": {"type": ["number", "null"]},
            "nifty_resistance_2": {"type": ["number", "null"]},
            "bank_nifty_close": {"type": ["number", "null"]},
            "bank_nifty_support_1": {"type": ["number", "null"]},
            "bank_nifty_support_2": {"type": ["number", "null"]},
            "bank_nifty_resistance_1": {"type": ["number", "null"]},
            "bank_nifty_resistance_2": {"type": ["number", "null"]},
            "trend_direction": {
                "type": "string",
                "enum": ["upward", "downward", "sideways"],
            },
            "trend_strength": {
                "type": ["integer", "null"],
                "minimum": 1,
                "maximum": 5,
            },
            "global_impact": {
                "type": ["string", "null"],
                "enum": ["positive", "negative", "neutral", None],
            },
            "headline": {
                "type": ["string", "null"],
                "description": "One-line summary of market view",
            },
            "overall_view": {
                "type": ["string", "null"],
                "description": "Full narrative paragraph",
            },
            "sectors": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "sector": {"type": "string"},
                        "trend": {"type": ["string", "null"]},
                        "outlook": {"type": ["string", "null"]},
                        "rank": {"type": ["integer", "null"]},
                    },
                    "required": ["sector"],
                },
            },
        },
    },
}

STOCK_IDEA_TOOL: dict[str, Any] = {
    "name": "extract_stock_idea",
    "description": "Extract stock recommendation details from Stock Bullet or Big Catch report",
    "input_schema": {
        "type": "object",
        "required": ["published_date", "symbol", "company_name", "idea_type", "stop_loss"],
        "properties": {
            "published_date": {"type": "string", "description": "YYYY-MM-DD"},
            "symbol": {
                "type": "string",
                "description": "NSE stock symbol e.g. RELIANCE",
            },
            "company_name": {"type": "string"},
            "idea_type": {
                "type": "string",
                "enum": ["stock_bullet", "big_catch"],
            },
            "entry_price": {
                "type": ["number", "null"],
                "description": "Single entry price if given",
            },
            "entry_zone_low": {"type": ["number", "null"]},
            "entry_zone_high": {"type": ["number", "null"]},
            "target_1": {"type": ["number", "null"]},
            "target_2": {"type": ["number", "null"]},
            "lt_target": {
                "type": ["number", "null"],
                "description": "Long-term target if given",
            },
            "stop_loss": {"type": "number"},
            "timeframe": {"type": ["string", "null"]},
            "rationale": {
                "type": ["string", "null"],
                "description": "Key technical reasoning",
            },
            "technical_params": {
                "type": ["object", "null"],
                "properties": {
                    "ema_200": {"type": ["number", "null"]},
                    "rsi_14": {"type": ["number", "null"]},
                    "support_1": {"type": ["number", "null"]},
                    "support_2": {"type": ["number", "null"]},
                    "resistance_1": {"type": ["number", "null"]},
                    "resistance_2": {"type": ["number", "null"]},
                },
            },
        },
    },
}

SECTOR_VIEWS_TOOL: dict[str, Any] = {
    "name": "extract_sector_views",
    "description": (
        "Extract sector analysis from Sector Trends or Fortnightly report"
    ),
    "input_schema": {
        "type": "object",
        "required": ["report_date", "sectors"],
        "properties": {
            "report_date": {"type": "string"},
            "sectors": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["sector"],
                    "properties": {
                        "sector": {"type": "string"},
                        "trend": {"type": ["string", "null"]},
                        "outlook": {"type": ["string", "null"]},
                        "rank": {"type": ["integer", "null"]},
                        "top_picks": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "symbol": {"type": "string"},
                                    "resistance_levels": {
                                        "type": "array",
                                        "items": {"type": "number"},
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
    },
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
class _DecimalEncoder(json.JSONEncoder):
    """Serialize Decimal values in JSONB-bound dicts to float (storage boundary only)."""

    def default(self, o: Any) -> Any:
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)


def _sanitize_jsonb(data: Any) -> Any:
    """Round-trip through JSON to strip any Decimal values before JSONB insert."""
    if data is None:
        return None
    return json.loads(json.dumps(data, cls=_DecimalEncoder))


def _to_decimal(v: Any) -> Optional[Decimal]:
    """Convert a numeric value from Claude to Decimal. Returns None if v is None."""
    if v is None:
        return None
    return Decimal(str(v))


def _parse_date(date_str: Optional[str]) -> Optional[date]:
    """Parse YYYY-MM-DD string from Claude into a date object."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        logger.warning("goldilocks_extractor.invalid_date", date_str=date_str)
        return None


def _compute_quality_score(data: dict[str, Any], expected_fields: list[str]) -> Decimal:
    """Fraction of expected fields that are non-null."""
    if not expected_fields:
        return Decimal("0.00")
    non_null = sum(1 for f in expected_fields if data.get(f) is not None)
    score = Decimal(str(round(non_null / len(expected_fields), 2)))
    return score


async def _call_llm(
    tool: dict[str, Any],
    system_prompt: str,
    user_text: str,
    document_id: str,
    max_tokens: int = 1000,
) -> Optional[dict[str, Any]]:
    """Call Gemini for structured JSON extraction. Returns parsed dict or None.

    Uses Gemini's JSON mode with the tool's input_schema as guidance.
    Falls back to Claude if GOOGLE_API_KEY is not set.
    """
    import os

    google_key = os.environ.get("GOOGLE_API_KEY", "")
    truncated = user_text[:_MAX_TEXT_CHARS]
    last_exc: Exception | None = None

    # Build the JSON schema description from the tool
    schema = tool.get("input_schema", {})
    properties = schema.get("properties", {})
    required = schema.get("required", [])
    schema_desc = json.dumps(properties, indent=2)

    prompt = (
        f"{system_prompt}\n\n"
        f"Return a JSON object with these fields:\n{schema_desc}\n\n"
        f"Required fields: {required}\n\n"
        f"Return ONLY valid JSON, no markdown, no explanation.\n\n"
        f"Text to extract from:\n\n{truncated}"
    )

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            if google_key:
                result = await _call_gemini(google_key, prompt, document_id)
            else:
                result = await _call_claude_fallback(truncated, tool, system_prompt, document_id)

            if result is not None:
                logger.info(
                    "goldilocks_extractor.llm_success",
                    document_id=document_id,
                    tool=tool["name"],
                    attempt=attempt,
                    provider="gemini" if google_key else "claude",
                )
                return result

            return None

        except Exception as exc:
            last_exc = exc
            logger.warning(
                "goldilocks_extractor.attempt_failed",
                document_id=document_id,
                tool=tool["name"],
                attempt=attempt,
                error=str(exc),
            )
            if attempt < _MAX_RETRIES:
                await asyncio.sleep(_RETRY_BACKOFF_BASE ** attempt)

    raise ClaudeExtractionError(message=str(last_exc), attempts=_MAX_RETRIES)


async def _call_gemini(
    api_key: str, prompt: str, document_id: str,
) -> Optional[dict[str, Any]]:
    """Call Google Gemini API for JSON extraction."""
    import httpx

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{_GEMINI_MODEL}:generateContent"

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "temperature": 0.1,
        },
    }

    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(
            url,
            params={"key": api_key},
            json=payload,
        )
        resp.raise_for_status()
        result = resp.json()

    # Parse Gemini response
    candidates = result.get("candidates", [])
    if not candidates:
        logger.warning("gemini_no_candidates", document_id=document_id)
        return None

    text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
    if not text:
        return None

    # Clean potential markdown wrapping
    text = text.strip()
    if text.startswith("```json"):
        text = text[7:]
    if text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        logger.warning("gemini_json_parse_error", document_id=document_id, error=str(exc)[:100])
        return None


async def _call_claude_fallback(
    truncated: str, tool: dict, system_prompt: str, document_id: str,
) -> Optional[dict[str, Any]]:
    """Fallback to Claude if no Gemini key."""
    settings = get_settings()
    import anthropic  # type: ignore[import]

    client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
    response = await client.messages.create(
        model="claude-3-5-haiku-20241022",
        max_tokens=1000,
        system=system_prompt,
        tools=[tool],
        tool_choice={"type": "any"},
        messages=[{"role": "user", "content": f"Extract structured data:\n\n{truncated}"}],
    )
    for block in response.content:
        if getattr(block, "type", None) == "tool_use" and block.name == tool["name"]:
            return block.input
    return None


# ---------------------------------------------------------------------------
# Public extraction functions
# ---------------------------------------------------------------------------
async def extract_trend_friend(
    document_id: uuid.UUID,
    raw_text: str,
    session: AsyncSession,
) -> bool:
    """Extract Trend Friend daily market view and upsert into de_goldilocks_market_view.

    Also upserts each sector into de_goldilocks_sector_view.
    Computes quality_score as fraction of expected fields populated.

    Args:
        document_id: UUID of the source document (for logging).
        raw_text: Full text of the Trend Friend report.
        session: Async SQLAlchemy session (caller must manage transaction).

    Returns:
        True on success, False if Claude returns no data or validation fails.
    """
    doc_id_str = str(document_id)

    system_prompt = (
        "You are a financial data extractor. Extract structured market data from this "
        "Goldilocks Research Trend Friend report. Be precise with numeric values — copy "
        "exact numbers from the text. If a value is not mentioned, return null. "
        "Do not hallucinate levels."
    )

    data = await _call_llm(
        tool=TREND_FRIEND_TOOL,
        system_prompt=system_prompt,
        user_text=raw_text,
        document_id=doc_id_str,
        max_tokens=1000,
    )

    if data is None:
        return False

    report_date = _parse_date(data.get("report_date"))
    if report_date is None:
        logger.warning(
            "goldilocks_extractor.missing_report_date",
            document_id=doc_id_str,
        )
        return False

    # Numeric price fields
    numeric_fields = [
        "nifty_close", "nifty_support_1", "nifty_support_2",
        "nifty_resistance_1", "nifty_resistance_2",
        "bank_nifty_close", "bank_nifty_support_1", "bank_nifty_support_2",
        "bank_nifty_resistance_1", "bank_nifty_resistance_2",
    ]

    # Quality score: fraction of expected fields that are non-null
    quality_expected = [
        "report_date", "nifty_close", "nifty_support_1", "nifty_resistance_1",
        "bank_nifty_close", "trend_direction", "trend_strength", "global_impact",
        "headline", "overall_view",
    ]
    quality_score = _compute_quality_score(data, quality_expected)

    logger.info(
        "goldilocks_extractor.trend_friend_quality",
        document_id=doc_id_str,
        report_date=str(report_date),
        quality_score=str(quality_score),
    )

    # Upsert market view row
    market_row: dict[str, Any] = {
        "report_date": report_date,
        "trend_direction": data.get("trend_direction"),
        "trend_strength": data.get("trend_strength"),
        "global_impact": data.get("global_impact"),
        "headline": data.get("headline"),
        "overall_view": data.get("overall_view"),
        "updated_at": sa.func.now(),
    }
    for field in numeric_fields:
        market_row[field] = _to_decimal(data.get(field))

    stmt = pg_insert(DeGoldilocksMarketView).values(**market_row)
    update_cols = {
        col: getattr(stmt.excluded, col)
        for col in market_row
        if col != "report_date"
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=["report_date"],
        set_=update_cols,
    )
    await session.execute(stmt)

    # Upsert sector rows from the sectors list
    sectors: list[dict[str, Any]] = data.get("sectors") or []
    for sector_data in sectors:
        sector_name = sector_data.get("sector")
        if not sector_name:
            continue

        sector_row: dict[str, Any] = {
            "report_date": report_date,
            "sector": sector_name,
            "trend": sector_data.get("trend"),
            "outlook": sector_data.get("outlook"),
            "rank": sector_data.get("rank"),
            "top_picks": None,  # Trend Friend sectors don't have top_picks
            "updated_at": sa.func.now(),
        }
        sec_stmt = pg_insert(DeGoldilocksSectorView).values(**sector_row)
        sec_update_cols = {
            col: getattr(sec_stmt.excluded, col)
            for col in sector_row
            if col not in ("report_date", "sector")
        }
        sec_stmt = sec_stmt.on_conflict_do_update(
            index_elements=["report_date", "sector"],
            set_=sec_update_cols,
        )
        await session.execute(sec_stmt)

    logger.info(
        "goldilocks_extractor.trend_friend_done",
        document_id=doc_id_str,
        report_date=str(report_date),
        sectors_count=len(sectors),
    )
    return True


async def extract_stock_idea(
    document_id: uuid.UUID,
    raw_text: str,
    session: AsyncSession,
) -> bool:
    """Extract stock recommendation and insert into de_goldilocks_stock_ideas.

    Idempotent: skips insert if a row for this document_id already exists.

    Args:
        document_id: UUID of the source document.
        raw_text: Full text of the Stock Bullet or Big Catch report.
        session: Async SQLAlchemy session (caller must manage transaction).

    Returns:
        True on success or if already exists, False if Claude returns no data.
    """
    doc_id_str = str(document_id)

    # Idempotency check — skip if already extracted for this document
    existing = await session.execute(
        sa.select(DeGoldilocksStockIdeas.id).where(
            DeGoldilocksStockIdeas.document_id == document_id
        )
    )
    if existing.scalar_one_or_none() is not None:
        logger.info(
            "goldilocks_extractor.stock_idea_already_exists",
            document_id=doc_id_str,
        )
        return True

    system_prompt = (
        "You are a financial data extractor. Extract structured stock recommendation "
        "data from this Goldilocks Research report. Copy exact price levels from the text. "
        "If a value is not mentioned, return null. Do not hallucinate price levels."
    )

    data = await _call_llm(
        tool=STOCK_IDEA_TOOL,
        system_prompt=system_prompt,
        user_text=raw_text,
        document_id=doc_id_str,
        max_tokens=800,
    )

    if data is None:
        return False

    price_fields = [
        "entry_price", "entry_zone_low", "entry_zone_high",
        "target_1", "target_2", "lt_target", "stop_loss",
    ]

    # Sanitize technical_params JSONB (may contain numeric values from Claude)
    raw_tech_params = data.get("technical_params")
    technical_params = _sanitize_jsonb(raw_tech_params)

    published_date = _parse_date(data.get("published_date"))

    idea_row: dict[str, Any] = {
        "id": uuid.uuid4(),
        "document_id": document_id,
        "published_date": published_date,
        "symbol": data.get("symbol"),
        "company_name": data.get("company_name"),
        "idea_type": data.get("idea_type"),
        "timeframe": data.get("timeframe"),
        "rationale": data.get("rationale"),
        "technical_params": technical_params,
        "status": "active",
    }
    for field in price_fields:
        idea_row[field] = _to_decimal(data.get(field))

    await session.execute(
        sa.insert(DeGoldilocksStockIdeas).values(**idea_row)
    )

    logger.info(
        "goldilocks_extractor.stock_idea_done",
        document_id=doc_id_str,
        symbol=data.get("symbol"),
        idea_type=data.get("idea_type"),
    )
    return True


async def extract_sector_views(
    document_id: uuid.UUID,
    raw_text: str,
    session: AsyncSession,
) -> bool:
    """Extract sector views from Sector Trends or Fortnightly report.

    Upserts each sector into de_goldilocks_sector_view.

    Args:
        document_id: UUID of the source document.
        raw_text: Full text of the Sector Trends or Fortnightly report.
        session: Async SQLAlchemy session (caller must manage transaction).

    Returns:
        True on success, False if Claude returns no data.
    """
    doc_id_str = str(document_id)

    system_prompt = (
        "You are a financial data extractor. Extract structured sector analysis "
        "from this Goldilocks Research report. For each sector mentioned, extract "
        "the trend, outlook, rank, and top stock picks. Be precise."
    )

    data = await _call_llm(
        tool=SECTOR_VIEWS_TOOL,
        system_prompt=system_prompt,
        user_text=raw_text,
        document_id=doc_id_str,
        max_tokens=1000,
    )

    if data is None:
        return False

    report_date = _parse_date(data.get("report_date"))
    if report_date is None:
        logger.warning(
            "goldilocks_extractor.sector_views_missing_date",
            document_id=doc_id_str,
        )
        return False

    sectors: list[dict[str, Any]] = data.get("sectors") or []

    for sector_data in sectors:
        sector_name = sector_data.get("sector")
        if not sector_name:
            continue

        # Sanitize top_picks for JSONB (may contain numeric resistance_levels)
        raw_top_picks = sector_data.get("top_picks")
        top_picks = _sanitize_jsonb(raw_top_picks) if raw_top_picks else None

        sector_row: dict[str, Any] = {
            "report_date": report_date,
            "sector": sector_name,
            "trend": sector_data.get("trend"),
            "outlook": sector_data.get("outlook"),
            "rank": sector_data.get("rank"),
            "top_picks": top_picks,
            "updated_at": sa.func.now(),
        }
        sec_stmt = pg_insert(DeGoldilocksSectorView).values(**sector_row)
        sec_update_cols = {
            col: getattr(sec_stmt.excluded, col)
            for col in sector_row
            if col not in ("report_date", "sector")
        }
        sec_stmt = sec_stmt.on_conflict_do_update(
            index_elements=["report_date", "sector"],
            set_=sec_update_cols,
        )
        await session.execute(sec_stmt)

    logger.info(
        "goldilocks_extractor.sector_views_done",
        document_id=doc_id_str,
        report_date=str(report_date),
        sectors_count=len(sectors),
    )
    return True


async def extract_general_views(
    document_id: uuid.UUID,
    raw_text: str,
    session: AsyncSession,
) -> bool:
    """Extract general investment views using the existing claude_extract pipeline.

    Calls extract_views_from_text() and inserts results into de_qual_extracts.
    Runs for ALL document types as the base-layer extraction.

    Args:
        document_id: UUID of the source document.
        raw_text: Full text of the document.
        session: Async SQLAlchemy session (caller must manage transaction).

    Returns:
        True on success (even if zero views found), False on extraction error.
    """
    doc_id_str = str(document_id)
    truncated = raw_text[:_MAX_TEXT_CHARS]

    try:
        views = await extract_views_from_text(
            raw_text=truncated,
            document_id=doc_id_str,
        )
    except ClaudeExtractionError as exc:
        logger.error(
            "goldilocks_extractor.general_views_failed",
            document_id=doc_id_str,
            error=str(exc),
        )
        return False

    if not views:
        logger.info(
            "goldilocks_extractor.general_views_empty",
            document_id=doc_id_str,
        )
        return True

    rows = []
    for view in views:
        quality_raw = view.get("quality_score")
        quality_score = (
            Decimal(str(quality_raw)) if quality_raw is not None else None
        )
        rows.append(
            {
                "id": uuid.uuid4(),
                "document_id": document_id,
                "asset_class": view.get("asset_class"),
                "entity_ref": view.get("entity_ref"),
                "direction": view.get("direction"),
                "timeframe": view.get("timeframe"),
                "conviction": view.get("conviction"),
                "view_text": view.get("view_text"),
                "source_quote": view.get("source_quote"),
                "quality_score": quality_score,
            }
        )

    if rows:
        await session.execute(sa.insert(DeQualExtracts).values(rows))

    logger.info(
        "goldilocks_extractor.general_views_done",
        document_id=doc_id_str,
        views_inserted=len(rows),
    )
    return True
