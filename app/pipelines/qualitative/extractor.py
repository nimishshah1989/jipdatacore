"""Claude Extractor Pipeline — structured extraction of market views from raw text."""

import json
from typing import Dict, Any, List

import anthropic
from pydantic import BaseModel, SecretStr
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.logging import get_logger
from app.config import get_settings
from app.models.qualitative import DeQualExtracts

logger = get_logger(__name__)


class ClaudeExtractor:
    """Uses Claude 3.5 Sonnet to extract deterministic market insights."""

    def __init__(self, api_key: SecretStr = None):
        settings = get_settings()
        key = api_key.get_secret_value() if api_key else settings.anthropic_api_key.get_secret_value()
        # Initialize async anthropic client
        self.client = anthropic.AsyncAnthropic(api_key=key)

    async def extract_views(self, raw_text: str, document_id: str) -> List[Dict[str, Any]]:
        """Call Claude to analyze the text and return structured JSON."""
        
        system_prompt = """
        You are an elite financial analyst. Read the provided text and extract specific market views, targets, or economic forecasts.
        Return ONLY valid JSON matching this schema:
        {
          "views": [
             {
               "asset_class": "equity|debt|currency|commodity|macro",
               "entity_ref": "Specific name of stock, bond, index, or metric (e.g. NIFTY, RBI Repo Rate, Reliance Industries)",
               "direction": "bullish|bearish|neutral",
               "timeframe": "short_term|medium_term|long_term",
               "conviction": "high|medium|low",
               "view_text": "A concise 1-2 sentence summary of the view",
               "source_quote": "Exact 1-2 sentences quoted from the text proving this view",
               "quality_score": 0.0 to 1.0 (Your confidence that this is a definitive, actionable view rather than vague commentary)
             }
          ]
        }
        If no concrete views are present, return: {"views": []}
        """
        
        try:
            response = await self.client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=1500,
                temperature=0.0,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": f"Extract market views from this text:\n\n{raw_text}"}
                ]
            )
            
            # Parse JSON block from Claude
            content = response.content[0].text
            start_idx = content.find('{')
            end_idx = content.rfind('}') + 1
            if start_idx >= 0 and end_idx > 0:
                json_str = content[start_idx:end_idx]
                data = json.loads(json_str)
                views = data.get("views", [])
                
                # Filter down by quality score
                high_quality_views = [v for v in views if v.get("quality_score", 0) >= 0.70]
                
                # Inject document reference
                for v in high_quality_views:
                    v["document_id"] = document_id
                    
                return high_quality_views
            return []
            
        except Exception as e:
            logger.error(f"Claude extraction failed for doc {document_id}: {e}")
            return []

    async def persist_extracts(self, views: List[Dict[str, Any]], session: AsyncSession) -> int:
        if not views:
            return 0
            
        stmt = insert(DeQualExtracts).values(views)
        await session.execute(stmt)
        await session.commit()
        return len(views)
