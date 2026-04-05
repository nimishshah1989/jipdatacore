"""Morningstar resilient async client."""

import asyncio
from typing import Dict, Any, Optional

import httpx
from pydantic import SecretStr

from app.logging import get_logger
from app.config import get_settings

logger = get_logger(__name__)


class MorningstarClient:
    """Async client for Morningstar API with backoff and rate-limiting."""

    def __init__(self, api_key: Optional[SecretStr] = None):
        settings = get_settings()
        self.api_key = api_key.get_secret_value() if api_key else settings.morningstar_access_code.get_secret_value()
        self.base_url = "https://msgrdp.morningstar.com/v1"
        self._client = httpx.AsyncClient(timeout=30.0)

    async def get_fund_details(self, identifier: str, id_type: str = "mstarid") -> Dict[str, Any]:
        """Fetch unified fund details.
        Endpoint: /v1/{id_type}/{identifier}?datapoints=...
        """
        # Note: Spec C10 mandates a unified endpoint. We pull everything needed for the master here.
        datapoints = "Name,CategoryName,BroadCategoryGroup,NetExpenseRatio,ManagerName,TotalNetAssets,InceptionDate,Benchmark,AlphaM36,BetaM36,StandardDeviationM36,SharpeM36"
        url = f"{self.base_url}/{id_type}/{identifier}"
        
        params = {
            "datapoints": datapoints,
            "accesscode": self.api_key
        }

        # Exponential backoff on 429
        max_retries = 3
        for attempt in range(max_retries):
            try:
                resp = await self._client.get(url, params=params)
                
                if resp.status_code == 429:
                    wait_time = (2 ** attempt) * 2  # 2s, 4s, 8s
                    logger.warning(f"Morningstar 429 Rate Limit for {identifier}. Backing off {wait_time}s.")
                    await asyncio.sleep(wait_time)
                    continue
                    
                if resp.status_code == 404:
                    logger.warning(f"Morningstar 404: Fund {identifier} not found.")
                    return {}
                    
                resp.raise_for_status()
                data = resp.json()
                
                # Mstar API returns an array of matched entities usually, or direct object
                if isinstance(data, list) and len(data) > 0:
                    return data[0]
                return data
                
            except httpx.HTTPError as e:
                if attempt == max_retries - 1:
                    logger.error(f"Morningstar API error on final attempt for {identifier}: {e}")
                    raise
                await asyncio.sleep((2 ** attempt))
                
        return {}

    async def close(self):
        await self._client.aclose()
