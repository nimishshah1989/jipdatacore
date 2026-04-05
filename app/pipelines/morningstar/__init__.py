"""Morningstar data ingestion pipelines.

Provides:
- MorningstarClient: async HTTP client with retry + rate limiting
- FundMasterPipeline: weekly fund detail refresh (category, expense ratio, etc.)
- HoldingsPipeline: monthly portfolio holdings fetch
- RiskPipeline: risk statistics (Sharpe, alpha, beta, drawdown)
- resolve_isin: ISIN → instrument_id resolution helper
"""

from app.pipelines.morningstar.client import MorningstarClient, RateLimitExceeded
from app.pipelines.morningstar.fund_master import FundMasterPipeline
from app.pipelines.morningstar.holdings import HoldingsPipeline
from app.pipelines.morningstar.risk import RiskPipeline
from app.pipelines.morningstar.isin_resolver import resolve_isin, resolve_isin_batch

__all__ = [
    "MorningstarClient",
    "RateLimitExceeded",
    "FundMasterPipeline",
    "HoldingsPipeline",
    "RiskPipeline",
    "resolve_isin",
    "resolve_isin_batch",
]
