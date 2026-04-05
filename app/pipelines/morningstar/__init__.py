"""Export Morningstar integration."""

from app.pipelines.morningstar.client import MorningstarClient
from app.pipelines.morningstar.fund_master import MorningstarFundMasterPipeline

__all__ = [
    "MorningstarClient",
    "MorningstarFundMasterPipeline"
]
