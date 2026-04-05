"""Export flow pipelines."""

from app.pipelines.flows.fii_dii import InstitutionalFlowsPipeline
from app.pipelines.flows.fo_summary import FoSummaryPipeline

__all__ = [
    "InstitutionalFlowsPipeline",
    "FoSummaryPipeline"
]
