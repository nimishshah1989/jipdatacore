"""Institutional flow pipelines — FII/DII flows, F&O summary, and MF category flows."""

from app.pipelines.flows.mf_category_flows import MfCategoryFlowsPipeline

__all__ = ["MfCategoryFlowsPipeline"]
