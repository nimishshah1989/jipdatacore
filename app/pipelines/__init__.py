"""Pipeline framework — base classes and utilities for all data ingestion pipelines."""

from app.pipelines.framework import BasePipeline, ExecutionResult, PipelineResult
from app.pipelines.guards import acquire_pipeline_lock, release_pipeline_lock
from app.pipelines.validation import (
    AnomalyRecord,
    apply_data_status,
    check_freshness,
    check_quarantine_threshold,
    record_anomalies,
)

__all__ = [
    # Framework
    "BasePipeline",
    "PipelineResult",
    "ExecutionResult",
    # Guards
    "acquire_pipeline_lock",
    "release_pipeline_lock",
    # Validation
    "AnomalyRecord",
    "record_anomalies",
    "check_quarantine_threshold",
    "apply_data_status",
    "check_freshness",
]
