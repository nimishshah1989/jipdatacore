"""Shared types for computation QA pipeline.

All phases (pre_qa, compute, post_qa, spot_check, mstar_xval) produce
QAReport objects with StepResult items. The tracker API reads these
from de_pipeline_log.track_status (JSONB).
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

IST = timezone(timedelta(hours=5, minutes=30))


def _now_ist() -> datetime:
    return datetime.now(tz=IST)


@dataclass
class StepResult:
    """Result of a single QA check or computation step.

    Supports two usage patterns:
    - Pre-QA style: StepResult(step_name="check_x", status="running") then mark_complete()
    - Post-QA style: StepResult(name="check_x", status="passed", message="OK")
    """

    step_name: str = ""  # e.g. "technicals", "check_ohlcv_coverage"
    status: str = "running"  # passed | failed | warning | skipped | running
    rows_affected: int = 0
    details: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=_now_ist)
    completed_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    # Aliases for convenience
    name: str = ""
    message: str = ""

    def __post_init__(self) -> None:
        # Allow using 'name' as alias for 'step_name'
        if self.name and not self.step_name:
            self.step_name = self.name
        elif self.step_name and not self.name:
            self.name = self.step_name
        # Store message in errors list if provided
        if self.message and self.message not in self.errors:
            self.errors.append(self.message)

    def mark_complete(self, status: str = "passed") -> None:
        self.status = status
        self.completed_at = _now_ist()
        delta = self.completed_at - self.started_at
        self.duration_ms = delta.total_seconds() * 1000

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.duration_ms is not None:
            return round(self.duration_ms / 1000, 2)
        if self.completed_at is None:
            return None
        return round((self.completed_at - self.started_at).total_seconds(), 2)

    def to_dict(self) -> dict[str, Any]:
        """Serialise step to a dict."""
        return {
            "step_name": self.step_name,
            "name": self.step_name,
            "status": self.status,
            "message": self.message or (self.errors[0] if self.errors else ""),
            "rows_affected": self.rows_affected,
            "details": self.details,
            "errors": self.errors,
            "duration_ms": self.duration_ms,
        }


@dataclass
class QAReport:
    """Report for an entire QA phase (pre_qa, compute, post_qa, etc.)."""

    phase: str = ""  # pre_qa | compute | post_qa | spot_check | mstar_xval
    business_date: Any = None  # date
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    steps: list[StepResult] = field(default_factory=list)
    started_at: datetime = field(default_factory=_now_ist)
    completed_at: Optional[datetime] = None
    duration_ms: Optional[float] = None

    @property
    def overall_status(self) -> str:
        """Compute overall status from step statuses."""
        if any(s.status == "failed" for s in self.steps):
            return "failed"
        if any(s.status == "warning" for s in self.steps):
            return "warning"
        if self.steps and all(s.status == "skipped" for s in self.steps):
            return "skipped"
        return "passed"

    @classmethod
    def from_steps(cls, phase: str, business_date: Any, steps: list[StepResult]) -> "QAReport":
        """Create a report from a list of steps."""
        report = cls(phase=phase, business_date=business_date)
        report.steps = list(steps)
        return report

    def add_step(self, step: StepResult) -> None:
        """Append a step result."""
        self.steps.append(step)

    def mark_complete(self) -> None:
        """Seal the report with final timing."""
        self.completed_at = _now_ist()
        delta = self.completed_at - self.started_at
        self.duration_ms = delta.total_seconds() * 1000

    @property
    def passed(self) -> int:
        return sum(1 for s in self.steps if s.status == "passed")

    @property
    def passed_count(self) -> int:
        return self.passed

    @property
    def warnings(self) -> int:
        return sum(1 for s in self.steps if s.status == "warning")

    @property
    def warning_count(self) -> int:
        return self.warnings

    @property
    def failed(self) -> int:
        return sum(1 for s in self.steps if s.status == "failed")

    @property
    def failed_count(self) -> int:
        return self.failed

    @property
    def skipped(self) -> int:
        return sum(1 for s in self.steps if s.status == "skipped")

    def summary(self) -> dict[str, Any]:
        """Alias for to_dict()."""
        return self.to_dict()

    def to_dict(self) -> dict[str, Any]:
        """Serialise to dict for JSON storage."""
        return {
            "run_id": self.run_id,
            "phase": self.phase,
            "business_date": str(self.business_date),
            "overall_status": self.overall_status,
            "passed": self.passed_count,
            "warnings": self.warning_count,
            "failed": self.failed_count,
            "duration_ms": self.duration_ms,
            "steps": [
                {
                    "step_name": s.step_name,
                    "name": s.step_name,
                    "status": s.status,
                    "message": s.message or (s.errors[0] if s.errors else ""),
                    "rows_affected": s.rows_affected,
                    "details": s.details,
                    "errors": s.errors,
                    "duration_ms": s.duration_ms,
                }
                for s in self.steps
            ],
        }

    def to_json(self) -> str:
        """Serialise to JSON string for storage in de_pipeline_log.track_status."""

        def _serialise(obj: Any) -> Any:
            if isinstance(obj, datetime):
                return obj.isoformat()
            if isinstance(obj, date):
                return obj.isoformat()
            if isinstance(obj, uuid.UUID):
                return str(obj)
            return obj

        return json.dumps(self.to_dict(), default=_serialise, indent=2)

    @staticmethod
    def from_json(raw: str) -> "QAReport":
        """Deserialise from JSON string."""
        data = json.loads(raw)
        steps = [
            StepResult(
                step_name=s.get("step_name", s.get("name", "")),
                status=s["status"],
                started_at=datetime.fromisoformat(s["started_at"]) if s.get("started_at") else _now_ist(),
                completed_at=datetime.fromisoformat(s["completed_at"]) if s.get("completed_at") else None,
                rows_affected=s.get("rows_affected", 0),
                details=s.get("details", {}),
                errors=s.get("errors", []),
                duration_ms=s.get("duration_ms"),
            )
            for s in data.get("steps", [])
        ]
        return QAReport(
            run_id=data.get("run_id", str(uuid.uuid4())),
            business_date=date.fromisoformat(data["business_date"]) if data.get("business_date") else None,
            phase=data.get("phase", ""),
            steps=steps,
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else _now_ist(),
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            duration_ms=data.get("duration_ms"),
        )
