"""Asset check factory — per-table data quality.

Two checks per table (both auto-applied):
  1. freshness_check  — MAX(date_col) is within max_lag_hours
  2. rowcount_delta   — today's rowcount within ±5% of prior period

The checks read from the existing data_engine RDS via psycopg2.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
)

from dagster_app.registry import TABLE_SPECS, TableSpec
from dagster_app.resources import RdsConnection


# ---------------------------------------------------------------------------
# Freshness check — distinct from FreshnessPolicy (which is declarative).
# This check executes a SQL query and compares MAX(date_col) to wall clock.
# ---------------------------------------------------------------------------


def _make_freshness_check(spec: TableSpec):
    @asset_check(
        name=f"{spec.table}__freshness",
        asset=spec.table,
        description=f"Asserts MAX({spec.date_col}) is within {spec.max_lag_hours}h.",
    )
    def _check(context: AssetCheckExecutionContext, rds: RdsConnection):
        with rds.cursor() as cur:
            cur.execute(f"SELECT MAX({spec.date_col}) FROM {spec.table}")  # noqa: S608
            row = cur.fetchone()
            max_dt = row[0] if row else None

        if max_dt is None:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description=f"{spec.table} is empty",
                metadata={"max_date": None, "lag_hours": -1},
            )

        # Normalise to UTC datetime
        if isinstance(max_dt, datetime):
            max_utc = max_dt if max_dt.tzinfo else max_dt.replace(tzinfo=timezone.utc)
        else:
            # date type → end-of-day UTC
            max_utc = datetime.combine(max_dt, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=23, minutes=59)

        lag = datetime.now(timezone.utc) - max_utc
        lag_hours = lag.total_seconds() / 3600

        if lag_hours <= spec.fresh_lag_hours:
            sev, passed, status = AssetCheckSeverity.WARN, True, "GREEN"
        elif lag_hours <= spec.max_lag_hours:
            sev, passed, status = AssetCheckSeverity.WARN, True, "AMBER"
        else:
            sev, passed, status = AssetCheckSeverity.ERROR, False, "RED"

        return AssetCheckResult(
            passed=passed,
            severity=sev,
            description=f"{status}: lag {lag_hours:.1f}h vs SLA {spec.max_lag_hours}h",
            metadata={
                "max_date": str(max_dt),
                "lag_hours": round(lag_hours, 2),
                "fresh_lag_hours": spec.fresh_lag_hours,
                "max_lag_hours": spec.max_lag_hours,
                "status": status,
                "criticality": spec.criticality,
            },
        )

    return _check


# ---------------------------------------------------------------------------
# Row-count ±5% delta check.
# Compares today's row count for the latest date_col vs prior date.
# ---------------------------------------------------------------------------


def _make_rowcount_check(spec: TableSpec):
    if not spec.rowcount_check:
        return None

    @asset_check(
        name=f"{spec.table}__rowcount_delta",
        asset=spec.table,
        description=f"Asserts row count for latest {spec.date_col} is within ±5% of prior period.",
    )
    def _check(context: AssetCheckExecutionContext, rds: RdsConnection):
        with rds.cursor() as cur:
            cur.execute(f"""
                SELECT {spec.date_col} AS d, COUNT(*) AS c
                FROM {spec.table}
                GROUP BY {spec.date_col}
                ORDER BY {spec.date_col} DESC
                LIMIT 2
            """)  # noqa: S608
            rows = cur.fetchall()

        if len(rows) < 2:
            return AssetCheckResult(
                passed=True,
                severity=AssetCheckSeverity.WARN,
                description="Less than 2 periods — cannot compare",
                metadata={"rows_available": len(rows)},
            )

        latest_date, latest_count = rows[0]
        prior_date, prior_count = rows[1]

        if prior_count == 0:
            return AssetCheckResult(
                passed=True,
                severity=AssetCheckSeverity.WARN,
                description="Prior period had 0 rows — skipping ratio check",
                metadata={"latest_count": latest_count, "prior_count": 0},
            )

        delta_pct = abs(latest_count - prior_count) / prior_count * 100
        passed = delta_pct <= 5.0
        return AssetCheckResult(
            passed=passed,
            severity=AssetCheckSeverity.ERROR if not passed else AssetCheckSeverity.WARN,
            description=f"Δ {delta_pct:.2f}% (latest {latest_count} vs prior {prior_count})",
            metadata={
                "latest_date": str(latest_date),
                "latest_count": latest_count,
                "prior_date": str(prior_date),
                "prior_count": prior_count,
                "delta_pct": round(delta_pct, 2),
                "threshold_pct": 5.0,
            },
        )

    return _check


def build_all_checks():
    checks = []
    for spec in TABLE_SPECS:
        checks.append(_make_freshness_check(spec))
        rc = _make_rowcount_check(spec)
        if rc is not None:
            checks.append(rc)
    return checks
