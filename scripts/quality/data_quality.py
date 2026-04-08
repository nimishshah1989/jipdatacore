"""Daily data quality monitoring script for JIP Data Core.

Runs 16 quality checks across the database and stores results in
de_data_quality_checks. Scheduled at 06:00 IST via cron.

Exit codes:
  0 — all checks pass (or only warnings)
  1 — one or more checks fail
"""

import argparse
import sys
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Bootstrap — load .env via shared db helper so credentials are available
# ---------------------------------------------------------------------------

# Add project root to path so we can import from scripts/compute/db.py
_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root / "scripts" / "compute"))
from db import get_sync_url  # noqa: E402


# ---------------------------------------------------------------------------
# IST timezone
# ---------------------------------------------------------------------------
IST = timezone(timedelta(hours=5, minutes=30))


# ---------------------------------------------------------------------------
# Check definitions
# Each tuple: (name, category, target_table, sql, warn_threshold, fail_threshold)
#
# Threshold semantics depend on category:
#   freshness   — metric is hours since last data; lower is better
#                 pass  : metric < warn_threshold
#                 warn  : warn_threshold <= metric < fail_threshold
#                 fail  : metric >= fail_threshold
#
#   completeness / reliability — metric is a percentage or count; higher is better
#                 pass  : metric > warn_threshold
#                 warn  : fail_threshold < metric <= warn_threshold
#                 fail  : metric <= fail_threshold
#
#   consistency — metric is an error count; lower is better
#                 pass  : metric == 0
#                 warn  : 0 < metric <= 1
#                 fail  : metric > 1
# ---------------------------------------------------------------------------
CHECKS = [
    # ------------------------------------------------------------------
    # FRESHNESS (hours since last data)
    # ------------------------------------------------------------------
    (
        "equity_ohlcv_fresh",
        "freshness",
        "de_equity_ohlcv",
        "SELECT EXTRACT(EPOCH FROM NOW() - MAX(date)::timestamp) / 3600"
        " FROM de_equity_ohlcv_y2026",
        48,
        72,
    ),
    (
        "mf_nav_fresh",
        "freshness",
        "de_mf_nav_daily",
        "SELECT EXTRACT(EPOCH FROM NOW() - MAX(nav_date)::timestamp) / 3600"
        " FROM de_mf_nav_daily_y2026",
        48,
        72,
    ),
    (
        "rs_scores_fresh",
        "freshness",
        "de_rs_scores",
        "SELECT EXTRACT(EPOCH FROM NOW() - MAX(date)::timestamp) / 3600"
        " FROM de_rs_scores WHERE entity_type='equity'",
        48,
        72,
    ),
    (
        "regime_fresh",
        "freshness",
        "de_market_regime",
        "SELECT EXTRACT(EPOCH FROM NOW() - MAX(date)::timestamp) / 3600"
        " FROM de_market_regime",
        48,
        72,
    ),
    (
        "etf_fresh",
        "freshness",
        "de_etf_ohlcv",
        "SELECT EXTRACT(EPOCH FROM NOW() - MAX(date)::timestamp) / 3600"
        " FROM de_etf_ohlcv",
        72,
        120,
    ),
    (
        "global_fresh",
        "freshness",
        "de_global_prices",
        "SELECT EXTRACT(EPOCH FROM NOW() - MAX(date)::timestamp) / 3600"
        " FROM de_global_prices",
        72,
        120,
    ),
    # ------------------------------------------------------------------
    # COMPLETENESS (percentages)
    # ------------------------------------------------------------------
    (
        "equity_sector_coverage",
        "completeness",
        "de_instrument",
        "SELECT ROUND(COUNT(CASE WHEN sector IS NOT NULL THEN 1 END)::numeric"
        " / NULLIF(COUNT(*), 0) * 100, 2) FROM de_instrument",
        85,
        70,
    ),
    (
        "mf_holdings_mapped",
        "completeness",
        "de_mf_holdings",
        "SELECT ROUND(COUNT(CASE WHEN instrument_id IS NOT NULL THEN 1 END)::numeric"
        " / NULLIF(COUNT(*), 0) * 100, 2) FROM de_mf_holdings",
        95,
        80,
    ),
    (
        "tech_adx_coverage",
        "completeness",
        "de_equity_technical_daily",
        "SELECT ROUND(COUNT(adx_14)::numeric / NULLIF(COUNT(*), 0) * 100, 2)"
        " FROM de_equity_technical_daily"
        " WHERE date = (SELECT MAX(date) FROM de_equity_technical_daily)",
        80,
        50,
    ),
    # ------------------------------------------------------------------
    # CONSISTENCY
    # ------------------------------------------------------------------
    (
        "breadth_arithmetic",
        "consistency",
        "de_breadth_daily",
        "SELECT COUNT(*) FROM de_breadth_daily"
        " WHERE advance + decline + unchanged != total_stocks AND total_stocks > 0",
        0,
        1,
    ),
    # ------------------------------------------------------------------
    # ROW COUNTS (absolute minimums)
    # ------------------------------------------------------------------
    (
        "equity_ohlcv_count",
        "completeness",
        "de_equity_ohlcv",
        "SELECT SUM(n_live_tup) FROM pg_stat_user_tables"
        " WHERE relname LIKE 'de_equity_ohlcv_y%%'",
        3500000,
        3000000,
    ),
    (
        "rs_scores_count",
        "completeness",
        "de_rs_scores",
        "SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = 'de_rs_scores'",
        12000000,
        10000000,
    ),
    (
        "macro_count",
        "completeness",
        "de_macro_values",
        "SELECT COUNT(*) FROM de_macro_values",
        100000,
        50000,
    ),
    (
        "qualitative_count",
        "completeness",
        "de_qual_documents",
        "SELECT COUNT(*) FROM de_qual_documents",
        10,
        1,
    ),
    # ------------------------------------------------------------------
    # PIPELINE HEALTH
    # ------------------------------------------------------------------
    (
        "pipeline_success_7d",
        "reliability",
        "de_pipeline_log",
        "SELECT ROUND("
        "  COUNT(CASE WHEN status = 'success' THEN 1 END)::numeric"
        "  / NULLIF(COUNT(*), 0) * 100, 2"
        ") FROM de_pipeline_log WHERE started_at > NOW() - INTERVAL '7 days'",
        80,
        50,
    ),
]

# ---------------------------------------------------------------------------
# DDL — created inline on every run (idempotent)
# ---------------------------------------------------------------------------
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS de_data_quality_checks (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    check_name     VARCHAR(100) NOT NULL,
    check_category VARCHAR(30)  NOT NULL,
    target_table   VARCHAR(100) NOT NULL,
    status         VARCHAR(20)  NOT NULL,
    metric_value   NUMERIC(18,4),
    threshold_value NUMERIC(18,4),
    detail         TEXT,
    checked_at     TIMESTAMPTZ  DEFAULT NOW(),
    business_date  DATE
);
CREATE INDEX IF NOT EXISTS idx_dqc_checked ON de_data_quality_checks(checked_at);
CREATE INDEX IF NOT EXISTS idx_dqc_status  ON de_data_quality_checks(status);
"""


# ---------------------------------------------------------------------------
# Status determination helpers
# ---------------------------------------------------------------------------

def _freshness_status(metric: Decimal, warn: int, fail: int) -> str:
    """Lower is better. metric is hours since last data."""
    if metric < warn:
        return "pass"
    if metric < fail:
        return "warn"
    return "fail"


def _completeness_status(metric: Decimal, warn: int, fail: int) -> str:
    """Higher is better. metric is a percentage or absolute count."""
    if metric > warn:
        return "pass"
    if metric > fail:
        return "warn"
    return "fail"


def _consistency_status(metric: Decimal, _warn: int, _fail: int) -> str:
    """metric is an error count; zero errors is the only acceptable state."""
    if metric == 0:
        return "pass"
    if metric <= 1:
        return "warn"
    return "fail"


_STATUS_FN = {
    "freshness": _freshness_status,
    "completeness": _completeness_status,
    "reliability": _completeness_status,
    "consistency": _consistency_status,
}


# ---------------------------------------------------------------------------
# Core check runner
# ---------------------------------------------------------------------------

def _ts() -> str:
    """Return a formatted IST timestamp string for log lines."""
    return datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S IST")


def run_check(
    cur: "psycopg2.extensions.cursor",
    name: str,
    category: str,
    table: str,
    sql: str,
    warn_thresh: int,
    fail_thresh: int,
    business_date: "datetime.date | None" = None,
    verbose: bool = False,
) -> str:
    """Execute one quality check and insert its result.

    Returns the status string: 'pass', 'warn', or 'fail'.
    If the check SQL raises an error the status is recorded as 'fail'
    and execution continues (session is not poisoned — we use a savepoint).
    """
    raw_value: Decimal = Decimal("0")
    status: str = "fail"
    detail: str = ""

    try:
        cur.execute("SAVEPOINT dq_check")
        cur.execute(sql)
        row = cur.fetchone()
        cur.execute("RELEASE SAVEPOINT dq_check")

        raw_value = Decimal(str(row[0])) if row and row[0] is not None else Decimal("0")

        status_fn = _STATUS_FN.get(category, _completeness_status)
        status = status_fn(raw_value, warn_thresh, fail_thresh)

        # Human-readable detail
        if category == "freshness":
            detail = (
                f"{float(raw_value):.1f} hours since last record "
                f"(warn={warn_thresh}h, fail={fail_thresh}h)"
            )
        elif category == "consistency":
            detail = (
                f"{int(raw_value)} inconsistent row(s) found "
                f"(warn if >0, fail if >1)"
            )
        else:
            detail = (
                f"metric={float(raw_value):,.2f} "
                f"(warn_below={warn_thresh:,}, fail_below={fail_thresh:,})"
            )

    except Exception as exc:  # noqa: BLE001
        try:
            cur.execute("ROLLBACK TO SAVEPOINT dq_check")
        except Exception:
            pass
        status = "fail"
        raw_value = Decimal("0")
        detail = f"Check SQL error: {exc}"

    # Insert result
    cur.execute(
        """
        INSERT INTO de_data_quality_checks
              (check_name, check_category, target_table,
               status, metric_value, threshold_value, detail,
               checked_at, business_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s)
        """,
        (
            name,
            category,
            table,
            status,
            float(raw_value),
            float(fail_thresh),
            detail,
            business_date,
        ),
    )

    if verbose:
        icon = {"pass": "[PASS]", "warn": "[WARN]", "fail": "[FAIL]"}.get(status, "[????]")
        print(f"{_ts()}  {icon}  {name:<35}  {detail}")

    return status


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------

def ensure_table(conn: "psycopg2.extensions.connection") -> None:
    """Create de_data_quality_checks and its indexes if they do not exist."""
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE_SQL)
    conn.commit()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="JIP Data Core — daily data quality checks"
    )
    parser.add_argument(
        "--check",
        metavar="NAME",
        default=None,
        help="Run a single named check instead of all checks",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Print per-check details to stdout",
    )
    args = parser.parse_args()

    db_url = get_sync_url()
    if not db_url:
        print(f"{_ts()}  [ERROR]  DATABASE_URL not set — aborting", file=sys.stderr)
        sys.exit(1)

    print(f"{_ts()}  Connecting to database…")

    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
    except Exception as exc:
        print(f"{_ts()}  [ERROR]  Cannot connect to database: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        ensure_table(conn)
        print(f"{_ts()}  de_data_quality_checks table ready")

        # Determine today's business date in IST
        business_date = datetime.now(tz=IST).date()

        # Filter checks if --check NAME was supplied
        checks_to_run = CHECKS
        if args.check:
            checks_to_run = [c for c in CHECKS if c[0] == args.check]
            if not checks_to_run:
                known = ", ".join(c[0] for c in CHECKS)
                print(
                    f"{_ts()}  [ERROR]  Unknown check '{args.check}'.\n"
                    f"  Known checks: {known}",
                    file=sys.stderr,
                )
                conn.close()
                sys.exit(1)

        print(f"{_ts()}  Running {len(checks_to_run)} check(s) for {business_date}…")

        counts = {"pass": 0, "warn": 0, "fail": 0}

        with conn.cursor() as cur:
            for name, category, table, sql, warn_thresh, fail_thresh in checks_to_run:
                status = run_check(
                    cur=cur,
                    name=name,
                    category=category,
                    table=table,
                    sql=sql,
                    warn_thresh=warn_thresh,
                    fail_thresh=fail_thresh,
                    business_date=business_date,
                    verbose=args.verbose,
                )
                counts[status] = counts.get(status, 0) + 1

        conn.commit()

        # Print summary
        total = sum(counts.values())
        print(
            f"{_ts()}  Summary: "
            f"{counts['pass']} pass  |  "
            f"{counts['warn']} warn  |  "
            f"{counts['fail']} fail  "
            f"(total {total})"
        )

        if counts["fail"] > 0:
            print(
                f"{_ts()}  [ALERT]  {counts['fail']} check(s) FAILED — "
                "review de_data_quality_checks for details",
                file=sys.stderr,
            )
            sys.exit(1)

        sys.exit(0)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
