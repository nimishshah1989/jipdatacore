"""CLI script to run computation modules for a given business date.

Usage:
    python scripts/run_computations.py --date 2026-04-06 --step all
    python scripts/run_computations.py --date 2026-04-06 --step rs
    python scripts/run_computations.py --dry-run

Arguments:
    --date      YYYY-MM-DD  Business date (default: today in IST)
    --step      One of: technicals|rs|breadth|regime|sectors|fund_derived|all
    --dry-run   Print dependency plan without executing
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup — ensure project root is on sys.path when run as a script
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def _load_dotenv() -> None:
    """Load .env file from project root into os.environ.

    Uses python-dotenv if available, otherwise falls back to a manual parse.
    """
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return

    try:
        from dotenv import load_dotenv  # type: ignore[import]

        load_dotenv(env_path)
        return
    except ImportError:
        pass

    # Manual parse fallback
    with env_path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def _setup_structlog() -> None:
    """Configure structlog for console output."""
    import structlog

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def _get_database_url() -> str:
    """Return async database URL from environment.

    Raises:
        RuntimeError: If DATABASE_URL is not set.
    """
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. "
            "Copy .env.example to .env and fill in credentials."
        )
    # Ensure asyncpg driver
    if url.startswith("postgresql://") and "+asyncpg" not in url:
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


def _today_ist() -> date:
    """Return today's date in IST (UTC+5:30)."""
    from datetime import timezone, timedelta

    ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(tz=ist).date()


# ---------------------------------------------------------------------------
# Dependency plan (for --dry-run)
# ---------------------------------------------------------------------------

STEP_DEPENDENCIES: dict[str, list[str]] = {
    "technicals": [],
    "rs": ["technicals"],
    "breadth": ["technicals", "rs"],
    "regime": ["technicals", "rs", "breadth"],
    "sectors": ["technicals", "rs"],
    "fund_derived": ["technicals", "rs"],
}

VALID_STEPS = list(STEP_DEPENDENCIES.keys()) + ["all"]


def _print_dependency_plan(step: str) -> None:
    """Print the dependency plan for the selected step(s) without executing."""
    print("\nComputation Dependency Plan")
    print("=" * 50)
    if step == "all":
        steps_to_show = list(STEP_DEPENDENCIES.keys())
    else:
        steps_to_show = [step]

    for s in steps_to_show:
        deps = STEP_DEPENDENCIES.get(s, [])
        dep_str = " → ".join(deps + [s]) if deps else s
        print(f"  {dep_str}")

    print("\n[dry-run] No computation was executed.")


# ---------------------------------------------------------------------------
# Per-step async runners
# ---------------------------------------------------------------------------

async def _run_single_step(
    step: str,
    business_date: date,
) -> None:
    """Run a single named computation step.

    Args:
        step: Step name (one of the STEP_DEPENDENCIES keys).
        business_date: Date for which to run computation.
    """
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from app.computation.breadth import compute_breadth
    from app.computation.fund_derived import compute_fund_derived_metrics
    from app.computation.regime import compute_market_regime
    from app.computation.rs import compute_rs_scores
    from app.computation.sectors import compute_sector_metrics
    from app.computation.runner import run_technicals_for_date, _persist_sector_rs, SECTOR_BENCHMARK

    database_url = _get_database_url()
    engine = create_async_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        echo=False,
    )
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    try:
        async with session_factory() as session:
            async with session.begin():
                if step == "technicals":
                    rows = await run_technicals_for_date(session, business_date)
                    print(f"technicals: {rows} rows upserted")
                elif step == "rs":
                    rows = await compute_rs_scores(session, business_date, entity_type="equity")
                    print(f"rs: {rows} rows upserted")
                elif step == "breadth":
                    result = await compute_breadth(session, business_date)
                    print(f"breadth: {result} row(s) upserted")
                elif step == "regime":
                    label = await compute_market_regime(session, business_date)
                    print(f"regime: {label}")
                elif step == "sectors":
                    sector_dict = await compute_sector_metrics(
                        session, business_date, benchmark=SECTOR_BENCHMARK
                    )
                    persisted = await _persist_sector_rs(session, business_date, sector_dict)
                    print(f"sectors: {len(sector_dict)} sectors computed, {persisted} rows upserted")
                elif step == "fund_derived":
                    rows = await compute_fund_derived_metrics(
                        session, business_date, benchmark=SECTOR_BENCHMARK
                    )
                    print(f"fund_derived: {rows} rows upserted")
    finally:
        await engine.dispose()


async def _run_all(business_date: date) -> dict:
    """Run the full computation pipeline and return the QAReport as a dict.

    Args:
        business_date: Date for which to run all computations.

    Returns:
        QAReport serialised as a dict.
    """
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from app.computation.runner import run_full_computation_pipeline

    database_url = _get_database_url()
    engine = create_async_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        echo=False,
    )
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    try:
        async with session_factory() as session:
            async with session.begin():
                report = await run_full_computation_pipeline(session, business_date)
    finally:
        await engine.dispose()

    return report.to_dict()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run JIP Data Engine computation modules",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Business date in YYYY-MM-DD format (default: today in IST)",
    )
    parser.add_argument(
        "--step",
        type=str,
        default="all",
        choices=VALID_STEPS,
        help=(
            "Computation step to run. One of:\n"
            "  technicals, rs, breadth, regime, sectors, fund_derived, all\n"
            "(default: all)"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show dependency plan without executing any computation",
    )
    return parser.parse_args()


def main() -> None:
    _load_dotenv()
    _setup_structlog()

    args = _parse_args()

    # Resolve business_date
    if args.date:
        try:
            business_date = date.fromisoformat(args.date)
        except ValueError:
            print(f"ERROR: Invalid date format '{args.date}'. Expected YYYY-MM-DD.")
            sys.exit(1)
    else:
        business_date = _today_ist()

    print("\nJIP Computation Runner")
    print(f"  business_date : {business_date.isoformat()}")
    print(f"  step          : {args.step}")
    print(f"  dry-run       : {args.dry_run}")
    print()

    if args.dry_run:
        _print_dependency_plan(args.step)
        return

    results_path = PROJECT_ROOT / "docs" / "computation-results.json"

    if args.step == "all":
        report_dict = asyncio.run(_run_all(business_date))

        # Save to docs/computation-results.json
        results_path.parent.mkdir(parents=True, exist_ok=True)
        with results_path.open("w") as fh:
            json.dump(report_dict, fh, indent=2)

        # Print summary
        print("\n--- Computation Pipeline Summary ---")
        print(f"  Phase         : {report_dict['phase']}")
        print(f"  Date          : {report_dict['business_date']}")
        print(f"  Overall pass  : {report_dict['passed']}")
        print(f"  Total rows    : {report_dict['total_rows']}")
        print()
        for step in report_dict["steps"]:
            status = "PASS" if step["passed"] else "FAIL"
            err = f"  error={step['error']}" if step["error"] else ""
            print(f"  [{status}] {step['step']:15s}  rows={step['rows_affected']}{err}")

        print(f"\nResults saved to: {results_path}")

        if not report_dict["passed"]:
            sys.exit(1)
    else:
        asyncio.run(_run_single_step(args.step, business_date))


if __name__ == "__main__":
    main()
