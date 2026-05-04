"""Atlas-M0 Job 1 -- gap-fill orchestrator.

Runs (or prints commands for) the three sub-tasks of Job 1:

  1. Audit  -> writes reports/atlas_m0_gap_report.json with per-instrument
              gap details from the live DB.
  2. INTL   -> seeds INTL_SPX and INTL_MSCIWORLD via Stooq->yfinance.
  3. Stocks -> invokes the existing BHAV backfill range that covers all
              partial stocks (the per-instrument fetch_to is implied -- we
              run a single range backfill from 2011-04-01 onwards; the
              checkpoint in bhav_backfill.py skips dates already ingested).
  4. MFs    -> invokes scripts/backfill_mf_nav.py which already filters to
              eligible funds with no NAV.

Operator-driven: this script orchestrates and reports; the heavy long-running
ingestions (BHAV backfill takes ~30 min, AMFI iteration takes ~hours) run as
subprocesses. Pass --dry-run to print the commands without executing them.

Usage:
    # Audit + INTL + (instructions for) the long-running steps:
    python scripts/atlas_m0_gap_fill.py --stooq-root /opt/stooq/d_us_txt

    # Audit only (cheap, ~30s):
    python scripts/atlas_m0_gap_fill.py --skip-intl --skip-stocks --skip-mfs

    # Run everything end-to-end (be patient -- BHAV takes 30 min):
    python scripts/atlas_m0_gap_fill.py --stooq-root /opt/stooq/d_us_txt --run-stocks --run-mfs
"""
from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

_REPO_ROOT = Path(__file__).parent.parent
ATLAS_HISTORY_START = date(2011, 4, 1)


def _python() -> str:
    return os.environ.get("PYTHON", sys.executable or "python")


def _run(cmd: list[str], dry_run: bool) -> int:
    printable = " ".join(shlex.quote(c) for c in cmd)
    print(f"\n+ {printable}", flush=True)
    if dry_run:
        return 0
    return subprocess.call(cmd)


def step_audit(target_date: date, dry_run: bool) -> int:
    return _run(
        [
            _python(),
            str(_REPO_ROOT / "scripts" / "atlas_m0_gap_audit.py"),
            "--target-date",
            target_date.isoformat(),
        ],
        dry_run=dry_run,
    )


def step_intl(stooq_root: Optional[Path], target_date: date, dry_run: bool) -> int:
    cmd = [
        _python(),
        str(_REPO_ROOT / "scripts" / "atlas_m0_seed_intl.py"),
        "--start",
        ATLAS_HISTORY_START.isoformat(),
        "--end",
        target_date.isoformat(),
    ]
    if stooq_root is not None:
        cmd.extend(["--stooq-root", str(stooq_root)])
    return _run(cmd, dry_run=dry_run)


def step_stocks(target_date: date, workers: int, dry_run: bool) -> int:
    """Range BHAV backfill -- the existing pipeline iterates per-date and
    writes ALL instruments present on that date in one pass, so a single
    range from 2011-04-01..T-1 covers every partial-stock gap. The
    checkpoint file makes re-runs cheap.
    """
    return _run(
        [
            _python(),
            "-m",
            "app.pipelines.equity.bhav_backfill",
            "--start-date",
            ATLAS_HISTORY_START.isoformat(),
            "--end-date",
            target_date.isoformat(),
            "--workers",
            str(workers),
            "--no-monitor",
        ],
        dry_run=dry_run,
    )


def step_mfs(dry_run: bool) -> int:
    return _run(
        [
            _python(),
            str(_REPO_ROOT / "scripts" / "backfill_mf_nav.py"),
        ],
        dry_run=dry_run,
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--target-date",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=1),
        help="T-1 cutoff (default yesterday)",
    )
    p.add_argument(
        "--stooq-root",
        type=Path,
        default=None,
        help="Path to extracted Stooq d_us_txt dump root (used by INTL step)",
    )
    p.add_argument("--workers", type=int, default=5, help="BHAV workers (default 5)")
    p.add_argument("--dry-run", action="store_true", help="Print commands without executing")

    p.add_argument("--skip-audit", action="store_true")
    p.add_argument("--skip-intl", action="store_true")
    p.add_argument(
        "--run-stocks",
        action="store_true",
        help="Execute the BHAV backfill (otherwise the command is printed for the operator).",
    )
    p.add_argument("--skip-stocks", action="store_true", help="Skip BHAV step entirely.")
    p.add_argument(
        "--run-mfs",
        action="store_true",
        help="Execute the AMFI MF backfill (otherwise print the command).",
    )
    p.add_argument("--skip-mfs", action="store_true", help="Skip MF step entirely.")
    args = p.parse_args()

    rc = 0

    if not args.skip_audit:
        print("\n=== Atlas-M0 Job 1 / Step 1: Audit ===")
        rc |= step_audit(args.target_date, args.dry_run)

    if not args.skip_intl:
        print("\n=== Atlas-M0 Job 1 / Step 2: INTL seed ===")
        rc |= step_intl(args.stooq_root, args.target_date, args.dry_run)

    if not args.skip_stocks:
        print("\n=== Atlas-M0 Job 1 / Step 3: Stocks (BHAV range backfill) ===")
        if args.run_stocks:
            rc |= step_stocks(args.target_date, args.workers, args.dry_run)
        else:
            # Print-only: long-running, operator should launch with monitor up
            print(
                "\nLong-running step. To execute, re-run with --run-stocks "
                "(or run the command below directly):"
            )
            step_stocks(args.target_date, args.workers, dry_run=True)

    if not args.skip_mfs:
        print("\n=== Atlas-M0 Job 1 / Step 4: MFs (AMFI backfill) ===")
        if args.run_mfs:
            rc |= step_mfs(args.dry_run)
        else:
            print("\nLong-running step. To execute, re-run with --run-mfs:")
            step_mfs(dry_run=True)

    print("\n=== Atlas-M0 Job 1 orchestrator complete ===")
    print(
        "Next: review reports/atlas_m0_gap_report.json, then run "
        "scripts/atlas_m0_readiness.py to generate the readiness report."
    )
    return rc


if __name__ == "__main__":
    sys.exit(main())
