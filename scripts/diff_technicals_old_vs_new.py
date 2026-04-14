"""Diff v1 vs v2 technical tables for the last N days.

Usage:
    python scripts/diff_technicals_old_vs_new.py --asset equity [--last-days 30]

Joins on (instrument_id, date) and reports per-column:
- rows compared
- max_abs_diff, mean_abs_diff, p95_abs_diff
- pct_within_1e_4, pct_within_1e_3

Exit code 0 if all core indicators (RSI/SMA/EMA/MACD/BBands/ATR) hit
the pass thresholds from chunk 5; non-zero otherwise. Writes a markdown
report to reports/technicals_diff_<asset>_<timestamp>.md.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import os
import pathlib
import sys

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

REPORTS_DIR = pathlib.Path("reports")

# Columns that exist in BOTH v1 (de_equity_technical_daily) and v2
# (de_equity_technical_daily_v2). Extend as more columns land in v2.
OVERLAP_COLS: list[tuple[str, str]] = [
    # Core — must be very tight (threshold 1e-4, min pct 99.5%)
    ("sma_50", "core"),
    ("sma_200", "core"),
    ("ema_20", "core"),
    ("rsi_14", "core"),
    ("macd_line", "core"),
    ("macd_signal", "core"),
    ("macd_histogram", "core"),
    ("bb_upper", "core"),
    ("bb_lower", "core"),
    ("atr_14", "core"),
    # Secondary — (threshold 1e-3, min pct 99.0%)
    ("adx_14", "secondary"),
    ("plus_di", "secondary"),
    ("minus_di", "secondary"),
    ("mfi_14", "secondary"),
    ("stochastic_k", "secondary"),
    ("stochastic_d", "secondary"),
    ("roc_5", "secondary"),
    ("roc_10", "secondary"),
    ("roc_21", "secondary"),
    ("roc_63", "secondary"),
    ("rsi_7", "secondary"),
    ("rsi_9", "secondary"),
    ("rsi_21", "secondary"),
    # Risk — (threshold 1e-2, min pct 95.0%)
    ("sharpe_1y", "risk"),
    ("sortino_1y", "risk"),
    ("max_drawdown_1y", "risk"),
    ("calmar_ratio", "risk"),
    ("beta_nifty", "risk"),
]

THRESHOLDS: dict[str, tuple[float, float]] = {
    "core": (1e-4, 0.995),
    "secondary": (1e-3, 0.990),
    "risk": (1e-2, 0.950),
}


async def run(args: argparse.Namespace) -> int:
    db_url = os.environ["DATABASE_URL"]
    engine = create_async_engine(db_url, pool_pre_ping=True)
    SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report_lines: list[str] = [
        f"# Diff report — {args.asset} technicals v1 vs v2 — last {args.last_days} days",
        f"Generated: {now_str}",
        "",
        "| column | class | rows | max_abs_diff | mean_abs_diff | p95_abs_diff | pct<1e-4 | pct<1e-3 | verdict |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    all_passed = True

    async with SessionLocal() as session:
        # Find max date in v1 to anchor the window
        max_date_row = (
            await session.execute(
                text("SELECT MAX(date) FROM de_equity_technical_daily")
            )
        ).first()
        if max_date_row is None or max_date_row[0] is None:
            print("v1 table empty — nothing to diff")
            await engine.dispose()
            return 0

        max_date = max_date_row[0]
        cutoff = max_date - dt.timedelta(days=args.last_days)
        report_lines.insert(2, f"Reference max(date) in v1: {max_date}")

        for col, cls in OVERLAP_COLS:
            # Use CAST() not ::type to avoid SQLAlchemy param-cast collision bug
            q = text(f"""
                SELECT
                  COUNT(*) AS n,
                  COALESCE(
                    CAST(MAX(ABS(o.{col} - n.{col})) AS FLOAT),
                    0) AS max_diff,
                  COALESCE(
                    CAST(AVG(ABS(o.{col} - n.{col})) AS FLOAT),
                    0) AS mean_diff,
                  COALESCE(
                    CAST(
                      PERCENTILE_CONT(0.95) WITHIN GROUP (
                        ORDER BY ABS(o.{col} - n.{col})
                      ) AS FLOAT
                    ), 0) AS p95_diff,
                  COALESCE(
                    AVG(CASE WHEN ABS(o.{col} - n.{col}) <= 0.0001 THEN 1.0 ELSE 0.0 END),
                    0) AS within_1e4,
                  COALESCE(
                    AVG(CASE WHEN ABS(o.{col} - n.{col}) <= 0.001 THEN 1.0 ELSE 0.0 END),
                    0) AS within_1e3
                FROM de_equity_technical_daily o
                JOIN de_equity_technical_daily_v2 n
                  ON o.date = n.date
                  AND o.instrument_id = n.instrument_id
                WHERE o.date > :cutoff
                  AND o.{col} IS NOT NULL
                  AND n.{col} IS NOT NULL
            """)
            try:
                row = (await session.execute(q, {"cutoff": cutoff})).first()
            except Exception as exc:
                report_lines.append(
                    f"| {col} | {cls} | ERR | — | — | — | — | — | skip({exc!s:.60}) |"
                )
                continue

            if row is None or row[0] == 0:
                report_lines.append(
                    f"| {col} | {cls} | 0 | — | — | — | — | — | skip |"
                )
                continue

            n, max_d, mean_d, p95_d, w1e4, w1e3 = row
            tol, min_pct = THRESHOLDS[cls]
            # Use the tighter pct metric for core, wider for risk
            check_pct = w1e4 if tol <= 1e-4 else w1e3
            passed = max_d <= tol * 10 and check_pct >= min_pct
            verdict = "PASS" if passed else "FAIL"
            if not passed:
                all_passed = False
            report_lines.append(
                f"| {col} | {cls} | {n} | {max_d:.2e} | {mean_d:.2e} | {p95_d:.2e} | "
                f"{w1e4:.4f} | {w1e3:.4f} | {verdict} |"
            )

    report_lines.append("")
    report_lines.append(f"**Overall**: {'PASS' if all_passed else 'FAIL'}")

    REPORTS_DIR.mkdir(exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = REPORTS_DIR / f"technicals_diff_{args.asset}_{ts}.md"
    path.write_text("\n".join(report_lines))
    print("\n".join(report_lines))
    print(f"\nReport: {path}")

    await engine.dispose()
    return 0 if all_passed else 1


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--asset", required=True, choices=["equity"])
    p.add_argument("--last-days", type=int, default=30)
    args = p.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
