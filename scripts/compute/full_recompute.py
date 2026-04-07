"""Full recomputation — runs all computation steps in dependency order.

Steps:
1. Technicals (SMA50/200) via SQL
2. Technicals (EMA/RSI/MACD/ADX/Bollinger) via vectorized pandas
3. RS scores (equity + MF + sector)
4. Breadth + Regime
5. Fund metrics (Sharpe/Sortino/StdDev/MaxDD/Beta + derived RS + alpha + sector exposure)

Usage:
    python -m scripts.compute.full_recompute
    python -m scripts.compute.full_recompute --start-date 2025-01-01  # incremental
"""

import argparse
import asyncio
import subprocess
import sys
import time


def run_step(label: str, module: str, args: list[str] = None):
    """Run a computation step as a subprocess."""
    t0 = time.time()
    print(f"\n{'='*60}", flush=True)
    print(f"STEP: {label}", flush=True)
    print(f"{'='*60}", flush=True)

    cmd = [sys.executable, "-m", module] + (args or [])
    result = subprocess.run(cmd, capture_output=False)

    elapsed = time.time() - t0
    status = "OK" if result.returncode == 0 else f"FAILED (exit {result.returncode})"
    print(f"\n  {label}: {status} ({elapsed:.0f}s)", flush=True)
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default=None, help="For incremental runs")
    args = parser.parse_args()

    t_start = time.time()
    extra = ["--start-date", args.start_date] if args.start_date else []

    steps = [
        ("Technicals (SQL: SMA50/200)", "scripts.compute.technicals_sql", extra or ["--start-date", "2007-01-01"]),
        ("Technicals (Pandas: EMA/RSI/MACD/ADX)", "scripts.compute.technicals_pandas",
         (["--start-date", args.start_date, "--filter-date", args.start_date] if args.start_date
          else ["--start-date", "2016-04-01"])),
        ("RS Scores (equity + MF + sector)", "scripts.compute.rs_scores", []),
        ("Breadth + Regime", "scripts.compute.breadth_regime", []),
        ("Fund Metrics (all)", "scripts.compute.fund_metrics", []),
    ]

    results = []
    for label, module, step_args in steps:
        ok = run_step(label, module, step_args)
        results.append((label, ok))
        if not ok:
            print(f"\n*** {label} FAILED — stopping ***", flush=True)
            break

    print(f"\n{'='*60}", flush=True)
    print(f"FULL RECOMPUTE COMPLETE in {time.time()-t_start:.0f}s ({(time.time()-t_start)/60:.1f} min)", flush=True)
    print(f"{'='*60}", flush=True)
    for label, ok in results:
        print(f"  {'OK' if ok else 'FAIL'} {label}")


if __name__ == "__main__":
    main()
