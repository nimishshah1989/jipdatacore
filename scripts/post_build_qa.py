#!/usr/bin/env python3
"""
Post-Build QA — runs after all chunks are complete.
Validates the full engine is ready for deployment.
"""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
RESULTS = []


def run(cmd: str, label: str) -> tuple[bool, str]:
    """Run a command, return (passed, output)."""
    print(f"\n{'='*60}")
    print(f"QA CHECK: {label}")
    print(f"{'='*60}")
    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True,
        cwd=str(PROJECT_ROOT), timeout=300
    )
    output = result.stdout + result.stderr
    passed = result.returncode == 0
    status = "PASS" if passed else "FAIL"
    print(f"  Result: {status}")
    if not passed:
        print(f"  Output: {output[-500:]}")
    RESULTS.append({"check": label, "passed": passed, "output": output[-200:]})
    return passed, output


def main():
    print("\n" + "="*60)
    print("  FORGE POST-BUILD QA — JIP DATA ENGINE")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("="*60)

    # Update dashboard
    subprocess.run(
        "python3 scripts/update_dashboard.py --phase post-build --detail 'Running QA checks...'",
        shell=True, cwd=str(PROJECT_ROOT)
    )

    # 1. Full test suite
    run("python3 -m pytest tests/ -v --tb=short", "Full Test Suite")

    # 2. Lint (F821 = undefined names, fatal)
    passed_lint, lint_out = run(
        "python3 -m ruff check . --select F821", "Undefined Names (F821)"
    )

    # 3. Full lint
    run("python3 -m ruff check . --select E,F,W -q", "Full Lint (E,F,W)")

    # 4. Type check
    run("python3 -m mypy app/ --ignore-missing-imports", "Type Check (mypy)")

    # 5. Check all pipelines extend BasePipeline
    run(
        "python3 -c \""
        "import ast, pathlib, sys; "
        "errors = []; "
        "[errors.append(f'{p}: missing BasePipeline') "
        "for p in pathlib.Path('app/pipelines').rglob('*.py') "
        "if 'pipeline' in p.stem.lower() and '__init__' not in p.stem "
        "and 'framework' not in p.stem "
        "and 'BasePipeline' not in p.read_text()]; "
        "print(f'{len(errors)} pipelines missing BasePipeline'); "
        "[print(e) for e in errors]; "
        "sys.exit(1 if errors else 0)\"",
        "All Pipelines Extend BasePipeline"
    )

    # 6. Check no float() on financial values
    run(
        "python3 -c \""
        "import pathlib, re, sys; "
        "hits = []; "
        "[hits.extend([(str(p), i+1, line.strip()) "
        "for i, line in enumerate(p.read_text().splitlines()) "
        "if re.search(r'float\\(', line) and 'test' not in str(p)]) "
        "for p in list(pathlib.Path('app/pipelines').rglob('*.py')) + "
        "list(pathlib.Path('app/computation').rglob('*.py'))]; "
        "print(f'{len(hits)} float() usages found'); "
        "[print(f'  {h[0]}:{h[1]}: {h[2]}') for h in hits[:20]]; "
        "sys.exit(1 if hits else 0)\"",
        "No float() in Pipelines/Computation"
    )

    # 7. Check ON CONFLICT in all pipeline execute methods
    run(
        "python3 -c \""
        "import pathlib, sys; "
        "missing = []; "
        "[missing.append(str(p)) "
        "for p in pathlib.Path('app/pipelines').rglob('*.py') "
        "if 'pipeline' in p.stem.lower() and '__init__' not in p.stem "
        "and 'framework' not in p.stem "
        "and 'on_conflict' not in p.read_text().lower() "
        "and 'execute' in p.read_text()]; "
        "print(f'{len(missing)} pipelines missing ON CONFLICT'); "
        "[print(f'  {m}') for m in missing]; "
        "sys.exit(1 if missing else 0)\"",
        "All Pipelines Use ON CONFLICT Upsert"
    )

    # 8. Check no print() in production code
    run(
        "python3 -c \""
        "import pathlib, re, sys; "
        "hits = []; "
        "[hits.extend([(str(p), i+1) "
        "for i, line in enumerate(p.read_text().splitlines()) "
        "if re.search(r'^\\s*print\\(', line)]) "
        "for p in pathlib.Path('app').rglob('*.py')]; "
        "print(f'{len(hits)} print() statements found'); "
        "[print(f'  {h[0]}:{h[1]}') for h in hits[:20]]; "
        "sys.exit(1 if hits else 0)\"",
        "No print() in Production Code"
    )

    # 9. Docker build check
    run("docker build -t jip-data-engine:qa-check . 2>&1 | tail -5", "Docker Build")

    # 10. App import check (can the app start?)
    run(
        "python3 -c \"from app.main import app; print(f'Routes: {len(app.routes)}')\"",
        "App Import Check"
    )

    # Summary
    passed_count = sum(1 for r in RESULTS if r["passed"])
    total_count = len(RESULTS)
    all_passed = passed_count == total_count

    print("\n" + "="*60)
    print(f"  QA RESULTS: {passed_count}/{total_count} checks passed")
    print("="*60)
    for r in RESULTS:
        icon = "PASS" if r["passed"] else "FAIL"
        print(f"  [{icon}] {r['check']}")

    # Save results
    results_file = PROJECT_ROOT / "docs" / "qa-results.json"
    with open(results_file, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "passed": passed_count,
            "total": total_count,
            "all_passed": all_passed,
            "checks": RESULTS,
        }, f, indent=2)

    # Update dashboard
    status = "complete" if all_passed else "post-build"
    detail = f"QA: {passed_count}/{total_count} passed" + ("" if all_passed else " — ISSUES FOUND")
    subprocess.run(
        f"python3 scripts/update_dashboard.py --phase {status} --detail '{detail}'",
        shell=True, cwd=str(PROJECT_ROOT)
    )

    if not all_passed:
        print("\nQA FAILED — fix issues before deployment")
        sys.exit(1)

    print("\nALL QA CHECKS PASSED — ready for deployment")
    return 0


if __name__ == "__main__":
    sys.exit(main())
