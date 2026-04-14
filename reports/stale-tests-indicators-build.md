# Stale tests found during indicators overhaul build

**Date**: 2026-04-14
**Context**: IND-C1 verification ran `pytest tests/computation/` inside a fresh `docker build .` image. 428 passed, 3 failed. None of the failures are caused by the indicators overhaul changes (verified by `grep -l` confirming zero existing code imports `pandas_ta_classic`, `empyrical`, or `indicators_v2`). Filing these as pre-existing follow-up tickets.

## Failure 1 — hardcoded host path
**Test**: `tests/computation/test_global_technicals.py::TestMainFunction::test_module_has_main_guard` (line 436)
**Error**: `FileNotFoundError: [Errno 2] No such file or directory: '/Users/nimishshah/projects/jip data core/scripts/compute/global_technicals.py'`
**Root cause**: Test opens a hardcoded absolute path. The file `scripts/compute/global_technicals.py` does not exist in the repo (`ls scripts/` confirms no `compute/` subdirectory). Either the file was moved/deleted in a later refactor, or the test was written against a planned-but-never-shipped script path.
**Fix**: Delete the test (the file it guards doesn't exist) or relocate the `main` guard check to whichever file actually has a `__main__` block for global technicals.
**Priority**: P3 — cosmetic; does not affect production behavior.

## Failure 2 — MagicMock > int comparison
**Test**: `tests/computation/test_runner.py::TestRunFullComputationPipeline::test_all_steps_pass`
**Error**: `'>' not supported between instances of 'MagicMock' and 'int'` in the `intermarket` step
**Symptom**: `report.overall_status == 'failed'` because the intermarket step raises on a comparison between a MagicMock (test fixture session) and an integer.
**Root cause**: The intermarket step in `app/computation/runner.py` compares a value coming out of the (mocked) DB session to an int. Under older pandas/numpy, the MagicMock's auto-generated `__gt__` returned a truthy MagicMock. Under pandas 3.x / numpy 2.x (pulled fresh on today's docker build via `>=` constraints), this comparison raises `TypeError` instead.
**Fix**: Update the test fixture to explicitly return a numeric value from the mocked query, or stub the intermarket step's DB call with a concrete `AsyncMock` return_value instead of relying on MagicMock's default behavior.
**Priority**: P2 — blocks CI when the test suite is run; does not affect production behavior (production uses a real DB session, not MagicMock).

## Failure 3 — stale step count assertion
**Test**: `tests/computation/test_runner.py::TestRunFullComputationPipeline::test_breadth_failure_does_not_skip_regime_or_sectors`
**Error**: `assert len(report.steps) == 6` but got `12`
**Root cause**: The runner has been extended with additional steps over time (stochastic, disparity, bollinger_width, oscillator_weekly, oscillator_monthly, index_pivots, intermarket, divergence) without the test being updated. The test was written when the runner had 6 steps; it now has 12.
**Fix**: Update the assertion to `len(report.steps) == 12` or make it dynamic via the actual step registry.
**Priority**: P2 — same as failure 2, blocks CI.

## Pre-existing dep drift (informational)
Today's fresh `pip install ".[dev]"` via Docker pulled:
- `pandas 3.0.2` (project pin is `>=2.2.0`)
- `numpy 2.4.4` (project pin is `>=1.26.0`)
- `sqlalchemy 2.0.49`

These are major version bumps from what was likely installed when the tests last passed. The indicators overhaul did not cause this — the `>=` constraints in `pyproject.toml` pull whatever is latest on every fresh build. Future fix: pin more deps exact, or adopt `uv` / `poetry` with a lockfile.

## Proof that IND-C1 did not cause these
- `grep -l "pandas_ta\|empyrical\|indicators_v2" tests/` returns no matches in the failing test files
- `grep -l "pandas_ta\|empyrical\|indicators_v2" app/` returns only `app/computation/indicators_v2/__init__.py` (my new empty file)
- The 3 failures all occur in code paths that `app/computation/runner.py` exercises, which doesn't import anything from `indicators_v2`
- The 428 passing tests include `tests/computation/test_technicals.py` (79 tests) — the old hand-rolled indicator module still works, confirming no formula code regressed

## Next steps
File 3 GitHub issues in the JIP Data Core repo for P2/P3 cleanup. Proceeding with IND-C2 (migrations) unblocked.
