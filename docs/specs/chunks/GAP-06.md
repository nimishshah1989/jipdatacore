# GAP-06 — Extend risk_metrics.py for 1y/3y/5y windows

## Goal
Update `app/computation/indicators_v2/risk_metrics.py` to compute risk metrics
across three windows (252/756/1260 days) in a single pass and output all the
new columns from GAP-05.

## Scope
- Rewrite `compute_risk_series` to accept `windows: list[tuple[str, int]]` parameter
  defaulting to `[("1y", 252), ("3y", 756), ("5y", 1260)]`
- For each window, compute via `empyrical.roll_sharpe_ratio`, `roll_sortino_ratio`,
  `roll_max_drawdown`, `roll_beta`, `roll_alpha_beta` — all vectorized
- Compute treynor_ratio and downside_risk manually (no roll_ variant in empyrical)
- For calmar/omega/information_ratio keep the vectorized pandas pattern from
  commit f3d0a3b but extend to multi-window
- Output DataFrame with 1y + 3y + 5y columns matching GAP-05 schema
- Extend `compute_hv_series` to output `volatility_1y/3y/5y` aliases
- Keep backward-compatible: existing 1y-only callers continue to work
- Update `_RISK_COLUMNS` frozenset in `strategy_loader.py` to include new columns
- Update/add tests:
    test_risk_series_3y_matches_empyrical_scalar
    test_risk_series_5y_matches_empyrical_scalar
    test_get_schema_columns_includes_multi_year_risk

## Acceptance criteria
- [ ] `compute_risk_series` returns a DataFrame with ≥ 20 risk columns
- [ ] New columns match empyrical scalar calls on a 1500-row synthetic series
  within 1e-4 tolerance for 3y, same for 5y
- [ ] Existing 1y parity tests still pass (no regression)
- [ ] `test_vectorized_risk_metrics_finish_under_5_seconds` still passes on 4800 rows
- [ ] Golden fixtures regenerated (new column set)
- [ ] Commit subject starts with `GAP-06`
- [ ] `state.db` shows `GAP-06` with `status='DONE'`

## Steps for the inner session
1. Read current risk_metrics.py + its tests
2. Extract common roll-compute logic into a helper that takes a window parameter
3. Loop over [("1y", 252), ("3y", 756), ("5y", 1260)] and produce one column set per window
4. Add downside_risk (stdev of negative returns × sqrt(annualization)) and
   treynor_ratio (annualized_return / beta) as vectorized
5. Write parity tests for 3y and 5y windows
6. Regenerate golden fixtures via `scripts/regenerate_indicators_v2_golden.py`
7. Rebuild docker image, run full tests, commit

## Out of scope
- Backfilling the new columns with production data (GAP-07)
- Schema changes (GAP-05)
- MF-specific risk rewrite (GAP-10)

## Dependencies
- Upstream: GAP-05
- Downstream: GAP-07, GAP-10
