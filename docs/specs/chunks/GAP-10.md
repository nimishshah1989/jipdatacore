# GAP-10 — Migrate de_mf_derived_daily to empyrical-based risk metrics

## Goal
Rewrite the old hand-rolled MF risk metrics in `de_mf_derived_daily` to use the
same empyrical-based engine that v2 technicals use. The table already has the
right 1y/3y/5y column structure (sharpe/sortino/max_dd/volatility/stddev/beta/
info_ratio/treynor) but values were computed by the legacy formulae — same
code category the user flagged as broken.

## Scope
- New module `app/computation/mf_risk_v2.py` that:
    1. Loads NAV series per mstar_id from `de_mf_nav_daily`
    2. Calls `compute_risk_series` from indicators_v2/risk_metrics.py with
       multi-window [1y, 3y, 5y]
    3. UPSERTs into `de_mf_derived_daily` (only the risk columns — preserve
       the manager_alpha, nav_rs_composite, derived_rs_composite, coverage_pct
       columns which come from different code)
- Preserve the existing population where it has values from other sources
- `scripts/backfill_mf_risk_v2.py` — one-off full rebuild of risk columns
- Parity test: recompute for one known fund (HDFC Flexi Cap Regular Growth)
  and verify values differ from legacy (expected) but match empyrical scalar

## Acceptance criteria
- [ ] `app/computation/mf_risk_v2.py` exists with a `compute_and_upsert_mf_risk` coroutine
- [ ] `scripts/backfill_mf_risk_v2.py` exists and runs successfully
- [ ] `SELECT COUNT(*) FROM de_mf_derived_daily WHERE sharpe_3y IS NOT NULL AND nav_date = CURRENT_DATE - 2` ≥ 800
- [ ] Parity test asserts empyrical-match within 1e-4 on a synthetic NAV series
- [ ] Non-risk columns in de_mf_derived_daily (manager_alpha, rs_composite) unchanged
- [ ] Commit subject starts with `GAP-10`
- [ ] `state.db` shows `GAP-10` with `status='DONE'`

## Steps for the inner session
1. Read `de_mf_derived_daily` schema to see which columns are risk (migrate)
   vs other (preserve)
2. Write `mf_risk_v2.py` using the same bulk-load + compute pattern as
   indicators_v2 engine
3. Write parity test
4. Run against a single fund for validation
5. Full backfill against prod
6. Verify counts + commit

## Out of scope
- MF technicals in `de_mf_technical_daily` — that's GAP-02
- Non-risk columns in de_mf_derived_daily
- Adding new MF-specific metrics

## Dependencies
- Upstream: GAP-06 (needs multi-window risk_metrics.py)
- Downstream: none
