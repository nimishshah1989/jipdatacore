# GAP-04 — Index technicals rerun with overflow clamp fix

## Goal
Rerun `backfill_indicators_v2.py --asset index` after GAP-03 lands the historical
data. Fix the 40-error overflow cascade from the last index run (happened despite
commit 396e196's clamp). Target: 135/135 indices with technicals.

## Scope
- Diagnose the overflow: last run hit `numeric field overflow` on a chunk that
  then transaction-aborted. The clamp at ±999,999.9999 in `_to_decimal_row` is
  in place, but something still exceeded it. Candidate cause: computed z-score
  or linreg slope on a degenerate constant-price series.
- Fix: either tighten the clamp to ±99,999.9999 for the narrow-precision
  indicator columns, OR add per-column precision awareness in `_to_decimal_row`
  via the model's `Column.type.precision/scale`
- Truncate `de_index_technical_daily`, clear the index cursor
- Run full backfill with the fix
- Verify 0 errors, ≥ 130/135 instruments processed (some may legitimately be
  too short even after GAP-03)

## Acceptance criteria
- [ ] Last run report shows `errors=0`
- [ ] `de_index_technical_daily` has ≥ 130 distinct index_codes
- [ ] All 7 critical sectoral indices present: PHARMA, REALTY, PVT BANK,
  OIL & GAS, HEALTHCARE, CONSUMER DURABLES, FINANCIAL SERVICES
- [ ] GENERATED consistency query returns 0 inconsistencies
- [ ] Commit subject starts with `GAP-04`
- [ ] `state.db` shows `GAP-04` with `status='DONE'`

## Steps for the inner session
1. Grep the last backfill error report for the triggering column
2. Update `_to_decimal_row` in `engine.py` to clamp per-column precision, OR
   tighten the global clamp
3. Add a unit test for the overflow clamp behavior
4. Rebuild + deploy engine.py to EC2
5. Truncate index table, clear cursor, run full backfill
6. Commit the fix + results report

## Out of scope
- Adding more indices beyond what's in de_index_master
- Changing other asset classes' behavior

## Dependencies
- Upstream: GAP-03
- Downstream: none
