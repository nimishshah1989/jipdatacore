# GAP-11 — Repair de_rs_daily_summary (empty)

## Goal
`de_rs_daily_summary` has 0 rows while `de_rs_scores` has 14.7M. The daily
summary view/table is broken. Investigate, fix the population code, backfill.

## Scope
- Investigate what's supposed to populate `de_rs_daily_summary` — grep for it
  in `app/computation/`, `app/pipelines/`, `scripts/`
- Determine if it's meant to be a derived aggregation of de_rs_scores (per
  instrument per date) or a different rollup
- Find the bug: missing INSERT, broken JOIN, silent exception
- Fix the computation code
- Write a one-off backfill script to populate historical rows
- Add a regression test

## Acceptance criteria
- [ ] Root cause documented in the commit message
- [ ] `SELECT COUNT(*) FROM de_rs_daily_summary` > 1M after backfill
- [ ] `SELECT COUNT(DISTINCT date) FROM de_rs_daily_summary` matches de_rs_scores date range
- [ ] Regression test that asserts nightly_compute populates de_rs_daily_summary
- [ ] Commit subject starts with `GAP-11`
- [ ] `state.db` shows `GAP-11` with `status='DONE'`

## Steps for the inner session
1. `grep -rn 'de_rs_daily_summary' app/ scripts/ alembic/`
2. Read the existing RS computation in `app/computation/rs.py`
3. Identify the missing piece — summary rollup or view definition
4. Fix the code
5. Write backfill script
6. Run backfill against prod
7. Verify counts
8. Commit

## Out of scope
- Redesigning the RS computation
- Modifying de_rs_scores
- Performance tuning of the RS pipeline

## Dependencies
- Upstream: none
- Downstream: none
