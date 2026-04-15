# GAP-07 — Rerun risk backfill for all 4 asset classes (1y/3y/5y)

## Goal
Populate the new multi-year risk columns from GAP-05 for every instrument in
equity, ETF, global, and index v2 technical tables. The engine now produces
them (GAP-06) but existing rows need a rerun.

## Scope
- Reuse existing backfill script (`scripts/backfill_indicators_v2.py`) with
  NO code change — just rerun against production RDS for all 4 asset classes
- The backfill uses pg_insert ON CONFLICT DO UPDATE (via COPY staging) so
  existing rows will have the new columns filled in place; no truncation
  needed
- Run sequentially (equity → etf → global → index) to avoid RDS contention
- Verify: ≥ 90% of eligible rows have non-NULL sharpe_3y; ≥ 70% have
  non-NULL sharpe_5y (short-history instruments won't reach 5y)
- Produce per-asset reports

## Acceptance criteria
- [ ] `SELECT COUNT(*) FROM de_equity_technical_daily WHERE sharpe_3y IS NOT NULL` > 2M
- [ ] `SELECT COUNT(*) FROM de_equity_technical_daily WHERE sharpe_5y IS NOT NULL` > 1M
- [ ] Same for etf, global, index at appropriate scales
- [ ] Zero errors in any of the 4 backfill runs
- [ ] Commit subject starts with `GAP-07`
- [ ] `state.db` shows `GAP-07` with `status='DONE'`

## Steps for the inner session
1. Verify GAP-05 migration + GAP-06 risk_metrics are deployed on EC2
  (rsync latest code)
2. Run `python scripts/backfill_indicators_v2.py --asset equity` on EC2
  (ON CONFLICT path will update existing rows)
3. Repeat for etf, global, index — one at a time
4. Verify population counts per table
5. Commit a summary report

## Out of scope
- MF backfill (MF is on a different derived_daily path — see GAP-10)
- Code changes (engine + schema already done in GAP-05/06)

## Dependencies
- Upstream: GAP-05, GAP-06
- Downstream: none
