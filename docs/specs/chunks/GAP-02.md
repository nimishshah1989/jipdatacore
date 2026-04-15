# GAP-02 — MF technicals backfill (IND-C10)

## Goal
Run the full MF indicators v2 backfill against the ~800 eligible equity-regular-growth
funds now that `purchase_mode` is populated (GAP-01). The wrapper code already exists
from IND-C10 planning but was never executed.

## Scope
- Add `mf` support to `scripts/backfill_indicators_v2.py` generic runner
  (if not already wired — check first; it was in the C10 spec)
- Create `app/computation/indicators_v2/assets/mf.py` if absent, mirroring the
  equity wrapper but with: source_model=DeMfNavDaily, id_column=mstar_id,
  date_column=nav_date, close_col='nav', open/high/low/volume=None,
  min_history_days=250
- Ensure the MF strategy loader filters volume/OHLC indicators correctly
  (per Fix 13 — MF single-price subset already in strategy.yaml)
- Run the full MF backfill against production RDS via EC2
- Produce `reports/mf_backfill_<date>.md` with counts

## Acceptance criteria
- [ ] `de_mf_technical_daily` has rows for ≥ 800 eligible funds
- [ ] Latest row per fund has non-NULL `sma_50`, `rsi_14`, `macd_line`, `sharpe_1y`, `volatility_20d`
- [ ] Zero `ERR` / `FAIL` status lines in the backfill report
- [ ] GENERATED booleans consistent: `SELECT COUNT(*) FROM de_mf_technical_daily WHERE sma_50 IS NOT NULL AND above_50dma != (close_adj > sma_50)` returns 0
- [ ] Commit subject starts with `GAP-02`
- [ ] `state.db` shows `GAP-02` with `status='DONE'`

## Steps for the inner session
1. Read `app/computation/indicators_v2/assets/` — check if `mf.py` exists
2. If not, create it following the equity.py template with MF-specific spec
3. Verify MF eligibility filter in the wrapper applies: purchase_mode=1,
   broad_category='Equity', non-ETF, non-index, non-IDCW, exists in de_mf_nav_daily
4. Add `mf` to `run_generic_backfill` dispatcher in the backfill script
5. Build docker image, run backfill on EC2 with `--asset mf`
6. Verify row counts in prod; run generated-column consistency query
7. Commit code + report

## Out of scope
- Migrating old `de_mf_derived_daily` to empyrical (that's GAP-10)
- Adding MF to the nightly runner (separate task)

## Dependencies
- Upstream: GAP-01
- Downstream: none (GAP-10 is a separate rework)
