# GAP-14 — Deep NAV backfill for 133 older eligible MFs + young-fund partial technicals

## Goal
Close the two remaining holes in MF technical coverage after GAP-02/13:

1. **133 eligible funds ≥1 year old** have <252 NAV rows in JIP despite mfpulse
   having richer history. Backfill them from `mfpulse_reimagined.nav_daily`
   covering inception→today.
2. **198 eligible funds <1 year old** are silently skipped by the technicals
   engine's 252-day history guard. Relax the guard to compute whatever the
   data supports (SMA/EMA/RSI/MACD/Bollinger etc.) and NULL the risk/ROC252
   columns that legitimately can't be computed. Frontend will show a "fund
   age < 1 year — limited metrics" badge from the absence of those columns.

Outcome: `de_mf_technical_daily` covers all 878 eligible funds, with 1y risk
metrics populated only where history allows.

## Scope decision rationale

From audit on 2026-04-15 (see history of this conversation):
- 878 eligible funds (equity, regular, growth, non-ETF, non-index)
- 549 have technicals (GAP-02 output)
- 331 missing — split: 198 are <1y old (genuinely young), 133 are ≥1y old
  with <252 NAV rows in JIP
- mfpulse sample (F00001DD9Z) shows 934 rows back to 2022-02 for an older
  fund — confirming mfpulse has the data the 133 older funds need.

## Scope

### Fix A — NAV backfill from mfpulse for 133 older eligible funds

Add `scripts/backfill_mf_nav_deep.py`:

- Query JIP for the 331 eligible mstar_ids with <252 NAV rows (the exact
  query below).
- For each, read `mfpulse_reimagined.nav_daily` via the mf-pulse docker
  container for ALL rows (no date filter — take whatever mfpulse has).
- UPSERT into `de_mf_nav_daily` with ON CONFLICT (mstar_id, nav_date) DO
  NOTHING so this is idempotent and doesn't overwrite curated rows.
- Log per-fund: mfpulse rows fetched, JIP rows inserted, final coverage.

Target query (lives in the script, not run here):
```sql
WITH elig AS (
  SELECT mstar_id, inception_date FROM de_mf_master
  WHERE purchase_mode=1 AND broad_category='Equity' AND is_active
    AND NOT is_etf AND NOT is_index_fund
    AND fund_name !~* '\y(IDCW|Dividend|Segregated|Direct)\y'
),
ns AS (SELECT mstar_id, count(*) n FROM de_mf_nav_daily GROUP BY mstar_id)
SELECT e.mstar_id FROM elig e
LEFT JOIN ns USING (mstar_id)
WHERE COALESCE(ns.n, 0) < 252
```

### Fix B — Relax technicals engine min_history guard

In `app/computation/indicators_v2/engine.py`, the `min_history_days` field
on `AssetSpec` currently causes funds with fewer rows to be fully skipped.
Change the behavior so:

- A fund with ≥20 NAV rows still goes through the engine.
- pandas-ta columns that can't be computed for the given history length
  (e.g. SMA_200 for a 100-day fund, ROC_252 for a 6-month fund) land as
  NULL in the output row — this is pandas-ta's native behavior; we just
  need to stop the pre-engine skip.
- Risk columns (sharpe_1y, sortino_1y, calmar_ratio, max_drawdown_1y,
  beta_nifty, alpha_nifty) stay NULL when history <252 days — no change
  to `risk_metrics.py` needed since the rolling windows return NaN for
  short series.

Update `assets/mf.py` so `MF_SPEC.min_history_days = 20` (or whatever
minimum the shortest enabled indicator needs, likely SMA(5)=5 or RSI(7)=7).

### Re-run MF technicals

After Fix A lands (NAV backfilled) and Fix B is deployed (engine relaxed),
run `scripts/backfill_indicators_v2.py --asset mf --from 2015-01-01` to
recompute. Expected result: `SELECT COUNT(DISTINCT mstar_id) FROM
de_mf_technical_daily` ≥ 870 (allowing ~8 funds with <20 NAV rows to still
be skipped).

## Acceptance criteria

- [ ] `scripts/backfill_mf_nav_deep.py` exists, is idempotent, runs against
  the 331 target mstar_ids.
- [ ] `SELECT count(distinct mstar_id) FROM de_mf_nav_daily WHERE mstar_id
  IN (<the 133 older eligible funds>)` is 133 after backfill.
- [ ] Of the 133 older eligible funds, ≥ 120 now have ≥ 252 NAV rows
  (some may be genuinely missing from mfpulse too — document the residual).
- [ ] `app/computation/indicators_v2/engine.py` or `assets/mf.py` relaxed
  to allow funds with ≥20 NAV rows through the engine.
- [ ] `SELECT count(distinct mstar_id) FROM de_mf_technical_daily` ≥ 780
  (conservative — 549 baseline + 133 older + ~100 of the 198 young that
  have ≥20 rows).
- [ ] At least one young fund (<1y old) has rows in `de_mf_technical_daily`
  where SMA_20 and RSI_14 are non-NULL but sharpe_1y is NULL.
- [ ] Commit subject starts with `GAP-14`.

## Steps for the inner session

1. Read `scripts/bootstrap_purchase_mode_from_mfpulse.py` to see how the
   mfpulse Postgres is reached (docker exec + DATABASE_URL env).
2. Write `scripts/backfill_mf_nav_deep.py` following that connection pattern.
3. Run it; capture counts. Should insert ~80k NAV rows.
4. Read `app/computation/indicators_v2/engine.py` and
   `app/computation/indicators_v2/assets/mf.py` to locate the min_history
   guard. Lower to 20.
5. Re-run `scripts/backfill_indicators_v2.py --asset mf` — this will
   recompute ALL eligible funds; idempotent via the existing COPY-via-
   staging upsert path.
6. Validate acceptance SQL counts.
7. Commit.

## Out of scope
- Non-equity / non-regular / non-growth funds (respect user scope from GAP-13).
- Morningstar API re-ingestion (mfpulse is the source).
- Frontend "fund age <1y" badge — separate Atlas ticket.
- Changing risk metric semantics for short windows.

## Dependencies
- Upstream: GAP-01 (purchase_mode), GAP-13 (initial NAV expansion),
  GAP-02 (MF technicals baseline). All DONE.
- Downstream: none.
