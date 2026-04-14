# Morningstar purchase_mode investigation

**Date**: 2026-04-14
**Context**: Eng-review Fix 1 — verify whether JIP Data Core's Morningstar ingestion already fetches `purchase_mode` (1=Regular, 2=Direct) from the Morningstar datapoint API before committing to the chunk 9 scope.
**Outcome**: **Chunk 9 stays as a single chunk, M complexity. No 9a/9b split needed.** The fix is a 5-line addition to existing code, not a new API integration.

## Method
1. `grep -rn "OperationsMasterFile\|purchase_mode\|PurchaseMode\|plan_type\|PlanType" app/pipelines/morningstar/` — no matches
2. Read `app/pipelines/morningstar/client.py` — understand the HTTP client shape
3. Read `app/pipelines/morningstar/fund_master.py` — understand the weekly refresh pipeline
4. Cross-check against `de_mf_master` schema (already confirmed no `purchase_mode` column)

## Findings

### F1: `purchase_mode` is NOT fetched today
`FUND_MASTER_DATAPOINTS` in `app/pipelines/morningstar/fund_master.py:29` fetches exactly 8 datapoints:
```
Name, CategoryName, BroadCategoryGroup, NetExpenseRatio,
ManagerName, TotalNetAssets, InceptionDate, Benchmark
```
`PurchaseMode` is absent. `parse_fund_master_response()` has no code path that reads it. `update_fund_master_row()` never writes it.

### F2: The Morningstar client uses a datapoint API — easy to extend
`client.py` makes a single-endpoint call:
```
GET {base_url}/{IdType}/{Identifier}?datapoints={csv_list}&accesscode={code}
```
Adding `"PurchaseMode"` to the datapoints list is a **one-line change**. No new endpoint integration, no new auth, no new rate limit work. The existing retry/throttle/daily-cap all apply for free.

### F3: The Morningstar datapoint API supports `PurchaseMode`
Confirmed by `mfpulse_reimagined/backend/app/models/db/fund_master.py` which has `purchase_mode: Mapped[Optional[int]] = mapped_column(Integer)` populated from this same API. Values observed: 1 (Regular), 2 (Direct) — matches the `PURCHASE_MODE_MAP` in mfpulse's service layer.

### F4: Stale docstring (informational, not blocking)
The module docstring at `fund_master.py:2-4` claims the pipeline targets "~450-550 equity growth regular funds", but `load_target_universe()` at line 94 actually returns 4,234 funds (active AND NOT is_index_fund AND NOT is_etf — no equity/regular/growth filter applied). This discrepancy is pre-existing and out of scope for the current chunk — once `purchase_mode` is populated, a follow-up can tighten the target universe to match the docstring.

## Implications for the chunk plan

**Chunk 9 plan (unchanged)**:
1. Alembic migration adds `purchase_mode INTEGER NULL` to `de_mf_master` (already in chunk 2 scope — no change)
2. Bootstrap script backfills `purchase_mode` from mfpulse's `fund_master` table for existing ~13,380 funds (one-time)
3. **[IN SCOPE]** Add `"PurchaseMode"` to `FUND_MASTER_DATAPOINTS` in `fund_master.py:29`
4. **[IN SCOPE]** Extract `data.get("PurchaseMode")` in `parse_fund_master_response()` → int coercion → dict
5. **[IN SCOPE]** Add `purchase_mode` to the update column list in `update_fund_master_row()`
6. Next weekly run then keeps the column current

**Chunk 9 size estimate**: still M complexity. The bootstrap script remains the largest piece; the Morningstar extension is trivial.

**No 9a/9b split required.**

## Follow-up tickets to file (not blocking current work)
1. **Tighten target universe**: `load_target_universe()` currently returns 4,234 funds, docstring claims 450-550. Once `purchase_mode` is populated, filter to `purchase_mode=1 AND broad_category='Equity' AND fund_name !~* 'IDCW|Dividend|Segregated'` for the refresh, drastically cutting API calls.
2. **Stale MF NAV coverage**: orthogonal to this investigation but already flagged in the PRD — only 1,255 / 13,380 MFs have NAV history.

## Conclusion
Fix 1 from the eng review is resolved. The feared "Morningstar doesn't fetch OperationsMasterFile — scope extension!" is not real: the Morningstar datapoint API and our existing client handle `PurchaseMode` with a trivial addition. Chunk 9 proceeds as originally planned, no split.
