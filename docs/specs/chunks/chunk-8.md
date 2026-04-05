# Chunk 8: MF Ingestion Pipeline

**Layer:** 3
**Dependencies:** C4
**Complexity:** Medium
**Status:** pending

## Files

- `app/pipelines/mf/__init__.py`
- `app/pipelines/mf/amfi.py`
- `app/pipelines/mf/lifecycle.py`
- `app/pipelines/mf/returns.py`
- `app/pipelines/mf/dividends.py`
- `app/pipelines/mf/eod.py`
- `tests/pipelines/mf/test_amfi.py`
- `tests/pipelines/mf/test_lifecycle.py`
- `tests/pipelines/mf/test_returns.py`

## Acceptance Criteria

- [ ] **AMFI NAV download:** Fetches `https://www.amfiindia.com/spages/NAVAll.txt` daily; parses pipe-delimited format; extracts scheme code (amfi_code), scheme name, NAV, date
- [ ] **Freshness validation:** Row count >= 1000; source date matches expected business date; checksum computed; duplicate file skipped via `de_source_files` check
- [ ] **NAV ingestion:** `INSERT INTO de_mf_nav_daily ON CONFLICT (nav_date, mstar_id) DO UPDATE SET nav=EXCLUDED.nav, ...`; filter to target equity universe (equity + Growth + Regular, ~450-550 funds)
- [ ] **Post-insert validation (v1.8):**
  - NAV spike: `abs(nav_change_pct) > 15` → `anomaly_type='nav_spike', severity='warning'`
  - Zero NAV: `nav <= 0` → `anomaly_type='zero_nav', severity='critical'`
- [ ] **MF lifecycle management (v1.7):**
  - Merge detection: if old scheme no longer appears in AMFI file, mark `is_active=FALSE` in `de_mf_master`
  - Set `merged_into_mstar_id` on old scheme
  - INSERT `de_mf_lifecycle` events: `merge_from` and `merge_into`
  - Closure detection: set `closure_date` and `is_active=FALSE`
- [ ] **Return computation:** Compute `return_1d` through `return_10y` from NAV series; store in `de_mf_nav_daily`; use `NUMERIC(10,4)` for all return columns (cumulative returns routinely exceed 100%)
  - `return_1d`: today vs yesterday
  - `return_1w`: today vs 5 trading days ago
  - `return_1m`: today vs 21 trading days ago
  - `return_3m`: today vs 63 trading days ago
  - `return_6m`: today vs 126 trading days ago
  - `return_1y`: today vs 252 trading days ago
  - `return_3y` / `return_5y` / `return_10y`: CAGR from respective periods
- [ ] **IDCW dividend handling (v1.9 — explicit source required):**
  - Source: AMFI historical dividend files or BSE Star MF / NSE NMF II feeds — NEVER infer from NAV drops
  - INSERT `de_mf_dividends` for each dividend event
  - Recompute `nav_adj` from earliest affected record_date forward (mirrors equity adj_factor logic)
  - `nav_adj = nav × cumulative_factor` where `cumulative_factor = product of all adj_factors where record_date <= nav_date`
- [ ] **Data status gating:** Batch-promote `raw → validated` or `raw → quarantined` after validation
- [ ] MF pipeline runs as Track B, isolated from other tracks — failure does not block Track A or C-E
- [ ] All tests use mocked AMFI HTTP responses and synthetic NAV data — no live network calls

## Notes

**AMFI NAV file format (`NAVAll.txt`):**
```
Scheme Code;ISIN Div Payout/ IDCW;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date
```
Tab/semicolon delimited. One line per scheme per day. The file contains ALL schemes (13,000+). Pipeline must filter to target universe before inserting.

**Target universe (~450-550 funds):** Filter from `de_mf_master` where `broad_category` is equity-type AND fund_name contains `Accumulated`/`Growth` (not `IDCW`) AND fund_name contains `Regular` (not `Direct`). These are the funds relevant to MF Pulse.

**AMFI code → mstar_id mapping:** The `de_mf_master` table has `amfi_code` field. Join AMFI file on `amfi_code` to get `mstar_id` for insert. Not all Morningstar funds have AMFI codes — null `amfi_code` is permitted.

**NAV line in AMFI file example:**
```
120503;INF205K01UP5;-;Aditya Birla Sun Life Frontline Equity Fund - Growth - Regular Plan;579.2;05-Apr-2026
```

**IDCW note (v1.9):** The heuristic-based dividend detection (infer from NAV drops) was REMOVED in v1.9. March 2020 crash caused mass false positives — equity funds gapping 10-15% were incorrectly tagged as dividends. Dividends must now be sourced EXPLICITLY from AMFI dividend history files at `https://www.amfiindia.com/net-asset-value/nav-history`.

**MF NAV SLA:** Must complete by 22:30 IST. AMFI typically publishes by 21:00 IST. If delayed >24 hours, use previous NAV with stale flag.

**Return computation approach:** For each mstar_id, fetch historical NAV from `de_mf_nav_daily` and compute returns. Run incrementally (only today's NAV needs new return values). Use `close_adj` equivalent which is `nav_adj` for IDCW funds, `nav` for all others.
