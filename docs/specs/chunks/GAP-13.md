# GAP-13 — Expand MF NAV coverage to ALL equity regular growth schemes

## Goal
Current JIP MF coverage: 1,255 funds with NAV data in `de_mf_nav_daily`.
Target: every **equity + regular + growth** mutual fund should have daily
NAV history. User scope clarification (2026-04-15): "don't unnecessarily
increase workload — equity regular growth only, nothing else".

From the audit:
- 4,234 active equity non-ETF non-index funds exist in master (unfiltered)
- Name-filter for regular + growth (NOT IDCW, NOT Dividend, NOT Direct,
  NOT Segregated): ~1,456 funds
- Of those, 941 already have NAV data
- **Target: expand the ~515 currently missing to ~1,456 total**
- Do NOT chase Direct-plan funds, Dividend/IDCW variants, or non-equity.

## Mandatory step 0: inventory existing sources

Before scraping anything external, check sister project DBs for NAV data:

1. **mfpulse_reimagined DB** — the canonical MF database in the JIP ecosystem.
   Uses different Morningstar endpoint (bulk /universeid/) so has broader
   coverage. Access by:
   ```bash
   docker exec mf-pulse python -c "
   import os, psycopg2
   c = psycopg2.connect(os.environ['DATABASE_URL'])
   cur = c.cursor()
   cur.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name ILIKE '%nav%'\")
   for r in cur.fetchall(): print(r[0])
   "
   ```
2. **fie2 DB** — `mf_nav_history` exists but only has 25 funds (checked audit).
   Probably not useful here, but log what it has for completeness.
3. **AMFI daily NAV files** — the authoritative public source (amfiindia.com),
   free, no rate limits. CSV format, one file per day. Use if DB sources
   are insufficient.

## Scope
- Run step 0 inventory — produce `reports/mf_source_inventory_<date>.md`
- Write `scripts/backfill_mf_nav.py` that:
  - Queries mfpulse_reimagined's NAV history table for mstar_id → nav_date → nav
  - Matches mstar_id to JIP's de_mf_master
  - UPSERTs to `de_mf_nav_daily` with ON CONFLICT DO NOTHING
  - For funds mfpulse doesn't have, fall back to AMFI daily NAV files
    (https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx)
    parsing AMFI's semicolon-separated format
- Filter target universe TIGHTLY to equity + regular + growth only:
  ```sql
  WHERE is_active
    AND broad_category = 'Equity'
    AND NOT is_etf
    AND NOT is_index_fund
    AND (purchase_mode = 1 OR purchase_mode IS NULL)   -- Regular only, preferred
    AND fund_name !~* '\b(IDCW|Dividend|Segregated|Direct)\b'
  ```
- Run full backfill ONLY for the ~515 target-filter funds missing NAV today
- Verify coverage: ≥ 1,400 funds with NAV data matching the tight filter
  (up from current ~941)

## Acceptance criteria
- [ ] `reports/mf_source_inventory_<date>.md` documents what mfpulse and AMFI have
- [ ] `scripts/backfill_mf_nav.py` exists and is idempotent
- [ ] `SELECT COUNT(DISTINCT mstar_id) FROM de_mf_nav_daily` covering the tight filter ≥ 1,400 (up from ~941)
- [ ] Tight-filter eligibility query returns ≥ 1,400 funds with NAV
- [ ] No unnecessary non-equity-regular-growth funds fetched (respect user scope)
- [ ] Commit subject starts with `GAP-13`

## Out of scope
- Fund master reconciliation (separate task)
- Historical NAV beyond what sources provide
- Non-equity funds

## Dependencies
- Upstream: GAP-01 (purchase_mode populated first for filtering)
- Downstream: GAP-02 re-run with expanded universe
