# GAP-09 — Sector mapping reconciliation table

## Goal
Create a formal mapping between JIP's 31 internal stock sector names
(Banking, IT, Pharma, Automobile...) and NSE sectoral index codes
(NIFTY BANK, NIFTY IT, NIFTY PHARMA, NIFTY AUTO...). Without this table,
Atlas cannot programmatically answer "what are the technicals of my stock's
sector index".

## Scope
- Alembic migration 013: new table `de_sector_mapping`
    jip_sector_name VARCHAR(50) PRIMARY KEY
    primary_nse_index VARCHAR(50) NOT NULL
      REFERENCES de_index_master(index_code)
    secondary_nse_indices TEXT[]  -- for sectors that map to multiple indices
    notes TEXT
    created_at, updated_at TIMESTAMPTZ DEFAULT NOW()
- Alembic migration 013 also inserts 31 rows covering every distinct
  JIP sector name from `SELECT DISTINCT sector FROM de_instrument WHERE is_active AND sector IS NOT NULL`
- Hand-author the mapping with comments explaining any ambiguous cases
  (e.g. "Financial Services" → NIFTY FIN SERVICE + NIFTY FINSEREXBNK)
- Verify with a cross-check query: every JIP sector has a mapping row

## Acceptance criteria
- [ ] Migration 013 applied to prod
- [ ] `SELECT COUNT(*) FROM de_sector_mapping` = 31 (or = distinct JIP sectors)
- [ ] `SELECT jip_sector_name FROM (SELECT DISTINCT sector FROM de_instrument WHERE is_active AND sector IS NOT NULL) s LEFT JOIN de_sector_mapping m ON m.jip_sector_name = s.sector WHERE m.jip_sector_name IS NULL` returns 0 rows
- [ ] Every primary_nse_index FK resolves to a row in de_index_master
- [ ] Commit subject starts with `GAP-09`
- [ ] `state.db` shows `GAP-09` with `status='DONE'`

## Steps for the inner session
1. Query prod for distinct JIP sector names
2. Map each to the corresponding NSE index code manually
3. Write migration 013 with CREATE TABLE + INSERT rows
4. Apply to prod
5. Run verification queries
6. Commit

## Out of scope
- Backfilling sector-level technicals (Atlas consumes via JOIN at read time)
- Changing the stock sector taxonomy itself
- Historical tracking of sector reclassifications

## Dependencies
- Upstream: none
- Downstream: none
