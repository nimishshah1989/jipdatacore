# Chunk 9 — purchase_mode bootstrap + Morningstar ingestion fix

**Complexity**: M
**Blocks**: chunk-10
**Blocked by**: chunk-2

## Goal
Populate the `de_mf_master.purchase_mode` column (added in chunk 2) by bootstrapping from the mfpulse_reimagined database, and fix the JIP Morningstar ingestion to capture this field on every weekly refresh so the column stays current.

## Files
- **Create**: `scripts/bootstrap_purchase_mode_from_mfpulse.py`
  - Reads `MFPULSE_DATABASE_URL` from env (new var — document in `.env.example`)
  - Connects to mfpulse Postgres via sync psycopg2 (one-off script, no async needed)
  - `SELECT mstar_id, purchase_mode FROM fund_master WHERE purchase_mode IS NOT NULL` → dict
  - Batches UPDATE into JIP's `de_mf_master`: `UPDATE de_mf_master SET purchase_mode = :pm, updated_at = now() WHERE mstar_id = :mid`
  - Reports: `funds_in_mfpulse`, `funds_matched_jip`, `funds_updated`, `funds_unmatched`
  - Writes audit log to `reports/purchase_mode_bootstrap_{date}.log`
  - Idempotent — running twice is safe
- **Modify**: `app/pipelines/morningstar/` (exact file TBD — builder must first locate the OperationsMasterFile handler via `grep -r OperationsMasterFile app/pipelines/morningstar/`)
  - If OperationsMasterFile is already fetched: add `purchase_mode` to the field extraction and the `de_mf_master` upsert
  - If OperationsMasterFile is NOT fetched: **STOP and flag to user**. This becomes a scope extension — may need a new Morningstar API endpoint integration (chunk 9a/9b split). Do not silently add API calls without approval.
- **Create**: `.env.example` entry
  - `MFPULSE_DATABASE_URL=postgresql://user:pass@host:5432/mfpulse_db` (with comment)

## Network reachability check (pre-flight)
Before coding, verify from the EC2 jumpbox:
```bash
psql "$MFPULSE_DATABASE_URL" -c "SELECT COUNT(*) FROM fund_master WHERE purchase_mode IS NOT NULL"
```

If mfpulse DB is on a different RDS / VPC / account and not reachable from JIP EC2:
- **Fallback**: run the bootstrap from local dev with SSH tunnels to both DBs, or export CSV from mfpulse and upload to JIP EC2 to load via `\copy`
- Document the chosen path in the script header

## Expected results after bootstrap
```sql
-- Before
SELECT COUNT(*) FROM de_mf_master WHERE purchase_mode IS NOT NULL;
-- 0

-- After
SELECT COUNT(*), purchase_mode FROM de_mf_master WHERE purchase_mode IS NOT NULL GROUP BY purchase_mode;
-- Expect: roughly equal split of 1s and 2s, total ~13,380 minus any NULL in mfpulse

-- Eligibility count
SELECT COUNT(*) FROM de_mf_master m
WHERE m.purchase_mode = 1
  AND m.broad_category = 'Equity'
  AND m.is_active AND NOT m.is_etf AND NOT m.is_index_fund
  AND m.fund_name !~* '\b(IDCW|Dividend|Segregated)\b'
  AND EXISTS (SELECT 1 FROM de_mf_nav_daily n WHERE n.mstar_id = m.mstar_id);
-- Expect: ~800 funds
```

## Acceptance criteria
- Bootstrap script runs cleanly, reports match expected
- `SELECT COUNT(DISTINCT purchase_mode) FROM de_mf_master` returns 2 (values 1 and 2)
- Eligibility query returns ~800 (±50) funds
- If Morningstar ingestion modified: next scheduled run captures `purchase_mode` for new/updated funds
- If Morningstar ingestion lacks OperationsMasterFile: ticket filed, chunk 10 can proceed with bootstrapped-but-stale data
- `pytest tests/` all green
- `ruff`, `mypy` clean

## Verification commands
```bash
python scripts/bootstrap_purchase_mode_from_mfpulse.py
psql -h ... -c "SELECT COUNT(*), purchase_mode FROM de_mf_master GROUP BY purchase_mode"
```
