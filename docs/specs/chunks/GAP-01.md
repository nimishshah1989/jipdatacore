# GAP-01 — Bootstrap de_mf_master.purchase_mode from mfpulse_reimagined

## Goal
Populate `de_mf_master.purchase_mode` (currently NULL for all 13,380 funds) by reading
from the mfpulse_reimagined production DB that lives on the same EC2 host. This
unblocks the MF technicals eligibility filter (IND-C10 / GAP-02) without needing
to rewrite JIP's broken Morningstar client.

## Scope
- Create `scripts/bootstrap_purchase_mode_from_mfpulse.py` (new): reads
  `fund_master.mstar_id, purchase_mode` from mfpulse's Postgres, UPDATEs JIP's
  `de_mf_master.purchase_mode` via batched SQL. Idempotent. Skips NULLs.
- Discover mfpulse DB URL by reading `/home/ubuntu/mfpulse_reimagined/.env` on EC2.
  Do not hardcode credentials into the script — read from env var
  `MFPULSE_DATABASE_URL_SYNC`.
- Execute the script once from the EC2 host against production RDS.
- Produce `reports/purchase_mode_bootstrap_<timestamp>.md` with before/after
  counts of populated funds broken down by purchase_mode value.

## Acceptance criteria
- [ ] Script exists and is invocable with `python scripts/bootstrap_purchase_mode_from_mfpulse.py`
- [ ] `SELECT COUNT(*) FROM de_mf_master WHERE purchase_mode IS NOT NULL` ≥ 13,000
- [ ] `SELECT purchase_mode, COUNT(*) FROM de_mf_master GROUP BY purchase_mode` shows both 1 and 2
- [ ] Eligibility query `WHERE purchase_mode=1 AND broad_category='Equity' AND NOT is_etf AND NOT is_index_fund AND EXISTS(SELECT 1 FROM de_mf_nav_daily WHERE mstar_id=de_mf_master.mstar_id)` returns ~800 funds
- [ ] Commit subject starts with `GAP-01`
- [ ] `state.db` shows `GAP-01` with `status='DONE'`
- [ ] Tests still green: `docker run --rm -v $(pwd)/tests:/app/tests:ro jip-data-engine:ind-c11 python -m pytest tests/computation/test_indicators_v2_engine.py tests/computation/test_indicators_v2_golden.py -q`

## Steps for the inner session
1. SSH to EC2 (13.206.34.214 via `~/.ssh/jsl-wealth-key.pem`), read `/home/ubuntu/mfpulse_reimagined/.env` for DATABASE_URL_SYNC
2. Write `scripts/bootstrap_purchase_mode_from_mfpulse.py` using psycopg2 sync (not async — one-off script)
3. Test on a small subset (--limit 10) first, verify both DBs are reachable
4. Run full bootstrap against prod RDS from EC2
5. Verify counts + produce report
6. Commit the script + report

## Out of scope
- Modifying JIP's Morningstar client (deferred)
- Fixing `fund_master.py` to fetch PurchaseMode natively
- Running the MF technical backfill — that's GAP-02

## Dependencies
- Upstream: none
- Downstream: GAP-02
