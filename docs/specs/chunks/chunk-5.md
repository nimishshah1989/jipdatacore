# Chunk 5: Data Migrations

**Layer:** 2
**Dependencies:** C2
**Complexity:** High
**Status:** done

## Files

- `app/migrations/__init__.py`
- `app/migrations/equity_ohlcv.py`
- `app/migrations/mf_nav.py`
- `app/migrations/mf_master.py`
- `app/migrations/mf_holdings.py`
- `app/migrations/index_constituents.py`
- `app/migrations/client_data.py`
- `app/migrations/runner.py`
- `tests/migrations/test_equity_ohlcv.py`
- `tests/migrations/test_mf_nav.py`
- `tests/migrations/test_mf_master.py`

## Acceptance Criteria

- [ ] **Equity OHLCV:** Migrate 1.4M rows from `fie_v3.compass_stock_prices` to `de_equity_ohlcv`; fix VARCHAR→DATE for date column; fix DOUBLE→NUMERIC(18,4) for price/volume columns; map ticker → `instrument_id` via `de_instrument`
- [ ] **MF NAV:** Migrate ~5M rows from `fie2-db-1.mf_pulse.nav_daily` (filtered from 25.8M total); filter criteria: equity category + `Accumulated` (Growth) distribution + `Regular` plan only; map `mstar_id` FK
- [ ] **MF Master:** Migrate 13,380 records from `fie2-db-1.mf_pulse.fund_master` to `de_mf_master`; map all 44 Morningstar columns (Name, ISIN, CategoryName, BroadCategoryGroup, NetExpenseRatio, ManagerName, TotalNetAssets, primary_benchmark, etc.)
- [ ] **MF Holdings:** Migrate 2M+ rows from `fie2-db-1.mf_pulse.fund_holding_detail` to `de_mf_holdings`; include sector/weight_pct
- [ ] **Index Constituents:** Migrate 4,638 rows from `fie_v3.index_constituents` to `de_index_constituents`; map ticker → `instrument_id`
- [ ] **Client Data:** Migrate `client_portal.cpp_*` tables (366K rows); encrypt PAN/phone/email using envelope encryption before insert; compute HMAC blind indexes (8-char truncated)
- [ ] Migration logging: every migration writes to `de_migration_log` (source_db, source_table, target_table, rows_read, rows_written, rows_errored, status, checksum_source, checksum_dest)
- [ ] Migration errors: per-row errors written to `de_migration_errors` with source row (JSONB) and error_reason
- [ ] Validation gate 1: `rows_written >= rows_read * 0.999` (allow 0.1% error rate)
- [ ] Validation gate 2: MIN and MAX date in destination within 1 day of source
- [ ] Validation gate 3: 500 random rows spot-checked — values match source exactly
- [ ] Validation gate 4: no `nav <= 0` in `de_mf_nav_daily`
- [ ] Validation gate 5: no future dates in any migrated table
- [ ] Validation gate 6: all `instrument_id` FKs resolve — no orphan price rows
- [ ] Validation gate 7: all `mstar_id` FKs resolve — no orphan NAV rows
- [ ] Validation gate 8: all return columns accept values >100.00 (NUMERIC(10,4) confirmed)
- [ ] Migration runner (`runner.py`) is idempotent — can be re-run without creating duplicates (uses ON CONFLICT)
- [ ] Pre-migration backup commands documented in runner (commented out, for human to run manually)
- [ ] All INSERTs use `ON CONFLICT DO NOTHING` or `DO UPDATE` on natural keys

## Notes

**Source databases:**
- `fie_v3` on RDS `fie-db.c7osw6q6kwmw.ap-south-1.rds.amazonaws.com` — equity OHLCV, index constituents
- `fie2-db-1` Docker container on EC2 — MF data (`mf_pulse` database)
- `client_portal` on RDS — client PII

**fie2-db-1 connection:** Read via Docker exec or port-forwarded Postgres connection. The container has the `mf_pulse` database containing all MF source data. The legacy `mf_engine` database is deprecated — do not use it.

**MF NAV filter (25.8M → ~5M):** Keep only rows where the fund matches: equity broad category + `Accumulated` in distribution_status + `Regular` in plan_name. Drop rows with `nav <= 0`.

**Equity OHLCV type corrections:**
- `trade_date`: VARCHAR in source → DATE in target (parse format `YYYY-MM-DD`)
- `close_price`, `open_price`, `high_price`, `low_price`: DOUBLE PRECISION in source → NUMERIC(18,4)
- Set `data_status = 'validated'` for all migrated rows (they are pre-validated historical data)
- Set `source_file_id = NULL`, `pipeline_run_id = NULL` (no lineage for migrated data)

**Client data encryption:** Must use the envelope encryption flow from spec Section 3.7.1 — generate per-client DEK, encrypt with KMS CMK, store encrypted DEK in `de_client_keys`, then encrypt PAN/phone/email fields using DEK + AES-256-GCM. Compute truncated HMAC blind indexes.

**Migration batching:** Process in batches of 10,000 rows to avoid memory pressure and allow incremental progress tracking in `de_migration_log`.

**Pre-migration backups (for human to run before migration):**
```bash
# MF NAV
docker exec fie2-db-1 pg_dump -U fie mf_pulse -t nav_daily --no-owner --no-privileges -f /home/ubuntu/nav_daily_backup.sql

# Equity prices
PGPASSWORD=... pg_dump -h fie-db... -U fie_admin -d fie_v3 -t compass_stock_prices --no-owner --no-privileges -f /home/ubuntu/compass_stock_prices_backup.sql
```
