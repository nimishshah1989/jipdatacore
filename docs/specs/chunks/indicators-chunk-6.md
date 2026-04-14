# Chunk 6 — Equity cutover: dump → drop → rename

**Complexity**: M
**Blocks**: chunk-7
**Blocked by**: chunk-5

## Goal
Execute the destructive cutover: back up the old `de_equity_technical_daily` to a compressed SQL dump, drop it, rename `de_equity_technical_daily_v2` in place. Verify `breadth.py` still works (it reads the GENERATED boolean columns which must be preserved on the new table).

## Files
- **Create**: `scripts/cutover_equity_indicators_v2.sh`
  - Bash script (run from a machine that can reach RDS — EC2 jumpbox)
  - Steps:
    1. `mkdir -p /var/backups/jip/indicators_cutover`
    2. `pg_dump --host=... --username=... --dbname=data_engine --table=public.de_equity_technical_daily --no-owner --no-privileges -Fc -f /var/backups/jip/indicators_cutover/de_equity_technical_daily_pre_v2_$(date +%Y%m%d_%H%M%S).dump`
    3. Print dump size, SHA256
    4. Prompt for explicit `yes` confirmation before destructive step
    5. Run the Alembic cutover migration
  - Safety: script refuses to run if dump size < 1 GB (sanity check on 4M-row table)
- **Create**: `alembic/versions/XXX_cutover_equity_indicators_v2.py`
  - `op.execute("DROP TABLE de_equity_technical_daily CASCADE")`
  - `op.rename_table("de_equity_technical_daily_v2", "de_equity_technical_daily")`
  - Also rename indexes: `de_equity_technical_daily_v2_pkey` → `de_equity_technical_daily_pkey`, etc.
  - Downgrade: creates table from models and warns "data not restored automatically — restore from dump"
- **Modify**: `app/models/computed.py`
  - Replace the old `DeEquityTechnicalDaily` class definition with one that matches the new schema (all ~130 cols). The class name stays the same so no imports break.
  - Keep the old class in a side file `_legacy_models.py` for a week if paranoid, but not strictly needed
- **Modify**: `app/models/indicators_v2.py`
  - Remove `DeEquityTechnicalDailyV2` class (no longer a v2 twin)
  - Re-export `DeEquityTechnicalDaily` from `app/models/computed.py` under the old name

## Execution runbook (for the builder)
1. **Precondition check**: `diff_technicals_old_vs_new.py --asset equity --last-days 30` must exit 0. Do NOT proceed otherwise.
2. Run `scripts/cutover_equity_indicators_v2.sh` from EC2 jumpbox
3. After rename, run:
   - `pytest tests/computation/test_breadth.py -v` — must pass
   - `pytest tests/computation/test_indicators_v2_golden.py -v` — must pass
   - `SELECT COUNT(*), MAX(date) FROM de_equity_technical_daily;` — matches expected ≈ 8M rows
   - `SELECT above_50dma, above_200dma FROM de_equity_technical_daily WHERE date = (SELECT MAX(date) FROM de_equity_technical_daily) LIMIT 5;` — returns non-null booleans
4. Hit the observatory dashboard: `curl https://data.jslwealth.in/api/v1/observatory/pulse` — should be green
5. If anything breaks: `pg_restore` from the dump file back into `de_equity_technical_daily_old`, investigate, fix, retry

## Acceptance criteria
- `pg_dump` file exists, is >1 GB compressed, SHA256 recorded in `reports/cutover_equity_{date}.log`
- Migration applied cleanly
- `de_equity_technical_daily` now has ~130 columns (was 40)
- `test_breadth.py` passes — confirms GENERATED columns survived
- `test_runner.py` passes with no code changes
- Observatory dashboard green
- `scripts/diff_technicals_old_vs_new.py` no longer applicable for equity (no v2 table to diff), but script should handle this gracefully (exit with message "no v2 table found — cutover complete")

## Rollback plan
If breadth or any downstream read breaks after rename:
```bash
# From EC2
psql -h ... -c "ALTER TABLE de_equity_technical_daily RENAME TO de_equity_technical_daily_broken;"
pg_restore -h ... -d data_engine /var/backups/jip/indicators_cutover/de_equity_technical_daily_pre_v2_*.dump
```
Keep the dump file for **30 days minimum** before archival.

## Verification commands
```bash
bash scripts/cutover_equity_indicators_v2.sh
pytest tests/computation/test_breadth.py tests/computation/test_runner.py -v
curl -s https://data.jslwealth.in/api/v1/observatory/pulse | jq
```
