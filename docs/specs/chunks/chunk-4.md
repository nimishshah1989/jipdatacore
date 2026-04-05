# Chunk 4: Pipeline Framework

**Layer:** 2
**Dependencies:** C2
**Complexity:** High
**Status:** done

## Files

- `app/pipelines/__init__.py`
- `app/pipelines/framework.py`
- `app/pipelines/guards.py`
- `app/pipelines/validation.py`
- `app/pipelines/registry.py`
- `tests/pipelines/test_framework.py`
- `tests/pipelines/test_guards.py`
- `tests/pipelines/test_validation.py`

## Acceptance Criteria

- [ ] `acquire_pipeline_lock(pipeline_name, business_date, conn)` acquires a session-level `pg_advisory_lock` using `hashtext()` in Postgres for deterministic lock ID (NOT Python `hash()`)
- [ ] Lock uses `pg_try_advisory_lock` first (non-blocking); detects stale locks from dead sessions and force-releases them
- [ ] `release_pipeline_lock(lock_id, conn)` always called in `finally` block — no lock leaks
- [ ] Pipeline raises `PipelineLockError` if another live process holds the lock
- [ ] Trading calendar check: pipeline exits with `holiday_skip` status if `de_trading_calendar.is_trading = FALSE` for `business_date`
- [ ] System flags check: pipeline exits gracefully if `de_system_flags.INGESTION_ENABLED = FALSE`; logs `kill_switch` event
- [ ] Every pipeline run creates a `de_pipeline_log` entry with status `running` at start; updates to `complete`, `failed`, or `partial` on exit
- [ ] Source file registration: before processing any file, compute SHA-256 checksum, insert into `de_source_files` with `ON CONFLICT DO NOTHING`; capture `source_file_id` for lineage
- [ ] Freshness validation: source report date extracted and verified == expected business_date; rowcount >= minimum (BHAV: >500, AMFI: >1000); stale/future date anomaly logged
- [ ] Checksum deduplication: if file checksum already in `de_source_files`, skip processing (idempotent re-run)
- [ ] Post-ingestion anomaly detection framework: price spike, volume spike, NAV spike, zero NAV, negative value, future date, stale data — all write to `de_data_anomalies`
- [ ] Data status gating (Section 5.12): after validation, batch-promote `raw → validated` for clean rows; batch-promote `raw → quarantined` for critical anomaly rows
- [ ] Quarantine threshold guardrail (v1.9.1): if >5% of active tradeable instruments are quarantined for the business date, halt aggregate computations (breadth, RS, regime) and alert
- [ ] `de_pipeline_log.track_status` JSONB updated per-track (equity, mf, indices, flows, fo)
- [ ] Unit tests cover: lock acquisition, stale lock recovery, holiday skip, kill switch, checksum dedup, anomaly detection, quarantine threshold

## Notes

**Advisory lock implementation (v1.7 — exact spec):**
```python
lock_id = conn.execute(
    "SELECT hashtext(:key) & 2147483647",
    {"key": f"{pipeline_name}:{business_date.isoformat()}"}
).scalar()
```
The `& 2147483647` masks to positive 32-bit integer range. Python `hash()` is NOT deterministic across processes — always use Postgres `hashtext()`.

**Session-level vs transaction-level:** Use `pg_advisory_lock` (session-level), not `pg_advisory_xact_lock` (transaction-level). Session-level persists across multiple transactions within the pipeline run — required because pipelines commit in batches.

**Independent tracks (v1.8):** EOD pipeline runs 5 tracks (A: equity, B: MF, C: indices+VIX, D: flows, E: F&O) in parallel. Failure of one track does NOT block others. Pipeline status is `partial` if any track fails.

**Anomaly severity rules:**
- `critical`: blocks downstream computation for that instrument; promotes to quarantined
- `warning`: logged, reviewed in admin dashboard, pipeline continues
- `info`: logged only

**Data status flow:**
1. INSERT with `data_status = 'raw'`
2. Run validation rules (Section 5.11)
3. If no critical anomalies: `UPDATE data_status = 'validated'`
4. If critical anomaly: `UPDATE data_status = 'quarantined'`

**Quarantine storm (v1.9.1):** If `quarantined_count / active_tradeable_count > 0.05`, halt Steps 10-14 (breadth, RS, regime). Log reason. API returns `X-Data-Freshness: partial`. This prevents poisoned metrics from being permanently recorded (e.g., bad UDiFF parser truncating small caps).

**NSE Saturday sessions:** Trading calendar must include ad-hoc Saturday "Special Live Trading Sessions" (NSE DR site testing). Orchestrator cron must NOT restrict to Mon-Fri — always check `de_trading_calendar`.
