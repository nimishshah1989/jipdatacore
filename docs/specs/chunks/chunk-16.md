# Chunk 16: Orchestrator + Monitoring + Operational Readiness

**Layer:** 5
**Dependencies:** C7, C8, C9, C11
**Complexity:** High
**Status:** pending

## Files

- `app/orchestrator/__init__.py`
- `app/orchestrator/dag.py`
- `app/orchestrator/scheduler.py`
- `app/orchestrator/sla.py`
- `app/orchestrator/alerts.py`
- `app/orchestrator/reconciliation.py`
- `app/orchestrator/retry.py`
- `prometheus/prometheus.yml`
- `runbooks/nse_bhav_delayed.md`
- `runbooks/amfi_nav_delayed.md`
- `runbooks/db_cpu_spike.md`
- `runbooks/redis_down.md`
- `runbooks/pipeline_stuck.md`
- `runbooks/corporate_action_storm.md`
- `runbooks/disk_full.md`
- `runbooks/data_corruption.md`
- `tests/orchestrator/test_dag.py`
- `tests/orchestrator/test_sla.py`
- `tests/orchestrator/test_reconciliation.py`

## Acceptance Criteria

### Central Orchestrator (DAG Execution)

- [ ] **DAG execution:** Dependency graph from spec Section 7 enforced at runtime — not just documented
- [ ] **State machine per run:** pending → running → complete / failed / partial
- [ ] **Conditional branching:** If Track A (equity) fails, skip RS/regime (Steps 12-14) but continue Tracks B-E; if Track B (MF) fails, continue other tracks
- [ ] **Retry policies:**
  - Transient failures (network, API rate limit, HTTP 429/503): retry 3× with exponential backoff (1min/5min/15min)
  - Persistent failures (bad data, schema mismatch, parse error): fail immediately, do not retry
- [ ] **Crash recovery:** `--resume` flag reloads last incomplete run from `de_pipeline_log` and continues from last successful step
- [ ] **Startup pre-flight checks (v1.7):**
  - ClamAV daemon: `clamdscan --version`; restart if down; raise if still fails after restart
  - Redis: `redis.ping()`
  - RDS: `SELECT 1`

### Cron Schedule

- [ ] Pre-Market pipeline: 07:30 IST (`30 7 * * *` or trading day check)
- [ ] T+1 Delivery: 09:00 IST (`0 9 * * *`)
- [ ] EOD pipeline: 18:30 IST (`30 18 * * *`)
- [ ] RS computation: after EOD completes (triggered, not cron)
- [ ] Regime update: after RS completes (triggered, not cron)
- [ ] Reconciliation: 23:00 IST (`0 23 * * *`)
- [ ] Qualitative: every 30 minutes (`*/30 * * * *`)
- [ ] Full RS rebuild: Sunday 02:00 IST (`0 2 * * 0`)
- [ ] Morningstar master refresh: Sunday (`0 4 * * 0`)
- [ ] Morningstar holdings: 1st of month (`0 3 1 * *`)
- [ ] Recompute worker: every 15 min during 22:00–06:00 IST
- [ ] SLA enforcement check: every 15 minutes

### SLA Enforcement

- [ ] SLA deadlines checked every 15 minutes; if now > deadline AND pipeline not `complete`/`holiday_skip`:
  - INSERT `de_pipeline_log` entry with `status='sla_breach'`
  - Send Slack webhook to `#jip-alerts`: "SLA BREACH: {pipeline} not complete by {deadline}"
  - Send email to admin
- [ ] Slack webhook URL from AWS Secrets Manager (`SLACK_WEBHOOK_URL`)

### Prometheus Metrics

- [ ] `prometheus-fastapi-instrumentator` installed and configured
- [ ] `/metrics` endpoint exposed (unauthenticated, localhost only)
- [ ] Custom metrics tracked:
  - Pipeline: `pipeline_duration_seconds` (histogram, labels: pipeline_name, status)
  - Pipeline: `pipeline_rows_ingested_total` (counter, labels: pipeline_name)
  - Pipeline: `anomaly_count_total` (counter, labels: severity, entity_type)
  - API: `api_request_duration_seconds` (p50/p95/p99 via histogram)
  - API: `redis_cache_hits_total` / `redis_cache_misses_total`
  - Business: `data_freshness_lag_seconds` (gauge — seconds since last validated row)
  - Business: `quarantine_rate` (gauge — % of universe quarantined)
- [ ] `prometheus.yml` configured to scrape localhost:8010/metrics

### Reconciliation Pipeline (v1.9, daily 23:00 IST)

- [ ] **Check 1 (NSE vs yfinance):** For each NIFTY 50 constituent, compare NSE close vs yfinance close for today; if `abs(nse - yf) / nse > 0.02` (2% tolerance): INSERT `de_data_anomalies (anomaly_type='cross_source_mismatch', severity='warning')`
- [ ] **Check 2 (AMFI NAV vs backup):** For top 50 funds by AUM, compare AMFI NAV vs Morningstar/BSE Star MF; tolerance 0.1%
- [ ] **Check 3 (row count sanity):** equity validated count for today < 1000 → `severity='critical'`; MF validated count for today < 5000 → `severity='warning'`
- [ ] Results logged in `de_pipeline_log (pipeline_name='reconciliation')`

### Operational Runbooks (8 files in `/runbooks/`)

- [ ] `nse_bhav_delayed.md`: Track A fails at Step 1 past 19:30; check NSE site manually; if file available trigger replay; if NSE down mark `holiday_skip` and alert
- [ ] `amfi_nav_delayed.md`: Track B fails at Step 5 past 22:30; retry manually; if delayed >24h use previous NAV with stale flag
- [ ] `db_cpu_spike.md`: CloudWatch alert; identify slow query via `pg_stat_activity`; kill if recompute; check VACUUM; scale RDS if persistent
- [ ] `redis_down.md`: Circuit breaker open; verify `redis-cli ping`; restart `sudo systemctl restart redis`; if persistent operate in DB-only mode
- [ ] `pipeline_stuck.md`: `status='running'` for >2 hours; check `pg_locks` for held advisory locks; check worker process; force-release stale lock per Section 5.0
- [ ] `corporate_action_storm.md`: >50 recompute queue items pending; verify legitimacy (bulk ex-date?); increase batch window if legitimate; quarantine and investigate if error
- [ ] `disk_full.md`: EC2 >90% usage; `docker system prune -af`; check qualitative processing folder; verify S3 archival running
- [ ] `data_corruption.md`: Cross-source mismatch in reconciliation; identify source; quarantine affected rows; replay from correct source; document in `de_data_anomalies`

### Cost Controls

- [ ] Claude API: 200 docs/day cap, ~$50/day limit
- [ ] OpenAI embeddings: 200 docs/day
- [ ] Whisper API: 10 files/day
- [ ] Recompute queue throttling: max 2 concurrent workers, 50k rows/batch
- [ ] Log API call counts and estimated costs in `de_pipeline_log.track_status` JSONB

### VACUUM Strategy (v1.9)

- [ ] Configure aggressive autovacuum for high-churn tables via Alembic migration or RDS parameter group:
  - `de_equity_ohlcv`: `autovacuum_vacuum_scale_factor = 0.05`, `autovacuum_analyze_scale_factor = 0.02`
  - `de_mf_nav_daily`: same settings
  - `de_rs_daily_summary`: same settings
- [ ] Slow query logging: `log_min_duration_statement = 1000` in RDS parameter group (queries > 1 second logged)

### Docker Deployment (EC2)

- [ ] `Dockerfile` builds production image
- [ ] `docker-compose.yml` configures: FastAPI (port 8010), orchestrator dashboard (port 8099)
- [ ] Environment variables loaded from `.env` (except secrets — those from AWS Secrets Manager at startup)
- [ ] Health check configured in Dockerfile: `HEALTHCHECK --interval=30s CMD curl -f http://localhost:8010/health || exit 1`

## Notes

**Orchestrator implementation choice:** Spec mentions Prefect, Dagster, or custom `orchestrator.py + task_registry.json`. Given the project's Python-native approach, a lightweight custom orchestrator using `asyncio` task groups is sufficient for v1. Avoid Prefect/Dagster complexity unless the custom approach proves insufficient during sprint.

**Slack alerts channel:** `#jip-alerts`. Webhook URL in AWS Secrets Manager as `SLACK_WEBHOOK_URL`.

**Chaos testing (Section 12.6 of spec — pre-production QA):**
| Test | How to simulate | Expected behavior |
|------|----------------|-------------------|
| NSE file missing | Delete BHAV file before Step 1 | Track A fails, B-E continue |
| Partial BHAV file | Truncate to 50 rows | Freshness check rejects (rowcount < 500) |
| Redis down | `sudo systemctl stop redis` | Circuit breaker opens, DB fallback |
| Worker crash mid-recompute | `kill -9` process | Heartbeat stale after 15min, orchestrator resets |
| Quarantine storm | Inject 500 rows with price = -1 | Step 9.9 halts aggregates, admin alerted |
| Kill switch | `UPDATE de_system_flags SET value = FALSE WHERE key = 'INGESTION_ENABLED'` | All pipelines exit gracefully |

**EC2 deploy target:** `13.206.34.214`. Port 8010 internal only. Dashboard on `127.0.0.1:8099` (SSH tunnel). No public exposure.

**Estimated DB size post-build:** 25-30 GB on RDS. CloudWatch alerts: CPU > 80%, storage < 20GB free, connections > 80.
