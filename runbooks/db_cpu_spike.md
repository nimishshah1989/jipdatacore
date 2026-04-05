---
title: Database CPU Spike
severity: critical
oncall: infra
---

# Runbook: Database CPU Spike

## Symptoms
- RDS CloudWatch: CPU > 80% for > 5 minutes
- Slow API responses (> 2s latency on normally fast endpoints)
- Pipeline runs stalling (waiting for DB lock/query)
- Alert: `db_cpu_high` in CloudWatch

## Immediate Actions (< 5 minutes)

1. Identify top queries:
   ```sql
   SELECT pid, now() - pg_stat_activity.query_start AS duration,
          state, query, wait_event_type, wait_event
   FROM pg_stat_activity
   WHERE state != 'idle'
     AND query_start < NOW() - INTERVAL '30 seconds'
   ORDER BY duration DESC
   LIMIT 20;
   ```

2. Check for lock contention:
   ```sql
   SELECT pid, usename, pg_blocking_pids(pid) AS blocked_by, query
   FROM pg_stat_activity
   WHERE cardinality(pg_blocking_pids(pid)) > 0;
   ```

3. Check index hit rate:
   ```sql
   SELECT sum(idx_blks_hit) / NULLIF(sum(idx_blks_hit) + sum(idx_blks_read), 0) AS idx_hit_rate
   FROM pg_statio_user_tables;
   -- Should be > 0.95; if lower, consider VACUUM ANALYZE
   ```

## Investigation

### If long-running query:
```sql
-- Cancel a specific query (safe)
SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE pid = <PID>;

-- Terminate a connection (last resort)
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid = <PID>;
```

### If bulk insert from pipeline causing sequential scan:
- Check if `de_equity_eod` needs VACUUM:
  ```sql
  SELECT relname, n_dead_tup, n_live_tup, last_vacuum, last_autovacuum
  FROM pg_stat_user_tables
  WHERE relname IN ('de_equity_eod', 'de_mf_nav', 'de_pipeline_log')
  ORDER BY n_dead_tup DESC;
  ```

### If connection pool exhausted:
```bash
# Check PgBouncer stats
psql -h localhost -p 6432 -U pgbouncer pgbouncer -c "SHOW POOLS;"
```

## Mitigation

1. Pause non-critical pipelines:
   ```sql
   UPDATE de_system_flags SET value = false WHERE key = 'pipeline_qualitative_rss_enabled';
   ```

2. Increase RDS instance temporarily (via AWS Console)
   - Minimum: db.r5.large (2 vCPU, 16 GB)
   - Upgrade: db.r5.xlarge for sustained load

3. Kill all idle-in-transaction connections:
   ```sql
   SELECT pg_terminate_backend(pid)
   FROM pg_stat_activity
   WHERE state = 'idle in transaction'
     AND query_start < NOW() - INTERVAL '5 minutes';
   ```

## Escalation
- CPU > 95% for > 10 minutes → escalate to infra lead
- If RDS failover triggered → notify all platform leads immediately

## Recovery Verification
```bash
# Check RDS CPU via AWS CLI
aws cloudwatch get-metric-statistics \
  --namespace AWS/RDS \
  --metric-name CPUUtilization \
  --dimensions Name=DBInstanceIdentifier,Value=fie-db \
  --start-time $(date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 60 \
  --statistics Average
```
