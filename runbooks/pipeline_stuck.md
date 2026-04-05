---
title: Pipeline Stuck (status=running for > 30 minutes)
severity: warning
oncall: data-engineering
---

# Runbook: Pipeline Stuck

## Symptoms
- `de_pipeline_log` shows status=running for > 30 minutes for a pipeline
- No recent log output from the pipeline
- Advisory lock is held — prevents retry

## Immediate Actions (< 5 minutes)

1. Identify stuck pipelines:
   ```sql
   SELECT pipeline_name, business_date, run_number, status,
          started_at,
          NOW() - started_at AS running_duration
   FROM de_pipeline_log
   WHERE status = 'running'
     AND started_at < NOW() - INTERVAL '30 minutes'
   ORDER BY started_at ASC;
   ```

2. Check if the process is actually alive:
   ```bash
   docker exec jip-data-engine ps aux | grep python
   ```

3. Check logs for last activity:
   ```bash
   docker logs jip-data-engine --since 30m | grep <pipeline_name>
   ```

## Investigation

### If process is alive but making no progress:
- Could be waiting for external API (NSE, AMFI, yfinance)
- Check network connectivity:
  ```bash
  docker exec jip-data-engine curl -I https://www.nseindia.com
  ```
- If external API is down → kill pipeline, set up retry

### If process is dead but status=running:
- Crash occurred without cleanup — advisory lock is still held
- Must manually reset:

  ```sql
  -- Reset status to failed so next run can proceed
  UPDATE de_pipeline_log
  SET status = 'failed',
      completed_at = NOW(),
      error_detail = 'Manual reset: process died without cleanup'
  WHERE pipeline_name = '<pipeline_name>'
    AND business_date = '<date>'
    AND status = 'running';
  ```

  The advisory lock will be released automatically when the PostgreSQL session closes (process died).

## Recovery

1. After resetting status, trigger manual retry:
   ```bash
   curl -X POST http://localhost:8010/api/v1/pipelines/<name>/trigger \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -d '{"business_date": "2026-04-05", "force": true}'
   ```

2. Monitor the new run:
   ```sql
   SELECT status, rows_processed, error_detail
   FROM de_pipeline_log
   WHERE pipeline_name = '<pipeline_name>'
     AND business_date = CURRENT_DATE
   ORDER BY run_number DESC LIMIT 3;
   ```

## Prevention
- All pipelines should have HTTP timeouts configured (max 5 minutes)
- If this happens repeatedly for the same pipeline, add timeout enforcement in execute()

## Escalation
- If stuck pipeline is blocking Track A (nse_bhav, nse_indices) → critical → page immediately
- If non-critical pipeline → warning, can wait for manual investigation
