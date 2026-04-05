---
title: Corporate Action Storm (Large Volume)
severity: warning
oncall: data-engineering
---

# Runbook: Corporate Action Storm

## What Is This
On dividend record dates, bonus/split announcements, or index rebalance days,
the `nse_corporate_actions` pipeline may process 5-10x normal volume.
This can cause: slow ingestion, high DB CPU, RS computation delays.

## Symptoms
- `nse_corporate_actions` pipeline takes > 30 minutes (normal: 5 min)
- `rows_processed` > 5000 (normal: 200-500)
- RS computation SLA breached
- Anomaly rate elevated (bonus splits cause price discontinuities)

## Immediate Actions

1. Check row volume for today:
   ```sql
   SELECT COUNT(*) FROM de_corporate_actions
   WHERE action_date = CURRENT_DATE;
   ```

2. Check pipeline timing:
   ```sql
   SELECT run_number, rows_processed, rows_failed,
          EXTRACT(EPOCH FROM (completed_at - started_at)) AS duration_sec
   FROM de_pipeline_log
   WHERE pipeline_name = 'nse_corporate_actions'
   ORDER BY started_at DESC LIMIT 5;
   ```

3. Check anomaly count:
   ```sql
   SELECT COUNT(*) FROM de_anomalies
   WHERE pipeline_name = 'nse_corporate_actions'
     AND detected_date = CURRENT_DATE;
   ```

## Investigation

### High anomaly count on split/bonus days (EXPECTED):
- Price discontinuities after splits are NOT bugs — they are correct
- If anomaly count > 500, check if RS computation quarantine was triggered
- Check `de_system_flags` for `quarantine_rs_computation`:
  ```sql
  SELECT key, value FROM de_system_flags
  WHERE key LIKE '%quarantine%';
  ```

### If RS quarantined due to anomaly threshold:
- Review the anomalies manually:
  ```sql
  SELECT symbol, anomaly_type, expected_value, actual_value
  FROM de_anomalies
  WHERE pipeline_name = 'nse_corporate_actions'
    AND detected_date = CURRENT_DATE
  LIMIT 20;
  ```
- If all anomalies are expected (splits/bonuses) → manually clear quarantine:
  ```sql
  UPDATE de_system_flags SET value = false
  WHERE key = 'quarantine_rs_computation';
  ```
- Then re-trigger RS:
  ```bash
  curl -X POST http://localhost:8010/api/v1/pipelines/relative_strength/trigger \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    -d '{"business_date": "2026-04-05"}'
  ```

## Prevention
- On known record dates (published by NSE 7 days ahead), increase anomaly threshold
- Consider configuring `anomaly_threshold_override` in de_system_flags for high-activity dates

## Escalation
- If RS not complete by 21:00 IST on a corporate action storm day → warning only
- Downstream platforms should be informed to show "RS data may be delayed" banner
