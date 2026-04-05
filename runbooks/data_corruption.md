---
title: Data Corruption Detected
severity: critical
oncall: data-engineering
---

# Runbook: Data Corruption

## Symptoms
- Reconciliation check fails with large deviation (> 5%)
- Anomaly detector quarantines data: `data_status = 'quarantined'`
- Platform teams report incorrect prices or NAVs
- `de_anomalies` table shows systematic errors across many symbols

## Immediate Actions (< 10 minutes)

1. Identify scope of corruption:
   ```sql
   -- Check quarantined records
   SELECT source_name, COUNT(*), MIN(price_date), MAX(price_date)
   FROM de_equity_eod
   WHERE data_status = 'quarantined'
   GROUP BY source_name
   ORDER BY count DESC;
   ```

2. Check when the corruption was introduced:
   ```sql
   SELECT pipeline_name, business_date, status, rows_processed, error_detail
   FROM de_pipeline_log
   WHERE business_date >= CURRENT_DATE - INTERVAL '3 days'
     AND status IN ('failed', 'partial')
   ORDER BY business_date DESC, started_at DESC;
   ```

3. Notify platform leads immediately via #jip-alerts:
   - What data is affected (equity/MF/indices)
   - Date range affected
   - Estimated time to resolution

## Investigation

### If single-day corruption (bad source file):
```sql
-- Find the bad source file
SELECT sf.source_name, sf.file_date, sf.checksum, sf.row_count
FROM de_source_files sf
WHERE sf.file_date = '<affected_date>'
ORDER BY sf.ingested_at DESC;
```

- Download the original file from S3 archive:
  ```bash
  aws s3 ls s3://jsl-data-engine-archive/bhav/<date>/
  aws s3 cp s3://jsl-data-engine-archive/bhav/<date>/bhav.csv /tmp/bhav_check.csv
  ```
- Compare row count and spot-check prices against NSE website

### If multi-day corruption:
- This is a serious incident — stop all pipelines immediately:
  ```sql
  UPDATE de_system_flags SET value = false WHERE key = 'global_pipeline_enabled';
  ```
- Page VP Engineering and all platform leads

## Data Correction Procedure

### Step 1: Quarantine affected records
```sql
UPDATE de_equity_eod
SET data_status = 'quarantined'
WHERE price_date BETWEEN '<start_date>' AND '<end_date>'
  AND source_name = 'nse_bhav';
```

### Step 2: Re-ingest from archived source files
```bash
# Trigger pipeline with --reprocess flag
curl -X POST http://localhost:8010/api/v1/pipelines/nse_bhav/trigger \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"business_date": "<date>", "force": true, "reprocess": true}'
```

### Step 3: Validate corrected data
```sql
-- Compare against known-good spot values
SELECT symbol, close_price, open_price, high_price, low_price
FROM de_equity_eod
WHERE symbol IN ('RELIANCE', 'TCS', 'INFY', 'HDFCBANK')
  AND price_date = '<affected_date>'
ORDER BY symbol;
```

### Step 4: Release quarantine
```sql
UPDATE de_equity_eod
SET data_status = 'validated'
WHERE price_date = '<date>'
  AND data_status = 'quarantined'
  AND source_name = 'nse_bhav';
```

## Escalation
- Corruption affects > 3 trading days → critical, full incident
- Any client portfolio data affected → IMMEDIATE escalation to CTO
- Financial regulators may need to be informed if production data was distributed

## Post-Incident Requirements
- RCA document within 24 hours
- Update anomaly detection thresholds if corruption was not caught automatically
- Review archival integrity: all source files should be checksummed in de_source_files
