---
title: AMFI NAV Delayed
severity: warning
sla_deadline: "10:00 IST"
oncall: data-engineering
---

# Runbook: AMFI NAV Delayed

## Symptoms
- SLA alert fires: `amfi_nav` not complete by 10:00 IST
- MF platforms (MFPulse) will serve stale NAV data
- Morningstar reconciliation will fail if NAV is missing

## Immediate Actions (< 5 minutes)

1. Check pipeline status:
   ```sql
   SELECT pipeline_name, status, rows_processed, error_detail, started_at
   FROM de_pipeline_log
   WHERE pipeline_name = 'amfi_nav'
     AND business_date = CURRENT_DATE
   ORDER BY run_number DESC LIMIT 3;
   ```

2. Check AMFI website manually:
   - https://www.amfiindia.com/spages/NAVAll.txt
   - AMFI publishes at approx 22:00 IST previous day (for business days)
   - For next-day data, published by 09:00 IST

3. Check connectivity to AMFI:
   ```bash
   curl -I https://www.amfiindia.com/spages/NAVAll.txt
   ```

## Investigation

### If AMFI file not yet published:
- Check if today is a holiday (SEBI calendar)
- NAV may be delayed by 30-60 minutes on SEBI-prescribed dates
- Retry at 10:30 IST

### If file is malformed (PERSISTENT failure):
```bash
# Download raw file and inspect
curl https://www.amfiindia.com/spages/NAVAll.txt | head -50
```
- Check for unexpected encoding or format changes
- File is semicolon-delimited with scheme code in column 1

### If row count low (< 5000):
- Check reconciliation alert: `mf_row_count_sanity` 
- AMFI occasionally publishes partial files — retry after 30 min

## Manual Retry

```bash
curl -X POST http://localhost:8010/api/v1/pipelines/amfi_nav/trigger \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"business_date": "2026-04-05"}'
```

## Escalation
- 11:30 IST still failing → page lead engineer
- Notify MFPulse team to display "NAV as of [previous date]" banner

## Recovery Verification
```sql
SELECT COUNT(*), MAX(nav_date)
FROM de_mf_nav
WHERE nav_date = CURRENT_DATE;
-- Expected: > 8000 schemes
```
