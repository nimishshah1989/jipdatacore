---
title: NSE BHAV Copy Delayed
severity: critical
sla_deadline: "08:30 IST"
oncall: data-engineering
---

# Runbook: NSE BHAV Copy Delayed

## Symptoms
- SLA alert fires: `nse_bhav` not complete by 08:30 IST
- Pipeline status shows `pending` or `running` past deadline
- Downstream pipelines (RS, regime) will be skipped

## Immediate Actions (< 5 minutes)

1. Check pipeline status:
   ```sql
   SELECT pipeline_name, status, started_at, completed_at, error_detail
   FROM de_pipeline_log
   WHERE pipeline_name = 'nse_bhav'
     AND business_date = CURRENT_DATE
   ORDER BY run_number DESC
   LIMIT 5;
   ```

2. Check NSE website manually:
   - https://www.nseindia.com/market-data/securities-available-for-trading
   - Look for BHAV copy under "Day End Reports"

3. Check recent error logs:
   ```bash
   docker logs jip-data-engine --tail=100 | grep nse_bhav
   ```

## Investigation

### If pipeline is stuck (status=running > 30 min):
```bash
# Kill the stuck pipeline — advisory lock will be released automatically
docker exec jip-data-engine kill -15 $(pgrep -f nse_bhav)
```

### If BHAV file download failed:
- Check NSE website uptime: https://status.nseindia.com/
- NSE publishes BHAV at ~17:30 IST, T+1 day after market close
- If NSE delayed publishing: wait and trigger manual retry at 09:00 IST

### If file format changed:
- Error will contain `ParserError` or `KeyError`
- This is a PERSISTENT failure — investigate schema change
- Check NSE announcements at https://www.nseindia.com/resources/exchange-communication-circulars

## Manual Retry

```bash
curl -X POST http://localhost:8010/api/v1/pipelines/nse_bhav/trigger \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"business_date": "2026-04-05", "force": true}'
```

## Escalation
- 09:30 IST still not resolved → page VP Engineering
- If NSE website is down → downstream platforms will use T-1 data (acceptable for < 2 hours)
- Notify platform leads via #jip-alerts

## Recovery Verification
```sql
SELECT COUNT(*), MIN(price_date), MAX(price_date)
FROM de_equity_eod
WHERE price_date = CURRENT_DATE;
-- Expected: > 1900 rows
```
