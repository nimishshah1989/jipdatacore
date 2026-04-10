# ETF-C5: Deploy + End-to-End Verification

**Parent Plan:** [etf-expansion-chunk-plan.md](../etf-expansion-chunk-plan.md)
**Complexity:** Medium
**Dependencies:** C1, C2, C3, C4 (all must be complete)
**Blocks:** None (final chunk)

---

## Description

Deploy all changes from C1-C4 to EC2 production, run seeders and backfill on the production database, execute the full computation pipeline (technicals + RS) on the expanded universe, and verify end-to-end correctness.

---

## Files to Create/Modify

No new files. This is an operations chunk deploying all files from C1-C4:

| Source Chunk | Files to Deploy |
|-------------|----------------|
| C1 | `scripts/ingest/nse_etf_master.py`, `app/pipelines/etf/nse_etf_sync.py`, `app/pipelines/etf/__init__.py` |
| C2 | `scripts/ingest/etf_ingest.py` (modified), `scripts/ingest/etf_backfill.py` |
| C3 | `scripts/ingest/etf_enrich.py` |
| C4 | `app/pipelines/registry.py` (modified), `app/orchestrator/scheduler.py` (modified) |

**Target:** EC2 at 13.206.34.214

---

## Detailed Implementation Steps

### Step 1: Deploy to EC2

1. SCP all changed/new files to EC2:
   ```bash
   scp scripts/ingest/nse_etf_master.py ec2:~/jip-data-core/scripts/ingest/
   scp app/pipelines/etf/nse_etf_sync.py ec2:~/jip-data-core/app/pipelines/etf/
   scp app/pipelines/etf/__init__.py ec2:~/jip-data-core/app/pipelines/etf/
   scp scripts/ingest/etf_ingest.py ec2:~/jip-data-core/scripts/ingest/
   scp scripts/ingest/etf_backfill.py ec2:~/jip-data-core/scripts/ingest/
   scp scripts/ingest/etf_enrich.py ec2:~/jip-data-core/scripts/ingest/
   scp app/pipelines/registry.py ec2:~/jip-data-core/app/pipelines/
   scp app/orchestrator/scheduler.py ec2:~/jip-data-core/app/orchestrator/
   ```
2. Docker compose restart to pick up code changes:
   ```bash
   cd ~/jip-data-core && docker compose restart
   ```

### Step 2: Run NSE ETF Seeder

```bash
python scripts/ingest/nse_etf_master.py
```
- Verify: 67+ NSE ETFs in de_etf_master

### Step 3: Run NSE OHLCV Full-History Sync

```bash
# Via API trigger or direct execution
python -c "
from app.pipelines.etf.nse_etf_sync import NseEtfSyncPipeline
import asyncio
p = NseEtfSyncPipeline()
result = asyncio.run(p.execute())
print(result)
"
```
- Verify: NSE ETF OHLCV rows populated from 2016-04-01 onwards

### Step 4: Run Global ETF Seeder

```bash
python scripts/ingest/etf_ingest.py
```
- Verify: 163 global ETFs in de_etf_master

### Step 5: Backfill New Global Tickers

```bash
python scripts/ingest/etf_backfill.py --new-only
```
- Verify: all 33 new tickers have historical OHLCV

### Step 6: Run Enrichment

```bash
python scripts/ingest/etf_enrich.py
```
- Verify: category populated for 50%+ of tickers

### Step 7: Run Technicals on Expanded Universe

```bash
python scripts/computation/etf_technicals.py
```
- Verify: de_etf_technical_daily has rows for all ~230 tickers

### Step 8: Run RS Scores on Expanded Universe

```bash
python scripts/computation/etf_rs.py
```
- Verify: de_rs_scores has entries for all new tickers

### Step 9: Run Verification Queries

```sql
-- 1. Total master count
SELECT COUNT(*) AS total_etfs,
  SUM(CASE WHEN exchange = 'NSE' THEN 1 ELSE 0 END) AS nse,
  SUM(CASE WHEN exchange != 'NSE' THEN 1 ELSE 0 END) AS global
FROM de_etf_master;
-- Expected: total 230+, nse 67+, global 163

-- 2. Distinct tickers with recent data
SELECT COUNT(DISTINCT ticker) FROM de_etf_ohlcv
WHERE date >= CURRENT_DATE - INTERVAL '3 days';
-- Expected: 200+ (accounting for weekends/holidays)

-- 3. Technicals coverage
SELECT COUNT(DISTINCT ticker) FROM de_etf_technical_daily
WHERE date >= CURRENT_DATE - INTERVAL '7 days';
-- Expected: 200+

-- 4. RS scores coverage
SELECT COUNT(DISTINCT ticker) FROM de_rs_scores
WHERE ticker IN (SELECT ticker FROM de_etf_master)
  AND date >= CURRENT_DATE - INTERVAL '7 days';
-- Expected: 200+

-- 5. Data freshness check
SELECT exchange, MAX(o.date) AS latest_date, COUNT(DISTINCT o.ticker) AS tickers
FROM de_etf_ohlcv o
JOIN de_etf_master m ON m.ticker = o.ticker
GROUP BY exchange;
```

### Step 10: Next-Day Verification

After the next EOD schedule fires (~18:30 IST):
1. Check pipeline logs: `nse_etf_sync` completed after `equity_bhav`
2. Check pipeline logs: `etf_prices` completed for 163 global tickers
3. Run verification query #2 again -- count should match or increase
4. Check Observatory dashboard: `etf_ohlcv` stream shows green

---

## Acceptance Criteria

- [ ] 230+ tickers in de_etf_master
- [ ] 200+ tickers with OHLCV data from last business day
- [ ] etf_technicals computed for all tickers with sufficient history
- [ ] etf_rs scores generated for all tickers
- [ ] Observatory dashboard shows etf_ohlcv stream as green
- [ ] No errors in pipeline logs during deploy
- [ ] Next-day EOD pipeline runs successfully: nse_etf_sync fires after equity_bhav, etf_prices fires for global tickers
- [ ] Docker container healthy after restart
