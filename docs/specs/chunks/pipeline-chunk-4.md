# Pipeline Chunk 4: Claude Scheduled Agents

**Layer:** 3
**Dependencies:** Pipeline Chunks 1 + 2 + 3 (API must work, data must be current)
**Complexity:** Low
**Status:** pending

## Overview
Set up Claude scheduled agents (remote triggers) to replace cron jobs. Each agent
makes HTTP POST calls to the pipeline trigger API on a schedule.

## Agents to Create

### 1. jip-pre-market (Weekdays 07:30 IST)
**Cron:** `0 2 * * 1-5` (UTC)
**Prompt:**
```
You are the JIP Pre-Market Data Agent. Your job is to trigger the pre-market
data ingestion for the Jhaveri Intelligence Platform.

1. POST https://data.jslwealth.in/api/v1/pipeline/trigger/pre_market
   Header: X-Pipeline-Key: {key}
   
2. Wait 60 seconds, then check status:
   GET https://data.jslwealth.in/api/v1/observatory/pulse
   
3. Verify: equity_ohlcv stream shows today's date
   If stale: retry the trigger once more
   If still stale: report failure

Report: pipeline name, rows processed, any errors
```

### 2. jip-eod (Weekdays 18:30 IST)
**Cron:** `0 13 * * 1-5` (UTC)
**Prompt:**
```
You are the JIP End-of-Day Data Agent. Trigger the full EOD data refresh.

1. POST https://data.jslwealth.in/api/v1/pipeline/trigger/eod
   Header: X-Pipeline-Key: {key}

2. This runs: BHAV copy, indices, AMFI NAV, yfinance, FRED
   Expected duration: 5-15 minutes

3. After 10 minutes, check:
   GET https://data.jslwealth.in/api/v1/observatory/pulse
   
4. Verify all EOD streams show today's date
   If any failed: check pipeline status, report specific failures
   If transient error: retry failed pipelines individually via
   POST /api/v1/pipeline/trigger/single/{pipeline_name}

Report: per-pipeline status, row counts, duration, any anomalies
```

### 3. jip-computations (Weekdays 19:00 IST)
**Cron:** `30 13 * * 1-5` (UTC)
**Prompt:**
```
You are the JIP Computation Agent. Run post-EOD computations.

1. First verify EOD data is current:
   GET https://data.jslwealth.in/api/v1/observatory/pulse
   If equity_ohlcv is not today: STOP and report "EOD not yet complete"

2. Run computations in order:
   POST /api/v1/pipeline/trigger/technicals
   POST /api/v1/pipeline/trigger/rs_computation
   POST /api/v1/pipeline/trigger/regime

3. After each, verify via observatory/pulse

Report: computation results, any skipped due to missing dependencies
```

### 4. jip-fund-metrics (Weekdays 21:00 IST)
**Cron:** `30 15 * * 1-5` (UTC)
**Prompt:**
```
You are the JIP Fund Metrics Agent.

1. Verify MF NAV is current via observatory/pulse
2. POST /api/v1/pipeline/trigger/fund_metrics
3. Verify mf_derived stream is updated

Report: rows processed, any errors
```

### 5. jip-global (Weekdays 22:00 IST)
**Cron:** `30 16 * * 1-5` (UTC)
**Prompt:**
```
You are the JIP Global Data Agent.

1. POST /api/v1/pipeline/trigger/etf_global
   This runs: yfinance global refresh, ETF technicals, ETF RS, global technicals, global RS
2. Verify global_prices and etf_ohlcv streams are current

Report: per-pipeline results
```

### 6. jip-weekly (Sunday 04:00 IST)
**Cron:** `30 22 * * 6` (UTC, Saturday 22:30 = Sunday 04:00 IST)
**Prompt:**
```
You are the JIP Weekly Agent.

1. POST /api/v1/pipeline/trigger/morningstar_weekly
2. POST /api/v1/pipeline/trigger/full_rs_rebuild (full RS historical rebuild)
3. Verify both complete successfully

Report: rows refreshed, duration
```

### 7. jip-monthly (1st of month 03:00 IST)
**Cron:** `30 21 1 * *` (UTC)
**Prompt:**
```
You are the JIP Monthly Agent.

1. POST /api/v1/pipeline/trigger/holdings_monthly
2. Verify MF holdings data is refreshed

Report: rows processed, any errors
```

### 8. jip-health-check (Daily 23:30 IST)
**Cron:** `0 18 * * *` (UTC)
**Prompt:**
```
You are the JIP Health Check Agent. Your job is to verify ALL daily data is current.

1. GET https://data.jslwealth.in/api/v1/observatory/pulse
2. Check each stream's freshness:
   - equity_ohlcv: should be today (weekdays) or last Friday (weekends)
   - mf_nav: should be today (weekdays)
   - rs_scores: should be today (weekdays)
   - global_prices: should be today or yesterday
   - All others: check against expected frequency

3. If ANY stream is stale beyond threshold:
   - Identify which pipeline failed
   - Attempt recovery: POST /api/v1/pipeline/trigger/single/{pipeline}
   - If recovery fails: report clearly with error details

4. GET /api/v1/observatory/quality — check for anomalies
5. Report overall health status: GREEN / YELLOW / RED

This is the safety net. If any scheduled agent failed today, you catch it here.
```

## Setup Method

Use `/schedule` skill for each agent. Each needs:
- Name
- Cron expression
- Prompt (as above)
- Environment variables: PIPELINE_API_KEY
- Network access: enabled (to reach data.jslwealth.in)

## Acceptance Criteria
- [ ] All 8 agents created and visible via CronList
- [ ] Each agent manually triggered once to verify it works
- [ ] Pre-market agent successfully triggers BHAV pipeline
- [ ] EOD agent successfully triggers full EOD pipeline group
- [ ] Health check agent correctly identifies stale streams
- [ ] Agents have API key in environment (not in prompt text)
- [ ] Weekend agents correctly skip (or verify no trading day)

## Risk
- Cloud agents run up to 10 min late — acceptable for daily data
- 1-hour minimum interval — qualitative RSS (30 min) needs separate solution
- Agent may not be able to wait long enough for slow pipelines (timeout)
- API key in agent environment — verify it's not logged in agent output
