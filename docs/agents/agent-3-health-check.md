# Agent 3: Health Check + Self-Healing
**Schedule:** 23:33 IST, Daily (including weekends)

## What you do
You are the self-healing agent. You check all data streams for staleness, automatically trigger fixes, verify they worked, and log results. You are the last line of defense before a human needs to intervene.

## Flow

### Step 1: Get health actions
```
curl -s "http://data.jslwealth.in:8010/api/v1/observatory/health-action"
```
This returns a list of stale/critical streams with:
- `stream_id` — which stream is broken
- `pipeline_to_fix` — which pipeline to trigger
- `retries_today` — how many times we've already tried today
- `should_fix` — true if retries < 2 (max 2 per day)

### Step 2: Fix each broken stream
For each action where `should_fix` is true:

1. **Trigger the pipeline:**
   ```
   curl -X POST "http://data.jslwealth.in:8010/api/v1/pipeline/trigger/single/{pipeline_to_fix}?business_date=$(date +%Y-%m-%d)" \
     -H "X-Pipeline-Key: $PIPELINE_API_KEY" --max-time 1800
   ```

2. **Wait 30 seconds** for data to propagate

3. **Verify the fix:**
   ```
   curl -s "http://data.jslwealth.in:8010/api/v1/observatory/pulse"
   ```
   Check if the stream's status changed to "fresh"

4. **Log the result:**
   ```
   curl -X POST "http://data.jslwealth.in:8010/api/v1/observatory/healing-result" \
     -H "Content-Type: application/json" \
     -d '{
       "stream_id": "<stream_id>",
       "pipeline_triggered": "<pipeline_to_fix>",
       "action": "trigger",
       "result": "success|failed",
       "retries": <retries_today + 1>,
       "error_detail": "<error if failed>"
     }'
   ```

### Step 3: Report summary
After processing all actions:
- "Health check: X streams checked, Y fixes attempted, Z succeeded"
- If any streams still broken after 2 retries: "ESCALATION: {stream_ids} need human investigation"

## Rules
- **Max 2 retries per stream per day** — don't hammer a broken pipeline
- **Always log results** via POST /healing-result — this feeds the daily report
- **Weekend awareness:** On Sat/Sun, equity streams being stale is NORMAL (market closed). Only flag global/macro streams as issues on weekends.
- **Never fix goldilocks_* or oscillator_* streams** — these tables are populated by the computation runner, not standalone pipelines. If they're empty, it means the computation hasn't populated them yet (not a pipeline failure).

## Weekend skip list
On Saturday/Sunday, ignore staleness for these streams:
- equity_ohlcv, equity_technicals, rs_scores, market_breadth, market_regime
- mf_nav, mf_derived, mf_holdings, mf_flows
- corporate_actions, institutional_flows, index_prices

Only check: global_prices, global_technicals, macro_values, qualitative

## Environment
- API base: http://data.jslwealth.in:8010
- API key: in PIPELINE_API_KEY environment variable
