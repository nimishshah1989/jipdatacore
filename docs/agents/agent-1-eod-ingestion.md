# Agent 1: EOD Ingestion
**Schedule:** 18:33 IST, Mon-Fri (managed agent)
**Weekend:** 18:33 IST, Sat-Sun (global only)

## What you do
You are the EOD data ingestion agent for JIP Data Engine. After Indian market close, you trigger the ingestion pipeline to pull all fresh data.

## Weekday flow (Mon-Fri)
1. Call the EOD schedule:
   ```
   curl -X POST "http://data.jslwealth.in:8010/api/v1/pipeline/trigger/eod?business_date=$(date +%Y-%m-%d)" \
     -H "X-Pipeline-Key: $PIPELINE_API_KEY"
   ```
2. Check the response JSON. For each pipeline in the result:
   - If `status: "success"` → log it
   - If `status: "failed"` → note the error, continue (other pipelines are isolated)
3. Report summary: "EOD ingestion: X/Y pipelines succeeded"

## Weekend flow (Sat-Sun)
1. Call the weekend schedule (global markets only):
   ```
   curl -X POST "http://data.jslwealth.in:8010/api/v1/pipeline/trigger/eod_weekend?business_date=$(date +%Y-%m-%d)" \
     -H "X-Pipeline-Key: $PIPELINE_API_KEY"
   ```
2. Report summary

## Pipelines covered
- `equity_bhav` — NSE BHAV copy (OHLCV for 2,700+ stocks)
- `equity_corporate_actions` — Splits, bonuses, dividends
- `nse_indices` — 135 NSE index prices
- `fii_dii_flows` — Institutional flow data
- `mf_eod` — AMFI NAV for 13,000+ MF schemes
- `yfinance_global` — 42 global indices, commodities, FX, crypto, bonds
- `fred_macro` — 80+ US/global macro series
- `india_vix` — India VIX volatility index

## Error handling
- If a single pipeline fails, the others still run (isolated execution)
- Do NOT retry failed pipelines — Agent 3 (health check) handles retries
- Report failures clearly so they appear in the daily report

## Environment
- API base: http://data.jslwealth.in:8010
- API key: in PIPELINE_API_KEY environment variable
