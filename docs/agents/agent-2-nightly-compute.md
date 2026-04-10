# Agent 2: Nightly Compute
**Schedule:** 19:33 IST, Mon-Fri (managed agent)

## What you do
You run the full computation pipeline after EOD ingestion completes. This transforms raw price data into technical indicators, relative strength scores, market regime, fund metrics, and Goldilocks intelligence.

## Flow
1. **Pre-check:** Verify today's ingestion ran:
   ```
   curl -s "http://data.jslwealth.in:8010/api/v1/observatory/pulse" | \
     python3 -c "import json,sys; d=json.load(sys.stdin); ohlcv=[s for s in d['streams'] if s['stream_id']=='equity_ohlcv']; print(f'OHLCV: {ohlcv[0][\"status\"]} last={ohlcv[0][\"last_date\"]}')"
   ```
   - If equity_ohlcv is NOT fresh for today → warn but proceed (weekday holiday possible)

2. **Run nightly compute:**
   ```
   curl -X POST "http://data.jslwealth.in:8010/api/v1/pipeline/trigger/nightly_compute?business_date=$(date +%Y-%m-%d)" \
     -H "X-Pipeline-Key: $PIPELINE_API_KEY" --max-time 7200
   ```

3. **Check results:** Parse the response JSON. Report each step's status.

## Computation steps (in order)
1. `__validate_ohlcv__` — Mark raw OHLCV as validated
2. `equity_technicals_sql` — SMA 50/200
3. `equity_technicals_pandas` — EMA, RSI, MACD, ADX, Bollinger, stochastic
4. `relative_strength` — RS scores (1W-12M) for equity, MF, sectors
5. `market_breadth` — Advance/decline, breadth + regime classification
6. `mf_derived` — Sharpe, Sortino, MaxDD, Beta, Alpha, Treynor
7. `etf_technicals` — ETF technical indicators
8. `etf_rs` — ETF relative strength
9. `global_technicals` — Global instrument technicals
10. `global_rs` — Global relative strength
11. `__goldilocks_compute__` — Goldilocks scraper + PDF + LLM extraction

## Timeout handling
- Full pipeline may take 30-45 minutes
- If a step times out, subsequent steps still run
- mf_derived is the longest step (~20 min for full rebuild)

## Error handling
- Steps are isolated — one failure doesn't block others
- Report failures clearly
- Do NOT retry — Agent 3 handles retries at 23:33

## Environment
- API base: http://data.jslwealth.in:8010
- API key: in PIPELINE_API_KEY environment variable
- Max runtime: 2 hours (7200s)
