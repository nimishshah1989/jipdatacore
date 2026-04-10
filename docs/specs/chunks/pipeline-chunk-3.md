# Pipeline Chunk 3: Backfill All Stale Data

**Layer:** 2
**Dependencies:** Pipeline Chunks 1 + 2
**Complexity:** Low (execution) / High (verification)
**Status:** pending

## Overview
Use the new trigger API to catch up all 12 stale data streams to today (Apr 10, 2026).
This is an execution + verification chunk, not a code-writing chunk.

## Backfill Plan

### Phase 1: Foundation Data (must run first)
| Pipeline | Date Range | Expected |
|----------|------------|----------|
| Trading calendar | 2026 full year | Ensure all trading days populated |
| Instrument master | Today | Refresh NSE equity list |

### Phase 2: Ingestion (parallel-safe)
| Pipeline | Date Range | Est. Rows | Notes |
|----------|------------|-----------|-------|
| nse_bhav | Apr 7-10 | ~8,000 (2K/day) | Check for holidays first |
| nse_indices | Apr 7-10 | ~240 (60/day) | |
| amfi_nav | Apr 3-10 | ~30,000 | Only trading days |
| fii_dii_flows | Apr 7-10 | ~4-8 | |
| yfinance_global | Mar 31 - Apr 10 | ~400 | Global markets may have different holidays |
| fred_macro | Apr 1-10 | ~50-100 | Monthly series won't have new data daily |
| etf_ohlcv | Apr 3-10 | ~500 | |
| mf_category_flows | Feb 1 - Apr 10 | ~100-200 | Monthly data, investigate availability |

### Phase 3: Computations (must follow Phase 2)
| Computation | Date Range | Notes |
|-------------|------------|-------|
| Equity technicals (SQL) | Apr 7-10 | Depends on OHLCV |
| Equity technicals (pandas) | Apr 7-10 | |
| RS scores | Full rebuild | Run weekly full rebuild |
| Breadth + regime | Apr 7-10 | Depends on RS |
| MF derived metrics | Apr 3-10 | Depends on NAV |
| Fund metrics | Apr 3-10 | |
| ETF technicals + RS | Apr 3-10 | |
| Global technicals + RS | Mar 31 - Apr 10 | |

## Execution Method

Use the backfill endpoint:
```bash
# Phase 1: Foundation
curl -X POST "https://data.jslwealth.in/api/v1/pipeline/trigger/single/trading_calendar" \
  -H "X-Pipeline-Key: $KEY"

# Phase 2: Ingestion (can run in parallel)
curl -X POST "https://data.jslwealth.in/api/v1/pipeline/trigger/backfill" \
  -H "X-Pipeline-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"pipeline_names": ["nse_bhav", "nse_indices", "amfi_nav"], "start_date": "2026-04-07", "end_date": "2026-04-10"}'

# Phase 3: Computations (after Phase 2 completes)
curl -X POST "https://data.jslwealth.in/api/v1/pipeline/trigger/backfill" \
  -H "X-Pipeline-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"pipeline_names": ["equity_technicals", "rs_scores"], "start_date": "2026-04-07", "end_date": "2026-04-10"}'
```

## Verification

After each phase, verify via observatory:
```bash
curl -s "https://data.jslwealth.in/api/v1/observatory/pulse" | jq '.data.streams'
```

**Per-stream verification:**
1. Row count BEFORE and AFTER (query de_pipeline_log)
2. No gaps in date series (consecutive trading days)
3. OHLCV sanity: open/high/low/close relationships valid
4. NAV sanity: no negative NAVs, no >10% day-over-day jumps
5. RS scores: values between 0-100
6. Cross-check: NIFTY 50 close matches across NSE and yfinance (2% tolerance)

## Acceptance Criteria
- [ ] All 12 streams show today's date (Apr 10) in observatory/pulse
- [ ] No CRITICAL freshness alerts
- [ ] Row counts match expected ranges
- [ ] RS scores recomputed with full history
- [ ] Reconciliation checks pass (NSE vs yfinance, AMFI vs Morningstar)
- [ ] de_pipeline_log shows successful runs for all backfilled dates
- [ ] No anomalies flagged in de_data_anomalies for backfilled data

## Risk
- NSE website may rate-limit historical BHAV downloads
- Apr 9 (Gudi Padwa) or Apr 10 may be market holidays — verify trading calendar
- MF Category Flows source (AMFI) may not have Feb-Mar data available retroactively
- yfinance API may throttle on bulk historical requests
