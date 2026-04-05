# Chunk 11: Technical Indicators + RS + Breadth + Regime

**Layer:** 4
**Dependencies:** C7
**Complexity:** Very High
**Status:** pending

## Files

- `app/computation/__init__.py`
- `app/computation/technicals.py`
- `app/computation/rs.py`
- `app/computation/breadth.py`
- `app/computation/regime.py`
- `app/computation/recompute_worker.py`
- `tests/computation/test_technicals.py`
- `tests/computation/test_rs.py`
- `tests/computation/test_breadth.py`
- `tests/computation/test_regime.py`

## Acceptance Criteria

### Technical Indicators (~80 per stock per day)

- [ ] **Moving averages:** EMA 10, EMA 21, EMA 50, EMA 200; SMA 50, SMA 200
- [ ] **Momentum:** RSI (14-period); MACD (12/26/9); ROC at 5d, 10d, 21d, 63d timeframes
- [ ] **Weekly and monthly indicators:** Computed from weekly/monthly OHLCV aggregated from daily data
- [ ] **Volatility:** Annualised volatility (20d, 60d rolling std of daily returns × √252)
- [ ] **Risk metrics:** Beta (vs NIFTY 50); Sharpe ratio (1y); Sortino ratio (1y); Maximum drawdown (1y, 3y)
- [ ] **Volume signals:** Relative volume (today / 20d avg); OBV (On Balance Volume); MFI (Money Flow Index); delivery volume analysis (delivery % vs 20d avg)
- [ ] **Stored in `de_equity_technical_daily`:** Incremental update — only today's values; uses yesterday's SMA + today's close_adj
- [ ] Incremental SMA formula: `SMA_50_today = SMA_50_yesterday + (close_adj_today - close_adj_50_days_ago) / 50`
- [ ] `de_equity_technical_daily.above_50dma` and `above_200dma` are GENERATED ALWAYS columns — not set explicitly

### RS Computation

- [ ] **RS formula (from spec Section 5.8):** `rs_Nt = (entity_cumreturn_N - benchmark_cumreturn_N) / benchmark_rolling_std_N`
  - `cumreturn = (close_adj_today / close_adj_N_days_ago) - 1`
  - `benchmark_rolling_std_N = std of benchmark daily returns over same N-day window`
  - Lookback periods: `1w=5, 1m=21, 3m=63, 6m=126, 12m=252` trading days
- [ ] **RS composite:** `rs_1w×0.10 + rs_1m×0.20 + rs_3m×0.30 + rs_6m×0.25 + rs_12m×0.15`
- [ ] **Benchmarks:** NIFTY 50 (`vs_benchmark='nifty50'`), NIFTY 500 (`vs_benchmark='nifty500'`), NIFTY MIDCAP 100
- [ ] **Daily incremental RS:** Today only, ~9,000 calculations, target <30 seconds runtime
- [ ] **Full weekly RS rebuild (Sunday 02:00 IST):** Rebuild from 2010-01-01, only dates where `close_adj` changed; runs as background job
- [ ] **RS scores written to `de_rs_scores`:** `ON CONFLICT DO UPDATE`; `computation_version` = current version constant from config
- [ ] **RS daily summary (Step 13):** `INSERT INTO de_rs_daily_summary ... ON CONFLICT (date, instrument_id, vs_benchmark) DO UPDATE SET symbol=EXCLUDED.symbol, rs_composite=...`
- [ ] **Percentile ranking:** RS composite percentile across all active stocks for the same date
- [ ] Only `data_status = 'validated'` rows from `de_equity_ohlcv` used for RS computation
- [ ] Quarantine threshold check (v1.9.1): if >5% of universe quarantined, skip RS computation for that date

### Breadth Indicators (25 total)

- [ ] **Reads from `de_equity_technical_daily`** (NOT raw OHLCV — spec v1.8 requirement to avoid CPU spikes)
- [ ] **Advance/Decline:** advance count, decline count, unchanged count, total stocks, A/D ratio
- [ ] **% above DMA:** pct_above_200dma, pct_above_50dma
- [ ] **52-week highs/lows:** new_52w_highs, new_52w_lows
- [ ] Additional breadth indicators to fill 25 total: McClellan Oscillator, McClellan Summation Index, % above 20DMA, % in stage 2, high-low index, etc.
- [ ] Written to `de_breadth_daily` with `ON CONFLICT (date) DO UPDATE`
- [ ] Only `data_status = 'validated'` rows used

### Market Regime

- [ ] **Classification:** BULL / BEAR / SIDEWAYS / RECOVERY
- [ ] **Component scores (0-100):** breadth_score, momentum_score, volume_score, global_score, fii_score
- [ ] **Confidence score (0-100):** weighted composite of component scores
- [ ] **Indicator detail:** JSONB with full breakdown of signals contributing to classification
- [ ] `computation_version` stored in every regime row (v1.8)
- [ ] If Track A (equity) failed: `confidence *= 0.5`, `indicator_detail.data_quality = 'equity_stale'`
- [ ] Written to `de_market_regime` with `ON CONFLICT (computed_at) DO UPDATE`
- [ ] Regime update SLA: 23:30 IST

### Recompute Worker

- [ ] Background worker processes `de_recompute_queue` items with `status='pending'`
- [ ] Updates `heartbeat_at` every 60 seconds while processing
- [ ] Stale detection: orchestrator resets `status='pending'` for items where `heartbeat_at < NOW() - 15 min`
- [ ] Max 2 concurrent recompute workers; max 50,000 OHLCV rows per batch
- [ ] After recompute: marks `de_rs_scores` dirty for affected instrument from `from_date`

## Notes

**Architecture principle:** Stock is the unit of computation. ALL metrics computed at stock level from `close_adj`. Sectors and funds are aggregations.

**Technical table as breadth cache:** `de_equity_technical_daily` is a pre-computed cache for breadth computation. Computing 200DMA for ~5,000 stocks from 25M raw OHLCV rows daily would cause CPU spikes. The incremental SMA approach avoids full window reads.

**Full rebuild schedule:** Sunday 02:00 IST full RS rebuild ensures correctness after corporate action adjustments. Incremental daily build is fast (~30 seconds) but may accumulate rounding drift over weeks.

**Computation versioning (v1.8):** `computation_version` in `de_rs_scores` and `de_market_regime` tracks algorithm version for auditability. Increment when RS formula or regime logic changes. API header `X-Computation-Version` exposes this to clients.

**Universe for RS/breadth:** All stocks where `de_instrument.is_active = TRUE AND is_tradeable = TRUE` AND have a validated OHLCV row for today.

**RS computation performance:** ~2,000 active stocks × 3 benchmarks × 5 timeframes = ~30,000 RS scores per day. Vectorised numpy computation using pandas DataFrames should achieve <30 seconds.
