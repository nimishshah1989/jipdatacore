# Claude Code Briefing: JIP Data Engine v2.0 Build

## Context
Nimish has spent extensive time with Claude (chat) designing the Data Engine architecture. The conversation has evolved from v1.6 through v1.9.1 (infrastructure hardening through 5 audit rounds), and then a fundamental architectural shift to "stock-as-atom" — where stock is the unit of computation and everything (sectors, mutual funds, portfolios) is a weighted aggregation from stock-level metrics.

This briefing tells you what to inspect, what decisions are already made, and what the final PRD should contain.

---

## FILES TO INSPECT ON EC2 (13.206.34.214)

### 1. Existing Databases — Assess Reusable Data

**RDS (existing instance, ap-south-1):**
```bash
# Connect and list databases
psql -h <rds-endpoint> -U <user> -c "\l"

# fie_v3 database — equity data
psql -h <rds-endpoint> -U <user> -d fie_v3 -c "\dt"
# Key tables:
#   compass_stock_prices — 1.4M rows, has VARCHAR dates and DOUBLE precision (needs type fixing)
#   index_constituents — 4,638 rows (reusable)
#   index_prices — check what's there

# mf_engine database — fund master
psql -h <rds-endpoint> -U <user> -d mf_engine -c "\dt"
#   fund_master — 535 rows (Morningstar metadata, reusable as seed)

# client_portal database — client data
psql -h <rds-endpoint> -U <user> -d client_portal -c "\dt cpp_*"
#   cpp_* tables — 366K rows (client PII, needs encryption during migration)
```

**fie2-db-1 Docker container — MF NAV data:**
```bash
# Check if container is running
docker ps | grep fie2
# Connect
docker exec -it fie2-db-1 psql -U fie mf_pulse
# Key table: nav_daily — 25.8M rows
# Check: SELECT COUNT(*) FROM nav_daily;
# Check schema: \d nav_daily
# Check date range: SELECT MIN(date), MAX(date) FROM nav_daily;
# This data is REUSABLE — filter to equity/growth/regular plans
# saves downloading 10 years from AMFI
```

**mfpulse_reimagined database — CRITICAL TO INSPECT:**
```bash
# This database has Morningstar risk data (skewness, kurtosis, ratios)
# Find it — could be on RDS or in a Docker container
# List all tables, check what risk metrics are stored
# Key question: what Morningstar data is already downloaded that we can use?
# Specifically look for: holdings data, risk statistics, fund returns
```

**Champion Trader SQLite:**
```bash
# Location: check Docker volumes or /app/db_data/
docker exec champion ls /app/db_data/
# champion_trader.db — check schema, row counts
```

### 2. Local Files — Nimish's Machine

**Global Pulse data at `/Users/nimishshah/projects/global-pulse`:**
```
# Nimish needs to either:
# a) Upload the CSV files to EC2: scp -r /Users/nimishshah/projects/global-pulse ubuntu@13.206.34.214:/home/ubuntu/
# b) Or describe what's in each subfolder
#
# What to look for:
# - ETF universe list with tickers, AUM, country, sector classifications
# - Historical price CSVs for global ETFs/indices
# - Any mapping files (ticker → country, ticker → sector)
# - Date ranges covered
# - Data quality (missing dates, format issues)
```

### 3. Existing Market Pulse Code

```bash
# Location on EC2 — check current Market Pulse backend
# Understand: what endpoints does it call today?
# What database does it query?
# This helps plan the migration — which endpoints need to exist in the Data Engine
# for Market Pulse to switch over
ls /home/ubuntu/market-pulse/ || ls /home/ubuntu/jip-market-pulse/
```

### 4. Existing MF Pulse Code

```bash
# Check current MF Pulse and its Morningstar API integrations
# Identify: which 11 APIs does it call? (so we know what we're replacing)
# Check: is there an existing holdings API call we can reuse?
ls /home/ubuntu/mf-pulse/ || ls /home/ubuntu/jip-mf-pulse/
# Look for: API config files, Morningstar endpoint URLs, API keys
```

---

## ARCHITECTURE DECISIONS (ALREADY LOCKED)

### Stock-as-Atom Principle
- Stock is the unit of computation
- ALL metrics (RS, momentum, volatility, volume signals, risk metrics) computed at stock level from close_adj
- Sector metrics = market-cap-weighted aggregation of stock metrics
- Fund metrics = holding-weight aggregation of stock metrics
- This eliminates data duplication and ensures consistency across Market Pulse and MF Pulse

### Data Scope
- **Equities:** All NSE-listed stocks with BHAV data, 10 years (backfill from 2015)
- **Mutual Funds:** Equity category, growth option, regular plan only (~400-500 funds)
- **Fund data from Morningstar:** 2 APIs ONLY:
  - API 1: Fund master + metadata (weekly refresh)
  - API 2: Holdings with ISINs (monthly refresh)
- **NAV:** From AMFI daily file (free), NOT from Morningstar
- **All returns, risk metrics, derived metrics:** Computed by us, NOT from Morningstar
- **Sectors:** NSE industry classification (~20-22 sectors), mapped to stocks
- **Global:** Top 1,000 ETFs on NYSE/NASDAQ by AUM + major indices + macro
- **Indices:** All NSE indices, full available history
- **No IDCW/dividend MF plans** — can add later by expanding filter

### Infrastructure
- Single backend: core.jslwealth.in (port 8010)
- PostgreSQL on existing RDS, Redis on EC2
- Each product = separate Docker container, reads from Data Engine API
- No product has its own database
- All computation happens in the Data Engine
- Products can do additional frontend-level derivations from API responses

### What Carries From v1.9.1 (DO NOT CHANGE)
- Pipeline guard: hashtext() locks, session-level advisory locks
- Idempotency: ON CONFLICT on natural keys
- Data status gating: raw → validated → quarantined
- Data lineage: de_source_files + source_file_id FK
- Encryption: envelope encryption, truncated HMAC blind indexes
- Post-ingestion validation: anomaly detection
- Quarantine threshold guardrail (>5% = halt aggregates)
- Kill switch: de_system_flags
- SLA enforcement with Slack alerting
- Recompute queue with heartbeat
- Redis circuit breaker + stampede protection
- Schema evolution rules

---

## COMPLETE STOCK-LEVEL METRICS (de_equity_technical_daily)

~80 fields per stock per day. This is the MASTER computation table.

### Moving Averages (daily)
- ema_10, ema_21, ema_50, ema_200
- sma_50, sma_200 (for Weinstein / Champion Trader)
- ema_50_slope_20d, ema_200_slope_20d

### Moving Average Signals
- above_ema10, above_ema21, above_ema50, above_ema200
- above_sma50, above_sma200
- golden_cross_ema (ema_50 > ema_200)

### Weekly Indicators
- weekly_close
- weekly_rsi_14

### Monthly Indicators
- monthly_close
- monthly_rsi_14
- monthly_ema_12, monthly_ema_26

### Rate of Change
- roc_5d, roc_10d, roc_20d, roc_60d, roc_125d, roc_250d

### Relative Strength (vs Nifty 50)
- rs_1w, rs_1m, rs_3m, rs_6m, rs_12m
- rs_composite (weighted blend)
- rs_percentile (rank vs universe, 0-100)
- rs_trend (rs_1m - rs_3m, improving or deteriorating)

### MACD
- macd_line (ema_12 - ema_26 of close)
- macd_signal (ema_9 of macd_line)
- macd_histogram
- macd_cross_state (bullish_cross / bearish_cross / none)
- macd_cross_days_ago

### RSI
- rsi_14 (daily)
- rsi_zone (overbought >70 / oversold <30 / neutral)

### Momentum Patterns
- at_3m_high (close >= max close over 63 days)
- hh_hl_10d (higher highs AND higher lows over 10 days)
- momentum_score (composite 0-100)

### Period Reference Prices
- high_52w, low_52w
- at_52w_high (boolean), at_52w_low (boolean)
- prev_month_high, prev_quarter_high, prev_year_high

### Volatility
- volatility_20d, volatility_60d, volatility_250d (annualised std dev of daily returns)
- downside_deviation_60d
- atr_14

### Risk-Adjusted Returns
- beta_60d, beta_250d (vs Nifty 50)
- sharpe_60d, sharpe_250d
- sortino_60d, sortino_250d

### Drawdown
- drawdown_from_52w_high (%)
- max_drawdown_1y, max_drawdown_3y, max_drawdown_5y
- correlation_nifty50_60d

### Volume Signals
- volume_sma_20
- relative_volume (today / sma_20)
- volume_roc_5d
- delivery_pct, delivery_pct_sma_20
- obv (On Balance Volume)
- mfi_14 (Money Flow Index)
- price_volume_state (strong_rally / weak_rally / distribution / drying_up)
- up_volume_ratio_5d (volume on up days / total volume, 5d window)

### Market Cap
- market_cap (close × shares_outstanding)
- market_cap_category (large / mid / small / micro)

---

## COMPLETE FUND-LEVEL METRICS

### From Morningstar API 1 (weekly refresh)
mstar_id, amfi_code, isin, fund_name, amc_name, category_name, broad_category,
plan_type, option_type, inception_date, benchmark_name, expense_ratio, aum_cr,
is_active, fund_manager_name

### From Morningstar API 2 — Holdings (monthly refresh)
Per holding: mstar_id, as_of_date, holding_name, isin, instrument_id (resolved),
weight_pct, shares_held, market_value, sector_code (resolved), is_mapped

### From AMFI NAV (daily)
nav, nav_change, nav_change_pct
Returns: 1d, 1w, 1m, 3m, 6m, 1y, 3y, 5y, 10y (computed from NAV)
nav_52wk_high, nav_52wk_low

### NAV-Based Risk Metrics (computed, at 1Y/3Y/5Y windows)
std_deviation, downside_deviation
sharpe_ratio, sortino_ratio, treynor_ratio
information_ratio, tracking_error
alpha, beta, r_squared
max_drawdown, calmar_ratio
upside_capture_ratio, downside_capture_ratio
skewness, kurtosis
var_95 (Value at Risk)

### Holdings-Derived Metrics (daily, from holdings × stock metrics)
derived_rs_composite, derived_momentum, derived_volatility
sector_exposure (JSONB), top_sector, sector_concentration_hhi
large_cap_pct, mid_cap_pct, small_cap_pct
holdings_coverage_pct, holdings_as_of_date, holdings_staleness_days
pct_holdings_above_200dma, pct_holdings_above_50dma
pct_holdings_positive_rs
weighted_relative_volume, weighted_delivery_pct

### Manager Alpha Signal
nav_rs_composite (RS from NAV)
derived_rs_composite (RS from holdings × stock RS)
manager_alpha_signal (nav_rs - derived_rs)

---

## SECTOR-LEVEL METRICS (de_sector_metrics_daily)

Aggregated from constituent stocks, weighted by market cap:
sector_rs_composite, sector_return_1m, sector_momentum
sector_volatility, sector_breadth (% above 200 DMA)
stock_count, total_market_cap
sector_sentiment_score (composite of breadth indicators)

---

## BREADTH METRICS (de_breadth_daily)

Universe: all active tradeable stocks (Nifty 500 or full NSE)

### Short-Term (7)
pct_above_ema10, pct_above_ema21, pct_above_ema50
count_at_52w_high, count_at_52w_low
count_macd_bullish_cross_5d
pct_rsi_above_60

### Broad Trend (7)
pct_above_ema200
pct_monthly_above_ema12, pct_monthly_above_ema26
pct_monthly_rsi_above_50, pct_monthly_rsi_above_40
pct_weekly_rsi_above_50
pct_golden_cross

### Advance/Decline (5)
pct_above_prev_month_high, pct_above_prev_quarter_high, pct_above_prev_year_high
daily_ad_ratio
up_volume_ratio

### Momentum (3)
count_at_3m_high
pct_roc_20d_positive
count_hh_hl_10d

### Extremes (3)
pct_rsi_above_70
pct_rsi_below_30
ratio_52w_high_to_low (count_at_52w_high / count_at_52w_low)

These 25 breadth indicators feed into the Sentiment Score (0-100, Bullish-Bearish scale)
shown in the Market Pulse sentiment chart.

---

## GLOBAL ETFs

Top 1,000 ETFs on NYSE + NASDAQ by AUM.
Daily OHLCV from yfinance, 10 years history.

Classification per ETF:
- country_exposure (US / Europe / EM / Japan / China / India / Global)
- sector_exposure (Technology / Healthcare / Financials / Energy / Broad Market / etc.)
- asset_class (Equity / Bond / Commodity / REIT / Multi-Asset)
- strategy (Market Cap / Equal Weight / Factor / Thematic / Leveraged / Inverse)
- aum_usd_mm

Basic metrics per ETF:
- Returns: 1d, 1w, 1m, 3m, 6m, 1y, 3y, 5y
- RS vs S&P 500
- SMA 50, SMA 200, above/below flags

CHECK: /Users/nimishshah/projects/global-pulse — Nimish should upload these CSVs to EC2.
They may contain a curated ETF list with classifications that saves building from scratch.

---

## PIPELINES

### Daily EOD (18:30 IST)
Step 0: Master refresh (de_instrument from NSE)
Step 0.5: Corporate actions → adj_factor → recompute queue
Tracks A-E (parallel, failure-isolated):
  A: BHAV → de_equity_ohlcv (3 format parsers: pre-2010, standard, UDiFF)
  B: AMFI NAV → de_mf_nav_daily (equity/growth/regular only)
  C: NSE index prices → de_index_prices
  D: FII/DII → de_institutional_flows
  E: F&O summary → de_fo_summary
Step 9.6: Stock metrics → de_equity_technical_daily (~80 fields)
Step 9.65: Sector aggregation → de_sector_metrics_daily
Step 9.68: Fund derived metrics → de_mf_derived_daily
Step 9.7: Data status gating (raw → validated/quarantined)
Step 9.8: Update technical daily
Step 9.9: Quarantine threshold guardrail
Steps 10-14: Breadth, RS summary, regime
Steps 15-17: Partition check, cache invalidation, logging

### Weekly: Morningstar API 1 (fund master refresh)
### Monthly: Morningstar API 2 (holdings refresh → de_mf_holdings → trigger derived recompute)
### Daily 21:00: Goldilocks Research (Playwright automation)
### Pre-Market 07:30: Global indices + macro
### T+1 09:00: Delivery data

---

## QUALITATIVE PIPELINE — GOLDILOCKS RESEARCH

Playwright browser automation (NOT scraping):
1. Open Chromium, navigate to goldilocksresearch.com
2. Login with credentials from AWS Secrets Manager (GOLDILOCKS_EMAIL, GOLDILOCKS_PASSWORD)
3. Navigate content sections (daily summaries, audio notes, weekly/monthly PDFs)
4. Download all new content to staging folder
5. Feed into qualitative pipeline: ClamAV → extract → Claude API → embeddings
6. First run: historical sweep of all available content
7. Daily cron: only new content since last run

---

## EXISTING DATA REUSE STRATEGY

| Source | Location | Rows | Action |
|--------|----------|------|--------|
| MF NAV | fie2-db-1 Docker | 25.8M | Filter to equity/growth/regular → ~4-5M rows. Migrate, supplement with recent AMFI data |
| Equity OHLCV | RDS fie_v3 | 1.4M | Assess quality. If types clean enough, migrate. Otherwise backfill from NSE BHAV |
| Fund master | RDS mf_engine | 535 | Use as seed, refresh from Morningstar API 1 |
| Index constituents | RDS fie_v3 | 4,638 | Reuse with instrument_id mapping |
| MF risk data | mfpulse_reimagined | ? | INSPECT THIS — may have Morningstar risk stats useful for validation |
| Holdings | mfpulse_reimagined | ? | INSPECT THIS — holdings API data may already exist |
| Global ETFs | Local CSVs | ? | Nimish to upload to EC2 |
| Client data | RDS client_portal | 366K | Encrypt during migration per Section 3.7.1 |

---

## PRODUCTS AND PORTS

| Product | Subdomain | Port | Status |
|---------|-----------|------|--------|
| Data Engine | core.jslwealth.in | 8010 | New build |
| Market Pulse | app.jslwealth.in | 8004 | Migrate to Data Engine |
| MF Pulse | mf.jslwealth.in | 8005 | Rebuild against Data Engine |
| Champion Trader | ct.jslwealth.in | 8003 | Migrate later |
| Global Pulse | global.jslwealth.in | 8002 | Reconnect later |
| Simulator | sim.jslwealth.in | TBD | New, builds against Data Engine |
| Risk Engine | TBD | TBD | New, builds against Data Engine |

---

## WHAT CLAUDE CODE SHOULD DO FIRST

1. Inspect mfpulse_reimagined database — understand what Morningstar data exists
2. Inspect fie_v3 compass_stock_prices — assess data quality for migration vs re-fetch
3. Inspect fie2-db-1 nav_daily — confirm row count, date range, schema
4. Check if Nimish uploaded global-pulse CSVs to EC2
5. Check disk space: `df -h` (need 30GB+ free)
6. Confirm RDS PostgreSQL version: `SELECT version();`
7. Then proceed with schema creation and build

---

## NIMISH'S OPEN ITEMS (before build starts)

1. AWS Console: Create KMS keys, Secrets Manager entries, rotate credentials
2. Morningstar API 1: provide endpoint URL, auth method, response structure
3. Upload global-pulse CSVs to EC2
4. Store Goldilocks credentials in Secrets Manager
5. Create #jip-alerts Slack channel + webhook
6. Confirm: what is Ralph? (tooling question for Taskmaster integration)
