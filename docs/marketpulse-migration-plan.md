# MarketPulse → JIP Data Core Migration Plan

> Detailed plan to move MarketPulse (fie_v3) from its own database to JIP Data Core as the single source of truth for all market data.

---

## Executive Summary

MarketPulse currently maintains its own database (fie_v3) with 32 tables containing stock prices, RS scores, breadth data, index prices, and MF NAV. JIP Data Core has all of this data with 3-20x more coverage. The migration replaces MarketPulse's data tables with read-only access to JIP, while keeping MarketPulse's application state (portfolios, trades, alerts) in fie_v3.

**Key outcome:** MarketPulse goes from 640 stocks with no technicals to 2,281 stocks with 39 technical indicators, 12M RS scores, 18 fund risk metrics, and sector-level analytics.

---

## 1. Current MarketPulse Database (fie_v3) — 32 Tables

### Data Tables (REPLACE with JIP)

| MP Table | Rows | What It Stores | JIP Equivalent | JIP Advantage |
|----------|------|----------------|----------------|---------------|
| `compass_stock_prices` | 1,417,278 | Daily OHLCV for 640 stocks | `de_equity_ohlcv` (4,002,965 rows, 2,281 stocks) | 2.8x rows, 3.6x stocks |
| `compass_rs_scores` | 480 | RS for ~48 indices, 2 weeks | `de_rs_scores` (12,257,985 rows: equity+MF+sector) | 25,537x more data |
| `breadth_daily` | 30 | 6 days of daily breadth | `de_breadth_daily` (4,361 days) | 145x more history |
| `breadth_history` | 101,796 | 3 years sector-level breadth | `de_breadth_daily` + sector RS | Richer per-sector data |
| `index_prices` | ~5,000 | Index OHLCV | `de_index_prices` (138,088 rows) | 28x more data |
| `index_constituents` | 4,638 | 62 index memberships | `de_index_constituents` (2,598+) | Comparable |
| `compass_etf_prices` | 155,936 | Global ETF/index prices | `de_global_prices` (155,936 rows) | Same data |
| `mf_nav_history` | 74,402 | Fund NAV | `de_mf_nav_daily` (1,465,792 rows) | 20x more, pre-computed returns |

### Application Tables (KEEP in fie_v3)

| MP Table | Rows | Why It Stays |
|----------|------|-------------|
| `compass_model_state` | 13 | Active portfolio positions — application state |
| `compass_model_trades` | 16 | Trade execution history |
| `compass_model_nav` | 30 | Model portfolio NAV tracking |
| `compass_regime_configs` | 4 | Trading rules per regime |
| `compass_lab_runs` | 444 | Backtesting experiment results |
| `compass_decision_log` | 0 | Trade decision audit trail |
| `compass_discovered_rules` | 0 | ML-discovered trading rules |
| `portfolio_holdings` | 29 | Real portfolio stock positions |
| `portfolio_nav` | 2,474 | Real portfolio daily NAV |
| `portfolio_transactions` | 48 | Buy/sell transaction history |
| `portfolio_metrics` | 54 | Portfolio risk metrics |
| `model_portfolios` | 5 | Portfolio definitions |
| `microbaskets` | 4 | Thematic basket definitions |
| `microbasket_constituents` | 13 | Basket stock memberships |
| `alert_actions` | 17 | Alert trade decisions |
| `tradingview_alerts` | 17 | TradingView webhook alerts |
| `pms_nav_daily` | 3,696 | PMS NAV history |
| `pms_transactions` | 30 | PMS trade history |
| `sentiment_history` | 126 | Market sentiment composite |
| `stock_sentiment` | 6,611 | Per-stock sentiment scores |
| `breadth_threshold_flags` | 150 | Breadth alert triggers |
| `simulator_cache` | 1 | Cached simulation data |
| `drawdown_events` | — | Drawdown tracking |

---

## 2. Migration Phases

### Phase 1: Database Connection (Day 1-2)

**Create read-only JIP user for MarketPulse:**
```sql
-- Run on JIP RDS
CREATE USER marketpulse_reader WITH PASSWORD 'choose_a_strong_password';
GRANT CONNECT ON DATABASE data_engine TO marketpulse_reader;
GRANT USAGE ON SCHEMA public TO marketpulse_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO marketpulse_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO marketpulse_reader;
```

**Add to MarketPulse `.env`:**
```
JIP_DATABASE_URL=postgresql://marketpulse_reader:password@jip-data-engine.ctay2iewomaj.ap-south-1.rds.amazonaws.com:5432/data_engine
```

**Add dual-database support in MarketPulse `config.py`:**
```python
# Existing fie_v3 connection
FIE_DATABASE_URL = os.environ.get("DATABASE_URL")

# New JIP connection (read-only)
JIP_DATABASE_URL = os.environ.get("JIP_DATABASE_URL")

# Feature flag
USE_JIP_DATA = os.environ.get("USE_JIP_DATA", "false").lower() == "true"
```

### Phase 2: Router Rewrites (Day 3-7)

Six routers need changes. Each router gets a feature flag — flip `USE_JIP_DATA=true` to switch.

#### 2.1 `routers/compass.py` — RS Scores & Stock Screening

**Current query:**
```sql
SELECT * FROM compass_rs_scores 
WHERE date = :date AND instrument_type = 'index'
ORDER BY rs_score DESC
```

**New query (JIP):**
```sql
SELECT entity_id AS instrument_id, 
       rs_composite AS rs_score, 
       rs_1w, rs_1m, rs_3m, rs_6m, rs_12m
FROM de_rs_scores
WHERE date = :date 
  AND entity_type = 'sector' 
  AND vs_benchmark = 'NIFTY 50'
ORDER BY rs_composite DESC
```

**Column mapping:**
| MP Column | JIP Column |
|-----------|------------|
| `instrument_id` | `entity_id` |
| `rs_score` | `rs_composite` |
| `rs_momentum` | Compute: `rs_1w - rs_1m` (or use 1w component) |
| `quadrant` | Derive: LEADING/LAGGING/IMPROVING/WEAKENING from rs_composite + momentum |

**New capability:** Stock-level RS (not just index). Can now screen individual stocks:
```sql
SELECT i.current_symbol, rs.rs_composite, t.rsi_14, t.macd_line, i.sector
FROM de_rs_scores rs
JOIN de_instrument i ON i.id::text = rs.entity_id
JOIN de_equity_technical_daily t ON t.instrument_id = i.id AND t.date = rs.date
WHERE rs.date = :date AND rs.entity_type = 'equity' AND rs.vs_benchmark = 'NIFTY 50'
  AND rs.rs_composite > 5  -- strong RS
  AND t.rsi_14 < 30         -- oversold
  AND t.above_200dma = TRUE  -- above long-term trend
ORDER BY rs.rs_composite DESC
```

#### 2.2 `routers/breadth.py` — Market Breadth

**Current:** Computes breadth on-the-fly from `compass_stock_prices`
**New:** Direct read from pre-computed table

```sql
SELECT date, advance, decline, unchanged, total_stocks, 
       ad_ratio, pct_above_200dma, pct_above_50dma
FROM de_breadth_daily 
WHERE date >= :start_date 
ORDER BY date DESC
```

**New fields available:** `pct_above_200dma`, `pct_above_50dma` (MarketPulse never had these)

**For sector-level breadth** (currently in `breadth_history`):
```sql
SELECT i.sector, 
       SUM(CASE WHEN t.above_200dma THEN 1 ELSE 0 END) AS above_200dma,
       COUNT(*) AS total
FROM de_equity_technical_daily t
JOIN de_instrument i ON i.id = t.instrument_id
WHERE t.date = :date AND i.sector IS NOT NULL
GROUP BY i.sector
```

#### 2.3 `routers/indices.py` — Index Data

**Column mapping:**
| MP Column | JIP Column |
|-----------|------------|
| `index_name` | `index_code` |
| `close_price` | `close` |
| `open_price` | `open` |
| `high_price` | `high` |
| `low_price` | `low` |

```sql
SELECT date, close, open, high, low, volume, pe_ratio, pb_ratio, div_yield
FROM de_index_prices
WHERE index_code = :index_code AND date >= :start_date
ORDER BY date
```

#### 2.4 `price_service.py` — Stock Price Lookup

**Current:**
```python
SELECT close FROM compass_stock_prices WHERE ticker = :ticker AND date = :date
```

**New:**
```python
SELECT COALESCE(close_adj, close) AS close 
FROM de_equity_ohlcv e 
JOIN de_instrument i ON i.id = e.instrument_id 
WHERE i.current_symbol = :ticker AND e.date = :date
```

**Or for bulk price fetch (portfolio valuation):**
```python
SELECT i.current_symbol AS ticker, COALESCE(e.close_adj, e.close) AS close
FROM de_equity_ohlcv e 
JOIN de_instrument i ON i.id = e.instrument_id
WHERE i.current_symbol = ANY(:tickers) AND e.date = :date
```

#### 2.5 `routers/recommendations.py` — Stock Recommendations

**Current:** Screens on RS score only from 640 stocks
**New:** Screen on 39 technicals + RS + sector from 2,281 stocks

```sql
SELECT i.current_symbol, i.sector, i.company_name,
       rs.rs_composite, rs.rs_1m, rs.rs_3m,
       t.rsi_14, t.macd_histogram, t.adx_14,
       t.volatility_20d, t.beta_nifty, t.sharpe_1y,
       t.bollinger_upper, t.bollinger_lower, t.close_adj,
       t.relative_volume, t.obv
FROM de_rs_scores rs
JOIN de_instrument i ON i.id::text = rs.entity_id
JOIN de_equity_technical_daily t ON t.instrument_id = i.id AND t.date = rs.date
WHERE rs.date = (SELECT MAX(date) FROM de_rs_scores WHERE entity_type = 'equity')
  AND rs.entity_type = 'equity'
  AND rs.vs_benchmark = 'NIFTY 50'
  AND i.is_active = TRUE
ORDER BY rs.rs_composite DESC
```

#### 2.6 `routers/portfolios.py` — Portfolio Risk Metrics

**Current:** `portfolio_metrics` table with manually computed Sharpe, Sortino, etc.
**New:** Recompute using JIP's pre-computed stock-level metrics

For a portfolio with known holdings, portfolio-level metrics can be computed as weighted averages of stock-level metrics from `de_equity_technical_daily`.

---

## 3. Sentiment Integration

MarketPulse has two sentiment tables (`sentiment_history`, `stock_sentiment`) with a proprietary composite score. These stay in fie_v3 but can be ENRICHED with JIP data:

**Current stock_sentiment columns:**
- `above_10ema`, `above_21ema`, `above_50ema`, `above_200ema` — computed on-the-fly
- `golden_cross`, `rsi_daily`, `rsi_weekly`, `macd_bull_cross`

**With JIP, these become direct reads:**
```sql
SELECT 
    i.current_symbol AS ticker,
    t.ema_10 > t.close_adj AS above_10ema,  -- wait, reversed logic
    t.close_adj > t.ema_10 AS above_10ema,
    t.close_adj > t.ema_21 AS above_21ema,
    t.close_adj > t.ema_50 AS above_50ema,
    t.close_adj > t.ema_200 AS above_200ema,
    t.sma_50 > t.sma_200 AS golden_cross,
    t.rsi_14 AS rsi_daily,
    t.macd_histogram > 0 AND LAG(t.macd_histogram) OVER (ORDER BY t.date) <= 0 AS macd_bull_cross
FROM de_equity_technical_daily t
JOIN de_instrument i ON i.id = t.instrument_id
WHERE t.date = :date
```

No more on-the-fly computation. Pre-computed, accurate, available for 2,281 stocks.

---

## 4. Compass Model Trading Engine

The Compass trading engine (`compass_model_state`, `compass_model_trades`, `compass_regime_configs`) currently:

1. Reads RS scores from `compass_rs_scores` (480 rows, index-level)
2. Determines market regime on-the-fly
3. Executes trades based on regime-specific rules

**With JIP:**

1. Read RS from `de_rs_scores` — stock-level RS for individual positions, sector RS for allocation
2. Read pre-computed regime from `de_market_regime`
3. Access 39 technicals for entry/exit confirmation (RSI, MACD, ADX, Bollinger)

**Regime config mapping:**
```python
# Current: Compass computes regime from breadth on-the-fly
# New: Read directly
SELECT regime, confidence, breadth_score 
FROM de_market_regime 
WHERE date = :date 
ORDER BY computed_at DESC LIMIT 1
```

---

## 5. New Features Enabled by JIP

These are things MarketPulse CANNOT do today but can do immediately after migration:

### 5.1 Stock Screener with 39 Indicators
Filter stocks by any combination of RSI, MACD, Bollinger, ADX, RS, sector, beta, Sharpe.

### 5.2 Sector Rotation Dashboard
- Pre-computed sector RS for 29 sectors
- Historical sector RS for rotation timing
- Fund sector exposure mapping

### 5.3 MF Analytics
- 851 funds with Sharpe/Sortino/StdDev (1/3/5Y)
- Holdings-weighted RS and manager alpha
- Fund sector exposure (which sectors each fund is overweight/underweight)
- 1.5M NAV data points with pre-computed returns (1d to 10y)

### 5.4 Adjusted Price Analysis
- Corporate action adjusted prices for accurate long-term charts
- Proper split/bonus handling in RS and technical calculations

### 5.5 Historical Market Regime
- 4,361 days of BULL/BEAR/SIDEWAYS classification
- Backtest trading strategies against regime history

---

## 6. Table/Column Mapping Quick Reference

### Stock Prices
| fie_v3 | JIP | Notes |
|--------|-----|-------|
| `compass_stock_prices.ticker` | `de_instrument.current_symbol` | JOIN via instrument |
| `compass_stock_prices.date` | `de_equity_ohlcv.date` | Same |
| `compass_stock_prices.close` | `COALESCE(de_equity_ohlcv.close_adj, close)` | Adjusted price |
| `compass_stock_prices.open` | `de_equity_ohlcv.open` | Same |
| `compass_stock_prices.high` | `de_equity_ohlcv.high` | Same |
| `compass_stock_prices.low` | `de_equity_ohlcv.low` | Same |
| `compass_stock_prices.volume` | `de_equity_ohlcv.volume` | Same |

### RS Scores
| fie_v3 | JIP | Notes |
|--------|-----|-------|
| `compass_rs_scores.instrument_id` | `de_rs_scores.entity_id` | Text UUID for stocks, mstar_id for MF, sector name for sectors |
| `compass_rs_scores.instrument_type` | `de_rs_scores.entity_type` | 'equity', 'mf', 'sector' |
| `compass_rs_scores.rs_score` | `de_rs_scores.rs_composite` | Same formula |
| `compass_rs_scores.rs_momentum` | `de_rs_scores.rs_1w` | Use 1-week RS as momentum proxy |
| `compass_rs_scores.quadrant` | Derived | LEADING (rs>0, mom>0), WEAKENING (rs>0, mom<0), IMPROVING (rs<0, mom>0), LAGGING (rs<0, mom<0) |

### Index Prices
| fie_v3 | JIP | Notes |
|--------|-----|-------|
| `index_prices.index_name` | `de_index_prices.index_code` | Same names (NIFTY 50, etc.) |
| `index_prices.close_price` | `de_index_prices.close` | Same |

### MF NAV
| fie_v3 | JIP | Notes |
|--------|-----|-------|
| `mf_nav_history.fund_code` | `de_mf_master.amfi_code` or `mstar_id` | Need AMFI code mapping |
| `mf_nav_history.nav` | `de_mf_nav_daily.nav` | Same |
| (not available) | `de_mf_nav_daily.return_1d` through `return_10y` | Pre-computed returns |
| (not available) | `de_mf_derived_daily.sharpe_1y` etc. | 18 risk metrics |

---

## 7. Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| JIP RDS downtime | MarketPulse falls back to fie_v3 for critical paths (USE_JIP_DATA flag) |
| Query performance | JIP tables have proper indexes on (date, instrument_id). Add read replicas if needed |
| Schema changes | marketpulse_reader is SELECT-only. JIP schema changes don't break MP unless columns are dropped |
| Data freshness | JIP daily pipeline runs at 23:30 IST. MarketPulse needs data by 09:00 IST next day — 9.5 hour buffer |

---

## 8. Timeline

| Day | Task |
|-----|------|
| 1 | Create RDS read-only user. Add JIP connection to MarketPulse config. Test connectivity. |
| 2 | Rewrite `price_service.py` and `routers/indices.py` (simplest, most impactful). Deploy with feature flag OFF. |
| 3 | Rewrite `routers/compass.py` (RS scores). Map quadrant derivation. Test side-by-side. |
| 4 | Rewrite `routers/breadth.py`. Add pct_above_200dma/50dma (new feature). |
| 5 | Rewrite `routers/recommendations.py` with full 39-indicator screener. |
| 6 | Integration testing. Flip USE_JIP_DATA=true on staging. Compare outputs. |
| 7 | Deploy to production. Monitor for 24 hours. |
| 8-10 | Build new features: stock screener, MF analytics, sector rotation. |
| 14 | Drop redundant fie_v3 data tables. |

---

## 9. JIP Data Refresh Schedule

MarketPulse needs to know when JIP data is fresh:

| Data | Refresh Time (IST) | Frequency |
|------|-------------------|-----------|
| Equity OHLCV | 19:30 (after NSE close) | Daily |
| Technicals | 23:00 | Daily (after OHLCV) |
| RS Scores | 23:15 | Daily (after technicals) |
| Breadth + Regime | 23:20 | Daily |
| MF NAV | 22:30 (after AMFI publish) | Daily |
| Fund Derived | 23:30 | Daily |
| Holdings | 03:00 1st of month | Monthly |
| Index Constituents | 09:00 Sunday | Weekly |

MarketPulse should check `de_pipeline_log` for the latest successful run timestamp before serving data.

---

*Last updated: 2026-04-07*
*Author: JIP Data Engine Team*
