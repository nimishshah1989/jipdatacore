# Chunk 2 — Alembic migrations: v2 tables + purchase_mode

**Complexity**: M
**Blocks**: chunk-3, chunk-9
**Blocked by**: chunk-1

## Goal
Create all database schema changes in Alembic migrations: five new technical tables (3 v2 twins + 2 new), the `purchase_mode` column on `de_mf_master`, and matching SQLAlchemy models.

## Files
- **Create**: `alembic/versions/XXX_add_purchase_mode_to_mf_master.py`
  - `op.add_column("de_mf_master", sa.Column("purchase_mode", sa.Integer, nullable=True))`
  - No index (sparse small-cardinality column, not queried alone)
- **Create**: `alembic/versions/XXX_indicators_v2_tables.py`
  - Creates `de_equity_technical_daily_v2` (twin of current equity table, full ~130 cols)
  - Creates `de_etf_technical_daily_v2` (twin of current etf table)
  - Creates `de_global_technical_daily_v2` (twin of current global table)
  - Creates `de_index_technical_daily` (NEW — keyed on `(date, index_code)` FK to `de_index_master`)
  - Creates `de_mf_technical_daily` (NEW — keyed on `(nav_date, mstar_id)` FK to `de_mf_master`)
  - Each table has: primary key, FK with ON DELETE CASCADE, `ix_{table}_{id}` index, `ix_{table}_{id}_date` DESC composite index, `created_at`, `updated_at` with tz-aware timestamps
  - GENERATED STORED columns preserved: `above_50dma`, `above_200dma`, plus new `above_20ema`, `price_above_vwap`, `rsi_overbought`, `rsi_oversold`, `macd_bullish`, `adx_strong_trend`
- **Create**: `app/models/indicators_v2.py` — SQLAlchemy 2.0 `mapped_column()` definitions for all 5 tables, matching the migration exactly. One class per table: `DeEquityTechnicalDailyV2`, `DeEtfTechnicalDailyV2`, `DeGlobalTechnicalDailyV2`, `DeIndexTechnicalDaily`, `DeMfTechnicalDaily`
- **Modify**: `app/models/__init__.py` — re-export the 5 new model classes

## Column catalog (identical across all 5 tables where applicable)

### Price/OHLCV snapshot
- `close_adj Numeric(18,4)` — adjusted close for equity/etf, raw close for index/mf

### Overlap/Trend (Numeric(18,4))
- `sma_5, sma_10, sma_20, sma_50, sma_100, sma_200`
- `ema_5, ema_10, ema_20, ema_50, ema_100, ema_200`
- `dema_20, tema_20, wma_20, hma_20`
- `vwap, kama_20, zlma_20, alma_20`

### Momentum (Numeric(8,4) for bounded, Numeric(10,4) for unbounded)
- `rsi_7, rsi_9, rsi_14, rsi_21` — bounded
- `macd_line, macd_signal, macd_histogram` — unbounded
- `stochastic_k, stochastic_d` — bounded
- `cci_20` — can exceed ±100
- `mfi_14` — bounded
- `roc_5, roc_10, roc_21, roc_63, roc_252` — percent
- `tsi_13_25, williams_r_14, cmo_14, trix_15, ultosc`

### Volatility
- `bb_upper, bb_middle, bb_lower Numeric(18,4)` — price-scale
- `bb_width, bb_pct_b Numeric(8,4)`
- `atr_7, atr_14, atr_21 Numeric(18,4)`
- `natr_14 Numeric(8,4)` — percent
- `true_range Numeric(18,4)`
- `keltner_upper, keltner_middle, keltner_lower Numeric(18,4)`
- `donchian_upper, donchian_middle, donchian_lower Numeric(18,4)`
- `hv_20, hv_60, hv_252 Numeric(10,4)` — annualized percent

### Volume (BigInteger for aggregates, Numeric(18,4) for prices)
- `obv BigInteger`
- `ad BigInteger, adosc_3_10 Numeric(18,4)`
- `cmf_20 Numeric(8,4)`
- `efi_13 Numeric(18,4)`
- `eom_14 Numeric(18,4)`
- `kvo Numeric(18,4)`
- `pvt BigInteger`

### Trend strength
- `adx_14, plus_di, minus_di Numeric(8,4)`
- `aroon_up, aroon_down, aroon_osc Numeric(8,4)`
- `supertrend_10_3 Numeric(18,4), supertrend_direction SmallInteger`
- `psar Numeric(18,4)`

### Statistics
- `zscore_20 Numeric(10,4)`
- `linreg_slope_20 Numeric(18,4), linreg_r2_20 Numeric(8,4), linreg_angle_20 Numeric(8,4)`
- `skew_20, kurt_20 Numeric(10,4)`

### Risk metrics (empyrical)
- `risk_sharpe_1y, risk_sortino_1y, risk_calmar_1y Numeric(10,4)`
- `risk_max_drawdown_1y Numeric(10,4)` — negative number, e.g. -0.2345
- `risk_beta_nifty, risk_alpha_nifty Numeric(10,4)`
- `risk_omega, risk_information_ratio Numeric(10,4)`

### Derived booleans (GENERATED STORED)
- `above_50dma BOOL GENERATED ALWAYS AS (close_adj > sma_50) STORED`
- `above_200dma BOOL GENERATED ALWAYS AS (close_adj > sma_200) STORED`
- `above_20ema BOOL GENERATED ALWAYS AS (close_adj > ema_20) STORED`
- `price_above_vwap BOOL GENERATED ALWAYS AS (close_adj > vwap) STORED`
- `rsi_overbought BOOL GENERATED ALWAYS AS (rsi_14 > 70) STORED`
- `rsi_oversold BOOL GENERATED ALWAYS AS (rsi_14 < 30) STORED`
- `macd_bullish BOOL GENERATED ALWAYS AS (macd_line > macd_signal) STORED`
- `adx_strong_trend BOOL GENERATED ALWAYS AS (adx_14 > 25) STORED`

### Audit
- `created_at, updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()`

## Per-table key differences
| Table | PK | FK column | FK references |
|---|---|---|---|
| `de_equity_technical_daily_v2` | `(date, instrument_id)` | `instrument_id UUID` | `de_instrument(id)` |
| `de_etf_technical_daily_v2` | `(date, ticker)` | `ticker String(20)` | `de_etf_master(ticker)` |
| `de_global_technical_daily_v2` | `(date, ticker)` | `ticker String(30)` | `de_global_instrument_master(ticker)` |
| `de_index_technical_daily` | `(date, index_code)` | `index_code String(50)` | `de_index_master(index_code)` |
| `de_mf_technical_daily` | `(nav_date, mstar_id)` | `mstar_id String(20)` | `de_mf_master(mstar_id)` |

## Acceptance criteria
- `alembic upgrade head` applies cleanly to a fresh DB
- `alembic downgrade -2` reverses both migrations cleanly
- `alembic upgrade head` re-applies after downgrade
- `SELECT column_name FROM information_schema.columns WHERE table_name='de_equity_technical_daily_v2'` returns ≥130 columns
- `\d de_mf_master` shows `purchase_mode` column
- `pytest tests/ -v` — all existing tests still green
- `mypy app/models/indicators_v2.py --ignore-missing-imports` clean

## Verification commands
```bash
alembic upgrade head
alembic downgrade -2
alembic upgrade head
python -c "from app.models.indicators_v2 import DeEquityTechnicalDailyV2; print(DeEquityTechnicalDailyV2.__tablename__)"
```
