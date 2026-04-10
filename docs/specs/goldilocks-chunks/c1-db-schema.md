# C1: Database Schema & Models

**Complexity:** Medium
**Dependencies:** None
**Status:** pending

## Files
- alembic/versions/NNN_goldilocks_schema.py (new migration)
- app/models/goldilocks.py (new — SQLAlchemy models for goldilocks-specific tables)
- app/models/qualitative.py (modify — add report_type column to DeQualDocuments)

## What To Build

### New SQLAlchemy Models (app/models/goldilocks.py)

Use SQLAlchemy 2.0 mapped_column() syntax throughout. All tables prefixed `de_`.
All money/level columns: Numeric(18,4). All timestamps: TIMESTAMPTZ. All FKs: index=True.

**1. DeGoldilocksMarketView** — daily market view from Trend Friend reports
- report_date DATE PRIMARY KEY (one row per date)
- nifty_close Numeric(18,4)
- nifty_support_1 Numeric(18,4)
- nifty_support_2 Numeric(18,4)
- nifty_resistance_1 Numeric(18,4)
- nifty_resistance_2 Numeric(18,4)
- bank_nifty_close Numeric(18,4)
- bank_nifty_support_1 Numeric(18,4)
- bank_nifty_support_2 Numeric(18,4)
- bank_nifty_resistance_1 Numeric(18,4)
- bank_nifty_resistance_2 Numeric(18,4)
- trend_direction VARCHAR(20) CHECK IN ('upward', 'downward', 'sideways')
- trend_strength INTEGER CHECK BETWEEN 1 AND 5
- headline TEXT
- overall_view TEXT
- global_impact VARCHAR(20) CHECK IN ('positive', 'negative', 'neutral')
- created_at TIMESTAMPTZ NOT NULL (server_default=func.now())
- updated_at TIMESTAMPTZ NOT NULL (onupdate=func.now())

**2. DeGoldilocksSectorView** — sector rankings extracted from Trend Friend and Sector Trends reports
- report_date DATE NOT NULL
- sector VARCHAR(100) NOT NULL
- trend VARCHAR(20)
- outlook TEXT
- rank INTEGER (lower = stronger sector, nullable when rank not stated)
- top_picks JSONB (array of {"symbol": "RELIANCE", "resistance_levels": [2500, 2600]})
- PRIMARY KEY (report_date, sector)
- created_at TIMESTAMPTZ NOT NULL
- updated_at TIMESTAMPTZ NOT NULL

**3. DeGoldilocksStockIdeas** — individual stock recommendations from Stock Bullet and Big Catch reports
- id UUID PRIMARY KEY (default uuid4)
- document_id UUID FK → de_qual_documents.id (nullable=True, index=True)
- published_date DATE
- symbol VARCHAR(20)
- company_name VARCHAR(200)
- idea_type VARCHAR(20) CHECK IN ('stock_bullet', 'big_catch')
- entry_price Numeric(18,4) (point entry, nullable when zone given)
- entry_zone_low Numeric(18,4)
- entry_zone_high Numeric(18,4)
- target_1 Numeric(18,4)
- target_2 Numeric(18,4)
- lt_target Numeric(18,4) (long-term target, nullable)
- stop_loss Numeric(18,4)
- timeframe VARCHAR(50) (e.g., "3-6 months", "short term")
- rationale TEXT
- technical_params JSONB ({"ema_200": 1450.0, "rsi_14": 62.0, "support_1": 1380.0})
- status VARCHAR(20) DEFAULT 'active' CHECK IN ('active', 'target_1_hit', 'target_2_hit', 'sl_hit', 'expired', 'closed')
- status_updated_at TIMESTAMPTZ (nullable)
- created_at TIMESTAMPTZ NOT NULL
- updated_at TIMESTAMPTZ NOT NULL

**4. New computation tables:**

**DeOscillatorWeekly** — weekly stochastic and RSI values
- date DATE NOT NULL
- instrument_id UUID NOT NULL FK → de_instruments.id (index=True)
- stochastic_k Numeric(8,4)
- stochastic_d Numeric(8,4)
- rsi_14 Numeric(8,4)
- disparity_20 Numeric(8,4)
- PRIMARY KEY (date, instrument_id)
- created_at TIMESTAMPTZ NOT NULL

**DeOscillatorMonthly** — same structure as weekly but monthly bars
- date DATE NOT NULL
- instrument_id UUID NOT NULL FK → de_instruments.id (index=True)
- stochastic_k Numeric(8,4)
- stochastic_d Numeric(8,4)
- rsi_14 Numeric(8,4)
- disparity_20 Numeric(8,4)
- PRIMARY KEY (date, instrument_id)
- created_at TIMESTAMPTZ NOT NULL

**DeDivergenceSignals** — detected price-vs-oscillator divergences
- id UUID PRIMARY KEY (default uuid4)
- date DATE NOT NULL
- instrument_id UUID NOT NULL FK → de_instruments.id (index=True)
- timeframe VARCHAR(10) CHECK IN ('daily', 'weekly', 'monthly')
- divergence_type VARCHAR(20) CHECK IN ('bullish', 'bearish', 'triple_bullish', 'triple_bearish')
- indicator VARCHAR(20) CHECK IN ('rsi', 'stochastic', 'macd')
- price_direction VARCHAR(10) (e.g., 'lower_low', 'higher_high')
- indicator_direction VARCHAR(10) (e.g., 'higher_low', 'lower_high')
- strength INTEGER (1=regular, 2=strong, 3=triple — Gautam's strongest signal)
- created_at TIMESTAMPTZ NOT NULL

**DeFibLevels** — most recent swing fibonacci retracement levels per instrument
- date DATE NOT NULL (date the swing was identified)
- instrument_id UUID NOT NULL FK → de_instruments.id (index=True)
- swing_high Numeric(18,4)
- swing_low Numeric(18,4)
- fib_236 Numeric(18,4)
- fib_382 Numeric(18,4)
- fib_500 Numeric(18,4)
- fib_618 Numeric(18,4)
- fib_786 Numeric(18,4)
- PRIMARY KEY (date, instrument_id)
- created_at TIMESTAMPTZ NOT NULL

**DeIndexPivots** — daily pivot points computed for major indices only
- date DATE NOT NULL
- index_code VARCHAR(30) NOT NULL (e.g., 'NIFTY50', 'BANKNIFTY', 'NIFTYIT')
- pivot Numeric(18,4)
- s1 Numeric(18,4)
- s2 Numeric(18,4)
- s3 Numeric(18,4)
- r1 Numeric(18,4)
- r2 Numeric(18,4)
- r3 Numeric(18,4)
- PRIMARY KEY (date, index_code)
- created_at TIMESTAMPTZ NOT NULL

**DeIntermarketRatios** — computed ratio values for intermarket analysis
- date DATE NOT NULL
- ratio_name VARCHAR(50) NOT NULL (e.g., 'BANKNIFTY_NIFTY', 'GOLD_NIFTY', 'MICROCAP_NIFTY')
- value Numeric(18,4)
- sma_20 Numeric(18,4)
- direction VARCHAR(10) CHECK IN ('rising', 'falling', 'flat')
- PRIMARY KEY (date, ratio_name)
- created_at TIMESTAMPTZ NOT NULL

### Modify DeQualDocuments (app/models/qualitative.py)
Add column: report_type VARCHAR(30) nullable
CHECK IN ('trend_friend', 'big_picture', 'big_catch', 'stock_bullet', 'sector_trends',
          'fortnightly', 'concall', 'sound_byte', 'qa', 'snippet', 'usa_report')

Also add column: audio_duration_s INTEGER nullable
(needed for C3 video/audio transcription — seconds of audio)

### Modify de_equity_technical_daily (via migration only, no model change needed)
Add columns to existing table:
- stochastic_k Numeric(8,4)
- stochastic_d Numeric(8,4)
- disparity_20 Numeric(8,4) (((close - sma_20) / sma_20) * 100)
- disparity_50 Numeric(8,4) (((close - sma_50) / sma_50) * 100)
- bollinger_width Numeric(8,4) ((upper - lower) / middle * 100)

Note: If de_equity_technical_daily is already managed by an existing model,
add mapped_column entries there. Check app/models/ before touching this table.

## Migration Strategy
The existing raw-SQL-created tables (de_goldilocks_market_view, de_goldilocks_sector_view,
de_goldilocks_stock_ideas) are in the DB but not Alembic-managed. The migration must:
1. DROP the three existing raw tables (data is test/stub data, safe to drop)
2. CREATE them fresh with correct schema via Alembic
3. ADD new tables (oscillators, divergence, fib, pivots, intermarket)
4. ALTER de_qual_documents to add report_type and audio_duration_s
5. ALTER de_equity_technical_daily to add stochastic/disparity/bollinger columns

## Acceptance Criteria
- [ ] Migration runs clean: `alembic upgrade head`
- [ ] Migration is reversible: `alembic downgrade -1` works without error
- [ ] All new models importable: `from app.models.goldilocks import DeGoldilocksMarketView`
- [ ] Existing de_goldilocks_* raw tables dropped and recreated Alembic-managed
- [ ] DeQualDocuments.report_type and audio_duration_s columns added
- [ ] de_equity_technical_daily has stochastic_k, stochastic_d, disparity_20, disparity_50, bollinger_width
- [ ] All FK columns have index=True
- [ ] All price/level columns are Numeric(18,4), never Float
- [ ] All tables have created_at TIMESTAMPTZ (and updated_at where rows are updated)
- [ ] `ruff check . --select E,F,W` passes on new files
