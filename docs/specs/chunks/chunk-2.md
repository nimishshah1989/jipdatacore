# Chunk 2: Database Schema

**Layer:** 1
**Dependencies:** C1
**Complexity:** High
**Status:** done

## Files

- `alembic/versions/001_initial_schema.py` (or split into multiple migration files)
- `app/models/__init__.py`
- `app/models/instrument.py`
- `app/models/price.py`
- `app/models/mf.py`
- `app/models/flows.py`
- `app/models/computed.py`
- `app/models/qualitative.py`
- `app/models/client.py`
- `app/models/pipeline.py`
- `app/models/operational.py`

## Acceptance Criteria

- [ ] `alembic upgrade head` runs clean with zero errors on a fresh database
- [ ] All 40+ tables created as specified in spec Section 3 (full list below)
- [ ] All tables use `de_` prefix
- [ ] Required PostgreSQL extensions created: `pgcrypto`, `vector` (pgvector >= 0.4.0)
- [ ] `de_equity_ohlcv` partitioned by year 2000–2035 (36 partitions + default partition)
- [ ] `de_mf_nav_daily` partitioned by year 2006–2035 (30 partitions + default partition)
- [ ] `create_equity_partition(yr)` and `create_mf_nav_partition(yr)` functions created
- [ ] All primary keys, foreign keys, and unique constraints in place
- [ ] All CHECK constraints in place (data_status, action_type, entity_type, anomaly_type, etc.)
- [ ] All indexes created (see spec Section 3 for full list)
- [ ] `GENERATED ALWAYS AS` columns: `net_flow` in `de_institutional_flows`, `above_50dma` and `above_200dma` in `de_equity_technical_daily`
- [ ] Seed data inserted: `de_contributors` (goldilocks, bhaven, jeet, auto) and `de_system_flags` (INGESTION_ENABLED, API_ENABLED, QUALITATIVE_ENABLED, RECOMPUTE_ENABLED all TRUE)
- [ ] SQLAlchemy 2.0 models use `mapped_column()` syntax (not legacy `Column()`)
- [ ] Every FK column has `index=True`
- [ ] Money columns use `Numeric(18,4)`, return columns use `Numeric(10,4)`, percentage columns use `Numeric(6,2)`
- [ ] Alembic `downgrade()` implemented (migration is reversible)

## Tables Required

**Instrument Masters:**
- `de_instrument` — canonical equity instrument identity; current_symbol UNIQUE
- `de_market_cap_history` — temporal market cap classification (SEBI reclassifies bi-annually)
- `de_symbol_history` — ticker change history
- `de_index_master` — NSE index catalogue (60+ indices)
- `de_index_constituents` — index membership with effective_from/effective_to
- `de_mf_master` — MF fund master (13,380 funds from fie2-db-1); self-referential merged_into_mstar_id
- `de_mf_lifecycle` — fund events: launch, merge, closure, code/name/category change
- `de_macro_master` — macro indicator definitions (FRED, yfinance, NSE)
- `de_global_instrument_master` — global ETFs and indices
- `de_contributors` — named contributors (goldilocks, bhaven, jeet, auto)
- `de_trading_calendar` — NSE holidays + Saturday special sessions

**Price Data:**
- `de_equity_ohlcv` — partitioned OHLCV; symbol column is immutable historical snapshot
- `de_corporate_actions` — splits, bonuses, dividends (with dividend_type for multi-dividend same ex_date)
- `de_adjustment_factors_daily` — cumulative adj factor per instrument per date
- `de_recompute_queue` — async recompute jobs with heartbeat_at for stale worker recovery
- `de_data_anomalies` — post-ingestion anomalies with sparse typed entity columns
- `de_mf_nav_daily` — partitioned MF NAV; nav_adj for IDCW adjusted NAV
- `de_mf_dividends` — IDCW dividend events (sourced from AMFI, NOT inferred from NAV drops)
- `de_index_prices` — NSE index OHLCV
- `de_global_prices` — global instrument OHLCV
- `de_macro_values` — macro time series

**Flows:**
- `de_institutional_flows` — FII/DII daily flows; net_flow GENERATED ALWAYS
- `de_mf_category_flows` — monthly MF category AUM and flows

**Computed:**
- `de_equity_technical_daily` — pre-computed SMAs/EMAs; above_50dma/above_200dma GENERATED ALWAYS
- `de_rs_scores` — RS scores for stocks, sectors, MF categories, global
- `de_rs_daily_summary` — denormalised RS summary; PK is (date, instrument_id, vs_benchmark)
- `de_market_regime` — BULL/BEAR/SIDEWAYS/RECOVERY with component scores
- `de_breadth_daily` — 25 breadth indicators

**F&O:**
- `de_fo_summary` — daily PCR, OI, max pain

**Qualitative:**
- `de_qual_sources` — feed and upload source definitions
- `de_qual_documents` — ingested documents with embedding vector(1536)
- `de_qual_extracts` — structured Claude extractions with quality_score
- `de_qual_outcomes` — prediction outcome tracking

**Client (PII):**
- `de_clients` — encrypted PAN/phone/email + truncated HMAC blind indexes (8 chars)
- `de_client_keys` — append-only DEK store for envelope encryption
- `de_pii_access_log` — every PII field read logged
- `de_portfolios` — client portfolio definitions
- `de_portfolio_nav` — portfolio NAV history
- `de_portfolio_transactions` — trade ledger
- `de_portfolio_holdings` — point-in-time holdings
- `de_portfolio_risk_metrics` — risk metrics per portfolio per date

**Champion Trader:**
- `de_champion_trades` — stage analysis trade log

**Pipeline State:**
- `de_source_files` — file lineage: every ingested file registered before price/NAV inserts
- `de_pipeline_log` — pipeline run history with track_status JSONB
- `de_system_flags` — kill switches (INGESTION_ENABLED, API_ENABLED, etc.)
- `de_migration_log` — data migration audit trail
- `de_migration_errors` — per-row migration errors
- `de_request_log` — API request log (actor, IP, endpoint, status, duration)

## Notes

**Six non-negotiable schema rules (from spec):**
1. DATE columns store DATE type — never VARCHAR
2. Financial values: NUMERIC(18,4). Cumulative returns: NUMERIC(10,4). Percentages bounded 0-100: NUMERIC(6,2). Never FLOAT or DOUBLE PRECISION
3. Every table has `created_at TIMESTAMPTZ DEFAULT NOW()`
4. No triggers, no stored procedures
5. Every INSERT uses ON CONFLICT — full idempotency
6. No plaintext PII — HMAC blind indexes required for searchable PII

**Partition note:** `de_source_files` must be created BEFORE `de_equity_ohlcv` and `de_mf_nav_daily` because those tables reference `de_source_files(id)` via FK.

**Symbol contract:** The `symbol` column in `de_equity_ohlcv` is an immutable historical snapshot — never retroactively updated. API must resolve symbol → instrument_id before querying OHLCV to ensure PostgreSQL partition pruning works (partition key is `date`, not `symbol`).

**RS summary PK:** Uses `(date, instrument_id, vs_benchmark)` not `(date, symbol, vs_benchmark)` — changed in v1.7 for stability after symbol renames.

**HMAC blind index:** Truncated to 8 hex chars to force intentional collisions and prevent offline brute-force on low-entropy PAN/phone. Bucket search decrypts 2-3 rows in memory for exact match.

**PostgreSQL version requirement:** >= 12 required for `GENERATED ALWAYS AS ... STORED` support. Verify before running migrations: `SELECT version();`
