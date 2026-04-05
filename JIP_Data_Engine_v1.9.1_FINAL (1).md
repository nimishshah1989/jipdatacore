# Data Engine — Architecture Document v1.9.1 — FINAL
**Status:** Complete. 4 self-review rounds + 3 external model reviews + 4 rounds of consolidated audit remediation (56 total findings resolved).
**Total issues resolved:** v1.0→v1.9.1: 35 critical, 53 moderate, 23 minor
**Review history:** v1.0→v1.5 (internal) → v1.6 (3 external models) → v1.7 (3-audit remediation) → v1.8 (second audit round) → v1.9 (operational readiness + final audit) → v1.9.1 (cryptographic hardening + aggregate guardrails)

### v1.9.1 Change Log
| # | Severity | What changed | Source |
|---|----------|--------------|--------|
| 52 | CRITICAL | PII blind indexes: truncated to 8 chars to force collisions; search decrypts small bucket in memory | R8-F1 |
| 53 | MODERATE | Quarantine threshold guardrail: if >5% of equity universe quarantined, halt aggregate computations (breadth, RS, regime) | R8-F2 |
| 54 | MINOR | Redis circuit breaker: documented per-worker isolation behavior under multi-process Uvicorn | R8-F3 |
| 55 | MINOR | Corporate action adj_factor formula explicitly documented: `adj_factor = ratio_from / ratio_to` | R8-F4 |
| — | — | Added Section 12: Operational Readiness Checklist (sprint handoff) | R8-operational |

### v1.9 Change Log
| # | Severity | What changed | Source |
|---|----------|--------------|--------|
| 37 | CRITICAL | Data status gating: added `data_status` (raw/validated/quarantined) to all ingestion tables; API serves only `validated` rows | R6-F1 |
| 38 | CRITICAL | Data lineage: added `de_source_files` table + `source_file_id`/`pipeline_run_id` FK on ingestion tables | R6-F2 |
| 39 | CRITICAL | IDCW dividend: removed NAV-drop heuristic; sourced explicitly from AMFI dividend files / BSE Star MF | R7-F1 |
| 40 | CRITICAL | DEK rotation: `de_client_keys` now append-only with `key_version`; historical keys preserved for backup restoration | R7-F2 |
| 41 | MODERATE | SLA enforcement: defined pipeline completion deadlines with alerting on breach | R6-F3 |
| 42 | MODERATE | Kill switch: added `de_system_flags` table for global ingestion/API controls | R6-F7 |
| 43 | MODERATE | Anomalies: replaced generic `entity_id` VARCHAR with sparse typed columns (instrument_id UUID, mstar_id, ticker) | R7-F3 |
| 44 | MODERATE | Recompute queue: added `heartbeat_at` for stalled worker recovery + throttling limits | R7-F4, R6-F5 |
| 45 | MODERATE | Redis: added circuit breaker pattern (3 failures → bypass for 60s) | R6-F4 |
| 46 | MODERATE | Reconciliation pipeline (Section 5.13): cross-source validation for equity + MF | R6-F9 |
| 47 | MODERATE | Schema evolution rules: backward compatibility window, deprecation policy | R6-F6 |
| 48 | MODERATE | Qualitative extracts: added `quality_score` for downstream filtering | R6-F8 |
| 49 | MINOR | Added index on `de_pipeline_log.source_checksum` for duplicate detection | R7-F5 |
| 50 | MINOR | Trading calendar: documented NSE Saturday special sessions | R7-F6 |
| 51 | MINOR | Computation version exposed in API response headers | R6-F18 |

### v1.8 Change Log
| # | Severity | What changed | Source |
|---|----------|--------------|--------|
| 25 | CRITICAL | Corporate action UNIQUE constraint: added `dividend_type` column to handle multiple dividends on same ex_date | R5-F1 |
| 26 | CRITICAL | MF IDCW: added `nav_adj` column to `de_mf_nav_daily` and MF dividend adjustment pipeline | R5-F2 |
| 27 | CRITICAL | Added `de_data_anomalies` table and post-ingestion validation layer (price sanity, NAV drift, volume spikes) | R5-F8 |
| 28 | CRITICAL | Pipeline failure isolation: EOD pipeline restructured into independent tracks (equity/MF/macro/flows) with graceful degradation | R5-F9 |
| 29 | MODERATE | HMAC blind index rotation: added `hmac_version` column and rotation runbook to Section 3.7.1 | R5-F3 |
| 30 | MODERATE | Added `de_equity_technical_daily` table for incremental DMA computation; breadth reads from pre-computed values | R5-F4 |
| 31 | MODERATE | NSE UDiFF format: updated Section 4.1 to document three BHAV copy format variants | R5-F5 |
| 32 | MODERATE | Added `de_recompute_queue` for async corporate action recomputation at scale | R5-F7 |
| 33 | MODERATE | Cache stampede protection: added setnx lock pattern to Redis fallback contract | R5-F10 |
| 34 | MODERATE | Qualitative pipeline: added semantic deduplication, per-source rate limits, daily cost cap | R5-F11 |
| 35 | MODERATE | Computation versioning: added `computation_version` to RS scores and market regime tables | R5-F12 |
| 36 | MINOR | PgBouncer: increased pool size from 20 to 50, documented scaling guidance | R5-F6 |

### v1.7 Change Log
| # | Severity | What changed | Source |
|---|----------|--------------|--------|
| 1 | CRITICAL | Advisory lock: replaced Python `hash()` with deterministic `hashtext()` in Postgres | Doc1-F2, Doc3-F10 |
| 2 | CRITICAL | Advisory lock: switched to session-level `pg_advisory_lock` with explicit unlock and crash recovery | Doc1-F3 |
| 3 | CRITICAL | Added `de_adjustment_factors_daily` table and explicit corporate action recomputation logic | Doc1-F4 |
| 4 | CRITICAL | Added Master Refresh Pipeline (Step 0) to Daily EOD for new listings, symbol changes, delistings | Doc3-F2 |
| 5 | CRITICAL | Added Corporate Actions Ingestion (Step 0.5) to Daily EOD pipeline | Doc3-F3 |
| 6 | CRITICAL | RS summary: replaced DELETE/INSERT with atomic ON CONFLICT DO UPDATE | Doc2-F3, Doc3-F4 |
| 7 | CRITICAL | PII: added HMAC blind index columns (`pan_hash`, `email_hash`, `phone_hash`) for searchable encrypted fields | Doc2-F1 |
| 8 | CRITICAL | Market cap: moved to temporal `de_market_cap_history` table for point-in-time accuracy | Doc2-F2 |
| 9 | CRITICAL | MF return columns: changed NUMERIC(6,2) → NUMERIC(10,4) for cumulative returns exceeding 100% | Doc3-F1 |
| 10 | MODERATE | Added `volume_adj` BIGINT to `de_equity_ohlcv` for split-adjusted volume | Doc2-F4 |
| 11 | MODERATE | Added `create_mf_nav_partition()` function and call in EOD Step 15 | Doc2-F5 |
| 12 | MODERATE | RS summary PK: changed from `(date, symbol, vs_benchmark)` to `(date, instrument_id, vs_benchmark)` | Doc1-F9 |
| 13 | MODERATE | Encryption: specified full envelope encryption flow with KMS CMK, per-client DEK, nonce handling | Doc1-F7, Doc3-F6 |
| 14 | MODERATE | Redis: added configuration subsection with persistence, password, eviction, fallback requirements | Doc1-F8, Doc2-F6, Doc3-F8 |
| 15 | MODERATE | ClamAV: added pre-flight daemon check at orchestrator startup | Doc3-F5 |
| 16 | MODERATE | F&O summary: added ingestion step (Step 9.5) to Daily EOD pipeline | Doc3-F7 |
| 17 | MODERATE | AMFI lifecycle: defined merge NAV continuity and backfill logic | Doc1-F6 |
| 18 | MODERATE | Qualitative pipeline: lock granularity changed from per-day to per-document | Doc1-F10 |
| 19 | MODERATE | Symbol denormalization: documented as immutable historical snapshot with pipeline enforcement | Doc1-F1 |
| 20 | MINOR | Added `face_value_split` to corporate action CHECK constraint | Doc2-F8 |
| 21 | MINOR | Added self-referential FK and self-check on `de_mf_master.merged_into_mstar_id` | Doc3-F9 |
| 22 | MINOR | Qualitative pipeline: added Step r (S3 archival + local delete after ingestion) | Doc2-F7 |
| 23 | MINOR | Partition pruning: documented API must resolve symbol → instrument_id before querying OHLCV | Doc1-F5 |
| 24 | MINOR | Non-negotiable Rule 2 updated: cumulative returns use NUMERIC(10,4), not NUMERIC(6,2) | Doc3-F1 |

---

## 1. Overview and Principles

The Data Engine is the single source of truth for all data in the Jhaveri Intelligence Platform. One PostgreSQL database on AWS RDS, one FastAPI service (port 8010, internal only). All platforms are read-only consumers via authenticated internal API.

**No platform writes to the Data Engine except its own ingestion pipelines.**

### Six non-negotiable rules
1. DATE columns store DATE type — never VARCHAR
2. Financial values: NUMERIC(18,4). Percentages bounded [0-100]: NUMERIC(6,2). Ratios unbounded: NUMERIC(10,4). **Cumulative returns (which routinely exceed 100%): NUMERIC(10,4).** Never FLOAT or DOUBLE PRECISION.
3. Every table has created_at TIMESTAMPTZ DEFAULT NOW()
4. No application logic in the database — no triggers, no stored procedures
5. Every INSERT uses ON CONFLICT DO UPDATE or DO NOTHING on a NATURAL KEY — full idempotency always
6. No plaintext PII in any table — PAN, phone, email encrypted before insert. **Searchable PII fields require HMAC blind index columns.**

---

## 2. Pre-Sprint Security Actions (mandatory before build starts)

- Rotate jsl-wealth-key.pem — generate new keypair in AWS Console, replace on EC2, invalidate old
- Rotate fie-key.pem — same process
- Rotate database password — use `openssl rand -base64 32` — update all .env files
- Tighten RDS security group: port 5432 accepts from 172.31.10.182 only (EC2 private IP)
- JWT_SECRET: stored in AWS Secrets Manager, loaded via boto3 at FastAPI startup — never in .env or code
- All API keys (FRED, OpenAI, Anthropic, ClamAV) stored in AWS Secrets Manager
- Orchestrator dashboard: 127.0.0.1:8099 only — SSH tunnel to view
- HTTPS enforced on core.jslwealth.in and upload.jslwealth.in
- Install Redis on EC2: `sudo apt install redis-server` — configure per Section 9.1
- Install ClamAV on EC2: `sudo apt install clamav clamav-daemon && freshclam`
- **Create KMS CMK**: alias `data-engine-pii`, store ARN in Secrets Manager as `PII_KMS_KEY_ARN`. Create separate HMAC signing key, store as `PII_HMAC_KEY_ARN`. Document both ARNs in deployment runbook.
- **Create per-client Data Encryption Keys (DEKs)**: generate AES-256 DEK per client_id, encrypt DEK with KMS CMK (envelope encryption), store encrypted DEK in `de_client_keys` table. See Section 3.7.1 for full flow.

---

## 3. Database Schema

### 3.0 Required Extensions
```sql
-- Must run before any table creation
CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid(), encrypt()
CREATE EXTENSION IF NOT EXISTS vector;     -- pgvector embeddings (>= 0.4.0)

-- Verify pgvector: SELECT extversion FROM pg_extension WHERE extname = 'vector';
-- Verify RDS PostgreSQL >= 12 for GENERATED ALWAYS AS support
```

### 3.1 Instrument Master Tables

```sql
-- Canonical instrument identity — immutable surrogate key
-- symbol changes tracked in de_symbol_history, instrument_id never changes
-- NOTE: market_cap_cat removed from this table — see de_market_cap_history for temporal classification
CREATE TABLE de_instrument (
  instrument_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  current_symbol  VARCHAR(20) NOT NULL UNIQUE,
  isin            VARCHAR(12) UNIQUE,
  company_name    VARCHAR(200) NOT NULL,
  exchange        VARCHAR(10) NOT NULL DEFAULT 'NSE',  -- NSE/BSE
  series          VARCHAR(5) DEFAULT 'EQ',
  sector          VARCHAR(100),
  industry        VARCHAR(100),
  nifty_50        BOOLEAN DEFAULT FALSE,
  nifty_200       BOOLEAN DEFAULT FALSE,
  nifty_500       BOOLEAN DEFAULT FALSE,
  listing_date    DATE,
  bse_symbol      VARCHAR(20),          -- BSE equivalent if different
  is_active       BOOLEAN DEFAULT TRUE,
  is_suspended    BOOLEAN DEFAULT FALSE,
  suspended_from  DATE,
  delisted_on     DATE,
  is_tradeable    BOOLEAN DEFAULT TRUE,
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- [v1.7] Point-in-time market cap classification
-- SEBI reclassifies every 6 months; backtests must use classification as-of-date
CREATE TABLE de_market_cap_history (
  instrument_id   UUID NOT NULL REFERENCES de_instrument(instrument_id),
  cap_category    VARCHAR(20) NOT NULL CHECK (cap_category IN ('large','mid','small','micro')),
  effective_from  DATE NOT NULL,
  effective_to    DATE,  -- NULL = current classification
  source          VARCHAR(50) DEFAULT 'amfi',  -- amfi / sebi / manual
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (instrument_id, effective_from)
);
-- Only one active classification per instrument
CREATE UNIQUE INDEX idx_market_cap_active
  ON de_market_cap_history(instrument_id)
  WHERE effective_to IS NULL;

-- Symbol history for ticker changes
CREATE TABLE de_symbol_history (
  instrument_id   UUID NOT NULL REFERENCES de_instrument(instrument_id),
  old_symbol      VARCHAR(20) NOT NULL,
  new_symbol      VARCHAR(20) NOT NULL,
  effective_date  DATE NOT NULL,
  reason          TEXT,
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (instrument_id, effective_date)
);

CREATE TABLE de_index_master (
  index_code    VARCHAR(50) PRIMARY KEY,
  index_name    VARCHAR(200) NOT NULL,
  category      VARCHAR(50) CHECK (category IN ('broad','sectoral','thematic','strategy')),
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE de_index_constituents (
  index_code      VARCHAR(50) NOT NULL REFERENCES de_index_master(index_code),
  instrument_id   UUID NOT NULL REFERENCES de_instrument(instrument_id),
  weight_pct      NUMERIC(6,2),
  effective_from  DATE NOT NULL,
  effective_to    DATE,
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (index_code, instrument_id, effective_from)
);
-- Only one active record per instrument per index
CREATE UNIQUE INDEX idx_constituents_active
  ON de_index_constituents(index_code, instrument_id)
  WHERE effective_to IS NULL;

CREATE TABLE de_mf_master (
  mstar_id              VARCHAR(20) PRIMARY KEY,
  amfi_code             VARCHAR(20),
  isin                  VARCHAR(12),
  fund_name             VARCHAR(300) NOT NULL,
  amc_name              VARCHAR(200),
  category_name         VARCHAR(100),
  broad_category        VARCHAR(50),
  is_index_fund         BOOLEAN DEFAULT FALSE,
  is_etf                BOOLEAN DEFAULT FALSE,
  is_active             BOOLEAN DEFAULT TRUE,
  inception_date        DATE,
  closure_date          DATE,
  merged_into_mstar_id  VARCHAR(20)
    REFERENCES de_mf_master(mstar_id)
    CHECK (merged_into_mstar_id IS NULL OR merged_into_mstar_id != mstar_id),
  primary_benchmark     VARCHAR(100),
  expense_ratio         NUMERIC(6,4),
  investment_strategy   TEXT,
  created_at            TIMESTAMPTZ DEFAULT NOW(),
  updated_at            TIMESTAMPTZ DEFAULT NOW()
);

-- Scheme lifecycle history (mergers, closures, code changes)
CREATE TABLE de_mf_lifecycle (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  mstar_id        VARCHAR(20) NOT NULL REFERENCES de_mf_master(mstar_id),
  event_type      VARCHAR(30) NOT NULL CHECK (event_type IN
    ('launch','merge_from','merge_into','closure','code_change','name_change','category_change')),
  event_date      DATE NOT NULL,
  old_value       TEXT,
  new_value       TEXT,
  notes           TEXT,
  created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE de_macro_master (
  ticker        VARCHAR(20) PRIMARY KEY,
  name          VARCHAR(200) NOT NULL,
  source        VARCHAR(50) CHECK (source IN ('fred','yfinance','manual','nse')),
  unit          VARCHAR(50),
  frequency     VARCHAR(20) CHECK (frequency IN ('daily','weekly','monthly','quarterly')),
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE de_global_instrument_master (
  ticker          VARCHAR(20) PRIMARY KEY,
  name            VARCHAR(200) NOT NULL,
  instrument_type VARCHAR(20) CHECK (instrument_type IN ('index','etf')),
  exchange        VARCHAR(20),
  currency        VARCHAR(10),
  country         VARCHAR(50),
  category        VARCHAR(100),
  source          VARCHAR(50),
  created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE de_contributors (
  id            SERIAL PRIMARY KEY,
  name          VARCHAR(100) NOT NULL UNIQUE,
  role          VARCHAR(100) CHECK (role IN ('external_trader','fund_manager','principal','automated_feed')),
  is_admin      BOOLEAN DEFAULT FALSE,
  is_active     BOOLEAN DEFAULT TRUE,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);
-- Seed data:
-- INSERT INTO de_contributors (name, role, is_admin) VALUES
--   ('goldilocks','external_trader',FALSE),
--   ('bhaven','fund_manager',TRUE),
--   ('jeet','principal',TRUE),
--   ('auto','automated_feed',FALSE);

-- Trading calendar (NSE holidays)
-- Populate once per year in January from NSE holiday list
-- [v1.9] NSE occasionally schedules Saturday "Special Live Trading Sessions"
-- (typically for DR site failover testing). BHAV copies are published.
-- Calendar population must include these ad-hoc sessions.
-- Orchestrator cron must NOT restrict to Mon-Fri; check de_trading_calendar always.
CREATE TABLE de_trading_calendar (
  date          DATE PRIMARY KEY,
  is_trading    BOOLEAN NOT NULL DEFAULT TRUE,
  exchange      VARCHAR(10) NOT NULL DEFAULT 'NSE',
  notes         TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);
```

### 3.2 Price Data Tables

```sql
-- Partitioned by year — all price data uses instrument_id FK not raw symbol
-- SYMBOL CONTRACT [v1.7]: symbol column is an IMMUTABLE HISTORICAL SNAPSHOT.
-- It records the symbol as-of trade date. It is NEVER updated retroactively.
-- Pipeline enforcement: on insert, symbol must equal de_instrument.current_symbol at that date.
-- For API queries: ALWAYS resolve symbol → instrument_id first, then query on (instrument_id, date).
-- This avoids partition pruning bypass (partition key = date, not symbol).
CREATE TABLE de_equity_ohlcv (
  date            DATE NOT NULL,
  instrument_id   UUID NOT NULL REFERENCES de_instrument(instrument_id),
  symbol          VARCHAR(20) NOT NULL,   -- immutable historical snapshot (see contract above)
  open            NUMERIC(18,4),
  high            NUMERIC(18,4),
  low             NUMERIC(18,4),
  close           NUMERIC(18,4),
  close_adj       NUMERIC(18,4),
  open_adj        NUMERIC(18,4),   -- corporate-action adjusted open
  high_adj        NUMERIC(18,4),   -- corporate-action adjusted high
  low_adj         NUMERIC(18,4),   -- corporate-action adjusted low
  volume          BIGINT,
  volume_adj      BIGINT,          -- [v1.7] split-adjusted volume (historical volume × split factor)
  delivery_vol    BIGINT,
  delivery_pct    NUMERIC(6,2),
  trades          INTEGER,
  data_status     VARCHAR(20) NOT NULL DEFAULT 'raw'
    CHECK (data_status IN ('raw','validated','quarantined')),  -- [v1.9] API serves only 'validated'
  source_file_id  UUID REFERENCES de_source_files(id),         -- [v1.9] lineage: which file produced this row
  pipeline_run_id INTEGER REFERENCES de_pipeline_log(id),      -- [v1.9] lineage: which pipeline run
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (date, instrument_id)
) PARTITION BY RANGE (date);

-- Automated future partition creation function
CREATE OR REPLACE FUNCTION create_equity_partition(yr INTEGER) RETURNS VOID AS $$
DECLARE
  tbl_name TEXT := 'de_equity_ohlcv_' || yr;
  from_date TEXT := yr || '-01-01';
  to_date   TEXT := (yr + 1) || '-01-01';
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = tbl_name) THEN
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF de_equity_ohlcv FOR VALUES FROM (%L) TO (%L)',
      tbl_name, from_date, to_date
    );
    RAISE NOTICE 'Created partition: %', tbl_name;
  END IF;
END;
$$ LANGUAGE plpgsql;

-- [v1.7] MF NAV partition creation function (mirrors equity)
CREATE OR REPLACE FUNCTION create_mf_nav_partition(yr INTEGER) RETURNS VOID AS $$
DECLARE
  tbl_name TEXT := 'de_mf_nav_' || yr;
  from_date TEXT := yr || '-01-01';
  to_date   TEXT := (yr + 1) || '-01-01';
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = tbl_name) THEN
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF de_mf_nav_daily FOR VALUES FROM (%L) TO (%L)',
      tbl_name, from_date, to_date
    );
    RAISE NOTICE 'Created MF NAV partition: %', tbl_name;
  END IF;
END;
$$ LANGUAGE plpgsql;

-- Create partitions 2000–2035 at schema init
DO $$ BEGIN
  FOR yr IN 2000..2035 LOOP
    PERFORM create_equity_partition(yr);
  END LOOP;
END $$;
CREATE TABLE de_equity_ohlcv_default PARTITION OF de_equity_ohlcv DEFAULT;

CREATE INDEX idx_equity_instrument_date ON de_equity_ohlcv(instrument_id, date DESC);
CREATE INDEX idx_equity_symbol_date ON de_equity_ohlcv(symbol, date DESC);  -- for ad-hoc debug only; API must not use
CREATE INDEX idx_equity_date ON de_equity_ohlcv(date DESC);

-- Corporate action events (expanded model)
CREATE TABLE de_corporate_actions (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  instrument_id   UUID NOT NULL REFERENCES de_instrument(instrument_id),
  ex_date         DATE NOT NULL,
  action_type     VARCHAR(30) NOT NULL CHECK (action_type IN (
    'split','bonus','dividend','rights','buyback','merger_absorb',
    'merger_swap','demerger','capital_reduction','name_change','symbol_change',
    'face_value_split'
  )),
  dividend_type   VARCHAR(20) CHECK (dividend_type IN (
    'interim','final','special',NULL
  )),  -- [v1.8] required when action_type='dividend'; allows multiple dividends on same ex_date
  ratio_from      NUMERIC(18,8),   -- e.g. 1 old share
  ratio_to        NUMERIC(18,8),   -- becomes N new shares
  cash_value      NUMERIC(18,4),   -- for dividends
  new_instrument_id UUID REFERENCES de_instrument(instrument_id),  -- for mergers/demergers
  adj_factor      NUMERIC(18,8),   -- per-event price adjustment factor
  -- [v1.9.1] FORMULA: adj_factor = ratio_from / ratio_to
  -- Stock split 1:10 (1 old → 10 new): adj_factor = 1/10 = 0.1
  --   historical_price_adj = historical_price × 0.1 (prices go down to match current)
  -- Reverse split / consolidation 10:1 (10 old → 1 new): adj_factor = 10/1 = 10.0
  --   historical_price_adj = historical_price × 10.0 (prices go up to match current)
  -- Bonus 1:1 (1 bonus for 1 held): ratio_from=1, ratio_to=2, adj_factor = 1/2 = 0.5
  -- For dividends: adj_factor = (close_before_ex - dividend) / close_before_ex
  notes           TEXT,
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  -- [v1.8] UNIQUE includes dividend_type so final + special dividend on same ex_date won't collide
  UNIQUE (instrument_id, ex_date, action_type, dividend_type)
);

-- [v1.7] Cumulative adjustment factors per instrument per date
-- This is the SINGLE SOURCE OF TRUTH for price adjustment.
-- adjusted_price = raw_price × cumulative_factor
-- adjusted_volume = raw_volume / cumulative_factor (inverse for volume)
-- Maintained by corporate actions pipeline; recomputed from earliest affected ex_date forward.
CREATE TABLE de_adjustment_factors_daily (
  instrument_id     UUID NOT NULL REFERENCES de_instrument(instrument_id),
  date              DATE NOT NULL,
  cumulative_factor NUMERIC(18,8) NOT NULL DEFAULT 1.0,
  last_action_id    UUID REFERENCES de_corporate_actions(id),  -- most recent action contributing to this factor
  created_at        TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (instrument_id, date)
);
-- Recomputation logic (Section 5.9.1):
-- 1. Fetch all corporate actions for instrument ordered by ex_date ASC
-- 2. Walk forward: cumulative_factor = product of all adj_factors where ex_date <= date
-- 3. For each trading date from earliest affected ex_date to today:
--    INSERT INTO de_adjustment_factors_daily ON CONFLICT DO UPDATE
-- 4. Then: UPDATE de_equity_ohlcv SET
--      close_adj = close * f.cumulative_factor,
--      open_adj  = open  * f.cumulative_factor,
--      high_adj  = high  * f.cumulative_factor,
--      low_adj   = low   * f.cumulative_factor,
--      volume_adj = volume / f.cumulative_factor
--    FROM de_adjustment_factors_daily f
--    WHERE f.instrument_id = de_equity_ohlcv.instrument_id AND f.date = de_equity_ohlcv.date

-- [v1.8] Async recomputation queue for corporate action adjustments
-- At scale, inline recomputation of 20 years of OHLCV for one instrument can stall pipelines.
-- Pipeline enqueues; background worker processes in batches during off-peak hours.
CREATE TABLE de_recompute_queue (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  instrument_id   UUID NOT NULL REFERENCES de_instrument(instrument_id),
  from_date       DATE NOT NULL,       -- earliest affected date
  trigger_action_id UUID REFERENCES de_corporate_actions(id),
  priority        INTEGER DEFAULT 5 CHECK (priority BETWEEN 1 AND 10),  -- 1=highest
  status          VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending','processing','complete','failed')),
  heartbeat_at    TIMESTAMPTZ,         -- [v1.9] worker updates every 60s; stale if > 15min
  enqueued_at     TIMESTAMPTZ DEFAULT NOW(),
  started_at      TIMESTAMPTZ,
  completed_at    TIMESTAMPTZ,
  error_detail    TEXT
);
CREATE INDEX idx_recompute_queue_status ON de_recompute_queue(status, priority)
  WHERE status IN ('pending','processing');
-- [v1.9] Dedup: only one pending item per instrument (ON CONFLICT by instrument_id WHERE status='pending')
CREATE UNIQUE INDEX idx_recompute_queue_dedup ON de_recompute_queue(instrument_id)
  WHERE status = 'pending';
-- Worker constraints [v1.9]:
--   max_concurrent_recomputes = 2 (enforced by worker pool size)
--   max_rows_per_run = 50,000 OHLCV rows per batch
--   Schedule: every 15 minutes during 22:00–06:00 IST, on-demand during market hours
-- Stale worker recovery (orchestrator checks every 5 min):
--   UPDATE de_recompute_queue SET status='pending', heartbeat_at=NULL
--   WHERE status='processing' AND heartbeat_at < NOW() - INTERVAL '15 minutes'

-- [v1.8] Post-ingestion data anomaly detection
-- Every pipeline step writes anomalies here; human review via admin dashboard
CREATE TABLE de_data_anomalies (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  pipeline_name   VARCHAR(100) NOT NULL,
  business_date   DATE NOT NULL,
  entity_type     VARCHAR(20) NOT NULL CHECK (entity_type IN ('equity','mf','index','macro','flow')),
  -- [v1.9] Sparse typed columns instead of generic VARCHAR entity_id
  -- Prevents UUID cast errors when joining to typed tables
  instrument_id   UUID REFERENCES de_instrument(instrument_id),
  mstar_id        VARCHAR(20),
  ticker          VARCHAR(20),
  CHECK (
    (entity_type = 'equity' AND instrument_id IS NOT NULL AND mstar_id IS NULL AND ticker IS NULL) OR
    (entity_type = 'mf' AND mstar_id IS NOT NULL AND instrument_id IS NULL AND ticker IS NULL) OR
    (entity_type IN ('index','macro','flow') AND ticker IS NOT NULL AND instrument_id IS NULL AND mstar_id IS NULL)
  ),
  anomaly_type    VARCHAR(50) NOT NULL CHECK (anomaly_type IN (
    'price_spike','price_drop','volume_spike','nav_spike','nav_drop',
    'split_ratio_suspicious','missing_data','stale_data','duplicate_data',
    'negative_value','zero_nav','future_date'
  )),
  severity        VARCHAR(10) NOT NULL CHECK (severity IN ('critical','warning','info')),
  expected_range  TEXT,                   -- e.g. "close within ±20% of prev_close"
  actual_value    TEXT,                   -- e.g. "close=450, prev_close=150 (+200%)"
  is_resolved     BOOLEAN DEFAULT FALSE,
  resolved_by     VARCHAR(100),
  resolved_at     TIMESTAMPTZ,
  resolution_note TEXT,                   -- e.g. "confirmed stock split, adj_factor applied"
  detected_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_anomalies_unresolved ON de_data_anomalies(business_date DESC, severity)
  WHERE is_resolved = FALSE;

-- MF NAV — partitioned by year
-- [v1.8] nav_adj added for IDCW mutual fund dividend adjustment.
-- When an IDCW fund pays a dividend, NAV drops proportionally.
-- nav_adj = nav adjusted for all historical dividends (like close_adj for equities).
-- Without nav_adj, backtested IDCW returns are artificially suppressed.
CREATE TABLE de_mf_nav_daily (
  nav_date          DATE NOT NULL,
  mstar_id          VARCHAR(20) NOT NULL REFERENCES de_mf_master(mstar_id),
  nav               NUMERIC(18,4) NOT NULL CHECK (nav > 0),
  nav_adj           NUMERIC(18,4),       -- [v1.8] dividend-adjusted NAV for IDCW plans
  nav_change        NUMERIC(18,4),
  nav_change_pct    NUMERIC(10,4),      -- [v1.7] was NUMERIC(6,2), changed for >100% daily moves (NFO launches)
  return_1d         NUMERIC(10,4),      -- [v1.7] all return columns changed from NUMERIC(6,2)
  return_1w         NUMERIC(10,4),      -- cumulative returns routinely exceed 100%
  return_1m         NUMERIC(10,4),      -- e.g. 10-year return of 300% = 300.0000
  return_3m         NUMERIC(10,4),
  return_6m         NUMERIC(10,4),
  return_1y         NUMERIC(10,4),
  return_3y         NUMERIC(10,4),
  return_5y         NUMERIC(10,4),
  return_10y        NUMERIC(10,4),
  nav_52wk_high     NUMERIC(18,4),
  nav_52wk_low      NUMERIC(18,4),
  data_status     VARCHAR(20) NOT NULL DEFAULT 'raw'
    CHECK (data_status IN ('raw','validated','quarantined')),  -- [v1.9]
  source_file_id  UUID REFERENCES de_source_files(id),         -- [v1.9]
  pipeline_run_id INTEGER REFERENCES de_pipeline_log(id),      -- [v1.9]
  created_at        TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (nav_date, mstar_id)
) PARTITION BY RANGE (nav_date);

-- [v1.8] MF dividend events for IDCW plan NAV adjustment
-- [v1.9] CRITICAL: dividends must be sourced EXPLICITLY from AMFI dividend history files
-- or BSE Star MF / NSE NMF II feeds. NEVER infer from NAV drops (March 2020 would
-- cause mass false positives — equity funds gapping down 10-15% would be tagged as dividends).
CREATE TABLE de_mf_dividends (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  mstar_id        VARCHAR(20) NOT NULL REFERENCES de_mf_master(mstar_id),
  record_date     DATE NOT NULL,
  dividend_per_unit NUMERIC(18,4) NOT NULL CHECK (dividend_per_unit > 0),
  nav_before       NUMERIC(18,4),
  nav_after        NUMERIC(18,4),
  adj_factor       NUMERIC(18,8),  -- nav_after / nav_before (multiplicative chain, same as equity)
  source           VARCHAR(50) DEFAULT 'amfi',
  created_at       TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (mstar_id, record_date)
);
-- MF nav_adj recomputation mirrors equity logic:
-- cumulative_factor = product of all adj_factors where record_date <= nav_date
-- nav_adj = nav * cumulative_factor
-- Source: AMFI historical dividend text files (https://www.amfiindia.com/net-asset-value/nav-history)
-- Fallback: BSE Star MF dividend announcements

DO $$ BEGIN
  FOR yr IN 2006..2035 LOOP
    PERFORM create_mf_nav_partition(yr);
  END LOOP;
END $$;
CREATE TABLE de_mf_nav_default PARTITION OF de_mf_nav_daily DEFAULT;

CREATE INDEX idx_mf_nav_mstar_date ON de_mf_nav_daily(mstar_id, nav_date DESC);
CREATE INDEX idx_mf_nav_date ON de_mf_nav_daily(nav_date DESC);

CREATE TABLE de_index_prices (
  date          DATE NOT NULL,
  index_code    VARCHAR(50) NOT NULL REFERENCES de_index_master(index_code),
  open          NUMERIC(18,4),
  high          NUMERIC(18,4),
  low           NUMERIC(18,4),
  close         NUMERIC(18,4),
  volume        BIGINT,
  pe_ratio      NUMERIC(10,4),
  pb_ratio      NUMERIC(10,4),
  div_yield     NUMERIC(6,2),
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (date, index_code)
);
CREATE INDEX idx_index_prices_code_date ON de_index_prices(index_code, date DESC);

CREATE TABLE de_global_prices (
  date          DATE NOT NULL,
  ticker        VARCHAR(20) NOT NULL REFERENCES de_global_instrument_master(ticker),
  open          NUMERIC(18,4),
  high          NUMERIC(18,4),
  low           NUMERIC(18,4),
  close         NUMERIC(18,4),
  volume        BIGINT,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (date, ticker)
);
CREATE INDEX idx_global_prices_ticker_date ON de_global_prices(ticker, date DESC);

CREATE TABLE de_macro_values (
  date          DATE NOT NULL,
  ticker        VARCHAR(20) NOT NULL REFERENCES de_macro_master(ticker),
  value         NUMERIC(18,4) NOT NULL,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (date, ticker)
);
CREATE INDEX idx_macro_ticker_date ON de_macro_values(ticker, date DESC);
```

### 3.3 Flow Data

```sql
CREATE TABLE de_institutional_flows (
  date          DATE NOT NULL,
  category      VARCHAR(10) NOT NULL CHECK (category IN ('FII','DII')),
  market_type   VARCHAR(20) NOT NULL CHECK (market_type IN ('equity','debt','hybrid')),
  gross_buy     NUMERIC(18,4) NOT NULL CHECK (gross_buy >= 0),
  gross_sell    NUMERIC(18,4) NOT NULL CHECK (gross_sell >= 0),
  net_flow      NUMERIC(18,4) GENERATED ALWAYS AS (gross_buy - gross_sell) STORED,
  source        VARCHAR(50) DEFAULT 'nse',
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (date, category, market_type)
);
CREATE INDEX idx_flows_date ON de_institutional_flows(date DESC);

CREATE TABLE de_mf_category_flows (
  month_date        DATE NOT NULL,
  category          VARCHAR(100) NOT NULL,
  net_flow_cr       NUMERIC(18,4),
  gross_inflow_cr   NUMERIC(18,4),
  gross_outflow_cr  NUMERIC(18,4),
  aum_cr            NUMERIC(18,4),
  sip_flow_cr       NUMERIC(18,4),
  sip_accounts      INTEGER,
  folios            INTEGER,
  created_at        TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (month_date, category)
);
CREATE INDEX idx_mf_category_flows ON de_mf_category_flows(category, month_date DESC);
```

### 3.4 Derived / Computed Tables

```sql
-- [v1.8] Pre-computed technical indicators for efficient breadth computation
-- Updated incrementally in EOD pipeline; breadth reads from here, NOT raw OHLCV.
-- Computing 200DMA for ~5000 stocks from 25M raw OHLCV rows daily would cause CPU spikes.
CREATE TABLE de_equity_technical_daily (
  date            DATE NOT NULL,
  instrument_id   UUID NOT NULL REFERENCES de_instrument(instrument_id),
  sma_50          NUMERIC(18,4),     -- 50-day simple moving average of close_adj
  sma_200         NUMERIC(18,4),     -- 200-day simple moving average of close_adj
  ema_20          NUMERIC(18,4),     -- 20-day exponential moving average
  close_adj       NUMERIC(18,4),     -- denormalised for comparison (avoids OHLCV join)
  above_50dma     BOOLEAN GENERATED ALWAYS AS (close_adj > sma_50) STORED,
  above_200dma    BOOLEAN GENERATED ALWAYS AS (close_adj > sma_200) STORED,
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (date, instrument_id)
);
CREATE INDEX idx_tech_daily_date ON de_equity_technical_daily(date DESC);
-- Incremental update: only needs yesterday's SMA + today's close_adj
-- SMA_50_today = SMA_50_yesterday + (close_adj_today - close_adj_50_days_ago) / 50
-- Breadth Step 10 then: SELECT COUNT(*) FILTER (WHERE above_200dma) FROM de_equity_technical_daily WHERE date = :today

CREATE TABLE de_rs_scores (
  date          DATE NOT NULL,
  entity_type   VARCHAR(20) NOT NULL CHECK (entity_type IN ('stock','sector','mf_category','global')),
  entity_id     VARCHAR(50) NOT NULL,
  vs_benchmark  VARCHAR(50) NOT NULL,
  rs_1w         NUMERIC(10,4),
  rs_1m         NUMERIC(10,4),
  rs_3m         NUMERIC(10,4),
  rs_6m         NUMERIC(10,4),
  rs_12m        NUMERIC(10,4),
  rs_composite  NUMERIC(10,4),
  computation_version INTEGER NOT NULL DEFAULT 1,  -- [v1.8] tracks logic version for auditability
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (date, entity_type, entity_id, vs_benchmark)
);
CREATE INDEX idx_rs_entity_date ON de_rs_scores(entity_id, date DESC);
CREATE INDEX idx_rs_date_composite ON de_rs_scores(date DESC, rs_composite DESC);

-- [v1.7] Daily RS summary table — PK changed from (date, symbol, vs_benchmark)
-- to (date, instrument_id, vs_benchmark) for stable identity after symbol changes.
-- symbol column retained for display convenience but is NOT part of PK.
CREATE TABLE de_rs_daily_summary (
  date            DATE NOT NULL,
  instrument_id   UUID NOT NULL REFERENCES de_instrument(instrument_id),
  symbol          VARCHAR(20) NOT NULL,   -- display convenience, immutable snapshot at computation date
  sector          VARCHAR(100),
  vs_benchmark    VARCHAR(50) NOT NULL,
  rs_composite    NUMERIC(10,4),
  rs_1m           NUMERIC(10,4),
  rs_3m           NUMERIC(10,4),
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (date, instrument_id, vs_benchmark)
);
CREATE INDEX idx_rs_summary_date ON de_rs_daily_summary(date DESC, rs_composite DESC);
CREATE INDEX idx_rs_summary_sector ON de_rs_daily_summary(date DESC, sector, rs_composite DESC);
-- [v1.7] Populated by EOD pipeline using ON CONFLICT DO UPDATE (not DELETE/INSERT)

CREATE TABLE de_market_regime (
  computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  date            DATE NOT NULL,
  regime          VARCHAR(20) NOT NULL CHECK (regime IN ('BULL','BEAR','SIDEWAYS','RECOVERY')),
  confidence      NUMERIC(6,2) CHECK (confidence BETWEEN 0 AND 100),
  breadth_score   NUMERIC(6,2) CHECK (breadth_score BETWEEN 0 AND 100),
  momentum_score  NUMERIC(6,2) CHECK (momentum_score BETWEEN 0 AND 100),
  volume_score    NUMERIC(6,2) CHECK (volume_score BETWEEN 0 AND 100),
  global_score    NUMERIC(6,2) CHECK (global_score BETWEEN 0 AND 100),
  fii_score       NUMERIC(6,2) CHECK (fii_score BETWEEN 0 AND 100),
  indicator_detail JSONB,
  computation_version INTEGER NOT NULL DEFAULT 1,  -- [v1.8] tracks logic version for auditability
  PRIMARY KEY (computed_at)
);
CREATE INDEX idx_regime_date ON de_market_regime(date DESC, computed_at DESC);

-- [v1.8] Breadth reads from de_equity_technical_daily, NOT raw OHLCV
CREATE TABLE de_breadth_daily (
  date                DATE PRIMARY KEY,
  advance             INTEGER CHECK (advance >= 0),
  decline             INTEGER CHECK (decline >= 0),
  unchanged           INTEGER CHECK (unchanged >= 0),
  total_stocks        INTEGER CHECK (total_stocks > 0),
  ad_ratio            NUMERIC(10,4),
  pct_above_200dma    NUMERIC(6,2) CHECK (pct_above_200dma BETWEEN 0 AND 100),
  pct_above_50dma     NUMERIC(6,2) CHECK (pct_above_50dma BETWEEN 0 AND 100),
  new_52w_highs       INTEGER CHECK (new_52w_highs >= 0),
  new_52w_lows        INTEGER CHECK (new_52w_lows >= 0),
  created_at          TIMESTAMPTZ DEFAULT NOW()
);
```

### 3.5 F&O Summary

```sql
CREATE TABLE de_fo_summary (
  date              DATE PRIMARY KEY,
  pcr_oi            NUMERIC(10,4) CHECK (pcr_oi > 0),
  pcr_volume        NUMERIC(10,4) CHECK (pcr_volume > 0),
  total_oi          BIGINT,
  oi_change         BIGINT,
  fii_index_long    NUMERIC(18,4),
  fii_index_short   NUMERIC(18,4),
  fii_net_futures   NUMERIC(18,4),
  fii_net_options   NUMERIC(18,4),
  max_pain          NUMERIC(18,4),
  created_at        TIMESTAMPTZ DEFAULT NOW()
);
```

### 3.6 Qualitative Layer

```sql
CREATE TABLE de_qual_sources (
  id              SERIAL PRIMARY KEY,
  source_name     VARCHAR(100) NOT NULL UNIQUE,
  source_type     VARCHAR(50) NOT NULL CHECK (source_type IN
    ('audio_note','research_pdf','rss_feed','regulatory','central_bank','social','manual_text')),
  contributor_id  INTEGER REFERENCES de_contributors(id),
  feed_url        TEXT,
  is_active       BOOLEAN DEFAULT TRUE,
  created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE de_qual_documents (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id         INTEGER NOT NULL REFERENCES de_qual_sources(id),
  content_hash      VARCHAR(64) NOT NULL,   -- SHA-256 of raw_text
  source_url        TEXT,
  UNIQUE (source_id, content_hash),         -- prevents duplicate ingestion
  published_at      TIMESTAMPTZ,
  ingested_at       TIMESTAMPTZ DEFAULT NOW(),
  title             TEXT,
  original_format   VARCHAR(20) CHECK (original_format IN ('pdf','audio','text','url')),
  raw_text          TEXT NOT NULL,
  audio_url         TEXT,
  audio_duration_s  INTEGER,
  summary           TEXT,
  embedding         vector(1536),
  tags              TEXT[],
  processing_status VARCHAR(20) DEFAULT 'pending' CHECK (processing_status IN
    ('pending','processing','complete','failed','quarantine')),
  processing_error  TEXT,
  created_at        TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_qual_docs_source ON de_qual_documents(source_id, ingested_at DESC);
CREATE INDEX idx_qual_docs_status ON de_qual_documents(processing_status)
  WHERE processing_status IN ('pending','processing');
-- ivfflat index deferred until 10,000+ rows — run this after initial load:
-- CREATE INDEX idx_qual_docs_embedding ON de_qual_documents
--   USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

CREATE TABLE de_qual_extracts (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id     UUID NOT NULL REFERENCES de_qual_documents(id),
  asset_class     VARCHAR(50) CHECK (asset_class IN
    ('equity','debt','macro','sector','global','commodity','currency','real_estate')),
  entity_ref      VARCHAR(100),
  direction       VARCHAR(10) CHECK (direction IN ('bullish','bearish','neutral')),
  timeframe       VARCHAR(50),
  conviction      VARCHAR(10) CHECK (conviction IN ('high','medium','low')),
  view_text       TEXT NOT NULL,
  source_quote    TEXT,
  quality_score   NUMERIC(3,2) CHECK (quality_score BETWEEN 0 AND 1),  -- [v1.9] Claude extraction confidence; downstream uses only >= 0.70
  embedding       vector(1536),
  created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_qual_extracts_doc ON de_qual_extracts(document_id);
CREATE INDEX idx_qual_extracts_entity ON de_qual_extracts(entity_ref, created_at DESC);
-- ivfflat index deferred — run after initial load

CREATE TABLE de_qual_outcomes (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  extract_id      UUID NOT NULL REFERENCES de_qual_extracts(id),
  outcome_date    DATE NOT NULL,
  was_correct     BOOLEAN,
  actual_move_pct NUMERIC(10,4),
  entity_ref      VARCHAR(100),
  notes           TEXT,
  recorded_by     INTEGER REFERENCES de_contributors(id),
  recorded_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_outcomes_extract ON de_qual_outcomes(extract_id);
```

### 3.7 Client Portfolio Tables

```sql
-- PII encrypted at application layer before insert using envelope encryption (see 3.7.1)
-- PAN, phone, email stored as AES-256-GCM ciphertext (hex string)
-- [v1.7] HMAC blind index columns added for searchable PII (exact-match queries)
-- Decryption only in admin-scoped FastAPI endpoints
-- Every read of PII columns logged in de_pii_access_log
CREATE TABLE de_clients (
  client_id       VARCHAR(50) PRIMARY KEY,
  name            VARCHAR(200),
  email_enc       TEXT,    -- AES-256-GCM encrypted, per-client DEK
  phone_enc       TEXT,    -- AES-256-GCM encrypted
  pan_enc         TEXT,    -- AES-256-GCM encrypted
  pan_hash        VARCHAR(8),     -- [v1.9.1] TRUNCATED HMAC-SHA256 blind index (first 8 hex chars)
                                  -- Truncation forces collisions: search returns small bucket (2-3 rows),
                                  -- application decrypts bucket in memory for exact match.
                                  -- Full 64-char HMAC on low-entropy PAN (10 chars, known format ABCDE1234F)
                                  -- would be reversible via offline brute-force if HMAC key is compromised.
  email_hash      VARCHAR(8),     -- [v1.9.1] truncated HMAC-SHA256, same rationale
  phone_hash      VARCHAR(8),     -- [v1.9.1] truncated HMAC-SHA256 (10-digit Indian mobile = very low entropy)
  hmac_version    INTEGER NOT NULL DEFAULT 1,  -- [v1.8] tracks HMAC key version for rotation
  is_active       BOOLEAN DEFAULT TRUE,
  created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_clients_pan_hash ON de_clients(pan_hash) WHERE pan_hash IS NOT NULL;
CREATE INDEX idx_clients_email_hash ON de_clients(email_hash) WHERE email_hash IS NOT NULL;

-- [v1.7] Per-client encrypted Data Encryption Keys (envelope encryption)
-- [v1.9] APPEND-ONLY: key rotation adds new row, never overwrites.
-- Historical keys must remain available for decrypting old backups/snapshots.
CREATE TABLE de_client_keys (
  client_id         VARCHAR(50) NOT NULL REFERENCES de_clients(client_id),
  key_version       INTEGER NOT NULL DEFAULT 1,   -- [v1.9] increments on rotation
  encrypted_dek     TEXT NOT NULL,      -- DEK encrypted by KMS CMK, base64 encoded
  kms_key_id        VARCHAR(200) NOT NULL,  -- KMS CMK ARN used to encrypt this DEK
  is_active         BOOLEAN NOT NULL DEFAULT TRUE,  -- [v1.9] only one active version per client
  created_at        TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (client_id, key_version)
);
-- Only one active key per client
CREATE UNIQUE INDEX idx_client_keys_active ON de_client_keys(client_id) WHERE is_active = TRUE;

CREATE TABLE de_pii_access_log (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  accessed_by     VARCHAR(100) NOT NULL,   -- JWT subject
  client_id       VARCHAR(50) NOT NULL,
  fields_accessed TEXT[],
  purpose         TEXT,
  source_ip       INET,
  accessed_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_pii_log_client ON de_pii_access_log(client_id, accessed_at DESC);

CREATE TABLE de_portfolios (
  portfolio_id    VARCHAR(50) PRIMARY KEY,
  client_id       VARCHAR(50) NOT NULL REFERENCES de_clients(client_id),
  portfolio_name  VARCHAR(200),
  inception_date  DATE,
  strategy        VARCHAR(100),
  is_active       BOOLEAN DEFAULT TRUE,
  created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE de_portfolio_nav (
  date            DATE NOT NULL,
  portfolio_id    VARCHAR(50) NOT NULL REFERENCES de_portfolios(portfolio_id),
  nav             NUMERIC(18,4) NOT NULL CHECK (nav > 0),
  aum_cr          NUMERIC(18,4),
  units           NUMERIC(18,4),
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (date, portfolio_id)
);
CREATE INDEX idx_portfolio_nav_pid ON de_portfolio_nav(portfolio_id, date DESC);

CREATE TABLE de_portfolio_transactions (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  portfolio_id      VARCHAR(50) NOT NULL REFERENCES de_portfolios(portfolio_id),
  trade_date        DATE NOT NULL,
  instrument_id     UUID REFERENCES de_instrument(instrument_id),
  symbol            VARCHAR(20),
  transaction_type  VARCHAR(20) CHECK (transaction_type IN
    ('buy','sell','dividend','corporate_action','cash_in','cash_out')),
  quantity          NUMERIC(18,4),
  price             NUMERIC(18,4),
  amount            NUMERIC(18,4),
  source_ref        VARCHAR(100),  -- source system reference for idempotency
  UNIQUE (portfolio_id, trade_date, instrument_id, transaction_type, source_ref),
  created_at        TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_txn_portfolio ON de_portfolio_transactions(portfolio_id, trade_date DESC);

CREATE TABLE de_portfolio_holdings (
  date            DATE NOT NULL,
  portfolio_id    VARCHAR(50) NOT NULL REFERENCES de_portfolios(portfolio_id),
  instrument_id   UUID NOT NULL REFERENCES de_instrument(instrument_id),
  symbol          VARCHAR(20) NOT NULL,
  quantity        NUMERIC(18,4),
  avg_cost        NUMERIC(18,4),
  current_value   NUMERIC(18,4),
  weight_pct      NUMERIC(6,2) CHECK (weight_pct BETWEEN 0 AND 100),
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (date, portfolio_id, instrument_id)
);

CREATE TABLE de_portfolio_risk_metrics (
  date            DATE NOT NULL,
  portfolio_id    VARCHAR(50) NOT NULL REFERENCES de_portfolios(portfolio_id),
  cagr            NUMERIC(10,4),
  volatility      NUMERIC(10,4),
  sharpe_ratio    NUMERIC(10,4),
  max_drawdown    NUMERIC(10,4),
  alpha           NUMERIC(10,4),
  beta            NUMERIC(10,4),
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (date, portfolio_id)
);
```

#### 3.7.1 Encryption Flow (Envelope Encryption) [v1.7]

```
ENCRYPT (on PII write):
  1. Lookup de_client_keys for client_id
     - If no key exists: generate 256-bit DEK via os.urandom(32)
     - Encrypt DEK with KMS CMK: boto3.client('kms').encrypt(KeyId=PII_KMS_KEY_ARN, Plaintext=dek)
     - Store encrypted DEK in de_client_keys
  2. Generate random 12-byte nonce (IV) for AES-256-GCM
  3. Encrypt field: ciphertext = AES-GCM(key=dek, nonce=nonce, plaintext=field_value)
  4. Store as: base64(nonce || ciphertext || tag)  — single TEXT column
  5. Compute blind index: HMAC-SHA256(key=PII_HMAC_KEY, message=normalised_field_value)
     - Normalise: uppercase + strip whitespace for PAN; lowercase + strip for email
     - [v1.9.1] Truncate digest to first 8 hex chars before storing
       (forces collisions, prevents offline brute-force on low-entropy PAN/phone)
  6. Store truncated HMAC in pan_hash / email_hash / phone_hash

DECRYPT (on PII read — admin endpoints only):
  1. Fetch encrypted_dek from de_client_keys
  2. Decrypt DEK: boto3.client('kms').decrypt(CiphertextBlob=encrypted_dek)
  3. Decode stored value: extract nonce (first 12 bytes), ciphertext, tag
  4. Decrypt: plaintext = AES-GCM-decrypt(key=dek, nonce=nonce, ciphertext=ciphertext)
  5. Log access in de_pii_access_log

SEARCH (by PAN/email/phone) [v1.9.1 — truncated blind index bucket search]:
  1. Compute HMAC-SHA256 of search value (normalised same way as on write)
  2. Truncate to first 8 hex characters
  3. SELECT * FROM de_clients WHERE pan_hash = :truncated_hash
     — Returns small bucket of 2-3 rows (collisions are intentional)
  4. For each row in bucket: decrypt pan_enc using client's active DEK
  5. Compare decrypted PAN to search value in memory — return exact match
  Note: bucket size is small enough that decrypting 2-3 rows is negligible overhead

KEY ROTATION [v1.9 — append-only, never overwrite]:
  1. Generate new DEK, encrypt with current KMS CMK
  2. INSERT new row into de_client_keys with key_version = current_max + 1, is_active = TRUE
  3. UPDATE previous version: SET is_active = FALSE
  4. Decrypt all PII for client using OLD DEK (from previous key_version)
  5. Re-encrypt with NEW DEK
  6. Update encrypted columns in de_clients
  7. Historical DEKs remain in de_client_keys — required for restoring old DB snapshots
     (a 30-day-old backup contains ciphertext from the old DEK; without it, data is unrecoverable)
  8. Schedule: annually or on suspected compromise
  KMS CMK rotation: enable automatic annual rotation in AWS KMS console

BLIND INDEX (HMAC) KEY ROTATION [v1.8]:
  Triggered by: compliance mandate, key compromise, or scheduled rotation
  1. Store new HMAC key in Secrets Manager as PII_HMAC_KEY_V{N+1}
  2. For each client record:
     a. Decrypt PAN/email/phone using existing DEK (standard decrypt flow)
     b. Recompute HMAC using new key: HMAC-SHA256(new_key, normalised_value)
     c. UPDATE pan_hash, email_hash, phone_hash, hmac_version = N+1
  3. Commit in batches of 100 clients (avoid long transactions)
  4. After all records updated: retire old HMAC key from Secrets Manager
  5. Update application config to use new key version
  Note: during rotation window, search queries must check both old and new hash
  until hmac_version is uniform across all rows
```

### 3.8 Champion Trader

```sql
CREATE TABLE de_champion_trades (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  trade_date      DATE NOT NULL,
  instrument_id   UUID REFERENCES de_instrument(instrument_id),
  symbol          VARCHAR(20),
  direction       VARCHAR(10) CHECK (direction IN ('long','short')),
  entry_price     NUMERIC(18,4),
  exit_price      NUMERIC(18,4),
  quantity        NUMERIC(18,4),
  pnl             NUMERIC(18,4),
  stage           VARCHAR(20) CHECK (stage IN ('1','2','3','4','1A','2A')),
  signal_type     VARCHAR(50),
  stop_loss       NUMERIC(18,4),
  target_price    NUMERIC(18,4),
  notes           TEXT,
  source_ref      VARCHAR(100) UNIQUE,  -- natural key for idempotency
  created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_champion_date ON de_champion_trades(trade_date DESC);
```

### 3.9 Pipeline State and Operational Control

```sql
-- [v1.9] Source file lineage — tracks every ingested file for forensic traceability
-- MUST be created before price/NAV tables (referenced by source_file_id FK)
CREATE TABLE de_source_files (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_name     VARCHAR(100) NOT NULL,   -- e.g. 'nse_bhav', 'amfi_nav', 'nse_corporate_actions'
  file_name       VARCHAR(500),
  file_date       DATE NOT NULL,           -- business date the file represents
  checksum        VARCHAR(64) NOT NULL,    -- SHA-256 of raw file
  file_size_bytes BIGINT,
  row_count       BIGINT,
  format_version  VARCHAR(20),             -- e.g. 'udiff', 'legacy', 'standard'
  ingested_at     TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (source_name, file_date, checksum)
);

CREATE TABLE de_pipeline_log (
  id              SERIAL PRIMARY KEY,
  pipeline_name   VARCHAR(100) NOT NULL,
  business_date   DATE NOT NULL,
  run_number      INTEGER NOT NULL DEFAULT 1,
  status          VARCHAR(20) CHECK (status IN ('running','complete','failed','partial','holiday_skip','sla_breach')),
  started_at      TIMESTAMPTZ DEFAULT NOW(),
  completed_at    TIMESTAMPTZ,
  rows_processed  BIGINT DEFAULT 0,
  rows_failed     BIGINT DEFAULT 0,
  source_date     DATE,          -- date reported by source file
  source_rowcount BIGINT,        -- rowcount in source file
  source_checksum TEXT,          -- MD5 of source file
  error_detail    TEXT,
  track_status    JSONB,         -- [v1.9] per-track status: {"equity":"complete","mf":"failed",...}
  UNIQUE (pipeline_name, business_date, run_number)
);
CREATE INDEX idx_pipeline_log_checksum ON de_pipeline_log(source_checksum)
  WHERE source_checksum IS NOT NULL;  -- [v1.9] prevents full table scan for duplicate detection

-- [v1.9] Global system flags — kill switch and operational controls
-- Checked at pipeline start and API request entry
CREATE TABLE de_system_flags (
  key             VARCHAR(50) PRIMARY KEY,
  value           BOOLEAN NOT NULL DEFAULT TRUE,
  updated_by      VARCHAR(100),
  updated_at      TIMESTAMPTZ DEFAULT NOW(),
  reason          TEXT
);
-- Seed data:
-- INSERT INTO de_system_flags (key, value) VALUES
--   ('INGESTION_ENABLED', TRUE),
--   ('API_ENABLED', TRUE),
--   ('QUALITATIVE_ENABLED', TRUE),
--   ('RECOMPUTE_ENABLED', TRUE);
-- Usage in pipeline: IF NOT get_flag('INGESTION_ENABLED'): log + exit gracefully
-- Usage in API: IF NOT get_flag('API_ENABLED'): return 503 Service Unavailable

CREATE TABLE de_migration_log (
  id              SERIAL PRIMARY KEY,
  source_db       VARCHAR(100),
  source_table    VARCHAR(100),
  target_table    VARCHAR(100),
  rows_read       BIGINT,
  rows_written    BIGINT,
  rows_errored    BIGINT DEFAULT 0,
  status          VARCHAR(20),
  started_at      TIMESTAMPTZ DEFAULT NOW(),
  completed_at    TIMESTAMPTZ,
  checksum_source BIGINT,
  checksum_dest   BIGINT,
  notes           TEXT
);

CREATE TABLE de_migration_errors (
  id              SERIAL PRIMARY KEY,
  migration_id    INTEGER REFERENCES de_migration_log(id),
  source_row      JSONB,
  error_reason    TEXT,
  created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE de_request_log (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id      VARCHAR(50) NOT NULL,
  actor           VARCHAR(100),   -- JWT subject
  source_ip       INET,
  method          VARCHAR(10),
  endpoint        TEXT,
  status_code     INTEGER,
  duration_ms     INTEGER,
  requested_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_request_log_actor ON de_request_log(actor, requested_at DESC);
CREATE INDEX idx_request_log_time ON de_request_log(requested_at DESC);
```

---

## 4. Data Sources
*(unchanged from v1.5 — see original document)*

### 4.1 Indian Equity
- NSE BHAV copy: **three** format variants by era:
  - Pre-2010: `eq_DDMMYYYY_csv.zip` (legacy format)
  - 2010–June 2024: `sec_bhavdata_full_DDMMYYYY.csv` (standard format)
  - **[v1.8] July 2024 onwards: UDiFF (Unified Distilled File Format)** — different column layout, field names, and delimiter. Pipeline must detect format by header row and route to correct parser.
- NSE delivery data T+1 pipeline
- NSE corporate actions API
- NSE index historical: all 60+ indices
- India VIX historical
- NSE F&O summary
- **[v1.7] NSE master data (symbol changes, new listings, suspensions, delistings): fetched daily before EOD**

### 4.2 Mutual Funds
- AMFI daily NAV: https://www.amfiindia.com/spages/NAVAll.txt
- AMFI monthly AUM data
- Migration source: pg_dump of fie2-db-1 Docker nav_daily table (25.8M rows)
- **[v1.7] AMFI market cap classification list (bi-annual, fetched on SEBI reclassification dates)**

### 4.3 Global
- yfinance: ^GSPC, ^IXIC, ^DJI, ^FTSE, ^GDAXI, ^FCHI, ^N225, ^HSI, 000001.SS, ^AXJO, EEM, URTH
- yfinance ETFs: top 200 by AUM
- yfinance macro: DX-Y.NYB, CL=F, BZ=F, GC=F, SI=F, USDINR=X, USDJPY=X, EURUSD=X, USDCNH=X
- FRED: DGS10, DGS2, FEDFUNDS, T10Y2Y, CPIAUCSL, UNRATE
- All global dates normalised to calendar date (not IST) before storage

### 4.4 FII/DII
- Primary: NSE fiidiiTradeReact API — Fallback: SEBI CSV download on 403

### 4.5 Qualitative (automated)
- RBI, SEBI, ET Markets, Business Standard, Fed press releases, Mint Markets — RSS/scrape

### 4.6 Manual Upload (admin only)
- Endpoint: POST /api/v1/qualitative/upload
- Security: magic-byte verification → ClamAV scan → quarantine folder → only then process
- Audio: Whisper API transcription
- PDF: PyMuPDF text extraction → Claude API structured extraction
- Plain text: direct to raw_text
- Estimated cost per 35-min audio note: ~$0.35

---

## 5. Pipeline Architecture

### 5.0 Universal Pipeline Guard [v1.7 — rewritten]
```python
import hashlib

# Every pipeline MUST run this at start
def acquire_pipeline_lock(pipeline_name: str, business_date: date, conn):
    """
    [v1.7] Session-level advisory lock with deterministic hash.
    - Uses hashtext() in Postgres for deterministic lock ID (NOT Python hash()).
    - Uses session-level pg_advisory_lock (NOT pg_advisory_xact_lock) so lock
      persists across multiple transactions within the pipeline.
    - Caller MUST call release_pipeline_lock() in a finally block.
    """
    # Deterministic lock ID via Postgres hashtext — stable across all processes
    lock_id = conn.execute(
        "SELECT hashtext(:key) & 2147483647",
        {"key": f"{pipeline_name}:{business_date.isoformat()}"}
    ).scalar()

    # Try non-blocking first to detect stale locks
    acquired = conn.execute(
        "SELECT pg_try_advisory_lock(:id)", {"id": lock_id}
    ).scalar()

    if not acquired:
        # Check if holder is still alive
        stale = conn.execute("""
            SELECT pid FROM pg_locks
            WHERE locktype = 'advisory' AND objid = :id
            AND pid NOT IN (SELECT pid FROM pg_stat_activity WHERE state != 'idle')
        """, {"id": lock_id}).fetchone()
        if stale:
            # Force-release stale lock from dead session
            conn.execute("SELECT pg_advisory_unlock(:id)", {"id": lock_id})
            conn.execute("SELECT pg_advisory_lock(:id)", {"id": lock_id})
        else:
            raise PipelineLockError(f"Pipeline {pipeline_name} already running for {business_date}")

    # Check trading calendar
    is_trading = conn.execute(
        "SELECT is_trading FROM de_trading_calendar WHERE date = :d",
        {"d": business_date}
    ).scalar()
    if not is_trading:
        release_pipeline_lock(lock_id, conn)
        log_pipeline(pipeline_name, business_date, 'holiday_skip')
        return False, lock_id
    return True, lock_id

def release_pipeline_lock(lock_id: int, conn):
    """Always call in finally block."""
    conn.execute("SELECT pg_advisory_unlock(:id)", {"id": lock_id})
```

### 5.1 Idempotency Rule
Every INSERT uses ON CONFLICT on the NATURAL KEY — never on surrogate UUID/SERIAL:
```sql
-- Good: natural key
INSERT INTO de_equity_ohlcv (date, instrument_id, ...)
ON CONFLICT (date, instrument_id) DO UPDATE SET close = EXCLUDED.close, ...

-- Good: content hash for qualitative documents
INSERT INTO de_qual_documents (source_id, content_hash, ...)
ON CONFLICT (source_id, content_hash) DO NOTHING
```

### 5.2 Timezone Handling Rule
- All DATE values stored as calendar dates (not IST-converted)
- US data stored as US calendar date, Indian data as Indian calendar date
- All TIMESTAMPTZ stored as UTC

### 5.3 Freshness Validation Rule (every pipeline)
```python
# Before processing any source file:
# 1. Extract source report date from file header/metadata
# 2. Verify source_date == expected business_date (allow T+1 for some sources)
# 3. Verify rowcount >= minimum expected (NSE BHAV: > 500, AMFI: > 1000)
# 4. Compute SHA-256 checksum of file
# 5. Check if checksum already in de_source_files — if yes, skip (duplicate)
# [v1.9] 6. Register source file:
#    INSERT INTO de_source_files (source_name, file_name, file_date, checksum, file_size_bytes, row_count, format_version)
#    ON CONFLICT (source_name, file_date, checksum) DO NOTHING
#    Capture returned source_file_id for use in ingestion rows
# 7. Store source_date, source_rowcount, source_checksum in pipeline_log
# [v1.9] 8. Check de_system_flags: IF NOT get_flag('INGESTION_ENABLED'): log + exit gracefully
```

### 5.4 Daily EOD Pipeline (trigger: 18:30 IST)
```
Acquire session-level pg_advisory_lock for ('eod', today)  — see 5.0

[v1.9] Check de_system_flags: IF NOT get_flag('INGESTION_ENABLED'): log 'kill_switch' + exit
Check de_trading_calendar — skip if holiday

[v1.7] Step 0:   MASTER REFRESH — Fetch NSE master data (equity listing file)
                  - INSERT new instruments into de_instrument ON CONFLICT DO NOTHING
                  - For symbol changes: UPDATE de_instrument.current_symbol,
                    INSERT into de_symbol_history
                  - For suspensions/delistings: UPDATE is_suspended, is_active, delisted_on
                  - This MUST complete before any price ingestion

[v1.7] Step 0.5: CORPORATE ACTIONS — Fetch NSE corporateActions API
                  - INSERT into de_corporate_actions ON CONFLICT DO UPDATE
                  - [v1.8] For each new/modified action:
                    Enqueue into de_recompute_queue (priority=1 for today's actions)
                    For SAME-DAY actions only: run inline recomputation (cannot defer)
                    For HISTORICAL corrections: enqueue for background worker (see 5.9.1)
                  - [v1.8] VALIDATE: split ratios must be in [1:2, 1:10] range
                    flag suspicious ratios (e.g. 1:1000) in de_data_anomalies

=== [v1.8] INDEPENDENT TRACKS — run in parallel, failures isolated ===
Tracks are independent. If one fails, others continue.
API marks stale data with X-Data-Freshness header.

TRACK A: EQUITY (Steps 1-2)
  Step 1:  Download + validate NSE BHAV copy (freshness check)
           [v1.8] Detect format (pre-2010 / standard / UDiFF) by header row
  Step 2:  INSERT into de_equity_ohlcv ON CONFLICT DO UPDATE
           [v1.7] Pipeline enforcement: symbol = de_instrument.current_symbol for today
           [v1.8] POST-INSERT VALIDATION:
             For each inserted row:
               IF abs(close - prev_close) / prev_close > 0.20 AND no corporate action today:
                 INSERT de_data_anomalies (anomaly_type='price_spike', severity='warning')
               IF volume > 10 × avg_volume_20d:
                 INSERT de_data_anomalies (anomaly_type='volume_spike', severity='info')

TRACK B: MUTUAL FUNDS (Steps 5-6)
  Step 5:  Download AMFI NAV file — pre-insert scheme lifecycle check
           (join on mstar_id+isin, update de_mf_master.is_active for closures)
           [v1.7] On merge detection:
             - Stop NAV ingestion for old scheme (set is_active=FALSE)
             - Set merged_into_mstar_id on old scheme
             - INSERT de_mf_lifecycle event_type='merge_from' and 'merge_into'
             - Optionally create synthetic continuous NAV series (flagged is_synthetic=TRUE)
  Step 6:  INSERT into de_mf_nav_daily ON CONFLICT DO UPDATE
           [v1.8] POST-INSERT VALIDATION:
             IF abs(nav_change_pct) > 15: INSERT de_data_anomalies (anomaly_type='nav_spike')
             IF nav <= 0: INSERT de_data_anomalies (anomaly_type='zero_nav', severity='critical')
           [v1.8] For IDCW schemes: detect dividend events, INSERT de_mf_dividends,
                  recompute nav_adj from affected record_date forward

TRACK C: INDICES + VIX (Steps 3-4, 9)
  Step 3:  Download NSE index prices (all 60+ indices)
  Step 4:  INSERT into de_index_prices ON CONFLICT DO UPDATE
  Step 9:  Fetch India VIX → de_macro_values

TRACK D: FLOWS (Steps 7-8)
  Step 7:  Fetch FII/DII flows (primary NSE, fallback SEBI on 403)
  Step 8:  INSERT into de_institutional_flows ON CONFLICT DO UPDATE

TRACK E: F&O (Step 9.5)
  [v1.7] Step 9.5: Fetch NSE option-chain-indices for NIFTY/BANKNIFTY
                    Parse PCR, OI, max pain
                    INSERT into de_fo_summary ON CONFLICT DO UPDATE

=== END INDEPENDENT TRACKS ===

[v1.9] Step 9.7: DATA STATUS GATING (per Section 5.12)
                  For each track that completed:
                    Promote validated rows: data_status = 'raw' → 'validated'
                    Quarantine anomalous rows: data_status = 'raw' → 'quarantined'
                  Log quarantine count in de_pipeline_log.track_status

[v1.8] Step 9.8: UPDATE de_equity_technical_daily for today
                  Incremental SMA/EMA update using yesterday's values + today's close_adj
                  INSERT ON CONFLICT DO UPDATE

[v1.9.1] Step 9.9: QUARANTINE THRESHOLD GUARDRAIL
                  quarantined = SELECT COUNT(*) FROM de_equity_ohlcv
                    WHERE date = :today AND data_status = 'quarantined'
                  expected = SELECT COUNT(*) FROM de_instrument
                    WHERE is_active = TRUE AND is_tradeable = TRUE
                  IF quarantined / expected > 0.05:  -- more than 5% quarantined
                    HALT Steps 10-14 (breadth, RS, regime)
                    Log: "Aggregate computation halted: {quarantined}/{expected} rows quarantined"
                    Alert admin via Slack
                    Set track_status.aggregates = 'halted_quarantine_threshold'
                    API returns X-Data-Freshness: partial
                  -- Rationale: anomalies cluster (e.g. bad UDiFF parser truncating small caps).
                  -- Computing breadth/RS on a 60% universe would permanently record poisoned metrics.

Step 10: Compute breadth indicators
         [v1.8] Read from de_equity_technical_daily (NOT raw OHLCV):
           SELECT COUNT(*) FILTER (WHERE above_200dma) as pct_above_200dma,
                  COUNT(*) FILTER (WHERE above_50dma) as pct_above_50dma
           FROM de_equity_technical_daily t
           JOIN de_instrument i ON t.instrument_id = i.instrument_id
           JOIN de_equity_ohlcv o ON o.instrument_id = t.instrument_id AND o.date = t.date
           WHERE t.date = :today AND i.is_active = TRUE AND i.is_tradeable = TRUE
             AND o.data_status = 'validated'  -- [v1.9] only validated data
Step 11: INSERT into de_breadth_daily ON CONFLICT DO UPDATE
Step 12: Incremental RS computation (today only)
         [v1.8] computation_version = current version constant from config
Step 13: [v1.7] UPDATE de_rs_daily_summary using ON CONFLICT DO UPDATE:
         INSERT INTO de_rs_daily_summary (date, instrument_id, symbol, sector, vs_benchmark, rs_composite, rs_1m, rs_3m)
           SELECT :today, s.entity_id::uuid, i.current_symbol, i.sector, s.vs_benchmark,
                  s.rs_composite, s.rs_1m, s.rs_3m
           FROM de_rs_scores s
           JOIN de_instrument i ON i.instrument_id = s.entity_id::uuid
           WHERE s.date = :today AND s.entity_type = 'stock'
         ON CONFLICT (date, instrument_id, vs_benchmark) DO UPDATE SET
           symbol = EXCLUDED.symbol,
           sector = EXCLUDED.sector,
           rs_composite = EXCLUDED.rs_composite,
           rs_1m = EXCLUDED.rs_1m,
           rs_3m = EXCLUDED.rs_3m;
Step 14: UPDATE de_market_regime for today
         [v1.8] If Track A failed: regime computation runs with stale equity data,
                set confidence *= 0.5, add indicator_detail.data_quality = 'equity_stale'
Step 15: Call create_equity_partition(YEAR(today)+1) AND create_mf_nav_partition(YEAR(today)+1)
Step 16: Invalidate Redis cache keys: regime:current, rs:sectors:*, breadth:latest
Step 17: Write de_pipeline_log entry
         [v1.8] Include track-level status: which tracks succeeded/failed

Release session-level pg_advisory_lock in finally block  — see 5.0

Hard prerequisites (relaxed in v1.8):
  Steps 10-14 run if ANY of Tracks A-E complete (not all required)
  Steps 12-14 require Track A specifically (RS needs equity data)
  If Track A fails: Steps 10-11 still run (breadth from yesterday's technical daily)
  API returns X-Data-Freshness: partial when any track failed
```

### 5.5 Pre-Market Pipeline (trigger: 07:30 IST)
```
Acquire session-level pg_advisory_lock for ('premarket', today)
Step 1: Fetch global index closes (prior session)
Step 2: Fetch macro values via yfinance and FRED
Step 3: INSERT all ON CONFLICT DO UPDATE
Step 4: Update global_score in today's de_market_regime
Step 5: Invalidate Redis: global:indices, global:macro
Step 6: Write pipeline_log
Release lock in finally block
```

### 5.6 T+1 Delivery Pipeline (trigger: 09:00 IST)
```
Step 1: Get last trading day = MAX(date) WHERE is_trading=TRUE AND date < TODAY
        from de_trading_calendar
Step 2: Download NSE delivery data for last trading day
Step 3: UPDATE de_equity_ohlcv SET delivery_vol, delivery_pct
        WHERE date = last_trading_day
```

### 5.7 Qualitative Pipeline (trigger: every 30 minutes)
```
[v1.7] Lock granularity: per-document, NOT per-day
        — allows parallel ingestion of independent documents

Step 1: Poll RSS feeds for new items
Step 2: Check upload queue for new files in quarantine folder

For each new document:
  Acquire pg_advisory_lock for ('qual', hashtext(document_content_hash))

  a. Compute SHA-256 content_hash
  b. Check de_qual_documents for (source_id, content_hash) — skip if exists

  [v1.8] b2. SEMANTIC DEDUPLICATION:
     Compute embedding of title + first 500 chars
     Query existing documents from past 48 hours:
       SELECT id FROM de_qual_documents
       WHERE ingested_at > NOW() - INTERVAL '48 hours'
       AND 1 - (embedding <=> :new_embedding) > 0.92  -- cosine similarity threshold
     If match found: skip ingestion, log as 'semantic_duplicate' in de_pipeline_log

  [v1.8] b3. COST GUARDRAILS:
     Check daily spend: SELECT COUNT(*) FROM de_qual_documents
       WHERE DATE(ingested_at) = :today AND processing_status = 'complete'
     If count > 200: pause ingestion, alert admin (prevents runaway costs)
     Per-source rate limit: max 50 documents/day per source_id

  c. Insert document record (status: pending)

  For upload files (security gate — ALL must pass before processing):
  d. Verify magic bytes match declared MIME type (python-magic)
  e. Run ClamAV scan: subprocess.run(['clamdscan', file_path])
  f. If scan fails: update status='quarantine', alert admin, STOP
  g. Move file from quarantine to processing folder

  Content extraction by format:
  h. audio: Whisper API → raw_text
  i. pdf: PyMuPDF doc.get_text() → raw_text, fallback: Claude vision API
  j. text: file content → raw_text directly
  k. url: requests.get → BeautifulSoup text extraction → raw_text

  l. Update document: raw_text, status=processing
  m. Call Claude API for structured extract generation
     Model: claude-sonnet-4-20250514, extract all market views
  n. Insert extracts into de_qual_extracts
  o. Compute embeddings: openai.embeddings.create, text-embedding-3-small
  p. Store embeddings in document and extract records
  q. Update document: status=complete

  [v1.7] r. ARCHIVAL: Move original file to S3 cold-storage bucket
            (s3://jsl-data-engine-archive/qualitative/YYYY/MM/DD/)
            Delete from local EC2 processing folder after S3 upload confirmed
            Log archival in de_pipeline_log

  Release per-document lock in finally block

  On any failure: retry 3 times, exponential backoff 1m/5m/15m
  After 3 failures: status=failed, write processing_error, alert admin
```

### 5.8 RS Computation
- Daily: incremental, today only (~9,000 calculations, <30 seconds)
- Sunday 02:00 IST: full rebuild from 2010-01-01, only dates where close_adj changed
- RS formula: rs_Nt = (entity_cumreturn_N - benchmark_cumreturn_N) / benchmark_rolling_std_N
  - Lookback periods N: 1w=5, 1m=21, 3m=63, 6m=126, 12m=252 trading days
  - benchmark_rolling_std = std of benchmark daily returns over same N-day window
  - cumreturn = (close_adj_today / close_adj_N_days_ago) - 1
- Composite: rs_1w×0.10 + rs_1m×0.20 + rs_3m×0.30 + rs_6m×0.25 + rs_12m×0.15

### 5.9 BHAV Copy Backfill (background, post-sprint)
- Target: all NSE stocks 2000-01-03 to present
- After each corporate action insert: recompute via Section 5.9.1
- Rate limit: 2 req/sec to NSE

#### 5.9.1 Corporate Action Adjustment Recomputation [v1.7, updated v1.8]
```
[v1.8] Two modes:
  INLINE (same-day actions, priority=1): runs immediately in Step 0.5
  QUEUED (historical corrections, priority=5): background worker processes off-peak

ENQUEUE (Step 0.5):
  INSERT INTO de_recompute_queue (instrument_id, from_date, trigger_action_id, priority, status)
  VALUES (:iid, :earliest_affected_date, :action_id,
          CASE WHEN :ex_date = :today THEN 1 ELSE 5 END,
          'pending')

PROCESS (inline or background worker):
Input: instrument_id, earliest_affected_date

1. Fetch all corporate actions for instrument_id ordered by ex_date ASC:
   SELECT ex_date, adj_factor FROM de_corporate_actions
   WHERE instrument_id = :iid ORDER BY ex_date ASC

2. Compute cumulative factor chain:
   cumulative = 1.0
   For each action in chronological order:
     cumulative = cumulative × adj_factor

   For each trading date from listing_date to today:
     factor_at_date = product of all adj_factors where ex_date <= date

3. Upsert into de_adjustment_factors_daily:
   INSERT INTO de_adjustment_factors_daily (instrument_id, date, cumulative_factor)
   VALUES (:iid, :date, :factor)
   ON CONFLICT (instrument_id, date) DO UPDATE SET
     cumulative_factor = EXCLUDED.cumulative_factor

4. Apply to OHLCV:
   UPDATE de_equity_ohlcv o SET
     close_adj  = o.close  * f.cumulative_factor,
     open_adj   = o.open   * f.cumulative_factor,
     high_adj   = o.high   * f.cumulative_factor,
     low_adj    = o.low    * f.cumulative_factor,
     volume_adj = CASE WHEN f.cumulative_factor != 0
                  THEN (o.volume / f.cumulative_factor)::BIGINT
                  ELSE o.volume END
   FROM de_adjustment_factors_daily f
   WHERE f.instrument_id = o.instrument_id
     AND f.date = o.date
     AND o.instrument_id = :iid
     AND o.date >= :earliest_affected_date

5. Mark downstream recomputation needed:
   - RS scores for this instrument from earliest_affected_date
   - Any backtest results touching this instrument

6. [v1.8] Update queue status:
   UPDATE de_recompute_queue SET status='complete', completed_at=NOW()
   WHERE instrument_id = :iid AND status = 'processing'
   On failure: SET status='failed', error_detail = :error_message
```

### 5.10 Cache Cleanup (hourly cron)
```python
redis_client.execute_command('SCAN', 0, 'MATCH', 'de:*', 'COUNT', 1000)
# Redis TTLs handle expiry automatically — no manual cleanup needed
# de_api_cache table removed — Redis only
```

### 5.11 Post-Ingestion Validation Rules [v1.8]
```
Every ingestion step runs these checks AFTER successful insert.
Anomalies are written to de_data_anomalies — they do NOT block the pipeline
unless severity='critical'.

EQUITY (after Step 2):
  - Price continuity: abs(close - prev_close) / prev_close > 0.20
    AND no corporate action on this instrument for today
    → anomaly_type='price_spike', severity='warning'
  - Volume spike: volume > 10 × rolling_avg_volume_20d
    → anomaly_type='volume_spike', severity='info'
  - Negative values: any of open/high/low/close < 0
    → anomaly_type='negative_value', severity='critical' (blocks downstream)
  - Price range: high < low
    → anomaly_type='negative_value', severity='critical'

MUTUAL FUNDS (after Step 6):
  - NAV continuity: abs(nav_change_pct) > 15
    → anomaly_type='nav_spike', severity='warning'
  - Zero NAV: nav <= 0
    → anomaly_type='zero_nav', severity='critical'

CORPORATE ACTIONS (after Step 0.5):
  - Split ratio sanity: ratio_to / ratio_from > 100
    → anomaly_type='split_ratio_suspicious', severity='warning'
  - Dividend sanity: cash_value > close * 0.5
    → anomaly_type='price_spike', severity='warning' (dividend > 50% of price)

GENERAL (all pipelines):
  - Future dates: any date > today + 1
    → anomaly_type='future_date', severity='critical'
  - Stale data: source_date < business_date - 2
    → anomaly_type='stale_data', severity='warning'

Critical anomalies: pipeline continues but marks de_pipeline_log.status='partial'
Warning anomalies: pipeline continues normally, reviewed in admin dashboard
Info anomalies: logged only, no action required
```

### 5.12 Data Status Gating [v1.9]
```
All ingestion tables have data_status: raw → validated → quarantined.

FLOW:
  1. INSERT with data_status = 'raw'
  2. Run validation rules (Section 5.11)
  3. If no critical anomalies: UPDATE data_status = 'validated'
  4. If critical anomaly detected: UPDATE data_status = 'quarantined'

ENFORCEMENT:
  API layer MUST filter: WHERE data_status = 'validated'
  RS computation MUST filter: WHERE data_status = 'validated'
  Breadth computation MUST filter: WHERE data_status = 'validated'

  Quarantined rows are:
  - Visible only in admin dashboard
  - Reviewed manually
  - Either promoted to 'validated' (admin override) or deleted

  Raw rows that are not yet validated:
  - Treated as invisible to all consumers
  - Validation runs inline at end of each pipeline track
  - If validation itself fails: rows remain 'raw', logged in de_pipeline_log

BATCH VALIDATION SQL (end of Track A):
  UPDATE de_equity_ohlcv SET data_status = 'validated'
  WHERE date = :today AND data_status = 'raw'
  AND instrument_id NOT IN (
    SELECT instrument_id FROM de_data_anomalies
    WHERE business_date = :today AND severity = 'critical' AND is_resolved = FALSE
  );

  UPDATE de_equity_ohlcv SET data_status = 'quarantined'
  WHERE date = :today AND data_status = 'raw'
  AND instrument_id IN (
    SELECT instrument_id FROM de_data_anomalies
    WHERE business_date = :today AND severity = 'critical' AND is_resolved = FALSE
  );
```

### 5.13 SLA Enforcement [v1.9]
```
Pipeline SLA deadlines (all times IST):

  Pipeline          | Must complete by | Alert channel
  ------------------|------------------|---------------
  Pre-Market        | 08:00            | Slack + email
  Equity EOD        | 19:30            | Slack + email
  MF NAV            | 22:30            | Slack
  FII/DII flows     | 20:00            | Slack
  RS computation    | 23:00            | Slack
  Regime update     | 23:30            | Slack

ENFORCEMENT (runs every 15 minutes via cron):
  For each pipeline with SLA:
    SELECT status, completed_at FROM de_pipeline_log
    WHERE pipeline_name = :name AND business_date = :today

    IF now() > sla_deadline AND status NOT IN ('complete', 'holiday_skip'):
      INSERT alert into de_pipeline_log (status='sla_breach')
      Send Slack webhook: "SLA BREACH: {pipeline} not complete by {deadline}"
      Send email to admin

  SLA breaches are also visible in:
  - GET /admin/pipeline/status (existing endpoint)
  - de_pipeline_log.status = 'sla_breach' (new status value)
```

### 5.14 Reconciliation Pipeline [v1.9] (trigger: 23:00 IST daily)
```
Cross-source validation to catch silent data corruption.
Runs AFTER all EOD tracks complete.

CHECK 1: NSE close vs yfinance close (for NIFTY 50 constituents)
  For each instrument in NIFTY 50:
    nse_close = de_equity_ohlcv.close WHERE date = today
    yf_close  = fetch yfinance close for same symbol + date
    IF abs(nse_close - yf_close) / nse_close > 0.02:  -- 2% tolerance
      INSERT de_data_anomalies (anomaly_type='cross_source_mismatch', severity='warning')

CHECK 2: AMFI NAV vs backup source (for top 50 funds by AUM)
  For each top fund:
    amfi_nav = de_mf_nav_daily.nav WHERE nav_date = today
    backup_nav = fetch from Morningstar or BSE Star MF
    IF abs(amfi_nav - backup_nav) / amfi_nav > 0.001:  -- 0.1% tolerance
      INSERT de_data_anomalies (anomaly_type='cross_source_mismatch', severity='warning')

CHECK 3: Row count sanity
  equity_count = SELECT COUNT(*) FROM de_equity_ohlcv WHERE date = today AND data_status = 'validated'
  IF equity_count < 1000:  -- typically ~2000+ active stocks
    INSERT de_data_anomalies (anomaly_type='missing_data', severity='critical')

  mf_count = SELECT COUNT(*) FROM de_mf_nav_daily WHERE nav_date = today AND data_status = 'validated'
  IF mf_count < 5000:  -- typically ~13000 active schemes
    INSERT de_data_anomalies (anomaly_type='missing_data', severity='warning')

Log results in de_pipeline_log (pipeline_name='reconciliation').
```

---

## 6. Migration Plan

### Pre-migration step 0: Backups
```bash
# Champion SQLite
docker exec champion cp /app/db_data/champion_trader.db /app/db_data/champion_trader_backup.db
docker cp champion:/app/db_data/champion_trader_backup.db /home/ubuntu/data-engine-build/

# Nav daily Docker DB — read from dump, not live container
docker exec fie2-db-1 pg_dump -U fie mf_pulse -t nav_daily \
  --no-owner --no-privileges -f /tmp/nav_daily_backup.sql
docker cp fie2-db-1:/tmp/nav_daily_backup.sql /home/ubuntu/data-engine-build/
```

### Migration tasks

| Source | Target | Rows | Action |
|--------|--------|------|--------|
| fie2-db-1 nav_daily | de_mf_nav_daily | 25.8M | Validate nav>0, map mstar_id |
| RDS fie_v3 compass_stock_prices | de_equity_ohlcv | 1.4M | Fix VARCHAR→DATE, double→NUMERIC, map to instrument_id |
| RDS mf_engine fund_master | de_mf_master | 535 | Map 47 cols |
| RDS client_portal cpp_* | de_client_* | 366K | Encrypt PAN/phone/email before insert, compute HMAC blind indexes |
| RDS fie_v3 index_constituents | de_index_constituents | 4,638 | Map to instrument_id |
| RDS fie_v3 index_prices (bond codes) | DISCARD | 3.1M | Log in migration_log |
| Champion SQLite | de_champion_trades | ~0 | Schema only |

### Validation gates
1. Row count: destination >= source × 0.999
2. Date range: MIN and MAX within 1 day of source
3. Spot check: 500 random rows match exactly
4. No NAV <= 0 in de_mf_nav_daily
5. No future dates
6. All instrument_id FKs resolve (no orphan price rows)
7. All mstar_id FKs resolve (no orphan NAV rows)
8. **[v1.7] All pan_hash values resolve to exactly one client (no hash collisions in dataset)**
9. **[v1.7] All return_* columns accept values > 100.00 without truncation**

---

## 7. Orchestration
*(unchanged from v1.5 except distributed lock now formally specified in pipeline steps)*

### Master Orchestrator
- Python: /home/ubuntu/data-engine-build/orchestrator.py
- Task registry: task_registry.json
- Dashboard: 127.0.0.1:8099 (SSH tunnel only)
- Crash recovery: `python3 orchestrator.py --resume`
- **[v1.7] Startup pre-flight checks:**
  ```python
  # ClamAV daemon check — required before qualitative pipeline can run
  import subprocess
  result = subprocess.run(['clamdscan', '--version'], capture_output=True, timeout=5)
  if result.returncode != 0:
      subprocess.run(['sudo', 'systemctl', 'restart', 'clamav-daemon'], timeout=30)
      time.sleep(5)  # wait for daemon socket
      result = subprocess.run(['clamdscan', '--version'], capture_output=True, timeout=5)
      if result.returncode != 0:
          raise RuntimeError("ClamAV daemon failed to start — qualitative pipeline disabled")

  # Redis connectivity check
  redis_client.ping()

  # RDS connectivity check
  conn.execute("SELECT 1")
  ```

### Task dependency map
```
schema_creation (hour 0-2)
├── nav_migration          [parallel, hours 2-8]
├── stock_migration        [parallel, hours 2-6]
├── nse_index_ingestion    [parallel, hours 2-5]
├── global_macro_ingestion [parallel, hours 2-5]
└── qual_schema_setup      [parallel, hours 2-3]

stock_migration + nse_index_ingestion →
├── fii_dii_ingestion      [hours 6-8]
├── rs_computation         [hours 8-14]
└── breadth_computation    [hours 8-10]

ALL migrations →
├── api_layer_build        [hours 14-18]
└── upload_interface_build [hours 14-17]

ALL above →
└── qa_validation          [hours 20-24]
```

---

## 8. API Layer

```
BASE URL: http://localhost:8010/api/v1  (internal only)
SUBDOMAIN: core.jslwealth.in (HTTPS only, internal network)
AUTH: Bearer JWT, 24-hour expiry, refresh token rotation
      JWT_SECRET: AWS Secrets Manager ARN — loaded at FastAPI startup via boto3
      Refresh tokens: stored as bcrypt hash in Redis, rotated on every use
      Token revocation: DELETE from Redis on logout or password change
RATE LIMIT: 1000 req/minute per platform token
CACHE: Redis (TTL per endpoint)
       [v1.7] ALL endpoints MUST work without Redis (DB fallback path).
       Redis is a performance layer, NOT a correctness layer.
CORS: Disabled — internal API only, no browser access
REQUEST LOGGING: Every request logged to de_request_log (actor, IP, endpoint, status, duration)
SCHEMA MIGRATIONS: Alembic — all changes via migration files, never manual SQL
DB DRIVER: asyncpg (async endpoints), psycopg2 (Alembic migrations)

[v1.9] DATA GATING: All data-serving endpoints MUST include:
       WHERE data_status = 'validated'
       Quarantined/raw rows are INVISIBLE to consumers.

[v1.9] RESPONSE HEADERS (on all data endpoints):
       X-Data-Freshness: fresh | stale | partial
         fresh   = all pipeline tracks completed for today
         stale   = data is from yesterday or older
         partial = some tracks failed (see track_status in pipeline_log)
       X-Computation-Version: 1
         Tracks RS/regime algorithm version for client-side cache invalidation
       X-System-Status: normal | degraded
         degraded = any system flag is FALSE

## 8.1 API Versioning and Platform Migration

- Current version: v1 only. No v2 until explicitly planned.
- Breaking changes require new version prefix (/api/v2/...) — v1 maintained for 6 months minimum.
- Platform migration sequence (each platform — one weekend):
  1. Deploy Data Engine, verify all endpoints return correct data
  2. Run platform in shadow mode — compare old DB response vs Data Engine response for 1 week
  3. Once responses match, update platform .env: DATA_ENGINE_URL=http://localhost:8010
  4. Deploy updated platform — old DB becomes read-only backup
  5. After 30 days verified clean: decommission old DB connection

## 8.2 Symbol Resolution Rule [v1.7]

All API endpoints that accept a symbol parameter MUST:
1. Resolve symbol → instrument_id via:
   SELECT instrument_id FROM de_instrument WHERE current_symbol = :symbol
2. Query OHLCV/RS tables using instrument_id (NOT symbol) to ensure partition pruning
3. Return current_symbol in response for display

JWT flow for platforms:
  POST /api/v1/auth/token  {"client_id": "marketpulse", "secret": "<platform-secret>"}
  Response: {"access_token": "...", "refresh_token": "...", "expires_in": 86400}
  POST /api/v1/auth/refresh  {"refresh_token": "..."}

POST /auth/token                          — issue JWT
POST /auth/refresh                        — refresh JWT
GET  /health                              — no auth
GET  /regime/current                      — Redis TTL 1h
GET  /regime/history?from=&to=            — Redis TTL 24h
GET  /rs/sectors                          — de_rs_daily_summary, Redis TTL 1h
GET  /rs/stocks?sector=&min_rs=&limit=    — de_rs_daily_summary, Redis TTL 1h
GET  /rs/stock/{symbol}?from=&to=         — Redis TTL 1h
GET  /equity/ohlcv/{symbol}?from=&to=     — Redis TTL 24h
GET  /equity/universe?active=true         — Redis TTL 24h
GET  /indices/list                        — Redis TTL 24h
GET  /indices/{code}/history?from=&to=    — Redis TTL 24h
GET  /mf/nav/{mstar_id}?from=&to=        — Redis TTL 24h
GET  /mf/universe?category=              — Redis TTL 24h
GET  /mf/category-flows?from=&to=        — Redis TTL 24h
GET  /global/indices                      — Redis TTL 1h
GET  /global/macro                        — Redis TTL 1h
GET  /flows/fii-dii?from=&to=            — Redis TTL 24h
GET  /breadth/latest                      — Redis TTL 1h
GET  /breadth/history?from=&to=          — Redis TTL 24h
POST /qualitative/upload                  — admin JWT only, rate limited 10/hr
GET  /qualitative/search?q=&limit=       — semantic search, Redis TTL 24h per query
GET  /qualitative/recent?source=&limit=  — Redis TTL 30min
GET  /admin/pipeline/status              — admin JWT only
GET  /admin/migration/report             — admin JWT only
GET  /admin/anomalies?date=&resolved=    — [v1.9] admin JWT only, list anomalies
POST /admin/anomalies/{id}/resolve       — [v1.9] admin JWT only, mark resolved with note
POST /admin/data/override               — [v1.9] admin JWT only, promote quarantined → validated
POST /admin/pipeline/replay             — [v1.9] admin JWT only, re-run pipeline for specific date
POST /admin/system/flag                 — [v1.9] admin JWT only, set/unset system flags
```

## 8.3 Schema Evolution Rules [v1.9]

```
BACKWARD COMPATIBILITY:
  - No column drops for 30 days after deprecation notice
  - Deprecated columns: add comment "DEPRECATED v1.X — will be removed in v1.Y"
  - API response fields: deprecated fields return null for 30 days, then removed
  - Alembic migration naming: YYYYMMDD_HHMM_descriptive_name.py

SAFE CHANGES (no coordination required):
  - Adding nullable columns
  - Adding new tables
  - Adding indexes
  - Widening VARCHAR(N) to VARCHAR(M) where M > N
  - Widening NUMERIC precision

UNSAFE CHANGES (require migration plan):
  - Renaming columns → add new, copy data, deprecate old
  - Changing column types → add new column, migrate, drop old
  - Dropping columns → deprecate first, remove after 30 days
  - Changing PRIMARY KEY → create new table, migrate, swap

ALEMBIC DISCIPLINE:
  - Every schema change via Alembic migration — NEVER manual ALTER TABLE
  - Migrations must be reversible (implement downgrade())
  - Test migration on staging DB dump before production
  - Migration files committed to git BEFORE deployment
```

---

## 9. Infrastructure

- **Database:** `data_engine` on existing RDS — PostgreSQL 12+ required
- **Service:** FastAPI + Uvicorn, port 8010, /home/ubuntu/data-engine/, Docker
- **Cache:** Redis, port 6379, localhost only — configured per Section 9.1
- **Antivirus:** ClamAV, `/usr/bin/clamdscan` — with orchestrator pre-flight check (see Section 7)
- **Schema migrations:** Alembic — `alembic init migrations` in /home/ubuntu/data-engine/
- **DB drivers:** asyncpg (async), psycopg2 (Alembic)
- **Connection pooling:** PgBouncer, default pool size 50 (was 20 in v1.6). Scale to 100-200 if concurrent pipeline workers + API requests cause TimeoutErrors. Monitor via `SHOW POOLS` in PgBouncer admin console. RDS `max_connections` must be set to at least 2× PgBouncer pool size.
- **Domains:** core.jslwealth.in (port 8010 HTTPS), upload.jslwealth.in (port 8011 HTTPS)
- **Orchestrator:** 127.0.0.1:8099 only (SSH tunnel to view)
- **Pre-sprint disk recovery:** `docker system prune -af` — EXCLUDING fie2_pgdata volume
- **Post-build estimated DB size:** 25-30 GB
- **RDS CloudWatch alerts:** CPU > 80%, storage < 20GB free, connections > 80
- **Backup:** RDS automated daily, 7-day retention

### 9.1 Redis Configuration [v1.7]

```
# /etc/redis/redis.conf — required settings

# Authentication
requirepass <loaded-from-secrets-manager>

# Memory management
maxmemory 2gb
maxmemory-policy allkeys-lru

# Persistence — AOF for durability, RDB for snapshots
appendonly yes
appendfsync everysec
save 900 1
save 300 10
save 60 10000

# Network — localhost only
bind 127.0.0.1
protected-mode yes

# Daily RDB snapshot to S3 (cron job):
# 0 3 * * * /usr/local/bin/redis-cli BGSAVE && sleep 5 && aws s3 cp /var/lib/redis/dump.rdb s3://jsl-data-engine-archive/redis/dump-$(date +\%Y\%m\%d).rdb
```

**Fallback contract:** Every FastAPI endpoint that reads from Redis MUST have a DB fallback.
**[v1.8] Cache stampede protection:** On cache miss, use setnx lock to prevent thundering herd.
**[v1.9] Circuit breaker:** After 3 consecutive Redis failures, bypass Redis entirely for 60 seconds.
```python
# [v1.9] Redis circuit breaker state (in-process, not in Redis itself)
# [v1.9.1] NOTE: state is PER-WORKER. With 4 Uvicorn workers, a dead Redis instance
# will receive up to 12 attempts (3 per worker) before all circuits open.
# This is acceptable — the DB fallback handles all requests regardless.
# Do NOT move circuit state to Redis (circular dependency on the thing being monitored).
redis_failure_count = 0
redis_circuit_open_until = None

async def redis_get_safe(key: str) -> Optional[str]:
    global redis_failure_count, redis_circuit_open_until
    if redis_circuit_open_until and datetime.utcnow() < redis_circuit_open_until:
        return None  # circuit open — skip Redis entirely
    try:
        result = await redis.get(key)
        redis_failure_count = 0  # reset on success
        return result
    except Exception:
        redis_failure_count += 1
        if redis_failure_count >= 3:
            redis_circuit_open_until = datetime.utcnow() + timedelta(seconds=60)
            log.warning("Redis circuit breaker OPEN — bypassing for 60s")
        return None

async def get_regime_current():
    cached = await redis_get_safe("regime:current")  # [v1.9] uses circuit breaker
    if cached:
        return json.loads(cached)

    # [v1.8] Stampede protection: only one request populates cache
    lock_acquired = await redis.set("lock:regime:current", "1", nx=True, ex=5)
    if not lock_acquired:
        # Another request is populating — wait and retry cache
        await asyncio.sleep(0.05)  # 50ms
        cached = await redis.get("regime:current")
        if cached:
            return json.loads(cached)
        # Still no cache — fall through to DB (rare edge case)

    # DB fallback — always works even if Redis is down
    result = await db.fetch_one("SELECT * FROM de_market_regime ORDER BY computed_at DESC LIMIT 1")
    try:
        await redis.setex("regime:current", 3600, json.dumps(result))
    except Exception:
        pass  # Redis down — result still returned from DB
    return result
```

---

## 10. Data Retention

| Table | Retention | Reason |
|-------|-----------|--------|
| de_pipeline_log | 90 days | Operational |
| de_migration_errors | 30 days | Review and resolve |
| de_request_log | 90 days | Audit |
| de_pii_access_log | 7 years | Regulatory (SEBI) |
| de_migration_log | Permanent | Audit record |
| All price/NAV/flow tables | Permanent | Core historical data |
| de_qual_documents | Permanent | Immutable knowledge base |
| de_qual_extracts | Permanent | Immutable knowledge base |
| de_qual_outcomes | Permanent | Learning record |
| de_adjustment_factors_daily | Permanent | Core historical data |
| de_market_cap_history | Permanent | Backtesting requirement |
| de_equity_technical_daily | Permanent | Derived from OHLCV, needed for breadth |
| de_data_anomalies | 1 year | Operational review; resolved anomalies archived |
| de_recompute_queue | 90 days | Operational; completed items pruned |
| de_mf_dividends | Permanent | Core adjustment data for IDCW plans |
| Redis cache | Per TTL | Auto-expires |
| **S3 qualitative archive** | **Permanent** | **Cold storage for processed files** |

---

## 11. Known Limitations and Future Work

1. mstar_id to amfi_code mapping — not all 13,195 Morningstar funds have a direct AMFI match. Null amfi_code permitted. Full reconciliation post-sprint.
2. NSE real-time data — EOD only in v1. Intraday requires NSE co-location agreement or commercial data vendor.
3. BHAV copy before 2000 — data quality poor. Backfill starts 2000-01-01.
4. Simulation and Optimization Laboratory — Phase 2, after validated data foundation.
5. RA license compliance layer — Phase 2, after license receipt.
6. ivfflat indexes on qualitative tables — deferred until 10,000+ rows. Run manually post-initial-load. Schedule monthly REINDEX.
7. RDS version — confirm PostgreSQL >= 12 on existing RDS instance before sprint starts.
8. ~~PII encryption key management~~ **[v1.7] Resolved in Section 3.7.1.**
9. **[v1.7] AMFI market cap reclassification ingestion — semi-annual pipeline to update de_market_cap_history not yet scheduled. Manual trigger acceptable for v1; automate in v2.**
10. **[v1.7] Synthetic continuous NAV series for merged MF schemes — flagging mechanism (is_synthetic column) deferred. Initial implementation stops NAV for old scheme and maps to surviving scheme only.**
11. **[v1.7] NSE master data endpoint — exact URL and field mapping for Step 0 (Master Refresh) to be confirmed during sprint. Candidate: NSE equity listing CSV or corporateActions API secondary fields.**
12. **[v1.8] UDiFF column mapping — exact field names and delimiters for the July 2024+ NSE BHAV format to be confirmed during sprint by downloading a sample file. Parser must auto-detect format by header row.**
13. ~~**[v1.8] IDCW dividend source — heuristic-based.**~~ **[v1.9] Resolved: heuristic removed. Dividends sourced explicitly from AMFI historical dividend files / BSE Star MF (Section 3.2, de_mf_dividends).**
14. **[v1.8] Computation versioning migration — existing RS scores and regime records (pre-v1.8) will have computation_version=1 backfilled. No retroactive recomputation planned unless logic changes materially.**
15. **[v1.8] Semantic deduplication threshold (0.92 cosine similarity) is an initial estimate. Tune after observing false positive/negative rates across 1,000+ qualitative documents.**
16. **[v1.8] Recompute queue background worker — not yet scheduled in orchestrator task_registry.json. Add as cron job during sprint: every 15 min during 22:00–06:00 IST.**
17. **[v1.9] Observability stack (Prometheus + Grafana) — not specified in this architecture doc. Sprint deliverable: instrument FastAPI with prometheus-fastapi-instrumentator, expose /metrics endpoint, deploy Grafana dashboard on EC2 (port 3000, SSH tunnel only). Key metrics: pipeline duration, rows/sec, anomaly count, API p95 latency, Redis hit rate.**
18. **[v1.9] Alerting integration — SLA enforcement (Section 5.13) references Slack webhooks but no specific webhook URL or channel is configured. Sprint task: create #jip-alerts Slack channel, configure incoming webhook, store URL in Secrets Manager.**
19. **[v1.9] Operational runbooks — "What happens if..." procedures not yet written for: NSE down, AMFI delayed, DB crash, Redis crash, pipeline stuck, disk full, corporate action backfill storm. Sprint deliverable: create /home/ubuntu/data-engine/runbooks/ directory with one markdown file per scenario.**
20. **[v1.9] Pipeline replay capability — POST /admin/pipeline/replay endpoint specified but implementation must ensure: (a) replay is idempotent (ON CONFLICT handles re-inserts), (b) replay re-runs validation, (c) replay does not duplicate de_source_files entries. Verify during sprint QA.**
21. **[v1.9] PostgreSQL VACUUM strategy — not specified. Sprint task: configure autovacuum aggressively for high-churn tables (de_equity_ohlcv, de_mf_nav_daily, de_rs_daily_summary). Set autovacuum_vacuum_scale_factor = 0.05 and autovacuum_analyze_scale_factor = 0.02 for these tables.**
22. **[v1.9] Slow query logging — not configured. Sprint task: SET log_min_duration_statement = 1000 in RDS parameter group (logs queries > 1 second). Review weekly.**

---

## 12. Operational Readiness Checklist [v1.9.1]

This architecture document specifies **what the system is**. This section specifies **what must be built around it** for production operation. These are sprint deliverables, not architecture decisions — they require implementation choices (which tool, which config) that should be made during the build, not in advance.

### 12.1 Orchestration (Sprint Week 1)

The current pipeline architecture defines steps, dependencies, and locks. Production requires a **central execution controller** that enforces:

- **DAG execution**: dependency graph from Section 7, enforced at runtime (not just documented)
- **Retry policies per step**: transient failures (network, API rate limit) retry 3× with exponential backoff; persistent failures (bad data, schema mismatch) fail immediately
- **Conditional branching**: if Track A fails, skip Steps 12-14 but continue Track B-E (already specified in Section 5.4 — must be enforced by orchestrator)
- **State machine per pipeline run**: pending → running → complete/failed/partial

Options: Prefect (lightweight, Python-native), Dagster (asset-oriented), or custom lightweight orchestrator using the existing `orchestrator.py` + `task_registry.json` pattern. Decision during sprint based on complexity assessment.

### 12.2 Monitoring and Alerting (Sprint Week 1)

**Metrics (Prometheus + Grafana):**
- Pipeline: duration per step, rows/sec ingested, failure rate, anomaly count
- API: request count, p50/p95/p99 latency, error rate, cache hit ratio
- Infrastructure: DB connections in use, Redis memory, disk usage, CPU
- Business: data freshness lag (seconds since last validated row), quarantine rate

**Alerting (Slack webhook → #jip-alerts):**

| Alert | Trigger | Severity |
|-------|---------|----------|
| Pipeline failure | Any track status = 'failed' | Critical |
| SLA breach | Section 5.13 deadlines missed | Critical |
| Anomaly spike | >10 critical anomalies in single pipeline run | Warning |
| DB CPU | CloudWatch CPU > 80% for 5 min | Warning |
| Stale data | No validated equity rows for today by 20:00 IST | Critical |
| Quarantine storm | >5% equity universe quarantined (Section 5.4 Step 9.9) | Critical |
| Redis down | Circuit breaker open on all workers | Warning |
| Disk space | EC2 root volume > 80% | Warning |

### 12.3 Runbooks (Sprint Week 2)

One markdown file per scenario in `/home/ubuntu/data-engine/runbooks/`:

| Runbook | Trigger | Key steps |
|---------|---------|-----------|
| `nse_bhav_delayed.md` | Track A fails at Step 1 past 19:30 | Check NSE site manually; if file available, trigger replay; if NSE down, mark holiday_skip and alert |
| `amfi_nav_delayed.md` | Track B fails at Step 5 past 22:30 | Check AMFI site; retry manually; if delayed >24h, use previous NAV with stale flag |
| `db_cpu_spike.md` | CloudWatch alert | Identify slow query via `pg_stat_activity`; kill if recompute; check VACUUM; scale RDS if persistent |
| `redis_down.md` | Circuit breaker open | Verify service: `redis-cli ping`; restart: `sudo systemctl restart redis`; if persistent, operate in DB-only mode |
| `pipeline_stuck.md` | Pipeline status='running' for >2 hours | Check `pg_locks` for held advisory locks; check worker process; force-release stale lock per Section 5.0 |
| `corporate_action_storm.md` | >50 recompute queue items pending | Verify legitimacy (bulk ex-date?); if legitimate, increase batch window; if error, quarantine and investigate |
| `disk_full.md` | EC2 >90% usage | `docker system prune -af`; check qualitative processing folder; verify S3 archival running |
| `data_corruption.md` | Cross-source mismatch in reconciliation | Identify source (Section 5.14); quarantine affected rows; replay from correct source; document in de_data_anomalies |

### 12.4 Replay and Backfill (Sprint Week 2)

The system must support: "Re-run the entire EOD pipeline for 15 Jan 2024 cleanly."

Requirements:
- **Date-scoped execution**: `python orchestrator.py --replay --date 2024-01-15`
- **Idempotent by design**: all INSERTs use ON CONFLICT — replay cannot create duplicates
- **Isolation**: replay runs use a separate advisory lock namespace (`replay:eod:2024-01-15`) to avoid conflicting with live pipelines
- **Validation re-runs**: replayed data goes through the same raw → validated → quarantined gating
- **Source file dedup**: de_source_files ON CONFLICT prevents duplicate file registration

### 12.5 Cost Control (Sprint Week 3)

| Resource | Control | Limit |
|----------|---------|-------|
| Claude API (qualitative extraction) | Per-document, per-day cap | 200 docs/day, $50/day |
| OpenAI embeddings | Per-document | 200 docs/day |
| Whisper API (audio transcription) | Per-file | 10 files/day |
| RDS storage | CloudWatch alert | Alert at 80% of provisioned |
| EC2 compute | Recompute queue throttling | max 2 concurrent, 50k rows/batch |

Daily cost tracking: log API call counts and estimated costs in de_pipeline_log.track_status JSONB.

### 12.6 Chaos Testing (Pre-Production QA)

Before declaring production-ready, simulate each failure mode:

| Test | How to simulate | Expected behavior |
|------|----------------|-------------------|
| NSE file missing | Delete BHAV file before Step 1 | Track A fails, Tracks B-E continue, API returns stale equity + fresh MF |
| Partial BHAV file | Truncate file to 50 rows | Freshness check rejects (rowcount < 500), Track A skips |
| Redis down | `sudo systemctl stop redis` | Circuit breaker opens, all API requests fall through to DB |
| DB slow | Run `SELECT pg_sleep(10)` in a loop | Query timeouts logged, API returns 504 for affected endpoints |
| Worker crash mid-recompute | `kill -9` recompute worker process | Heartbeat stale after 15 min, orchestrator resets to pending |
| Quarantine storm | Inject 500 rows with price = -1 | Step 9.9 halts aggregates, admin alerted, API returns partial |
| Kill switch | `UPDATE de_system_flags SET value = FALSE WHERE key = 'INGESTION_ENABLED'` | All pipelines exit gracefully on next run |

### 12.7 API Response Contract (Sprint Week 2)

Every data endpoint returns structured metadata:
```json
{
  "data": [...],
  "meta": {
    "data_freshness": "fresh",
    "last_updated_at": "2025-04-02T13:00:00Z",
    "pipeline_status": "complete",
    "computation_version": 1,
    "system_status": "normal"
  },
  "pagination": {
    "page": 1,
    "page_size": 100,
    "total_count": 4521,
    "has_next": true
  }
}
```

Headers (duplicated for clients that only read headers):
```
X-Data-Freshness: fresh | stale | partial
X-Computation-Version: 1
X-System-Status: normal | degraded
```
