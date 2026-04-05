# CTO Build Plan — Chunks 7-16

**Date:** 2026-04-05
**Author:** CTO Orchestrator (autonomous build)
**Status:** APPROVED (CTO authority)

## Executive Summary

10 chunks across 4 waves. C1-C6 (infrastructure) complete. Building Layer 3 ingestion (C7-C10), Layer 4 computation (C11-C13), Layer 5 API + ops (C14-C16).

## Wave Execution Order

| Wave | Chunks | Parallelism | Dependencies Met |
|------|--------|-------------|-----------------|
| 1 | C7, C8, C9, C13, C15 | 5 parallel | C1-C6 done |
| 2 | C10, C11 | 2 parallel | C8 (W1), C7 (W1) |
| 3 | C12 | Sequential | C8+C11 (W1+W2) |
| 4 | C14, C16 | 2 parallel | C11+C12 (W2+W3) |

## Shared Patterns (all chunks)

All pipelines MUST use these established patterns from C1-C6:

1. **BasePipeline** (`app/pipelines/framework.py`): 8-step run() template. Subclass → implement `execute()` + optional `validate()`
2. **Idempotent upsert**: `pg_insert().on_conflict_do_update(index_elements=[...], set_={...})`
3. **Decimal(str(value))**: All financial values. Never float()
4. **Advisory locks**: `pg_try_advisory_lock(hashtext(pipeline_name || date))`
5. **Data status gating**: raw → validated → quarantined
6. **structlog**: All logging. Never print()
7. **mapped_column()**: SQLAlchemy 2.0 syntax
8. **Table prefix**: `de_` on all tables

## Shared Utilities Needed

### Already exists (from C1-C6):
- `app/pipelines/framework.py` — BasePipeline, ExecutionResult, PipelineResult
- `app/pipelines/calendar.py` — Trading calendar utilities
- `app/pipelines/guards.py` — System flag checks
- `app/pipelines/source_files.py` — File dedup with checksum
- `app/pipelines/validation.py` — Anomaly detection framework
- `app/pipelines/system_flags.py` — Feature flag management
- `app/config.py` — All external API credentials (Morningstar, FRED, OpenAI, Anthropic, Goldilocks)
- `app/db/session.py` — Async engine, session factory, get_db()

### New utilities (create in Wave 1):
- `app/utils/symbol_resolver.py` — symbol → instrument_id lookup with cache
- `app/utils/fetch_helpers.py` — HTTP retry logic (3× exponential backoff), NSE headers
- `app/utils/indian_format.py` — Lakh/crore formatting, IST timezone helpers

## Per-Chunk Specifications

### C7 — Equity Ingestion Pipeline
**Directory:** `app/pipelines/equity/`
**New models:** None (DeEquityOhlcv, DeCorporateActions, DeAdjustmentFactorsDaily exist)
**Pipeline classes:**
- `BhavCopyPipeline(BasePipeline)` — format auto-detect (pre-2010, standard, UDiFF), ≥500 row validation
- `CorporateActionPipeline(BasePipeline)` — split ratio validation, adjustment factor computation
- `DeliveryPipeline(BasePipeline)` — T+1 delivery data (09:00 IST trigger)
- `TechnicalUpdatePipeline(BasePipeline)` — incremental technical indicator update post-BHAV
**Key logic:** NSE download with retry, symbol enforcement vs `de_instrument.current_symbol`, anomaly detection (price spike >20%, volume spike >10×, negative values, range violations)
**Tests:** test_bhav_format_detection, test_symbol_enforcement, test_anomaly_detection, test_corporate_action_adjustment

### C8 — MF Ingestion Pipeline
**Directory:** `app/pipelines/mf/`
**New models:** None (DeMfNavDaily, DeMfMaster, DeMfLifecycle, DeMfDividends exist)
**Pipeline classes:**
- `AmfiNavPipeline(BasePipeline)` — AMFI pipe-delimited parse, equity growth regular filter (~450-550 funds)
- `MfReturnComputePipeline(BasePipeline)` — 1d through 10y return computation using nav_adj for IDCW
- `MfLifecyclePipeline(BasePipeline)` — merge/closure handling
**Key logic:** NAV spike validation (>15%), zero NAV check, 0.92 cosine similarity dedup, SLA by 22:30 IST
**Tests:** test_amfi_parse, test_nav_validation, test_return_computation_decimal, test_lifecycle_merge

### C9 — Supporting Pipelines
**Directory:** `app/pipelines/indices/`, `app/pipelines/flows/`, `app/pipelines/fno/`, `app/pipelines/global/`
**New models:** None (DeIndexPrices, DeInstitutionalFlows, DeFoSummary, DeGlobalPrices, DeMacroValues exist)
**Pipeline classes:**
- `IndexPricePipeline(BasePipeline)` — Track C: NSE indices + India VIX
- `FiiDiiFlowPipeline(BasePipeline)` — Track D: NSE API primary + SEBI CSV fallback
- `FnoSummaryPipeline(BasePipeline)` — Track E: option chain, PCR, max pain
- `GlobalPricePipeline(BasePipeline)` — yfinance tickers + FRED data (07:30 IST pre-market)
**Key logic:** Each track isolated, US dates as US date, Indian as Indian date
**Tests:** test_index_ingestion, test_fii_dii_fallback, test_pcr_calculation, test_global_fetch

### C10 — Morningstar Integration
**Directory:** `app/pipelines/morningstar/`
**New models:** None (DeMfMaster, DeMfHoldings exist; holdings.instrument_id nullable)
**Pipeline classes:**
- `MorningstarMasterPipeline(BasePipeline)` — Weekly Sunday fund master refresh
- `MorningstarHoldingsPipeline(BasePipeline)` — Monthly 1st holdings refresh, ISIN → instrument_id resolution
- `MorningstarRiskPipeline(BasePipeline)` — Sharpe, StDev, alpha, beta fetch
**Key logic:** Single v2.0 endpoint, rate limiting (per-second + per-day), AWS Secrets Manager credentials, 404 handling
**Tests:** test_rate_limiting, test_holdings_isin_resolve, test_risk_data_decimal

### C11 — Technicals + RS + Breadth + Regime (CRITICAL)
**Directory:** `app/computation/`
**New models:** Need `DeMfDerivedDaily` — but that's C12. C11 uses existing: DeEquityTechnicalDaily, DeRsScores, DeRsDailySummary, DeMarketRegime, DeBreadthDaily
**Pipeline/computation classes:**
- `TechnicalComputeEngine` — ~80 indicators per stock per day (EMA, RSI Wilder 14d, ADX, MFI, MACD 12/26/9, Bollinger, ROC)
- `RsComputeEngine` — RS formula: `(entity_cumreturn - benchmark_cumreturn) / benchmark_rolling_std`. Composite: 1w×0.10 + 1m×0.20 + 3m×0.30 + 6m×0.25 + 12m×0.15
- `BreadthComputeEngine` — 25 indicators (6 daily + 6 monthly + zone classification)
- `RegimeComputeEngine` — BULL/BEAR/SIDEWAYS/RECOVERY, 0-100 component scores
- `SentimentComputeEngine` — 5-layer composite (0.20/0.30/0.25/0.15/0.10)
- `RecomputeWorker` — processes de_recompute_queue, 2 concurrent, 50k rows/batch
**FORMULA ACCURACY IS CRITICAL:** All formulas MUST match docs/formulas/ exactly
**Key constants:** RF=7%, 252 trading days, drawdown thresholds per regime
**Tests:** test_ema_calculation, test_rsi_wilder_smoothing, test_rs_composite_weights, test_breadth_zones, test_regime_classification, test_sentiment_layers

### C12 — Sector + Fund Derived
**Directory:** `app/computation/`
**New models:** `DeMfDerivedDaily` (nav_date, mstar_id, derived_rs_composite, nav_rs_composite, manager_alpha, coverage_pct, sharpe_1y/3y, sortino_1y, max_drawdown_1y/3y, volatility_1y/3y, beta_vs_nifty)
**Computation classes:**
- `SectorMetricsEngine` — Market-cap weighted RS, rotation quadrant, sector breadth
- `FundDerivedEngine` — Holdings-weight aggregated metrics, manager alpha, NAV-based risk metrics
**Key logic:** market_cap from de_market_cap_history (effective_to IS NULL), point-in-time accuracy, Sharpe RF=7%
**Tests:** test_sector_rs_weighting, test_rotation_quadrant, test_fund_manager_alpha, test_sharpe_rf_7pct

### C13 — Qualitative Pipeline
**Directory:** `app/pipelines/qualitative/`
**New models:** None (DeQualSources, DeQualDocuments, DeQualExtracts exist)
**Pipeline classes:**
- `RssFeedPipeline(BasePipeline)` — 30min polling, SHA-256 content_hash dedup
- `QualUploadHandler` — POST /qualitative/upload (admin JWT, 10/hr, magic byte + ClamAV)
- `ContentExtractor` — Whisper (audio), PyMuPDF (PDF), BeautifulSoup (URL)
- `ClaudeExtractorPipeline` — claude-sonnet-4-20250514, market views, quality_score ≥0.70
- `EmbeddingPipeline` — OpenAI text-embedding-3-small, 1536d, 0.92 cosine dedup
**Key logic:** Advisory locks per-document, cost guardrails (200 docs/day, 50/source/day, 10 audio/day), S3 archival, Playwright for Goldilocks
**Tests:** test_rss_dedup, test_magic_byte_validation, test_cost_guardrails, test_quality_score_threshold

### C14 — Market Pulse + MF Pulse API
**Directory:** `app/api/v1/`
**New models:** None
**Endpoints:** ~25 REST endpoints with Redis caching (1h-24h TTL), symbol resolution, data status gating
- Auth: token, refresh (JWT rotation)
- Equity: OHLCV, universe, RS leaderboard, sector RS, single stock RS
- MF: NAV history, universe, category flows, derived metrics
- Market: regime, breadth, indices, global, macro, flows, F&O
- Qualitative: upload, semantic search, recent
- Admin: pipeline status, anomalies, resolve, override, replay, system flags
**Key logic:** Response envelope (data, meta, pagination), X-Data-Freshness header, p95 <200ms, DB fallback if Redis down
**Tests:** test_symbol_resolution, test_response_envelope, test_redis_cache_ttl, test_admin_jwt_scope

### C15 — Pipeline Dashboard
**Directory:** `dashboard/`
**Stack:** Standalone FastAPI on port 8099, HTML/CSS/JS served, calls localhost:8010/api/v1/admin/*
**Key features:** Live pipeline status, data ingestion progress, anomalies by severity, system health (Redis/DB/disk), SLA tracking, historical pipeline viewer, 30s auto-refresh
**Design:** White bg, teal accents (#1D9E75), desktop-first, data-dense, right-aligned numbers
**Tests:** test_dashboard_api_proxy, test_sla_display

### C16 — Orchestrator + Monitoring
**Directory:** `app/orchestrator/`
**New models:** None (DePipelineLog, DeSystemFlags exist)
**Classes:**
- `PipelineOrchestrator` — DAG execution, state machine (pending→running→complete/failed/partial)
- `CronScheduler` — All cron schedules from spec
- `SlaEnforcer` — Deadline monitoring, Slack alerts
- `ReconciliationEngine` — 3 daily checks (NSE vs yfinance 2%, AMFI vs backup 0.1%, row counts)
- `PrometheusMetrics` — pipeline_duration, rows_ingested, anomaly_count, api_latency, cache_hits
**Key logic:** Conditional branching (Track A fail → skip RS but continue B-E), --resume crash recovery, pre-flight checks, retry policies (transient 3× vs persistent fail-immediate), Docker compose, runbooks
**Tests:** test_dag_execution_order, test_conditional_branching, test_sla_enforcement, test_reconciliation_checks

## Risk Areas

1. **C11 formula accuracy** — Most critical. RS composite weights, Wilder RSI smoothing, breadth zone thresholds must match docs/formulas/ exactly. Agent gets all 5 formula docs.
2. **C12 point-in-time accuracy** — market_cap_history effective_to filtering must prevent look-ahead bias
3. **C13 external dependencies** — ClamAV, Whisper API, Claude API, OpenAI embeddings. All must degrade gracefully.
4. **C14 performance** — p95 <200ms requires Redis caching layer. Symbol resolution must use partition pruning.
5. **C16 DAG complexity** — Conditional branching logic needs thorough testing.

## Cross-Chunk Interfaces

```
C7 (equity OHLCV) ──→ C11 (technicals, RS) ──→ C12 (sector metrics) ──→ C14 (API)
C8 (MF NAV) ──→ C10 (Morningstar) ──→ C12 (fund derived) ──→ C14 (API)
C9 (flows, indices) ──→ C11 (breadth, regime) ──→ C14 (API)
C13 (qualitative) ──→ C14 (API)
C7+C8+C9+C11 ──→ C16 (orchestrator)
C15 (dashboard) ──→ C14 (admin API)
```

## Test Strategy

- Every module gets unit tests
- Financial computations: Decimal assertions with exact values
- Pipeline tests: mock external HTTP, test parse + transform + upsert
- API tests: httpx AsyncClient, test request/response schemas
- Integration: after each wave merge, full `pytest tests/ -v`
- Lint: `ruff check . --select E,F,W` after each wave
- Target: All tests pass before proceeding to next wave

## CTO Decisions

1. **Symbol resolver** — Create shared utility in Wave 1 (C7 needs it, C11/C14 reuse it)
2. **HTTP fetch helper** — Shared retry logic for NSE/AMFI/yfinance/FRED/Morningstar
3. **C15 stack** — Standalone FastAPI + Jinja2 templates (simplest, consistent with backend stack)
4. **C16 orchestrator** — Lightweight custom (per spec recommendation), not Prefect/Dagster
5. **New model DeMfDerivedDaily** — Created in C12, not before (to avoid premature schema changes)
