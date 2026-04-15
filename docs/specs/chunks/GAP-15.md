# GAP-15 — `/api/v1/instrument/{symbol}` deepdive endpoint

## Goal
Single FastAPI route that returns everything Atlas needs for a stock deepdive
page in one call. Joins data already in the DB — no new computations.

## Response shape (JSON)

```
{
  "instrument": {
    "symbol": "RELIANCE",
    "isin": "INE002A01018",
    "name": "Reliance Industries Ltd",
    "sector": "Oil & Gas",
    "industry": "Refineries",
    "instrument_id": "<uuid>",
    "listing_date": "1995-11-29",
    "face_value": 10.0
  },
  "fundamentals": {
    "as_of_date": "2026-04-15",
    "market_cap_cr": 1926475.0, "pe_ratio": 24.3, "pb_ratio": 2.8,
    "peg_ratio": null, "ev_ebitda": null,
    "roe_pct": 11.2, "roce_pct": 13.5,
    "operating_margin_pct": 18.9, "net_margin_pct": 9.1,
    "debt_to_equity": 0.42, "interest_coverage": null,
    "eps_ttm": 98.1, "book_value": 850.0,
    "dividend_per_share": 10.0, "dividend_yield_pct": 0.39,
    "promoter_holding_pct": 50.3, "pledged_pct": 0.0,
    "fii_holding_pct": 22.1, "dii_holding_pct": 16.7,
    "revenue_growth_yoy_pct": 8.4, "profit_growth_yoy_pct": 11.2,
    "high_52w": 3100.0, "low_52w": 2180.0
  },
  "price": {
    "last_close": 2950.0, "last_date": "2026-04-13",
    "change_1d_pct": 0.45, "change_1w_pct": 2.1, "change_1m_pct": 4.8,
    "change_3m_pct": 8.4, "change_1y_pct": 24.1
  },
  "technicals": {
    "as_of_date": "2026-04-13",
    "sma_20": 2920.1, "sma_50": 2850.5, "sma_200": 2700.0,
    "ema_20": 2930.0, "ema_50": 2860.0,
    "rsi_14": 58.3, "macd": 12.4, "macd_signal": 10.1,
    "bollinger_upper": 2980.0, "bollinger_lower": 2830.0,
    "atr_14": 42.1, "adx_14": 24.5,
    "above_50dma": true, "above_200dma": true
  },
  "risk": {
    "sharpe_1y": 0.82, "sharpe_3y": 0.65, "sharpe_5y": 0.54,
    "sortino_1y": 1.12, "max_drawdown_1y": -0.18,
    "beta_3y": 1.08, "treynor_3y": 0.12,
    "downside_risk_3y": 0.15
  },
  "relative_strength": {
    "rs_vs_nifty": 65, "rs_vs_sector": 72, "rs_rank_overall": 412,
    "rs_trend": "improving"
  },
  "sector_peers": [
    {"symbol": "ONGC", "pe": 8.2, "roe": 14.1, "change_1y_pct": 18.2},
    ...  // top 5 peers in same sector by market cap
  ],
  "recent_news": [
    {"headline": "...", "source": "Mint", "published_at": "2026-04-14T10:30:00+05:30",
     "summary": "...", "url": "..."},
    ...  // top 5 from goldilocks_extractions by recency, filtered to this instrument
  ],
  "meta": {
    "data_as_of": "2026-04-15T12:30:00+05:30",
    "completeness_pct": 98
  }
}
```

## Scope

### New files
- `app/api/v1/instrument_deepdive.py` — FastAPI router with GET
  `/instrument/{symbol}` and GET `/instrument/id/{instrument_id}`
- `app/services/instrument_deepdive_service.py` — pure-Python service that
  takes `(instrument_id, session)` and returns the pydantic response model
- `app/schemas/instrument_deepdive.py` — pydantic v2 response models

### Modifications
- `app/main.py` — register the new router under `/api/v1`

### Data sources (all existing, no new tables)
- `de_instrument` / `de_equity_master` — instrument metadata
- `de_equity_fundamentals` — latest snapshot
- `de_equity_ohlcv` — last_close, change windows (use window functions,
  one query for all)
- `de_equity_technical_daily` — latest row's technicals + risk columns
- `de_rs_daily_summary` — RS scores
- Sector peers: SELECT from `de_equity_fundamentals` f JOIN
  `de_equity_master` m WHERE m.sector = <this.sector>
  ORDER BY market_cap_cr DESC LIMIT 5
- `goldilocks_extractions` (or the RAG vector search on the symbol/name) —
  top 5 recent news. Fallback to empty list if not linkable.

### Performance targets
- p50 ≤ 250ms on t3.large for a popular stock (cached sector lookup)
- p99 ≤ 1s
- Every sub-query must hit an index (verify with EXPLAIN ANALYZE)

### Tests
- `tests/api/test_instrument_deepdive.py` — golden response snapshot for
  RELIANCE, TCS, HDFCBANK
- Missing-data graceful degradation: symbol with no fundamentals returns
  nulls not 404
- Unknown symbol → 404 with `{"detail": "symbol not found"}`

## Acceptance criteria
- [ ] `curl https://data.jslwealth.in/api/v1/instrument/RELIANCE` returns
  all 7 top-level sections (instrument, fundamentals, price, technicals,
  risk, relative_strength, sector_peers, recent_news) — any may be null/[]
  for gaps but the keys are always present
- [ ] Works for at least 5 test symbols: RELIANCE, TCS, HDFCBANK, INFY, ICICIBANK
- [ ] Golden snapshot tests exist and pass
- [ ] Router registered in `app/main.py`
- [ ] Commit subject starts with `GAP-15`
- [ ] `state.db` shows `GAP-15` DONE

## Steps for the inner session
1. Read `app/main.py` to find the right place to mount the router
2. Read `app/api/v1/observatory.py` as the reference pattern for a
   read-only join-heavy endpoint
3. Read `app/models/` to confirm column names on the 6 tables this joins
4. Write the service as a single async function that runs 6 queries
   in parallel via `asyncio.gather`
5. Write the response schemas
6. Write tests with golden fixtures
7. Smoke-test against localhost:8010 before committing
8. Commit

## Out of scope
- Historical fundamentals charts (GAP-17)
- Per-stock corporate announcements (GAP-18)
- ETF / MF / index deepdive — equity first, extend later if needed
- Any new computation or table creation

## Dependencies
- Upstream: GAP-06, GAP-07, GAP-08, GAP-09 (all DONE)
- Downstream: Atlas UI's deepdive page
