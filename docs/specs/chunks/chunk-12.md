# Chunk 12: Sector + Fund Derived Metrics

**Layer:** 4
**Dependencies:** C8, C11
**Complexity:** High
**Status:** pending

## Files

- `app/computation/sectors.py`
- `app/computation/fund_derived.py`
- `app/models/mf_derived.py`
- `alembic/versions/XXX_mf_derived_tables.py`
- `tests/computation/test_sectors.py`
- `tests/computation/test_fund_derived.py`

## Acceptance Criteria

### Sector Metrics

- [ ] **Sector aggregation:** Market-cap-weighted aggregation of constituent stock metrics from `de_rs_scores` and `de_equity_technical_daily`
- [ ] **Sector RS scores:** Compute RS for each sector (NIFTY sector indices as benchmark for sector boundaries); INSERT into `de_rs_scores` with `entity_type='sector'`
- [ ] **Sector momentum:** Weighted average momentum score across constituent stocks
- [ ] **Sector volatility:** Market-cap-weighted volatility of constituents
- [ ] **Sector breadth:** % of stocks in sector above 50DMA, 200DMA; advance/decline ratio within sector
- [ ] **Market cap weights:** Use `de_market_cap_history` with `effective_to IS NULL` for current classification; point-in-time accuracy for backtests
- [ ] **Sector definitions:** Use `de_instrument.sector` field populated during NSE master refresh; align with NIFTY sectoral index constituents
- [ ] Sector computation runs after RS computation (Step 12) completes each day

### MF Derived Metrics

- [ ] **Holdings × stock metrics:** For each fund, compute weighted average of stock-level metrics using `de_mf_holdings.weight_pct` as weights
- [ ] **Derived metrics computed per fund:** weighted RS composite, weighted sector exposure, weighted volatility, weighted beta
- [ ] **Holdings coverage tracking:** `coverage_pct = sum(weight_pct for holdings with resolved instrument_id) / 100`; funds with coverage < 50% flagged as unreliable
- [ ] **Manager alpha signal:** Compare NAV-based RS (from `de_mf_nav_daily`) vs holdings-derived RS (from stock metrics); difference = manager alpha signal
- [ ] **MF NAV-based risk metrics (from NAV series):** Sharpe ratio (1y, 3y), Sortino ratio, Max drawdown (1y, 3y), Annualised volatility, Beta vs NIFTY 50, Calmar ratio
- [ ] All derived metrics stored in new table `de_mf_derived_daily` (or similar — design the table as part of this chunk)
- [ ] `INSERT INTO de_mf_derived_daily ON CONFLICT DO UPDATE`
- [ ] Runs after both MF NAV pipeline (C8) and RS computation (C11) complete

### Schema Addition

- [ ] New Alembic migration for `de_mf_derived_daily` table:
  - `nav_date DATE NOT NULL`
  - `mstar_id VARCHAR(20) NOT NULL REFERENCES de_mf_master(mstar_id)`
  - `derived_rs_composite NUMERIC(10,4)` — holdings-weighted stock RS
  - `nav_rs_composite NUMERIC(10,4)` — NAV-based RS (copied from de_rs_scores for entity_type='mf_category')
  - `manager_alpha NUMERIC(10,4)` — nav_rs - derived_rs
  - `coverage_pct NUMERIC(6,2)` — % of holdings resolved to instrument_id
  - `sharpe_1y NUMERIC(10,4)`, `sharpe_3y NUMERIC(10,4)`
  - `sortino_1y NUMERIC(10,4)`
  - `max_drawdown_1y NUMERIC(10,4)`, `max_drawdown_3y NUMERIC(10,4)`
  - `volatility_1y NUMERIC(10,4)`, `volatility_3y NUMERIC(10,4)`
  - `beta_vs_nifty NUMERIC(10,4)`
  - `created_at TIMESTAMPTZ DEFAULT NOW()`
  - `PRIMARY KEY (nav_date, mstar_id)`

## Notes

**Architecture principle (from spec):** Fund metrics = holding-weight aggregation of stock metrics. This eliminates duplication and ensures consistency — the same stock metric appears consistently whether viewed directly, via sector, or via fund.

**Market cap weighting for sectors:** Use daily market cap (price × shares outstanding) where available, or use index weight from `de_index_constituents` as proxy. SEBI reclassifies large/mid/small cap bi-annually — use `de_market_cap_history` with `effective_to IS NULL` for current.

**Manager alpha interpretation:** Positive alpha = fund manager's active bets are adding value beyond what the holdings would predict. Negative alpha = the portfolio construction is destroying value vs a passive equivalent.

**Holdings coverage threshold:** If `coverage_pct < 50%`, the derived metrics are unreliable (too many holdings unresolved to instruments). Flag in derived table but do not suppress — let API consumers decide.

**MF RS computation:** MF RS is stored in `de_rs_scores` with `entity_type='mf_category'` (category-level RS). Individual fund NAV-based RS uses the same formula as equity RS but with NAV instead of close_adj, and benchmark is the category average NAV (not a price index).

**Computation order:** Sector computation depends on stock RS (C11). Fund derived depends on both MF NAV pipeline (C8) and stock RS (C11). This chunk is the last computation layer before API (C14).
