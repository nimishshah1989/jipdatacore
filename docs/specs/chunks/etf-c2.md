# ETF-C2: Global ETF Expansion + Backfill

**Parent Plan:** [etf-expansion-chunk-plan.md](../etf-expansion-chunk-plan.md)
**Complexity:** Medium
**Dependencies:** None (can run in parallel with C1)
**Blocks:** C3 (Enrichment), C5 (Deploy)

---

## Description

Expand the global ETF universe from 130 to 163 tickers by adding ~33 new entries to the existing `ETFS` dict in `etf_ingest.py`. Create a backfill script that uses yfinance `period="max"` to pull full historical OHLCV for all new tickers.

---

## Files to Create/Modify

| Action | File | Purpose |
|--------|------|---------|
| MODIFY | `scripts/ingest/etf_ingest.py` | Add ~33 new entries to ETFS dict |
| NEW | `scripts/ingest/etf_backfill.py` | yfinance max-history backfill for new/specified tickers |

---

## New Tickers to Add (~33)

### Fixed Income (5)
| Ticker | Name | Exchange | Sector |
|--------|------|----------|--------|
| AGG | iShares Core US Aggregate Bond | NYSE | Fixed Income |
| BNDX | Vanguard Total International Bond | NASDAQ | Fixed Income |
| TIP | iShares TIPS Bond | NYSE | Fixed Income |
| SHY | iShares 1-3 Year Treasury Bond | NASDAQ | Fixed Income |
| BND | Vanguard Total Bond Market | NASDAQ | Fixed Income |

### Commodities (3)
| Ticker | Name | Exchange | Sector |
|--------|------|----------|--------|
| PDBC | Invesco Optimum Yield Diversified Commodity | NASDAQ | Commodities |
| PPLT | abrdn Physical Platinum Shares | NYSE | Commodities |
| WEAT | Teucrium Wheat Fund | NYSE | Commodities |

### Thematic (20)
| Ticker | Name | Exchange | Sector |
|--------|------|----------|--------|
| ARKK | ARK Innovation | NYSE | Thematic - Innovation |
| BOTZ | Global X Robotics & AI | NASDAQ | Thematic - AI/Robotics |
| ROBO | ROBO Global Robotics & Automation | NYSE | Thematic - AI/Robotics |
| DRIV | Global X Autonomous & Electric Vehicles | NYSE | Thematic - EV |
| LIT | Global X Lithium & Battery Tech | NYSE | Thematic - EV |
| CIBR | First Trust NASDAQ Cybersecurity | NASDAQ | Thematic - Cybersecurity |
| BUG | Global X Cybersecurity | NYSE | Thematic - Cybersecurity |
| GNOM | Global X Genomics & Biotech | NYSE | Thematic - Genomics |
| BLOK | Amplify Transformational Data Sharing | NYSE | Thematic - Blockchain |
| URA | Global X Uranium | NYSE | Thematic - Uranium |
| ARKX | ARK Space Exploration & Innovation | NYSE | Thematic - Space |
| QCLN | First Trust NASDAQ Clean Edge Green Energy | NASDAQ | Thematic - Clean Energy |
| JETS | US Global Jets | NYSE | Thematic - Airlines |
| MSOS | AdvisorShares Pure US Cannabis | NYSE | Thematic - Cannabis |
| XHE | SPDR S&P Health Care Equipment | NYSE | Thematic - Healthcare |
| CLOU | Global X Cloud Computing | NYSE | Thematic - Cloud |
| AIQ | Global X Artificial Intelligence & Technology | NYSE | Thematic - AI |
| FINX | Global X FinTech | NASDAQ | Thematic - Fintech |
| IBIT | iShares Bitcoin Trust | NASDAQ | Thematic - Crypto |

### Frontier/Small Countries (4)
| Ticker | Name | Exchange | Sector |
|--------|------|----------|--------|
| FM | iShares MSCI Frontier and Select EM | NYSE | Frontier Markets |
| ENZL | iShares MSCI New Zealand | NASDAQ | New Zealand |
| PAK | Global X MSCI Pakistan | NYSE | Pakistan |
| NGE | Global X MSCI Nigeria | NYSE | Nigeria |

**Note:** ERUS (Russia) excluded -- likely delisted due to sanctions. Leveraged/inverse ETFs excluded per design doc (decay distorts RS scores).

---

## Detailed Implementation Steps

### Step 1: Modify `scripts/ingest/etf_ingest.py`

1. Add all ~33 new entries to the `ETFS` dict following the existing format
2. Each entry must include: `ticker`, `name`, `exchange`, `country`, `currency`, `sector`, `category`
3. Verify no duplicate tickers with existing 130 entries
4. Run `etf_ingest.py` to upsert all 163 entries into de_etf_master

### Step 2: Create `scripts/ingest/etf_backfill.py`

1. Accept CLI args:
   - `--tickers` (comma-separated list of specific tickers to backfill)
   - `--new-only` flag (backfill only tickers that have zero rows in de_etf_ohlcv)
   - `--batch-size` (default 50, matching existing yfinance batch convention)
2. Query de_etf_master for target tickers based on args
3. For each batch of tickers:
   - Call `yf.download(tickers, period="max", group_by="ticker")`
   - Handle column order: `group_by="ticker"` returns `(ticker, field)` multi-index (per recent fix in commit fdd9118)
   - For each ticker in batch:
     - Extract OHLCV DataFrame
     - Skip if empty (log warning)
     - Upsert into de_etf_ohlcv using `to_sql(method='multi', chunksize=5000)` or raw SQL INSERT...ON CONFLICT
   - Log per-ticker: ticker, date range, row count
   - Exponential backoff between batches (start 2s, max 30s)
4. Final summary log: total tickers processed, total rows inserted, tickers skipped/failed

### Step 3: Run Backfill

1. Execute: `python scripts/ingest/etf_backfill.py --new-only`
2. Verify all 33 new tickers have historical data
3. For young thematic ETFs (IBIT, ARKX, etc.), accept limited history -- this is expected

---

## Acceptance Criteria

- [ ] ETFS dict has 163 entries (130 existing + 33 new)
- [ ] de_etf_master has 163 global ETFs (exchange != 'NSE')
- [ ] de_etf_ohlcv has historical data for all new tickers (5+ years where available)
- [ ] etf_backfill.py handles yfinance errors gracefully (skip ticker + log, don't crash)
- [ ] etf_backfill.py handles the `(ticker, field)` column order from `group_by="ticker"`
- [ ] No duplicate rows in de_etf_ohlcv after backfill
- [ ] ruff + mypy clean

---

## Verification Queries

```sql
-- Count global ETFs in master
SELECT COUNT(*) FROM de_etf_master WHERE exchange != 'NSE';
-- Expected: 163

-- Check new tickers have OHLCV data
SELECT m.ticker, COUNT(o.date) AS days, MIN(o.date) AS first_date, MAX(o.date) AS last_date
FROM de_etf_master m
LEFT JOIN de_etf_ohlcv o ON o.ticker = m.ticker
WHERE m.ticker IN ('AGG','BNDX','TIP','SHY','BND','PDBC','PPLT','WEAT',
  'ARKK','BOTZ','ROBO','DRIV','LIT','CIBR','BUG','GNOM','BLOK','URA',
  'ARKX','QCLN','JETS','MSOS','XHE','CLOU','AIQ','FINX','IBIT',
  'FM','ENZL','PAK','NGE')
GROUP BY m.ticker
ORDER BY days;

-- Tickers with no data (should be 0 after backfill)
SELECT m.ticker FROM de_etf_master m
LEFT JOIN de_etf_ohlcv o ON o.ticker = m.ticker
WHERE m.exchange != 'NSE' AND o.ticker IS NULL;
```
