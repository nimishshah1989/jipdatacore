# ETF-C1: NSE India ETF Master + OHLCV Sync

**Parent Plan:** [etf-expansion-chunk-plan.md](../etf-expansion-chunk-plan.md)
**Complexity:** Medium
**Dependencies:** None
**Blocks:** C3 (Enrichment), C4 (Pipeline Wiring), C5 (Deploy)

---

## Description

Create a curated NSE India ETF master list and a pipeline that copies NSE ETF OHLCV data from `de_equity_ohlcv` (where BHAV data already flows) into `de_etf_ohlcv` for unified ETF querying.

1. Define an `NSE_ETFS` dict with ~67 curated NSE ETFs including metadata (ticker, name, exchange, country, currency, sector, category, benchmark)
2. Seeder function: upsert all entries into `de_etf_master` with `ON CONFLICT` on natural key
3. Create `NseEtfSyncPipeline` (BasePipeline subclass) that runs a SQL `INSERT...SELECT` from `de_equity_ohlcv JOIN de_instrument` where `current_symbol` matches NSE ETF tickers
4. Support both full-history mode (first run, from 2016-04-01) and daily mode (business_date only)

---

## Files to Create/Modify

| Action | File | Purpose |
|--------|------|---------|
| NEW | `scripts/ingest/nse_etf_master.py` | NSE ETF curated list dict + de_etf_master seeder |
| NEW | `app/pipelines/etf/nse_etf_sync.py` | Post-BHAV pipeline: copy NSE ETF OHLCV from de_equity_ohlcv to de_etf_ohlcv |
| MODIFY | `app/pipelines/etf/__init__.py` | Export NseEtfSyncPipeline |

---

## NSE ETF Curated List (~67 ETFs)

**Broad Index (20):**
NIFTYBEES, JUNIORBEES, SETFNIF50, SETFNN50, ICICINIFTY, ICICINXT50, UTINIFTETF, UTINEXT50, HDFCNIFETF, KOTAKNIFTY, MOM50, MOM100, LICNETFN50, MAN50ETF, MANXT50, ICICISENSX, HDFCSENETF, UTISENSETF, CPSEETF, ICICIB22

**Banking & Financial (10):**
BANKBEES, SETFNIFBK, KOTAKBKETF, PSUBNKBEES, KOTAKPSUBK, HBANKETF, ICICIBANKN, UTIBANKETF, SBIETFPB, NPBET

**Sectoral (8):**
INFRABEES, SBIETFIT, NETFIT, ICICITECH, NETFCONSUM, NETFDIVOPP, SBIETFQLTY, NETFMID150

**Gold (8):**
GOLDBEES, SETFGOLD, KOTAKGOLD, HDFCMFGETF, ICICIGOLD, GOLDSHARE, AXISGOLD, BSLGOLDETF

**Silver (2):**
SILVERBEES, ICICISLVR

**Debt & Liquid (6):**
LIQUIDBEES, LIQUIDIETF, GILT5YBEES, NETFLTGILT, SETF10GILT, LICNETFGSC

**Bharat Bond (4):**
EBBETF0425, EBBETF0430, EBBETF0431, EBBETF0433

**International (2):**
HNGSNGBEES, MAFANG

**Smart Beta (4):**
ICICIALPLV, ICICILOVOL, KOTAKNV20, ICICINV20

**Midcap (3):**
ICICIMCAP, ICICIM150, ICICI500

---

## Detailed Implementation Steps

### Step 1: `scripts/ingest/nse_etf_master.py`

1. Define `NSE_ETFS` as a list of dicts, each with keys: `ticker`, `name`, `exchange` (always "NSE"), `country` ("IN"), `currency` ("INR"), `sector`, `category`, `benchmark`
2. Create `async def seed_nse_etf_master(session)`:
   - For each ETF in NSE_ETFS, execute:
     ```sql
     INSERT INTO de_etf_master (ticker, name, exchange, country, currency, sector, category, benchmark)
     VALUES (:ticker, :name, 'NSE', 'IN', 'INR', :sector, :category, :benchmark)
     ON CONFLICT (ticker) DO UPDATE SET
       name = EXCLUDED.name, sector = EXCLUDED.sector,
       category = EXCLUDED.category, benchmark = EXCLUDED.benchmark,
       updated_at = NOW();
     ```
   - Use `executemany` or batch insert with `chunksize=50`
3. Add `if __name__ == "__main__"` block that creates async session and calls seeder
4. Log: total inserted, total updated, any errors

### Step 2: `app/pipelines/etf/nse_etf_sync.py`

1. Subclass `BasePipeline` as `NseEtfSyncPipeline`
2. Implement `execute(self, business_date=None)`:
   - If `business_date` is None, sync full history from `2016-04-01`
   - If `business_date` provided, sync only that date
   - Execute the SQL:
     ```sql
     INSERT INTO de_etf_ohlcv (date, ticker, open, high, low, close, volume)
     SELECT eo.date, i.current_symbol, eo.open, eo.high, eo.low, eo.close, eo.volume
     FROM de_equity_ohlcv eo
     JOIN de_instrument i ON i.id = eo.instrument_id
     WHERE i.current_symbol IN (SELECT ticker FROM de_etf_master WHERE exchange = 'NSE')
       AND eo.date >= :start_date
     ON CONFLICT (date, ticker) DO UPDATE
       SET close = EXCLUDED.close, open = EXCLUDED.open,
           high = EXCLUDED.high, low = EXCLUDED.low,
           volume = EXCLUDED.volume, updated_at = NOW();
     ```
   - Return `PipelineResult` with `rows_processed` count
3. Log: date range synced, rows inserted/updated, any symbols missing from de_instrument

### Step 3: `app/pipelines/etf/__init__.py`

- Add `from .nse_etf_sync import NseEtfSyncPipeline` to exports

---

## Acceptance Criteria

- [ ] 67+ NSE ETFs in de_etf_master with `exchange='NSE'`
- [ ] de_etf_ohlcv has NSE ETF rows (historical from de_equity_ohlcv)
- [ ] `NseEtfSyncPipeline.execute()` returns `rows_processed > 0`
- [ ] No duplicate rows (ON CONFLICT works correctly)
- [ ] Symbols not found in de_instrument are logged, not silently skipped
- [ ] ruff + mypy clean

---

## Verification Queries

```sql
-- Count NSE ETFs in master
SELECT COUNT(*) FROM de_etf_master WHERE exchange = 'NSE';
-- Expected: 67+

-- Check OHLCV data exists
SELECT COUNT(DISTINCT ticker) FROM de_etf_ohlcv
WHERE ticker IN (SELECT ticker FROM de_etf_master WHERE exchange = 'NSE');

-- Check for duplicates
SELECT date, ticker, COUNT(*)
FROM de_etf_ohlcv
GROUP BY date, ticker
HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- Verify symbol coverage (which NSE ETFs have NO data)
SELECT m.ticker FROM de_etf_master m
LEFT JOIN de_etf_ohlcv o ON o.ticker = m.ticker
WHERE m.exchange = 'NSE' AND o.ticker IS NULL;
```
