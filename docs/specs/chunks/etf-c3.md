# ETF-C3: Enrichment Script

**Parent Plan:** [etf-expansion-chunk-plan.md](../etf-expansion-chunk-plan.md)
**Complexity:** Low
**Dependencies:** C1 (NSE ETF Master), C2 (Global ETF Expansion)
**Blocks:** None (non-blocking enrichment)

---

## Description

Create a one-time enrichment script that populates metadata fields in `de_etf_master` using yfinance `Ticker.info`. This is a best-effort enrichment -- missing data does not block prices, technicals, or RS calculations.

---

## Files to Create/Modify

| Action | File | Purpose |
|--------|------|---------|
| NEW | `scripts/ingest/etf_enrich.py` | yfinance Ticker.info enrichment for de_etf_master |

---

## Detailed Implementation Steps

### Step 1: Create `scripts/ingest/etf_enrich.py`

1. Accept CLI args:
   - `--exchange` (optional filter: "NSE", "NYSE", "NASDAQ", or "all" default)
   - `--tickers` (optional comma-separated override)
   - `--dry-run` (print what would be updated, don't write)

2. Query `de_etf_master` for target tickers:
   ```sql
   SELECT ticker, exchange FROM de_etf_master WHERE is_active = true
   ORDER BY ticker;
   ```

3. For each ticker:
   - Determine yfinance symbol:
     - If `exchange = 'NSE'`: append `.NS` suffix (e.g., `NIFTYBEES` -> `NIFTYBEES.NS`)
     - Otherwise: use ticker as-is
   - Call `yf.Ticker(yf_symbol).info`
   - Extract fields (handle missing/null gracefully):
     - `category` -> `info.get('category')`
     - `sector` -> `info.get('sector')` or `info.get('quoteType')` for ETFs
     - `expense_ratio` -> `info.get('annualReportExpenseRatio')` or `info.get('totalExpenseRatio')`
     - `inception_date` -> `info.get('fundInceptionDate')` (convert from Unix timestamp if present)
     - `currency` -> `info.get('currency')`
     - `long_name` -> `info.get('longName')`
   - UPDATE `de_etf_master`:
     ```sql
     UPDATE de_etf_master
     SET category = COALESCE(:category, category),
         expense_ratio = COALESCE(:expense_ratio, expense_ratio),
         updated_at = NOW()
     WHERE ticker = :ticker;
     ```
     (Only update fields where new value is not null -- preserve existing data)

4. Throttling and error handling:
   - Sleep 1 second between requests
   - On HTTP 429 (rate limit): exponential backoff starting at 5s, max 60s, 3 retries
   - On any other error: log warning, skip ticker, continue
   - Never crash the entire script on a single ticker failure

5. Summary logging:
   - Total tickers attempted
   - Tickers enriched (at least one field updated)
   - Tickers failed (error during .info call)
   - Per-field: count of tickers where that field was populated

---

## Key Considerations

- **NSE `.NS` suffix:** yfinance requires `.NS` for NSE-listed securities. The ticker in de_etf_master is stored without suffix (e.g., `NIFTYBEES`), so the script must append `.NS` before calling yfinance
- **yfinance `.info` is unreliable:** Many fields return None, especially for newer or less popular ETFs. This is expected and acceptable
- **Rate limiting:** yfinance has aggressive rate limits. 1 req/sec is conservative but safe for ~230 tickers (~4 minutes total)
- **Idempotent:** Running multiple times is safe -- COALESCE preserves existing non-null values, only fills gaps

---

## Acceptance Criteria

- [ ] `category` populated for 50%+ of tickers in de_etf_master
- [ ] `expense_ratio` populated for 40%+ of tickers
- [ ] Script handles `.NS` suffix for NSE tickers correctly
- [ ] No crashes on missing/null info fields
- [ ] Rate limiting works: 1 req/sec baseline, backoff on 429
- [ ] Script can be re-run safely (idempotent updates)
- [ ] Dry-run mode shows updates without writing
- [ ] ruff + mypy clean

---

## Verification Queries

```sql
-- Enrichment coverage by field
SELECT
  COUNT(*) AS total,
  COUNT(category) AS has_category,
  COUNT(expense_ratio) AS has_expense_ratio,
  ROUND(100.0 * COUNT(category) / COUNT(*), 1) AS category_pct,
  ROUND(100.0 * COUNT(expense_ratio) / COUNT(*), 1) AS expense_ratio_pct
FROM de_etf_master;

-- Coverage by exchange
SELECT exchange,
  COUNT(*) AS total,
  COUNT(category) AS has_category,
  COUNT(expense_ratio) AS has_expense_ratio
FROM de_etf_master
GROUP BY exchange;

-- Tickers with no enrichment at all
SELECT ticker, exchange FROM de_etf_master
WHERE category IS NULL AND expense_ratio IS NULL
ORDER BY exchange, ticker;
```
