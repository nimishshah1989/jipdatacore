# ETF-C6: Observatory Metadata Fix

**Parent Plan:** [etf-expansion-chunk-plan.md](../etf-expansion-chunk-plan.md)
**Complexity:** Low
**Dependencies:** None (independent, can run anytime)
**Blocks:** None

---

## Description

Fix the Observatory API's metadata for the `etf_ohlcv` data stream. Currently references phantom columns (`aum_cr`, `tracking_error`) that do not exist in `de_etf_ohlcv`. Update field descriptions to match the actual table schema.

---

## Files to Create/Modify

| Action | File | Purpose |
|--------|------|---------|
| MODIFY | `app/api/v1/observatory.py` | Fix etf_ohlcv stream field descriptions and data dictionary |

---

## Detailed Implementation Steps

### Step 1: Identify Current Incorrect Metadata

Locate the `etf_ohlcv` stream definition in `app/api/v1/observatory.py`. It currently lists fields that don't exist in the actual `de_etf_ohlcv` table, specifically:
- `aum_cr` -- does not exist in de_etf_ohlcv
- `tracking_error` -- does not exist in de_etf_ohlcv

### Step 2: Update Field List to Match Actual Schema

The actual `de_etf_ohlcv` columns are:

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Trading date |
| ticker | VARCHAR(30) | ETF ticker symbol (PK with date) |
| open | NUMERIC(18,4) | Opening price |
| high | NUMERIC(18,4) | Day high |
| low | NUMERIC(18,4) | Day low |
| close | NUMERIC(18,4) | Closing price |
| volume | BIGINT | Trading volume |
| created_at | TIMESTAMPTZ | Row creation timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

### Step 3: Update the Stream Definition

Replace the field descriptions in the observatory endpoint to match the actual columns above. Remove all references to `aum_cr`, `tracking_error`, or any other columns that don't exist in the table.

### Step 4: Update Data Dictionary Entry

If there is a separate data dictionary section or endpoint that describes `etf_ohlcv`, update it as well to reflect the correct column list.

---

## Acceptance Criteria

- [ ] Observatory API returns correct field descriptions for `etf_ohlcv` stream
- [ ] No references to `aum_cr` anywhere in the etf_ohlcv metadata
- [ ] No references to `tracking_error` anywhere in the etf_ohlcv metadata
- [ ] Field list matches actual de_etf_ohlcv table: date, ticker, open, high, low, close, volume
- [ ] ruff clean

---

## Verification Steps

```bash
# After change, hit the observatory API
curl http://localhost:8010/api/v1/observatory/streams/etf_ohlcv | python -m json.tool

# Verify response contains only valid fields
# Should see: date, ticker, open, high, low, close, volume
# Should NOT see: aum_cr, tracking_error
```

```bash
# Search for phantom field references in codebase
grep -r "aum_cr" app/api/v1/observatory.py
# Expected: no matches

grep -r "tracking_error" app/api/v1/observatory.py
# Expected: no matches
```
