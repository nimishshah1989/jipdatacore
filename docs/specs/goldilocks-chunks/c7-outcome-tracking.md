# C7: Outcome Tracking

**Complexity:** Medium
**Dependencies:** C1 (de_goldilocks_stock_ideas, de_qual_outcomes schema), C5 (Claude extraction must have populated de_goldilocks_stock_ideas with active ideas)
**Status:** pending

## Files
- app/computation/outcome_tracker.py (new)
- tests/computation/test_outcome_tracker.py (new)

## Context

### Source tables (from C1 schema)
- `de_goldilocks_stock_ideas` — one row per stock recommendation. Columns relevant here: id (UUID), published_date (DATE), symbol (VARCHAR 20), entry_price (Numeric 18,4), entry_zone_low (Numeric 18,4), entry_zone_high (Numeric 18,4), target_1 (Numeric 18,4), target_2 (Numeric 18,4), stop_loss (Numeric 18,4), timeframe (VARCHAR 50), status (VARCHAR 20 — 'active', 'target_1_hit', 'target_2_hit', 'sl_hit', 'expired', 'closed'), status_updated_at (TIMESTAMPTZ).
- `de_qual_outcomes` — outcome records. Columns: id (UUID PK), extract_id (UUID FK → de_qual_extracts.id, nullable), outcome_date (DATE), was_correct (BOOLEAN), actual_move_pct (Numeric 18,4), created_at (TIMESTAMPTZ). Check app/models/qualitative.py for exact column names before building queries — do not assume.
- `de_equity_ohlcv` — daily OHLCV. Columns: instrument_id (UUID FK), date (DATE), high (Numeric 18,4), low (Numeric 18,4), close (Numeric 18,4), data_status (VARCHAR).
- `de_instruments` — instrument master. Columns: id (UUID), current_symbol (VARCHAR), isin (VARCHAR). Use current_symbol to match idea.symbol.

### Outcome logic rationale
Outcome tracking uses intraday high/low (not just close) to detect if a target or stop was touched during the day. This matches how Goldilocks defines idea outcomes — a stop can be hit even if the stock closes above it.

## What To Build

### outcome_tracker.py

All functions async, accept `AsyncSession`. Use Decimal for all financial arithmetic. Log with structlog.

---

**async def track_goldilocks_outcomes(session: AsyncSession) -> dict**

Returns summary dict: `{"checked": int, "target_hits": int, "sl_hits": int, "expirations": int, "errors": int}`

**Step 1 — Fetch active ideas**
```sql
SELECT id, symbol, published_date, entry_price, entry_zone_low, entry_zone_high,
       target_1, target_2, stop_loss, timeframe, status
FROM de_goldilocks_stock_ideas
WHERE status IN ('active', 'target_1_hit')
ORDER BY published_date
```
Expected scale: < 50 rows at any time. Load into memory, iterate in Python.

**Step 2 — Resolve instrument_id for each symbol**
Batch lookup to avoid N+1:
```sql
SELECT current_symbol, id AS instrument_id
FROM de_instruments
WHERE current_symbol = ANY(:symbols)
```
Build a `{symbol: instrument_id}` dict. If symbol not found: log warning, skip idea (do not fail batch).

**Step 3 — For each active idea, fetch price data**
Use a single batched query for all active instruments + their metrics since idea published_date:
```sql
SELECT
  instrument_id,
  MAX(high) AS max_high,
  MIN(low)  AS min_low,
  (array_agg(close ORDER BY date DESC))[1] AS latest_close,
  MAX(date) AS latest_date
FROM de_equity_ohlcv
WHERE instrument_id = ANY(:instrument_ids)
  AND date >= :min_published_date
  AND data_status = 'validated'
GROUP BY instrument_id
```
This is one query for all instruments, not one per idea. Map results back by instrument_id.

**Step 4 — Check outcomes per idea**

Determine effective entry price for P&L calculation:
```python
entry = idea.entry_price or (
    (idea.entry_zone_low + idea.entry_zone_high) / Decimal('2')
    if idea.entry_zone_low and idea.entry_zone_high
    else None
)
```
If entry is None: log warning, skip P&L calculation (can still track target/SL hits).

Outcome checks (evaluate in priority order):

**A. Target 2 hit (only if already target_1_hit):**
```python
if idea.status == 'target_1_hit' and idea.target_2 and metrics.max_high >= idea.target_2:
    new_status = 'target_2_hit'
    was_correct = True
```

**B. Target 1 hit (only if active):**
```python
if idea.status == 'active' and idea.target_1 and metrics.max_high >= idea.target_1:
    new_status = 'target_1_hit'
    was_correct = True
```

**C. Stop loss hit:**
```python
if idea.stop_loss and metrics.min_low <= idea.stop_loss:
    new_status = 'sl_hit'
    was_correct = False
```
If both target and SL hit since published: whichever date came first wins. Requires per-date price check when ambiguous (see edge cases below).

**D. Expiration:**
```python
timeframe_days = parse_timeframe(idea.timeframe)
expired = (today - idea.published_date).days > timeframe_days
if expired and idea.status == 'active':
    new_status = 'expired'
    was_correct = None  # NULL — ambiguous, not a clear win or loss
```

**Step 5 — Persist status change**
For each status change:
1. UPDATE de_goldilocks_stock_ideas SET status=:new_status, status_updated_at=NOW(), updated_at=NOW() WHERE id=:idea_id
2. Calculate actual_move_pct:
   ```python
   if entry and metrics.latest_close:
       actual_move_pct = ((metrics.latest_close - entry) / entry) * Decimal('100')
   else:
       actual_move_pct = None
   ```
3. INSERT into de_qual_outcomes:
   - extract_id = NULL (ideas are not in de_qual_extracts, they are in de_goldilocks_stock_ideas)
   - outcome_date = TODAY (date the outcome was detected, not the price-touch date)
   - was_correct = True/False/None
   - actual_move_pct = computed above
   - Only insert if no existing outcome row for this idea on this date (idempotent check: skip if already inserted today)

**Step 6 — Return summary dict**

---

**async def get_goldilocks_scorecard(session: AsyncSession) -> dict**

Returns:
```python
{
    "total_ideas": int,
    "active": int,
    "target_hit": int,       # status IN ('target_1_hit', 'target_2_hit')
    "sl_hit": int,
    "expired": int,
    "closed": int,
    "hit_rate": Decimal,     # target_hit / (target_hit + sl_hit), NULL if denominator is 0
    "avg_winner_pct": Decimal,   # mean actual_move_pct where was_correct=True
    "avg_loser_pct": Decimal,    # mean actual_move_pct where was_correct=False
    "win_loss_ratio": Decimal,   # abs(avg_winner / avg_loser), NULL if avg_loser is 0
    "by_type": {
        "stock_bullet": {"total": int, "target_hit": int, "sl_hit": int, "hit_rate": Decimal},
        "big_catch": {"total": int, "target_hit": int, "sl_hit": int, "hit_rate": Decimal},
    },
    "by_sector": {
        # Sector is not a column on de_goldilocks_stock_ideas by default.
        # Use technical_params JSONB field if sector key present, else omit by_sector.
        # If no sector data available: return empty dict.
        "Metals": {"total": int, "target_hit": int, "hit_rate": Decimal},
        ...
    },
    "data_as_of": str,  # IST timestamp of latest status_updated_at
}
```

Implementation: use SQL aggregation, not Python loops:
```sql
SELECT
  COUNT(*) AS total_ideas,
  COUNT(*) FILTER (WHERE status = 'active') AS active,
  COUNT(*) FILTER (WHERE status IN ('target_1_hit', 'target_2_hit')) AS target_hit,
  COUNT(*) FILTER (WHERE status = 'sl_hit') AS sl_hit,
  COUNT(*) FILTER (WHERE status = 'expired') AS expired,
  COUNT(*) FILTER (WHERE status = 'closed') AS closed,
  MAX(status_updated_at) AS data_as_of
FROM de_goldilocks_stock_ideas
```

For avg_winner_pct and avg_loser_pct: join de_qual_outcomes (one row per idea). Use ROUND(..., 4) for Decimal precision.

hit_rate: compute in Python from the counts above. Guard: if (target_hit + sl_hit) == 0 → hit_rate = None.

---

**Helper: parse_timeframe(timeframe: str) -> int**

Converts textual timeframe to number of days (use the maximum of any range):
- "1-2 weeks" → 14
- "2-6 weeks" → 42
- "1-3 months" → 91
- "3-6 months" → 182
- "3 months" → 91
- "6 months" → 182
- "6-12 months" → 365
- "12-18 months" → 547
- "1 year" → 365
- "long term" → 730 (2 years — conservative)
- If no match: log warning, return 365 as default

Implementation: normalize to lower case, strip whitespace, regex match for patterns `(\d+)\s*-\s*(\d+)\s*(week|month|year)` and `(\d+)\s*(week|month|year)`. Convert unit to days (week=7, month=30, year=365). Take the larger number of any range. Do not raise on unrecognized input — return default.

## Edge Cases

- **Symbol not in de_instruments:** Log warning with idea.id and symbol. Skip this idea, continue batch. Do not fail.
- **Both target and SL hit in history:** If max_high >= target_1 AND min_low <= stop_loss, the aggregate query cannot determine which came first. Resolve: run a secondary query fetching daily OHLCV rows since published_date, find the first date where high >= target_1 and the first date where low <= stop_loss, use the earlier date's outcome. Only trigger this secondary query when the ambiguous condition is detected.
- **NULL entry_price and NULL entry_zone:** actual_move_pct = NULL. Status update still happens (can still detect target/SL via price levels).
- **NULL target_1:** Cannot check for target hit. Log warning, skip target check, still check SL and expiry.
- **NULL stop_loss:** Cannot check SL. Log warning, skip SL check, still check target and expiry.
- **Idea with no OHLCV data:** instrument_id found but no matching rows in de_equity_ohlcv (new listing, data not yet loaded). Skip outcome check for this idea, log warning.
- **Idempotency:** Running track_goldilocks_outcomes twice on the same day must not double-insert de_qual_outcomes rows or double-update statuses. Check: if current status already equals the new_status, skip the UPDATE.
- **Decimal rounding:** actual_move_pct rounded to 4 decimal places before insert.

## Acceptance Criteria
- [ ] Active ideas fetched in a single query (no N+1)
- [ ] Instrument symbol resolution batched (one query for all symbols, not per-idea)
- [ ] Price metrics (max_high, min_low, latest_close) fetched in one batched query for all active instruments
- [ ] Target 1 hit correctly detected using max(high) >= target_1
- [ ] Target 2 hit only progresses ideas already in target_1_hit status
- [ ] Stop loss hit correctly detected using min(low) <= stop_loss
- [ ] Ambiguous target+SL resolved via per-date query (secondary lookup only when needed)
- [ ] Expired ideas marked after parse_timeframe(timeframe) days elapsed
- [ ] de_qual_outcomes inserted with was_correct and actual_move_pct for each status change
- [ ] Running twice on same day is idempotent (no duplicate outcomes, no status regression)
- [ ] get_goldilocks_scorecard uses SQL aggregation for counts, not Python loops
- [ ] hit_rate = None when no resolved ideas (no division-by-zero error)
- [ ] parse_timeframe handles all listed patterns plus unknown-input default
- [ ] Tests: mock OHLCV data to verify target hit, SL hit, expiry, and ambiguous case
- [ ] `pytest tests/ -v --tb=short` passes
- [ ] `ruff check . --select E,F,W` passes

## Tests (tests/computation/test_outcome_tracker.py)

Required test scenarios:

**test_parse_timeframe_weeks:** "2-6 weeks" → 42, "1-2 weeks" → 14

**test_parse_timeframe_months:** "3-6 months" → 182, "12-18 months" → 547

**test_parse_timeframe_unknown:** "soon" → 365 (default, no exception)

**test_target_hit_detection:** Given idea with target_1=100, max_high=102 → new_status='target_1_hit', was_correct=True

**test_sl_hit_detection:** Given idea with stop_loss=80, min_low=78 → new_status='sl_hit', was_correct=False

**test_no_change_when_neither_hit:** max_high=95, min_low=85, target_1=100, stop_loss=80 → no status change

**test_expiry_detection:** published_date = today - 400 days, timeframe = "12-18 months" (547 days) → NOT expired. published_date = today - 600 days → expired.

**test_idempotency:** Call track logic with same inputs twice, verify de_qual_outcomes has only one row.

**test_scorecard_hit_rate:** 3 target_hit, 1 sl_hit → hit_rate = Decimal('0.7500')

**test_scorecard_no_resolved_ideas:** 5 active, 0 resolved → hit_rate = None, no ZeroDivisionError

Use pytest with async fixtures. Mock the database session using `unittest.mock.AsyncMock` for session.execute. Do not call the real database in unit tests.
