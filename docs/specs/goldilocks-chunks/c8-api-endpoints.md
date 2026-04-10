# C8: Goldilocks API Endpoints

**Complexity:** Medium
**Dependencies:** C1 (models + schema), C5 (Claude extraction must have populated the Goldilocks tables with real data for integration testing)
**Status:** pending

## Files
- app/api/v1/goldilocks.py (new)
- app/api/v1/__init__.py (modify — add goldilocks router to all_routers list and __all__)

## Context

### Existing API patterns (replicate, do not deviate)
- All routers in `app/api/v1/` use `APIRouter` with prefix and tags.
- Sessions injected via `Depends(get_async_session)` — check `app/db/` for the exact dependency name used in existing routers (e.g. `equity.py` or `market.py`).
- All query params: `Optional[type] = None` with explicit defaults. Never bare required params.
- Error responses: `HTTPException` with specific status codes.
- Pydantic v2 response models for all endpoints.
- All money/level values: serialized as strings in JSON (use `mode='json'` or `model_config` with `json_encoders` for Decimal). Decimal must never appear as a float in the API response.
- Structlog for logging, never print().
- Return type annotations on every function.

### Existing __init__.py pattern
Current `app/api/v1/__init__.py` imports each router as `from app.api.v1.X import router as X_router` and adds to `all_routers` list and `__all__`. Follow the exact same pattern for goldilocks_router.

### Models referenced
From `app/models/goldilocks.py` (created in C1):
- `DeGoldilocksMarketView`
- `DeGoldilocksSectorView`
- `DeGoldilocksStockIdeas`
- `DeDivergenceSignals`

From `app/models/qualitative.py`:
- `DeQualOutcomes` (for scorecard)

## What To Build

### Pydantic v2 Response Schemas

Define all schemas at the top of `goldilocks.py` before the router.

```python
class SectorViewItem(BaseModel):
    sector: str
    trend: str | None
    outlook: str | None
    rank: int | None
    top_picks: list[dict] | None  # [{"symbol": "RELIANCE", "resistance_levels": [...]}]

class MarketViewResponse(BaseModel):
    report_date: date
    nifty_close: Decimal | None
    nifty_support_1: Decimal | None
    nifty_support_2: Decimal | None
    nifty_resistance_1: Decimal | None
    nifty_resistance_2: Decimal | None
    bank_nifty_close: Decimal | None
    bank_nifty_support_1: Decimal | None
    bank_nifty_support_2: Decimal | None
    bank_nifty_resistance_1: Decimal | None
    bank_nifty_resistance_2: Decimal | None
    trend_direction: str | None
    trend_strength: int | None
    headline: str | None
    overall_view: str | None
    global_impact: str | None
    sectors: list[SectorViewItem]

    model_config = ConfigDict(json_encoders={Decimal: str})
```

```python
class StockIdeaResponse(BaseModel):
    id: UUID
    published_date: date
    symbol: str
    company_name: str | None
    idea_type: str
    entry_price: Decimal | None
    entry_zone_low: Decimal | None
    entry_zone_high: Decimal | None
    target_1: Decimal | None
    target_2: Decimal | None
    lt_target: Decimal | None
    stop_loss: Decimal | None
    timeframe: str | None
    rationale: str | None
    status: str
    # Enriched fields (populated from de_equity_ohlcv join):
    current_price: Decimal | None       # latest close
    unrealized_pnl_pct: Decimal | None  # ((current_price - entry) / entry) * 100

    model_config = ConfigDict(json_encoders={Decimal: str})
```

```python
class DivergenceSignalResponse(BaseModel):
    id: UUID
    date: date
    instrument_id: UUID
    symbol: str | None          # joined from de_instruments
    company_name: str | None    # joined from de_instruments
    current_price: Decimal | None  # latest close from de_equity_technical_daily or ohlcv
    timeframe: str
    divergence_type: str
    indicator: str
    strength: int

    model_config = ConfigDict(json_encoders={Decimal: str})
```

```python
class ScorecardResponse(BaseModel):
    total_ideas: int
    active: int
    target_hit: int
    sl_hit: int
    expired: int
    closed: int
    hit_rate: Decimal | None
    avg_winner_pct: Decimal | None
    avg_loser_pct: Decimal | None
    win_loss_ratio: Decimal | None
    by_type: dict   # {"stock_bullet": {...}, "big_catch": {...}}
    by_sector: dict # {"Metals": {...}, ...} — may be empty dict if no sector data
    data_as_of: datetime | None

    model_config = ConfigDict(json_encoders={Decimal: str})
```

---

### Router: app/api/v1/goldilocks.py

```python
router = APIRouter(prefix="/goldilocks", tags=["goldilocks"])
```

---

**1. GET /api/v1/goldilocks/market-view**

```python
@router.get("/market-view", response_model=MarketViewResponse)
async def get_market_view(
    report_date: Optional[date] = None,
    session: AsyncSession = Depends(get_async_session),
) -> MarketViewResponse:
```

Logic:
1. If report_date is None: query latest available date:
   ```sql
   SELECT MAX(report_date) FROM de_goldilocks_market_view
   ```
   If result is NULL (no data at all): raise HTTPException(404, "No market view data available")
2. Fetch market view row:
   ```sql
   SELECT * FROM de_goldilocks_market_view WHERE report_date = :report_date
   ```
   If not found: raise HTTPException(404, detail=f"No market view for date {report_date}")
3. Fetch sector views for the same report_date:
   ```sql
   SELECT * FROM de_goldilocks_sector_view
   WHERE report_date = :report_date
   ORDER BY rank NULLS LAST, sector
   ```
4. Construct and return MarketViewResponse with sectors list populated.

---

**2. GET /api/v1/goldilocks/sector-views**

```python
@router.get("/sector-views", response_model=list[SectorViewItem])
async def get_sector_views(
    report_date: Optional[date] = None,
    sector: Optional[str] = None,
    session: AsyncSession = Depends(get_async_session),
) -> list[SectorViewItem]:
```

Logic:
1. If report_date is None: use latest available date from de_goldilocks_sector_view.
2. Build query:
   ```sql
   SELECT * FROM de_goldilocks_sector_view
   WHERE report_date = :report_date
   ```
   If sector param provided: add `AND LOWER(sector) = LOWER(:sector)`.
3. If no rows found: return empty list (not 404).
4. Order by rank NULLS LAST, sector.
5. Return list of SectorViewItem.

Note: The spec mentions "historical ranks" when sector is specified. For C8, return only the current report_date rows. Historical trend is a future enhancement — do not over-build.

---

**3. GET /api/v1/goldilocks/stock-ideas**

```python
@router.get("/stock-ideas", response_model=list[StockIdeaResponse])
async def get_stock_ideas(
    status: Optional[str] = "active",
    idea_type: Optional[str] = None,
    session: AsyncSession = Depends(get_async_session),
) -> list[StockIdeaResponse]:
```

Logic:
1. Validate status param: must be one of ('active', 'target_1_hit', 'target_2_hit', 'sl_hit', 'expired', 'closed', 'all'). If not: raise HTTPException(400, "Invalid status value").
2. Build base query:
   ```sql
   SELECT i.*,
     (SELECT close FROM de_equity_ohlcv o
      JOIN de_instruments inst ON inst.id = o.instrument_id
      WHERE inst.current_symbol = i.symbol
        AND o.data_status = 'validated'
      ORDER BY o.date DESC LIMIT 1) AS current_price
   FROM de_goldilocks_stock_ideas i
   WHERE 1=1
   ```
   If status != 'all': add `AND i.status = :status`
   If idea_type provided: add `AND i.idea_type = :idea_type`
   Order by: published_date DESC
3. Compute unrealized_pnl_pct in Python after fetching (scale is small — < 50 ideas):
   ```python
   entry = idea.entry_price or midpoint(idea.entry_zone_low, idea.entry_zone_high)
   if entry and current_price:
       pnl = ((current_price - entry) / entry) * Decimal('100')
   else:
       pnl = None
   ```
4. Return list of StockIdeaResponse. Empty list is valid (not 404).

---

**4. GET /api/v1/goldilocks/scorecard**

```python
@router.get("/scorecard", response_model=ScorecardResponse)
async def get_scorecard(
    session: AsyncSession = Depends(get_async_session),
) -> ScorecardResponse:
```

Logic:
1. Import and call `get_goldilocks_scorecard(session)` from `app.computation.outcome_tracker`.
2. The scorecard function (built in C7) handles all the SQL aggregation.
3. Wrap result in ScorecardResponse and return.
4. If outcome_tracker raises an unexpected error: let it propagate as 500 (do not swallow).

---

**5. GET /api/v1/goldilocks/divergences**

```python
@router.get("/divergences", response_model=list[DivergenceSignalResponse])
async def get_divergences(
    timeframe: Optional[str] = "weekly",
    min_strength: Optional[int] = 1,
    lookback_days: Optional[int] = 30,
    session: AsyncSession = Depends(get_async_session),
) -> list[DivergenceSignalResponse]:
```

Logic:
1. Validate timeframe: must be 'daily', 'weekly', or 'monthly'. If not: raise HTTPException(400).
2. Validate min_strength: must be 1, 2, or 3. If not: raise HTTPException(400).
3. Validate lookback_days: clamp to max 90 to avoid large result sets.
4. Query:
   ```sql
   SELECT d.*,
     inst.current_symbol AS symbol,
     inst.company_name,
     (SELECT close FROM de_equity_ohlcv o
      WHERE o.instrument_id = d.instrument_id
        AND o.data_status = 'validated'
      ORDER BY o.date DESC LIMIT 1) AS current_price
   FROM de_divergence_signals d
   JOIN de_instruments inst ON inst.id = d.instrument_id
   WHERE d.timeframe = :timeframe
     AND d.strength >= :min_strength
     AND d.date >= CURRENT_DATE - :lookback_days
   ORDER BY d.strength DESC, d.date DESC
   LIMIT 100
   ```
5. Return list of DivergenceSignalResponse. Empty list if no signals.

Note: Check actual column names on `de_instruments` — `company_name` may not exist. Look in `app/models/instruments.py` before building the query. Use whatever the actual company name column is, or omit it if not present.

---

### __init__.py modification

In `app/api/v1/__init__.py`:

1. Add import:
   ```python
   from app.api.v1.goldilocks import router as goldilocks_router
   ```
2. Add to `all_routers` list (position: after `qualitative_router`).
3. Add `"goldilocks_router"` to `__all__`.

Do not change any other entries in __init__.py.

## Edge Cases

- **No data yet:** All 5 endpoints must handle empty tables gracefully. Market-view and scorecard return 404 when no data. Stock ideas and divergences return empty list [].
- **Decimal serialization:** Pydantic v2 + FastAPI may serialize Decimal as float by default. Use `json_encoders={Decimal: str}` in model_config OR use `Annotated[Decimal, PlainSerializer(str)]`. Test that the JSON response contains "22679.5000" (string), not 22679.5 (float).
- **current_price subquery N+1:** The subquery for current_price in stock-ideas is a correlated subquery — acceptable at < 50 ideas scale. If this becomes a performance issue, replace with a lateral join or a separate batch fetch. Document this tradeoff in a comment.
- **NULL entry price:** unrealized_pnl_pct = None. StockIdeaResponse allows None for this field.
- **Invalid status param:** Return 400 with clear message listing valid values.
- **de_instruments company_name column:** Verify actual column name in app/models/instruments.py before using in query. The model may use a different field name.

## Acceptance Criteria
- [ ] All 5 endpoints return HTTP 200 with correct schemas when data exists
- [ ] GET /market-view with no date param returns latest available date's data
- [ ] GET /market-view with explicit date returns 404 when date not found
- [ ] GET /sector-views returns empty list when no data (not 404)
- [ ] GET /stock-ideas returns current_price and unrealized_pnl_pct for active ideas
- [ ] GET /stock-ideas?status=all returns all ideas regardless of status
- [ ] GET /scorecard calls outcome_tracker.get_goldilocks_scorecard
- [ ] GET /divergences filters by timeframe and min_strength correctly
- [ ] All Decimal values serialized as strings in JSON response (not floats)
- [ ] goldilocks_router registered in all_routers in __init__.py
- [ ] Endpoints accessible at /api/v1/goldilocks/* prefix
- [ ] Invalid params return 400, not 500
- [ ] `ruff check . --select E,F,W` passes on goldilocks.py and modified __init__.py
