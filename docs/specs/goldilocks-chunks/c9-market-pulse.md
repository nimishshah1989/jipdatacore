# C9: Market Pulse Combined Signal

**Complexity:** Medium
**Dependencies:** C6 (oscillators, divergences, pivots — de_oscillator_weekly, de_divergence_signals, de_index_pivots must be populated), C8 (goldilocks API data — de_goldilocks_market_view must be populated)
**Status:** pending

## Files
- app/services/market_pulse.py (new — combined signal service)
- app/api/v1/market.py (modify — add /pulse endpoint to existing market router)

## Context

### Existing market.py
`app/api/v1/market.py` already has a router with prefix `/market`. Inspect the file before modifying — identify the existing get_async_session dependency name, existing response model patterns, and existing endpoint list. Add the new /pulse endpoint without touching existing endpoints.

### Existing computation outputs (source tables)
- `de_market_regime` — market regime. Columns: date, regime (VARCHAR: 'BULL', 'CAUTIOUS', 'CORRECTION', 'BEAR'), regime_score (Numeric), or similar. Check `app/computation/regime.py` and `app/models/computed.py` for exact column names before building queries.
- `de_breadth_daily` — breadth. Columns: date, pct_above_200dma (Numeric), pct_above_50dma (Numeric), advance_decline_ratio (Numeric), composite_score (Numeric). Verify actual column names in models/computed.py.
- `de_rs_scores` — RS scores per instrument. Columns: instrument_id, date, rs_score, rs_rank_pct. For Nifty vs global benchmark: filter by instrument that represents NIFTY 50.
- `de_index_prices` — VIX data. The India VIX is stored here as index_name='INDIA VIX' (verify in data or check `app/pipelines/indices/vix.py`).
- `de_oscillator_weekly` — weekly stochastic (from C6).
- `de_divergence_signals` — divergence signals (from C6).
- `de_index_pivots` — daily pivot points (from C6).
- `de_goldilocks_market_view` — qualitative market view (from C5 extraction).
- `de_goldilocks_sector_view` — sector rankings (from C5 extraction).
- `de_goldilocks_stock_ideas` — active ideas for direction bias (from C5 extraction).

### Graceful degradation requirement
Market Pulse must return a valid response even when Goldilocks data is unavailable (tables empty, C5 not yet run). In that case: qual section is null, combined score falls back to quant-only, alignment = "QUANT_ONLY".

## What To Build

### market_pulse.py — Service Layer

All functions async, accept `AsyncSession`. All score computation with Decimal. Structlog logging.

---

**async def get_market_pulse(session: AsyncSession, target_date: date | None = None) -> MarketPulseData**

Returns a `MarketPulseData` dataclass or Pydantic model (define at top of file). If target_date is None, uses the most recent date with available data.

**Step 1 — Determine effective date**
```sql
SELECT MAX(date) FROM de_breadth_daily
```
Use this as the effective date if target_date is None. If the table is empty, fall back to today's date.

**Step 2 — Fetch quant signals (3 targeted queries)**

Query A — Regime:
```sql
SELECT regime, regime_score
FROM de_market_regime
WHERE date <= :target_date
ORDER BY date DESC LIMIT 1
```
If no row: regime_label = None, regime_score_raw = None.

Query B — Breadth:
```sql
SELECT pct_above_200dma, composite_score
FROM de_breadth_daily
WHERE date <= :target_date
ORDER BY date DESC LIMIT 1
```
Verify actual column names match `app/models/computed.py` before building this query.

Query C — RS (Nifty vs benchmark):
Fetch the RS score for NIFTY 50 instrument vs global benchmark from de_rs_scores. If instrument not found or no score: rs_score_raw = None.

**Step 3 — Fetch VIX percentile**
```sql
SELECT close AS vix_today,
  PERCENT_RANK() OVER (ORDER BY close) AS vix_percentile
FROM de_index_prices
WHERE index_name = 'INDIA VIX'
  AND date >= CURRENT_DATE - 365
  AND date <= :target_date
ORDER BY date DESC LIMIT 1
```
This is a window function — run as a CTE or subquery. vix_percentile is a float 0.0-1.0, multiply by 100 for display. If no VIX data: vix_percentile = None.

**Step 4 — Fetch Nifty stochastic (weekly)**
```sql
SELECT w.stochastic_k, w.stochastic_d
FROM de_oscillator_weekly w
JOIN de_instruments inst ON inst.id = w.instrument_id
WHERE inst.current_symbol = 'NIFTY 50'  -- verify actual symbol
  AND w.date <= :target_date
ORDER BY w.date DESC LIMIT 1
```
Stochastic zone classification:
- k < 20 → 'oversold'
- k > 80 → 'overbought'
- else → 'neutral'

**Step 5 — Fetch active divergences (weekly, strength >= 2)**
```sql
SELECT d.divergence_type, d.indicator, d.timeframe, d.strength,
  inst.current_symbol
FROM de_divergence_signals d
JOIN de_instruments inst ON inst.id = d.instrument_id
WHERE d.timeframe = 'weekly'
  AND d.strength >= 2
  AND d.date >= CURRENT_DATE - 14
ORDER BY d.strength DESC, d.date DESC
LIMIT 5
```
Return as list of dicts for the response.

**Step 6 — Fetch today's pivot levels for NIFTY 50**
```sql
SELECT s1, s2, r1, r2, pivot
FROM de_index_pivots
WHERE index_code = 'NIFTY50'
  AND date = :target_date
```
If no pivot for target_date (C6 not yet run, or holiday): return None for pivots section.

**Step 7 — Fetch Goldilocks qualitative signals (graceful degradation)**
```sql
SELECT trend_direction, trend_strength, headline, global_impact
FROM de_goldilocks_market_view
WHERE report_date <= :target_date
ORDER BY report_date DESC LIMIT 1
```
If no row: goldilocks_available = False, qual section = None.

```sql
SELECT sector, trend, rank
FROM de_goldilocks_sector_view
WHERE report_date = :goldilocks_date
ORDER BY rank NULLS LAST, sector
LIMIT 5
```

Active ideas direction bias:
```sql
SELECT
  COUNT(*) FILTER (WHERE technical_params->>'direction' = 'bullish' OR idea_type = 'big_catch') AS bullish_count,
  COUNT(*) FILTER (WHERE technical_params->>'direction' = 'bearish') AS bearish_count,
  COUNT(*) AS total_active
FROM de_goldilocks_stock_ideas
WHERE status = 'active'
```
Conviction direction: 'bullish' if bullish_count > bearish_count, 'bearish' if opposite, 'neutral' if equal or no ideas.

Note: `technical_params` is a JSONB column. The direction key may not exist — use `->>'direction'` which returns NULL if missing, and the FILTER silently handles NULLs.

**Step 8 — Compute scores (Python, Decimal arithmetic)**

Regime score mapping:
```python
REGIME_SCORES = {
    'BULL': Decimal('80'),
    'CAUTIOUS': Decimal('50'),
    'CORRECTION': Decimal('30'),
    'BEAR': Decimal('10'),
    None: Decimal('50'),  # default when unknown
}
regime_score = REGIME_SCORES.get(regime_label, Decimal('50'))
```

Breadth score: normalize pct_above_200dma (0-100 range, already a percentage).
```python
breadth_score = Decimal(str(pct_above_200dma)) if pct_above_200dma is not None else Decimal('50')
```

Sentiment score: use composite_score from de_breadth_daily if available, else 50.

RS score: normalize rs_score to 0-100 scale. Inspect de_rs_scores actual scale — it may already be 0-100 or may need normalization. Check rs.py computation. Default: 50 if unavailable.

Quant composite:
```python
quant_score = (
    regime_score * Decimal('0.30') +
    breadth_score * Decimal('0.30') +
    sentiment_score * Decimal('0.20') +
    rs_score * Decimal('0.20')
).quantize(Decimal('0.01'))
```

Qual score (only when goldilocks_available):
```python
DIRECTION_SCORES = {'upward': Decimal('80'), 'sideways': Decimal('50'), 'downward': Decimal('20'), None: Decimal('50')}
direction_score = DIRECTION_SCORES.get(trend_direction, Decimal('50'))
strength_score = Decimal(str(trend_strength)) * Decimal('20')  # 1-5 → 20-100

CONVICTION_SCORES = {'bullish': Decimal('80'), 'neutral': Decimal('50'), 'bearish': Decimal('20')}
conviction_score = CONVICTION_SCORES.get(conviction_direction, Decimal('50'))

qual_score = (
    direction_score * Decimal('0.40') +
    strength_score * Decimal('0.30') +
    conviction_score * Decimal('0.30')
).quantize(Decimal('0.01'))
```

Alignment logic:
```python
if not goldilocks_available:
    alignment = "QUANT_ONLY"
    combined_score = quant_score
else:
    diff = abs(quant_score - qual_score)
    alignment = "HIGH" if diff < 15 else "MODERATE" if diff < 30 else "DIVERGENT"
    combined_score = ((quant_score + qual_score) / Decimal('2')).quantize(Decimal('0.01'))

divergence_alert = alignment == "DIVERGENT"
```

**Step 9 — Top/avoid sectors**

From sector_views, take top 3 by rank (lowest rank = strongest):
- top_sectors: first 3 where trend in ('up', 'bullish', 'upward', 'positive')
- avoid_sectors: last 3 by rank (highest numbers) or where trend in ('down', 'bearish', 'negative')
- If no sector data: return empty lists.

**Step 10 — Build and return MarketPulseData**

---

**MarketPulseData (Pydantic v2 model, defined in market_pulse.py)**

```python
class QuantSignals(BaseModel):
    score: Decimal
    regime: str | None
    breadth: Decimal | None        # pct_above_200dma
    sentiment: Decimal | None
    rs_score: Decimal | None

class QualSignals(BaseModel):
    score: Decimal
    direction: str | None
    strength: int | None
    headline: str | None
    conviction: str | None  # 'bullish'/'neutral'/'bearish' from active ideas

class TechnicalSignals(BaseModel):
    nifty_stochastic_k: Decimal | None
    nifty_stochastic_d: Decimal | None
    nifty_stochastic_zone: str | None  # 'oversold'/'neutral'/'overbought'
    vix: Decimal | None
    vix_percentile: Decimal | None     # 0-100
    divergences: list[dict]
    pivot_s1: Decimal | None
    pivot_s2: Decimal | None
    pivot_r1: Decimal | None
    pivot_r2: Decimal | None
    pivot_pp: Decimal | None

class CombinedSignal(BaseModel):
    score: Decimal
    alignment: str  # 'HIGH'/'MODERATE'/'DIVERGENT'/'QUANT_ONLY'
    divergence_alert: bool

class MarketPulseData(BaseModel):
    date: date
    quant: QuantSignals
    qual: QualSignals | None    # None when Goldilocks data unavailable
    combined: CombinedSignal
    technicals: TechnicalSignals
    top_sectors: list[str]
    avoid_sectors: list[str]
    active_ideas: int
    goldilocks_available: bool
    data_as_of: datetime        # IST-aware timestamp of when this was assembled

    model_config = ConfigDict(json_encoders={Decimal: str})
```

---

### market.py modification

Add one endpoint to the existing `app/api/v1/market.py` router. Do not modify any other endpoint.

```python
@router.get("/pulse", response_model=MarketPulseData)
async def get_market_pulse_endpoint(
    report_date: Optional[date] = None,
    session: AsyncSession = Depends(get_async_session),
) -> MarketPulseData:
    from app.services.market_pulse import get_market_pulse
    return await get_market_pulse(session, target_date=report_date)
```

Lazy import inside the function body avoids circular import risk. The endpoint is accessible at GET /api/v1/market/pulse.

## Edge Cases

- **All quant tables empty:** Combined score = 50 (all defaults), alignment = "QUANT_ONLY", goldilocks_available = False. Do not raise — return a valid (if uninformative) pulse.
- **Goldilocks table empty:** qual = None, alignment = "QUANT_ONLY". Response still valid.
- **VIX percentile calculation:** If fewer than 30 rows of VIX history: PERCENT_RANK may produce inaccurate percentile. Still return the value with a note (do not suppress). The consumer decides how to display sparse data.
- **Breadth column names:** Check app/models/computed.py before writing SQL. Column may be `pct_above_200dma` or `above_200dma_pct` or similar. A mismatch here will cause a SQLAlchemy error at runtime.
- **Regime column names:** Same — verify `app/computation/regime.py` for the exact column name written to de_market_regime.
- **RS score scale:** Inspect `app/computation/rs.py` — determine if rs_score is already 0-100 or needs normalization before use in quant weighting. Document the normalization applied in a comment.
- **NIFTY 50 symbol in instruments:** The instrument representing NIFTY 50 for oscillator lookup may use a different current_symbol than "NIFTY 50". Cross-check with `de_oscillator_weekly` data in the DB before hardcoding the symbol string.
- **Stochastic not populated (C6 not run):** de_oscillator_weekly empty → nifty_stochastic = None. Pulse still returns.
- **Data freshness:** `data_as_of` must reflect actual data freshness. Use the latest date found in de_breadth_daily (or whichever table drives the date), not just "now()".

## Acceptance Criteria
- [ ] GET /api/v1/market/pulse returns valid response with all sections populated when all data available
- [ ] Quant score correctly weighted: regime 30%, breadth 30%, sentiment 20%, RS 20%
- [ ] Qual score computed from Goldilocks market view (direction 40%, strength 30%, conviction 30%)
- [ ] Combined score is average of quant + qual when both available
- [ ] Alignment "HIGH" when quant and qual differ by < 15 points
- [ ] Alignment "MODERATE" when 15-29 point difference
- [ ] Alignment "DIVERGENT" when >= 30 point difference, divergence_alert=True
- [ ] Alignment "QUANT_ONLY" when Goldilocks data unavailable, qual=None
- [ ] Nifty stochastic zone correctly classified (oversold/neutral/overbought)
- [ ] VIX percentile computed as 0-100
- [ ] Recent weekly divergences (strength >= 2, last 14 days) included in response
- [ ] Pivot levels for NIFTY50 included when de_index_pivots populated
- [ ] All Decimal values serialized as strings in JSON
- [ ] Endpoint handles completely empty DB gracefully (no 500 errors)
- [ ] No existing market.py endpoints broken by modification
- [ ] `ruff check . --select E,F,W` passes on both files
