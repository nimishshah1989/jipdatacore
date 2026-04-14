# Chunk 3 — Engine core

**Complexity**: L
**Blocks**: chunk-4, chunk-5, chunk-7, chunk-8, chunk-10
**Blocked by**: chunk-2

## Goal
Build the generic indicator computation engine: an `AssetSpec` dataclass, a `compute_indicators` entry point that loads OHLCV → runs pandas-ta strategy → computes risk metrics → upserts to the output table. Engine is asset-class-agnostic; per-asset wrappers (equity/etf/index/mf/global_) come in later chunks.

## Files
- **Create**: `app/computation/indicators_v2/spec.py`
  - `@dataclass class AssetSpec` with fields: `source_model`, `output_model`, `id_column`, `date_column`, `close_col`, `open_col`, `high_col`, `low_col`, `volume_col` (Optional), `min_history_days=250`, `asset_class_name`
- **Create**: `app/computation/indicators_v2/strategy.yaml`
  - Full catalog of indicators: each entry has `{name, kind, params, output_columns, applies_to: [equity, etf, index, mf, global]}`
  - Indicators that require volume (OBV, CMF, MFI, VWAP, AD, ADOSC, EFI, EOM, KVO, PVT) marked `requires_volume: true` — skipped for assets without volume (MFs)
- **Create**: `app/computation/indicators_v2/strategy_loader.py`
  - `load_strategy_for_asset(asset_class: str) -> pandas_ta.Strategy` — reads yaml, filters by `applies_to`, returns ready strategy object. Cached.
- **Create**: `app/computation/indicators_v2/risk_metrics.py`
  - `compute_risk_metrics(returns: pd.Series, benchmark_returns: pd.Series, as_of_date: date) -> dict[str, Decimal]`
  - Uses empyrical: `sharpe_ratio`, `sortino_ratio`, `calmar_ratio`, `max_drawdown`, `beta`, `alpha`, `omega_ratio`, `information_ratio`
  - 1-year window (trailing 252 trading days) ending at `as_of_date`
  - Returns dict with keys prefixed `risk_` matching schema column names
  - Returns `None` if < 252 days of history
- **Create**: `app/computation/indicators_v2/engine.py`
  - `async def compute_indicators(spec: AssetSpec, session: AsyncSession, instrument_ids: list, from_date: date, to_date: date, batch_size: int = 200)`
  - For each instrument:
    1. Load OHLCV as pandas DataFrame via `session.execute(select(spec.source_model)...)`. Dates ASC.
    2. Skip if < `spec.min_history_days` rows
    3. Run `df.ta.strategy(strategy)` — pandas-ta mutates df in place adding ~130 columns
    4. Compute returns from close column, compute risk metrics per row
    5. Quantize all float columns to `Decimal("0.0001")` at boundary via helper `_to_decimal_row(row)`
    6. Filter to `[from_date, to_date]` window
    7. Bulk upsert in batches of 200 via `pg_insert(spec.output_model).on_conflict_do_update(index_elements=[spec.date_column, spec.id_column], set_={...all non-PK cols...})`
  - Returns `CompResult` with counts: `instruments_processed`, `rows_written`, `skipped_insufficient_history`, `errors`
- **Create**: `tests/computation/test_indicators_v2_engine.py`
  - Unit tests with synthetic OHLCV data (LCG seed=42, 500 rows)
  - Test: engine produces all expected columns
  - Test: Decimal quantization exact
  - Test: NaN warmup handling (first 200 rows of SMA_200 are NaN, row 201 is first non-NaN)
  - Test: insufficient history skips cleanly
  - Test: upsert uses ON CONFLICT correctly (run twice, verify idempotent)

## Risk metric calculation detail
```python
from empyrical import sharpe_ratio, sortino_ratio, calmar_ratio, max_drawdown, beta, alpha, omega_ratio, information_ratio

def compute_risk_metrics(returns, benchmark_returns, as_of_date):
    window_returns = returns.loc[:as_of_date].tail(252)
    window_bench = benchmark_returns.loc[:as_of_date].tail(252)
    if len(window_returns) < 252:
        return None
    return {
        "risk_sharpe_1y": Decimal(str(round(sharpe_ratio(window_returns, annualization=252), 4))),
        # ... etc
    }
```

## Acceptance criteria
- `pytest tests/computation/test_indicators_v2_engine.py -v` all green
- Engine produces exactly the columns listed in the chunk-2 schema (no extras, no missing)
- Idempotent: running twice on same date range produces same row count
- `ruff check`, `mypy` clean
- No float values reach the database — everything Decimal or NULL

## Verification commands
```bash
pytest tests/computation/test_indicators_v2_engine.py -v --tb=short
ruff check app/computation/indicators_v2/ --select E,F,W
mypy app/computation/indicators_v2/ --ignore-missing-imports
```
