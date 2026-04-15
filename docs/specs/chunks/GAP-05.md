# GAP-05 — Alembic migration 011: multi-year risk columns

## Goal
Add 3-year and 5-year window variants for every risk metric column on all 5
v2 technical tables. Current state: only 1-year variants exist. Atlas needs
1/3/5y for long-term risk screening.

## Scope
- Create `alembic/versions/011_multi_year_risk_columns.py`
- ADD columns to: `de_equity_technical_daily`, `de_etf_technical_daily`,
  `de_global_technical_daily`, `de_index_technical_daily`, `de_mf_technical_daily`
- Columns to add (all `Numeric(10,4)` unless noted):
    sharpe_3y, sharpe_5y
    sortino_3y, sortino_5y
    calmar_3y, calmar_5y
    max_drawdown_3y, max_drawdown_5y
    volatility_3y, volatility_5y (annualized)
    beta_3y, beta_5y
    information_ratio_3y, information_ratio_5y
    treynor_1y, treynor_3y, treynor_5y
    downside_risk_1y, downside_risk_3y, downside_risk_5y
- Update SQLAlchemy models in `app/models/indicators_v2.py` to match
- Update migration's downgrade() to drop columns
- Apply migration to production RDS via raw-SQL extract pattern (alembic drift)

## Acceptance criteria
- [ ] Migration file exists and is syntactically valid
- [ ] `information_schema.columns` query shows all new columns on all 5 tables
- [ ] SQLAlchemy models import without error
- [ ] `pytest tests/computation/test_indicators_v2_engine.py` still green
- [ ] Commit subject starts with `GAP-05`
- [ ] `state.db` shows `GAP-05` with `status='DONE'`

## Steps for the inner session
1. Read `alembic/versions/008_indicators_v2_tables.py` + `010_rename_v2_columns_to_v1_names.py` for the existing column patterns
2. Write migration 011 using raw `op.execute("ALTER TABLE ... ADD COLUMN ...")` statements
3. Update `app/models/indicators_v2.py` — add mapped_column for each new column on each model class
4. Extract raw SQL from migration, apply to prod via EC2+psql
5. Verify columns exist in prod
6. Run test suite
7. Commit

## Out of scope
- Computing values for the new columns (GAP-06 + GAP-07)
- Column renames
- Dropping any existing columns

## Dependencies
- Upstream: none
- Downstream: GAP-06, GAP-07, GAP-10
