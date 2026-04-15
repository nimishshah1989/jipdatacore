# GAP-12 — Global ETF universe expansion (existing sources first)

## Goal
Expand the `de_global_instrument_master` ETF coverage from 83 to 100+ by
adding the top globally-traded ETFs not currently in the universe, and
backfill their OHLCV + technicals. **Check sister project DBs for
existing data before scraping yfinance.**

## Mandatory step 0: inventory existing sources

```bash
# fie2 (has compass_etf_prices table)
ssh ubuntu@13.206.34.214 "set -a; source /home/ubuntu/fie2/.env; set +a; \
  psql \$DATABASE_URL -c \"\\d compass_etf_prices\" -c \
  \"SELECT COUNT(DISTINCT symbol), COUNT(*), MIN(date), MAX(date) FROM compass_etf_prices\""
```

If fie2 has global ETF coverage we need, ingest from there instead of
yfinance. yfinance is a fallback, not primary.

## Scope
- Reference list: top 20+ global ETFs by AUM/volume not already in
  `de_global_instrument_master`. Candidates: QQQ, IVV, VTI, VEA, VWO,
  IEFA, AGG, BND, VOO, VIG, VGT, SOXX, XLE, XLF, XLK, XLV, XLU, XLP,
  XLY, XLI, XLB, XLRE, ARKK, EFA, EEM, TLT, IEF, HYG, LQD, SLV, GLD, USO
- Filter out ones already present: `SELECT ticker FROM de_global_instrument_master
  WHERE instrument_type='etf'`
- INSERT new rows into `de_global_instrument_master` with metadata
- For each new ticker, check fie2/compass_etf_prices first; fall back to yfinance
- Run `scripts/backfill_indicators_v2.py --asset global` for new tickers

## Acceptance criteria
- [ ] ≥ 17 new ETFs added to `de_global_instrument_master`
- [ ] Inventory report `reports/etf_source_inventory_<date>.md` documenting
  which ETFs came from fie2 vs yfinance
- [ ] OHLCV data from at least 2016 present for all new tickers
- [ ] Technical indicators populated for new tickers
- [ ] Commit subject starts with `GAP-12`

## Out of scope
- Non-ETF global instruments
- Creating a new ingestion path (reuse existing global_prices pipeline)
- Ranking logic (use static top-100 list)

## Dependencies
- Upstream: none
- Downstream: none
