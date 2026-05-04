# Data Core Readiness — Atlas-M0

> **STATUS: TEMPLATE / awaiting live-DB execution.** This file is regenerated
> by `scripts/atlas_m0_readiness.py` after Job 1 + Job 2 runs against the
> live RDS. The structure below documents what the runtime report contains
> and acts as the architect-sign-off scaffold.

Generated: _(populated by atlas_m0_readiness.py)_
Target date (T-1): _(populated by atlas_m0_readiness.py)_
Final call: **(GO / REVIEW / NO-GO — populated by readiness script)**

## 1. Job 1 — Gap Fill

### 1.1 Stocks (PARTIAL backfill)

- Active+tradeable instruments: _N_
- Meeting >=252 trading days before 2014-04-01: _M_ (_pct %_)
- DoD threshold: >=95 % — PASS / FAIL

Per-instrument detail in `reports/atlas_m0_gap_report.json`. Items flagged
`unfillable_pre_listing=true` (instruments listed after 2014-04-01) are
excluded from the 95 % threshold.

### 1.2 MFs (NAV current to T-1)

- Eligible MFs (Equity / Regular / Growth / non-IDCW / non-Direct): _N_
- Current to T-1: _M_ (_pct %_)
- DoD threshold: >=95 % — PASS / FAIL

### 1.3 International (INTL_SPX, INTL_MSCIWORLD)

| Ticker          | Earliest | Latest | Row count | Source                      |
|-----------------|----------|--------|-----------|-----------------------------|
| INTL_SPX        |  ...     |  ...   |  ...      | Stooq spx.us.txt            |
| INTL_MSCIWORLD  |  ...     |  ...   |  ...      | Stooq urth.us.txt (URTH ETF)|

INTL DoD: 2/2 populated for 2011-04-01..T-1 — PASS / FAIL

**Job 1 verdict: _filled at runtime_**

## 2. Job 2 — de_etf_holdings

- Table created: yes (migration `019_de_etf_holdings`).
- Model: `app/models/holdings.py::DeEtfHoldings`.
- Pipeline: `app/pipelines/morningstar/etf_holdings.py::EtfHoldingsPipeline`
  — fetches per-ticker holdings via Morningstar Direct, resolves ISINs to
  `de_instrument.id`, upserts into `de_etf_holdings` with conflict on
  `(ticker, instrument_id, as_of_date)`.

Coverage thresholds (DoD):
- ETFs in active `de_etf_master` universe: _N_
- Distinct ETFs with at least one holdings row: _M_
- Total holdings rows: _R_
- DoD threshold: >=80 distinct ETFs — PASS / FAIL

**Caveats to verify on first Morningstar ETF call (sign off below before bulk run):**

- [ ] Field names confirmed (`Holdings`, `Weighting`, `ExternalId`, `HoldingDate`)
- [ ] Top-N vs full disclosure documented (some ETFs return top-25 only)
- [ ] Historical disclosures available? (latest-only vs multi-month)
- [ ] Weight-as-percentage vs decimal: pipeline normalises >1 to /100; verify on smoke output

## 3. Job 3 — Cleanup

Per spec section 4.2 default: **keep all unless architect explicitly confirms.**

Migration `020_atlas_m0_cleanup_unused_tables` is committed but is a no-op
unless the env var `ATLAS_M0_CLEANUP_CONFIRM=drop_unused_jip_intel_tables`
is set during `alembic upgrade head`. Default behaviour is to log a skip
message and leave all tables intact.

Candidate tables (sourced from spec section 4.1):

| Table                        | Reason                          | Default action |
|------------------------------|---------------------------------|----------------|
| `de_rs_scores`               | prior RS methodology            | keep           |
| `de_rs_daily_summary`        | prior RS methodology            | keep           |
| `de_sector_breadth_daily`    | prior breadth measures          | keep           |
| `de_breadth_daily`           | prior breadth measures          | keep           |
| `de_equity_technical_daily`  | prior technicals (v1)           | keep           |
| `de_mf_derived_daily`        | prior MF derivations            | keep           |
| `de_mf_sector_exposure`      | prior MF sector view            | keep           |
| `de_fo_bhavcopy`             | F&O — Atlas does not consume    | keep           |
| `de_fo_summary`              | F&O — Atlas does not consume    | keep           |
| `de_bse_announcements`       | Atlas does not consume          | keep           |
| `de_market_cap_history`      | Atlas uses traded value proxy   | keep           |

Architect sign-off required before flipping the env var.

## 4. Update Frequency

Pipelines required by Atlas (spec section 5):

| Source                                     | SLA            | Verified active |
|--------------------------------------------|----------------|------------------|
| Equity / ETF / Index daily prices          | 22:00 IST T-1  | (live check)    |
| AMFI daily NAVs                            | 23:00 IST T-1  | (live check)    |
| Morningstar MF + ETF holdings              | <=5 biz days   | (live check)    |
| International benchmarks (yfinance)        | 23:00 IST T-1  | (live check)    |

The readiness script renders the latest run timestamp from
`de_pipeline_log` for each tracked pipeline.

## 5. Accepted Limitations

- **MSCI World** sourced via URTH (iShares MSCI World ETF) as proxy — the
  direct MSCI World index ticker is not freely distributable. The proxy
  accurately tracks the index for trend/regime use cases.
- **Pre-listing BHAV history** is unfillable for instruments that listed
  after 2014-04-01. These are flagged in the gap report and excluded from
  the 95 % threshold.
- **AMFI portal rate limits**: residual MF NAV gaps are filled from the
  mfpulse mirror first; AMFI is the fallback. Some legacy schemes may have
  no AMFI history at all and are reported as residuals.
- **Morningstar ETF holdings — top-N**: confirmed during smoke test;
  thematic ETFs holding <=25 names will be fully captured, broad ETFs
  (e.g. NIFTY 500 trackers) may show truncated tail. This is acceptable
  for Atlas-M5 dominant-sector classification but should be revisited if
  full holdings analytics are added later.

## 6. Final call

(Populated by readiness script.)

If GO → Atlas-M1 unblocked.
If REVIEW → proceed with the limitations above documented and signed.
If NO-GO → unresolved item is recorded here, M1 paused until fixed.

---

## How to (re)generate this report

```bash
# 1. Audit current state
python scripts/atlas_m0_gap_audit.py

# 2. Seed INTL tickers (Stooq dump optional)
python scripts/atlas_m0_seed_intl.py --stooq-root /opt/stooq/d_us_txt

# 3. Run gap fill (BHAV + MF + INTL together via orchestrator)
python scripts/atlas_m0_gap_fill.py \
    --stooq-root /opt/stooq/d_us_txt --run-stocks --run-mfs

# 4. Apply ETF holdings migration + run the new pipeline
alembic upgrade head
python -c "import asyncio; \
  from app.pipelines.morningstar.etf_holdings import EtfHoldingsPipeline; \
  from app.db.session import get_session; \
  from datetime import date; \
  async def go():
      async with get_session() as s:
          await EtfHoldingsPipeline().run(date.today(), s)
  asyncio.run(go())"

# 5. Regenerate this readiness report
python scripts/atlas_m0_readiness.py
```
