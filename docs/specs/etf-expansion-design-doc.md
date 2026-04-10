# ETF Universe Expansion — Design Doc

**Date:** 2026-04-10
**Author:** Forge Build
**Status:** Draft

---

## 1. Problem Statement

JIP's ETF coverage (130 US-listed ETFs) is insufficient for a wealth management intelligence platform. Missing:
- **NSE India ETFs** (~80-100 actively traded) — already flowing into de_equity_ohlcv via BHAV but not tagged or queryable as ETFs
- **Global coverage gaps** — no fixed income depth (only TLT/IEF/HYG/LQD/EMB), missing thematic ETFs (AI, uranium, cybersecurity), missing commodity breadth
- No unified view: Indian ETF advisors can't compare NIFTYBEES vs SPY vs EWJ in one screen

## 2. Current State

| Component | Count | Source | Status |
|-----------|-------|--------|--------|
| de_etf_master | 130 | etf_ingest.py ETFS dict | Active |
| de_etf_ohlcv | ~130 tickers | yfinance daily (etf_prices pipeline) | Green |
| de_etf_technical_daily | ~130 tickers | etf_technicals.py script | Working |
| de_rs_scores (ETF) | ~130 tickers | etf_rs.py (SPY + ^SPX benchmarks) | Working |
| NSE ETFs in de_equity_ohlcv | ~100+ symbols | BHAV pipeline (undifferentiated from equities) | Flowing but untagged |

**Key structural facts:**
- de_etf_ohlcv PK: `(date, ticker)` — ticker is VARCHAR(30)
- de_equity_ohlcv PK: `(date, instrument_id)` — instrument_id is UUID
- de_instrument has no `instrument_type` field — cannot distinguish ETFs from stocks
- etf_prices pipeline: yfinance batch of 50, period=5d, runs in EOD schedule
- Stooq data files exist on EC2 only (`~/all_etfs_134.tar.gz`, `~/etf_data.tar.gz`)

## 3. Design Decisions

### Decision 1: NSE ETF Identification — Hardcoded Curated List

**Options considered:**
- A) Parse UDiFF `FinInstrmTp` field from BHAV data — correct but requires parser changes + backfill
- B) Cross-reference de_mf_master.is_etf via ISIN — partial coverage, depends on Morningstar data
- C) **Hardcode a curated NSE ETF list** — immediate, allows us to add metadata (category, benchmark)

**Decision: Option C.** We need master data (name, category, benchmark, sector) anyway, which Options A/B don't provide. A Python dict like the existing ETFS constant is the right pattern. We'll curate ~80 high-liquidity NSE ETFs.

**Future improvement:** Add `instrument_type` to de_instrument and parse UDiFF FinInstrmTp for automated discovery.

### Decision 2: NSE ETF OHLCV — SQL Copy from de_equity_ohlcv

NSE ETFs already get daily prices via the BHAV pipeline. No need to use yfinance `.NS` tickers (unreliable, rate-limited). Instead:

```sql
INSERT INTO de_etf_ohlcv (date, ticker, open, high, low, close, volume)
SELECT eo.date, i.current_symbol, eo.open, eo.high, eo.low, eo.close, eo.volume
FROM de_equity_ohlcv eo
JOIN de_instrument i ON i.id = eo.instrument_id
WHERE i.current_symbol IN (SELECT ticker FROM de_etf_master WHERE exchange = 'NSE')
  AND eo.date >= '2016-04-01'
ON CONFLICT (date, ticker) DO UPDATE
  SET close = EXCLUDED.close, open = EXCLUDED.open,
      high = EXCLUDED.high, low = EXCLUDED.low, volume = EXCLUDED.volume,
      updated_at = NOW();
```

This runs as a new pipeline step after BHAV ingestion. Uses adjusted close from BHAV? **No** — use raw close, matching the global ETF convention in de_etf_ohlcv.

### Decision 3: Global ETF Expansion — Targeted Additions (~40 new tickers)

The existing 130 already cover 46 countries and 28 sectors. Gaps to fill:

**Fixed Income (5 new):**
AGG (US Agg Bond), BNDX (Intl Bond), TIP (TIPS), SHY (1-3yr Treasury), BND (Total Bond)

**Commodities (3 new):**
PDBC (Diversified Commodity), PPLT (Platinum), WEAT (Wheat) — DBA/DBC/GLD/SLV/USO already exist

**Thematic (20 new):**
| Ticker | Theme | Exchange |
|--------|-------|----------|
| ARKK | Disruptive Innovation | NYSE |
| BOTZ | AI & Robotics | NASDAQ |
| ROBO | Robotics & AI | NYSE |
| DRIV | Electric Vehicles | NYSE |
| LIT | Lithium & Battery | NYSE |
| CIBR | Cybersecurity | NYSE |
| BUG | Cybersecurity | NYSE |
| GNOM | Genomics | NYSE |
| BLOK | Blockchain | NYSE |
| URA | Uranium | NYSE |
| ARKX | Space | NYSE |
| QCLN | Clean Energy | NASDAQ |
| JETS | Airlines | NYSE |
| KWEB | China Internet (exists) | — |
| MSOS | Cannabis | NYSE |
| XHE | Healthcare Equipment | NYSE |
| CLOU | Cloud Computing | NYSE |
| AIQ | AI & Big Data | NYSE |
| FINX | Fintech | NASDAQ |
| IBIT | Bitcoin | NASDAQ |

**Frontier/Small Countries (5 new):**
FM (Frontier Markets), ERUS (Russia — may be delisted, verify), ENZL (New Zealand), EWI already exists — check for Pakistan (PAK), Nigeria (NGE)

**Leveraged/Inverse — EXCLUDED.** These decay, mislead RS scores, and don't belong in a wealth management platform.

**Total new global: ~33 tickers**

### Decision 4: Historical Backfill — yfinance `max` (Skip Stooq Tars)

**Options considered:**
- A) SCP Stooq tar files from EC2, parse, load — complex ticker mapping, only covers US ETFs, data may overlap with existing
- B) **yfinance `period="max"`** for all new tickers — simple, covers full history, works for all exchanges

**Decision: Option B.** Reasons:
1. Stooq tickers = yfinance tickers for US ETFs (no mapping needed anyway)
2. yfinance max pulls 20+ years of history for established ETFs
3. NSE ETFs get history from de_equity_ohlcv copy (no yfinance needed)
4. The Stooq tars were already loaded for the original 130 — no gap to fill there
5. Avoids SCP/tar/parse complexity for 33 new tickers that can be fetched in one yfinance call

### Decision 5: Batch Scaling — No Changes Needed

At ~250 total tickers (130 existing + 80 NSE + 33 new global), the etf_prices pipeline runs 5-6 batches of 50. Current 130 = ~3 batches, already working. **NSE ETFs won't go through yfinance at all** (they use the BHAV copy), so yfinance load stays at ~163 tickers = 4 batches.

### Decision 6: Enrichment — Separate, Non-Blocking Script

yfinance `Ticker.info` is unreliable and rate-limited. Run as a one-time enrichment script after the main build:
- Populate: category, sector, expense_ratio, benchmark, inception_date, currency
- Throttle: 1 req/sec, retry on failure
- Non-blocking: missing enrichment data doesn't affect prices or technicals

## 4. NSE India ETF Curated List (~80 ETFs)

Grouped by category. Only ETFs with meaningful AUM and liquidity:

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

**Total NSE: ~67 ETFs** (conservative — only liquid ones with established AUM)

## 5. Target Universe Summary

| Segment | Count | Source |
|---------|-------|--------|
| Existing global (NYSE/NASDAQ) | 130 | etf_ingest.py |
| New global (fixed income, thematic, commodity) | ~33 | New additions |
| NSE India | ~67 | New curated list |
| **Total** | **~230** | |

This is below the 250-300 target. We can expand NSE by adding more AMC duplicates (multiple Nifty 50 ETFs from different AMCs) or add more thematic/frontier ETFs. **230 is a solid, high-quality universe.** Quality > quantity for a wealth management platform.

## 6. Architecture Changes

### New Files
1. `scripts/ingest/nse_etf_master.py` — NSE ETF curated list + de_etf_master seeder
2. `app/pipelines/etf/nse_etf_sync.py` — Post-BHAV pipeline: copies NSE ETF OHLCV from de_equity_ohlcv → de_etf_ohlcv
3. `scripts/ingest/etf_enrich.py` — yfinance Ticker.info enrichment (one-time)
4. `scripts/ingest/etf_backfill.py` — yfinance max history for new global ETFs

### Modified Files
1. `scripts/ingest/etf_ingest.py` — Add ~33 new global ETFs to ETFS dict
2. `app/pipelines/registry.py` — Register nse_etf_sync pipeline
3. `app/orchestrator/scheduler.py` — Add nse_etf_sync after equity_bhav in DAG
4. `app/api/v1/observatory.py` — Update etf_ohlcv stream metadata

### No Schema Changes
de_etf_master, de_etf_ohlcv, de_etf_technical_daily all have sufficient columns. No Alembic migration needed.

## 7. Daily Automation Flow (Post-Build)

```
18:30 IST — EOD schedule fires:
  ├── equity_bhav (NSE BHAV download + parse)
  │     └── nse_etf_sync (NEW: copy NSE ETF rows → de_etf_ohlcv)
  ├── etf_prices (yfinance for 163 global ETFs, 4 batches of 50)
  └── ... other EOD pipelines

After EOD:
  ├── etf_technicals (compute 20 indicators for all ~230 ETFs)
  └── etf_rs (RS scores vs SPY and ^SPX for all ~230 ETFs)
```

## 8. Risks & Mitigations

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| NSE ETF symbols not in de_instrument | Medium | Verify against DB before inserting; BHAV may use different symbol variants |
| yfinance rate limit on 163 tickers | Low | Already running 130 successfully; add exponential backoff between batches |
| Some NSE ETFs have no BHAV data (low liquidity) | Medium | Skip on conflict; log missing symbols |
| yfinance max history incomplete for thematic ETFs (young) | Expected | Accept — these ETFs launched recently, limited history is the reality |
| Enrichment fields mostly null | High | Non-blocking; populate what's available, leave rest null |

## 9. Verification Criteria

1. `SELECT COUNT(DISTINCT ticker) FROM de_etf_master` → 230+
2. `SELECT COUNT(DISTINCT ticker) FROM de_etf_ohlcv WHERE date = CURRENT_DATE - 1` → 200+ (accounting for holidays)
3. All NSE ETFs have OHLCV through yesterday's BHAV date
4. etf_technicals runs without error on expanded universe
5. etf_rs produces scores for all new tickers
6. Observatory dashboard shows etf_ohlcv stream as green
7. Pipeline log shows nse_etf_sync completing after equity_bhav
