# Goldilocks Intelligence Engine — Design Document

## Problem Statement

Goldilocks Research (Gautam Shah, CMT/CFTe/MSTA) publishes daily/weekly/monthly technical analysis across 7 content streams. We subscribe to this service. The content is consumed manually — PDFs are read, con-calls are listened to, sound bytes are heard — but none of it is systematically captured, structured, or integrated into our quantitative decision engine.

**The gap:** Our JIP Data Core has a powerful quantitative backbone (RS scores, 39 technicals, breadth, regime, sentiment) but ZERO qualitative intelligence layer. Gautam's methodology — oscillator divergences, fibonacci retracements, intermarket ratios, sector rotation — overlaps with but extends beyond what we compute. His *judgment* about when and how to combine these signals is the real alpha.

## What We're Building

A complete pipeline to **assimilate all Goldilocks content, extract structured intelligence, replicate his analytical computations, and create a qualitative signal layer** that integrates with our existing quantitative engine.

## Content Inventory (Verified on EC2 2026-04-10)

| Content Type | Count | Format | Location | Status |
|---|---|---|---|---|
| Trend Friend Daily | 23 | PDF (encrypted, PAN password) | EC2 disk | Text not extracted |
| Big Picture Monthly | 4 | PDF (encrypted) | EC2 disk | Text not extracted |
| Big Catch (stock ideas) | 12 | PDF (encrypted) | EC2 disk | Text not extracted |
| India Fortnightly | 3 | PDF (encrypted) | EC2 disk | Text not extracted |
| Sector Trends | 3 | PDF (encrypted) | EC2 disk | Text not extracted |
| Stock Bullet | 3 | PDF (encrypted) | EC2 disk | Text not extracted |
| Monthly Con-Calls | 3 | MP4 video (streamed) | Site only | **Not downloaded** |
| Sound Bytes | 4 | MP3 audio (temp URLs) | Site only | URLs expired, need fresh scrape |
| Q&A with Gautam | ~10+ | HTML text | DB (22 rows) | Raw HTML, not extracted |
| Market Snippets | ~10+ | HTML text | DB | Raw HTML, not extracted |

**Total in DB:** 79 documents (51 PDF, 22 HTML, 5 audio URLs, 1 video URL)
**Actual extracted text:** 0 (PDFs have ~122 char HTML snippets, not real content)

### Con-Call Videos (The Gold Mine)

Three monthly con-call recordings hosted as plain MP4 files behind `controlsList="nodownload"` (browser UI trick only — HTTP GET works):

| Date | File | Size |
|---|---|---|
| 2026-04-06 | data-temp/27981775807571.mp4 | 76.7 MB |
| 2026-03-08 | data-temp/96001775807571.mp4 | 65.8 MB |
| 2026-02-04 | data-temp/81821775807571.mp4 | 69.9 MB |

Total: 212 MB. Estimated 90-180 minutes of Gautam explaining his complete thought process.

## Gautam's Decision Framework (from reading all 6 report types)

### Level 1: Macro Regime (Monthly — Big Picture, 51 pages)
- Weekly/Monthly Stochastic Oscillator — **triple divergence** setups
- Weekly RSI oversold/overbought zones (30/70 lines)
- Fibonacci retracements of major swings (23.6%, 38.2%, 50%, 61.8%)
- Channel patterns and trendlines (8-month buildups)
- Historical pattern matching ("every time RSI hit this in 15 years...")
- Time correction analysis
- **Output:** BULL / BEAR / INFLECTION POINT + conviction

### Level 2: Intermarket (Monthly)
- INR-Nifty correlation (USD-INR bottom = equity strength)
- Crude oil trajectory (range identification)
- Gold/Silver trend + mean reversion zones
- India VIX percentile
- BankNifty/Nifty ratio (financial sector as market health)
- Nifty vs Microcap ratio (where money is flowing)
- India vs World relative performance
- **Output:** RISK-ON / RISK-OFF / TAILWIND / HEADWIND

### Level 3: Breadth (Monthly + Daily)
- % stocks above 50-DMA / 200-DMA
- Stocks making 52-week lows (pain indicator)
- Disparity Index (price vs moving average deviation)
- Seasonality index
- "Weight of Evidence" composite
- **Output:** OVERSOLD / NEUTRAL / OVERBOUGHT

### Level 4: Sector Rotation (Fortnightly — Sector Trends)
- Sector vs Nifty relative strength charts
- Custom thematic indices (Jindal Group, Green Energy, Rare Earth)
- Sector ranking: trend + outlook + rank (1-8)
- Volume/volatility contraction before breakouts
- **Output:** Ranked sector list + top stocks per sector with resistance levels

### Level 5: Stock Selection (Stock Bullet / Big Catch)
- 200-EMA position, 14-day RSI
- Support/Resistance levels (2 each)
- Moving average alignment
- Ratio charts vs Nifty AND vs sector
- Entry Zone (range, not point), Target 1/2/LT, Stop Loss
- Timeframe: 2-6 weeks (Bullet) / 12-18 months (Catch)
- **Output:** BUY with exact levels + timeframe + rationale

### Level 6: Daily Execution (Trend Friend Daily)
- Nifty S1/S2/R1/R2, Bank Nifty S1/S2/R1/R2
- Trend direction + Trend Strength (1-5 visual scale)
- Global Impact flag
- Sector table: trend arrow, outlook text, rank 1-8
- Daily narrative + chart observations
- **Output:** Today's setup + positioning guidance

## What We Compute Today vs What Gautam Uses

### Already in JIP (quantitative backbone)
- RSI (14-day), ADX, MACD, Bollinger Bands, MFI
- EMA (10, 21, 50, 200), SMA (20, 50, 200)
- RS scores with quadrants (equity, sector, MF)
- Market breadth (6 daily + 6 monthly metrics)
- Market regime (Bull/Bear/Correction/Cautious)
- 5-layer sentiment composite (22 metrics)
- Sector RS with rotation quadrants

### Missing — Gautam Uses But We Don't Compute
1. **Stochastic Oscillator** (14,3,3) — his PRIMARY oscillation tool
2. **Divergence Detection** (price vs oscillator) — his STRONGEST signal
3. **Fibonacci Retracement Levels** — automatic swing + fib calculation
4. **Disparity Index** — mean reversion signal
5. **Pivot Points** (classic S1/S2/R1/R2) — daily index levels
6. **Intermarket Ratios** — BankNifty/Nifty, Microcap/Nifty, India VIX percentile
7. **Weekly/Monthly Timeframe Oscillators** — we only compute daily
8. **Volume/Volatility Contraction** — Bollinger width squeeze detection
9. **Custom Thematic Indices** — weighted baskets (Jindal, Green Energy, Rare Earth)
10. **Seasonality Score** — month-of-year historical returns

## System Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    CONTENT PIPELINE                       │
│                                                           │
│  Scraper ──→ Download ──→ Extract ──→ de_qual_documents  │
│  (7 pages)   (PDF/MP4/   (PyMuPDF/   (raw_text column)  │
│              MP3)         faster-                         │
│                           whisper)                        │
├─────────────────────────────────────────────────────────┤
│                 INTELLIGENCE EXTRACTION                    │
│                                                           │
│  de_qual_documents.raw_text                               │
│       ↓ Claude Sonnet API                                 │
│  de_qual_extracts (direction, conviction, timeframe)      │
│  de_goldilocks_market_view (S/R, trend, strength)         │
│  de_goldilocks_sector_view (sector, rank, outlook)        │
│  de_goldilocks_stock_ideas (entry/target/SL/timeframe)    │
├─────────────────────────────────────────────────────────┤
│               NEW COMPUTATIONS                            │
│                                                           │
│  Stochastic (daily/weekly/monthly)                        │
│  Divergence detection (RSI, Stochastic, MACD)             │
│  Fibonacci retracement levels                             │
│  Disparity Index                                          │
│  Pivot Points (indices)                                   │
│  Intermarket Ratios                                       │
│  Bollinger Width (squeeze detection)                      │
├─────────────────────────────────────────────────────────┤
│               OUTCOME TRACKING                            │
│                                                           │
│  de_goldilocks_stock_ideas (status=active)                │
│       ↓ daily check vs de_equity_ohlcv                    │
│  Auto-update: target_hit / sl_hit / expired               │
│       ↓                                                   │
│  de_qual_outcomes (was_correct, actual_move_pct)          │
├─────────────────────────────────────────────────────────┤
│               MARKET PULSE API                            │
│                                                           │
│  GET /api/v1/goldilocks/market-view                       │
│  GET /api/v1/goldilocks/sector-views                      │
│  GET /api/v1/goldilocks/stock-ideas                       │
│  GET /api/v1/goldilocks/divergences                       │
│  GET /api/v1/goldilocks/scorecard (accuracy tracking)     │
│  GET /api/v1/market/pulse (combined quant + qual)         │
└─────────────────────────────────────────────────────────┘
```

## Technical Decisions

### Transcription: faster-whisper (open source)
- Model: `small` (461M params, ~2 GB RAM)
- Quantization: int8 (fits on t3.large with 4.3 GB free)
- Speed: ~10x realtime on CPU = 3-6 min per 30-min recording
- Language: `hi` (handles Hindi-English code-switching)
- Cost: $0 (runs locally)
- Install: `pip3 install faster-whisper`

### PDF Extraction: PyMuPDF with PAN password
- All Goldilocks PDFs encrypted with subscriber PAN: `AICPJ9616P`
- PyMuPDF already installed on EC2
- `doc.authenticate(password)` then `page.get_text()`

### Video Download: Plain HTTP GET
- `controlsList="nodownload"` is browser-only UI restriction
- Authenticated `requests.Session` downloads the raw MP4
- `ffmpeg` extracts audio track for transcription

### Claude Extraction: claude-sonnet-4-20250514
- Structured output via tool calling
- Per-document cost: ~$0.02-0.05
- Total backfill: ~$2-4 for 79 documents
- Daily ongoing: ~$0.05/day (1 Trend Friend)

### New DB Tables

```sql
-- Weekly/monthly oscillators
de_oscillator_weekly (date, instrument_id, stochastic_k, stochastic_d, rsi_14, disparity_20)
de_oscillator_monthly (date, instrument_id, stochastic_k, stochastic_d, rsi_14, disparity_20)

-- Divergence signals
de_divergence_signals (id, date, instrument_id, timeframe, divergence_type, indicator, strength)

-- Fibonacci levels
de_fib_levels (date, instrument_id, swing_type, swing_price, fib_236, fib_382, fib_500, fib_618)

-- Intermarket ratios
de_intermarket_ratios (date, ratio_name, value, sma_20, direction)

-- Index pivot points
de_index_pivots (date, index_code, pivot, s1, s2, s3, r1, r2, r3)
```

### Columns Added to Existing Tables

```sql
ALTER TABLE de_equity_technical_daily ADD COLUMN stochastic_k NUMERIC(8,4);
ALTER TABLE de_equity_technical_daily ADD COLUMN stochastic_d NUMERIC(8,4);
ALTER TABLE de_equity_technical_daily ADD COLUMN disparity_20 NUMERIC(8,4);
ALTER TABLE de_equity_technical_daily ADD COLUMN disparity_50 NUMERIC(8,4);
ALTER TABLE de_equity_technical_daily ADD COLUMN bollinger_width NUMERIC(8,4);
```

## EC2 Resource Requirements

| Resource | Current | After Build |
|---|---|---|
| Disk | 54 GB used / 96 GB | +212 MB (videos) + ~1 GB (audio/transcripts) |
| RAM | 3.4 GB used / 7.6 GB | +2 GB during transcription (temporary) |
| Dependencies | pymupdf installed | + faster-whisper, ffmpeg |

## Constraints
- EC2 t3.large: 2 vCPU, 8 GB RAM — transcription must be serial, not parallel
- RDS private subnet — all DB operations from EC2 only
- Goldilocks content is subscriber-only — never expose raw text via public API
- Financial values: Decimal, never float
- All dates: IST timezone aware

## Success Criteria
1. All 51 PDFs text-extracted and stored in de_qual_documents.raw_text
2. All 3 con-call videos downloaded, transcribed, stored
3. Sound bytes re-scraped with fresh URLs, downloaded, transcribed
4. Claude extraction populates de_goldilocks_market_view, sector_view, stock_ideas
5. New computations (stochastic, divergence, fibonacci, pivots, intermarket) running daily
6. Outcome tracking auto-validates stock ideas against actual prices
7. API endpoints serve combined quantitative + qualitative market view
8. Scraper runs daily: new Trend Friend + any new content auto-processed
