# Goldilocks Intelligence Engine — PRD

## 1. Overview

Build a complete intelligence pipeline that assimilates all Goldilocks Research content (PDFs, video con-calls, audio sound bytes, HTML pages), extracts structured market views via Claude API, adds Gautam Shah's core computations to JIP Data Core, and serves a combined quantitative + qualitative signal layer via API.

## 2. Users & Stakeholders

- **Primary:** JSL Wealth advisors consuming Market Pulse
- **Secondary:** Nimish (portfolio decisions, regime assessment)
- **System:** Market Pulse frontend (read-only consumer of new API endpoints)

## 3. Goals

| Goal | Metric | Target |
|---|---|---|
| Content coverage | % of Goldilocks output captured | 100% of all published content |
| Extraction accuracy | Claude extraction quality_score | >= 0.70 mean |
| Freshness | Time from Goldilocks publish to structured data | < 4 hours for daily, < 24 hours for audio |
| Outcome tracking | % of stock ideas with automated outcome | 100% of active ideas tracked daily |
| Computation coverage | Gautam's indicators we can replicate | Stochastic, divergence, fibonacci, pivots, intermarket |

## 4. Non-Goals

- Frontend/UI for Goldilocks data (Market Pulse team handles this)
- Real-time intraday signals (daily pipeline is sufficient)
- Replacing Goldilocks subscription (we augment, not replace)
- Options data / PCR / max pain (separate data source, separate build)
- Custom thematic indices (Jindal, Green Energy — requires manual constituent definition, Phase 2)

## 5. System Components

### 5A. Content Pipeline (download + extract + store)

**5A.1 PDF Text Extraction**
- Decrypt all 51 PDFs using PAN password via PyMuPDF
- Update de_qual_documents.raw_text with full extracted text
- Tag each document with report_type (trend_friend, big_picture, sector_trends, fortnightly, stock_bullet, big_catch)
- Run on EC2 as one-time backfill + daily for new reports

**5A.2 Video Download + Transcription**
- Download 3 monthly con-call MP4s via authenticated HTTP GET
- Install ffmpeg on EC2: `sudo apt install -y ffmpeg`
- Extract audio: `ffmpeg -i input.mp4 -vn -ar 16000 -ac 1 -acodec pcm_s16le output.wav`
- Transcribe with faster-whisper (small model, int8, language="hi")
- Store transcript in de_qual_documents.raw_text
- Store audio duration in audio_duration_s column

**5A.3 Audio Sound Bytes**
- Re-scrape sound_byte.php with fresh session to get current MP3 URLs
- Download MP3 files
- Transcribe with faster-whisper
- Store transcript + duration

**5A.4 HTML Content Extraction**
- Q&A with Gautam: parse accordion/panel structure, extract Q&A pairs
- Market Snippets: parse timestamped entries
- Update raw_text with clean extracted text (strip nav/footer HTML)

**5A.5 Daily Scraper Enhancement**
- Enhance existing goldilocks_scraper.py to:
  - Auto-decrypt PDFs after download (using PAN password)
  - Extract text immediately and store in raw_text
  - Download con-call videos when new monthly appears
  - Download sound bytes with fresh URLs
  - Trigger transcription for new audio/video
  - Trigger Claude extraction for new documents

### 5B. Intelligence Extraction (Claude API)

**5B.1 Document Classification**
- Classify each document by report_type based on title/content
- Tag: trend_friend | big_picture | big_catch | stock_bullet | sector_trends | fortnightly | concall | sound_byte | qa | snippet

**5B.2 Trend Friend Extraction → de_goldilocks_market_view**
```
For each Trend Friend daily report, extract:
- report_date
- nifty_close, nifty_support_1/2, nifty_resistance_1/2
- bank_nifty_close, bank_nifty_support_1/2, bank_nifty_resistance_1/2
- trend_direction (upward/downward/sideways)
- trend_strength (1-5)
- headline (one-line summary)
- overall_view (full narrative paragraph)
- sector table: [{sector, trend, outlook, rank}]
```

**5B.3 Stock Idea Extraction → de_goldilocks_stock_ideas**
```
For each Stock Bullet / Big Catch, extract:
- published_date, symbol, company_name
- idea_type (stock_bullet / big_catch)
- entry_price (or entry_zone_low/high)
- target_1, target_2, lt_target
- stop_loss
- timeframe
- rationale (key technical reasoning)
- technical_params: {ema_200, rsi_14, support_1/2, resistance_1/2}
```

**5B.4 Sector View Extraction → de_goldilocks_sector_view**
```
For each Sector Trends / Fortnightly, extract:
- report_date
- outperforming_sectors: [{sector, outlook, top_picks: [{symbol, resistance_levels}]}]
- underperforming_sectors: [{sector, outlook}]
- thematic_calls: [{theme, outlook, stocks}]
```

**5B.5 General Market View Extraction → de_qual_extracts**
```
For all document types (including con-call transcripts), extract:
- asset_class, entity_ref, direction, timeframe, conviction
- view_text, source_quote, quality_score
```

### 5C. New Computations

**5C.1 Stochastic Oscillator**
```
%K = (Close - Low_N) / (High_N - Low_N) * 100
%D = SMA(%K, 3)
Default period N = 14, signal period = 3
Compute for: daily, weekly, monthly timeframes
```

**5C.2 Divergence Detection**
```
For each instrument, compare price action vs oscillator:
- Bullish divergence: price makes lower low, oscillator makes higher low
- Bearish divergence: price makes higher high, oscillator makes lower high
- Triple divergence: 3 consecutive divergent swings (Gautam's strongest signal)
Detect on: RSI, Stochastic, MACD
Timeframes: daily, weekly
```

**5C.3 Fibonacci Retracement**
```
Auto-detect significant swings (zigzag algorithm, min 5% move)
Calculate retracement levels: 23.6%, 38.2%, 50%, 61.8%, 78.6%
Store most recent swing's fib levels for each instrument
```

**5C.4 Disparity Index**
```
Disparity_N = ((Close - SMA_N) / SMA_N) * 100
Periods: 20, 50
Interpretation: extreme disparity = mean reversion likely
```

**5C.5 Pivot Points (Indices Only)**
```
Pivot = (High + Low + Close) / 3
S1 = 2*Pivot - High
S2 = Pivot - (High - Low)
S3 = Low - 2*(High - Pivot)
R1 = 2*Pivot - Low
R2 = Pivot + (High - Low)
R3 = High + 2*(Pivot - Low)
Compute daily for: NIFTY 50, BANK NIFTY, NIFTY IT, NIFTY METAL, etc.
```

**5C.6 Intermarket Ratios**
```
Ratios to compute daily:
- BANKNIFTY / NIFTY (financial sector health)
- NIFTY_MICROCAP / NIFTY (small vs large)
- NIFTY / MSCI_WORLD (India vs global, if data available)
- GOLD / NIFTY
For each ratio: value, SMA(20), direction (rising/falling/flat)
```

**5C.7 Bollinger Width (Squeeze Detection)**
```
BW = (Upper - Lower) / Middle * 100
Squeeze = BW < percentile_10(BW, lookback=252)
Expansion = BW > percentile_90(BW, lookback=252)
```

### 5D. Outcome Tracking

**5D.1 Daily Idea Monitor**
```
For each active idea in de_goldilocks_stock_ideas:
1. Fetch latest close from de_equity_ohlcv
2. Check: did close >= target_1? → update status, record outcome
3. Check: did close <= stop_loss? → update status, record outcome
4. Check: is timeframe expired? → mark expired, record final P&L
5. Insert into de_qual_outcomes with actual_move_pct
```

**5D.2 Scorecard**
```
Track rolling accuracy:
- Hit rate (% ideas that hit target before SL)
- Average return on winning ideas
- Average loss on losing ideas
- Win/loss ratio
- Accuracy by idea_type (bullet vs catch)
- Accuracy by sector
```

### 5E. API Endpoints

```
GET /api/v1/goldilocks/market-view?date=YYYY-MM-DD
  → Latest Trend Friend data: S/R levels, trend, sector table

GET /api/v1/goldilocks/sector-views?date=YYYY-MM-DD
  → Sector rankings with outlook and top picks

GET /api/v1/goldilocks/stock-ideas?status=active
  → Active stock ideas with levels and current P&L

GET /api/v1/goldilocks/scorecard
  → Accuracy metrics, hit rate, win/loss ratio

GET /api/v1/goldilocks/divergences?timeframe=weekly
  → Detected divergence signals (bullish/bearish/triple)

GET /api/v1/market/pulse
  → Combined view: quant signals + qual signals + alignment score
```

## 6. Data Flow

```
Daily 19:30 IST: BHAV copy → OHLCV
Daily 23:00 IST: Technicals + Stochastic + Disparity + BollingerWidth
Daily 23:15 IST: RS scores + Divergence detection
Daily 23:20 IST: Breadth + Regime + Pivot points + Intermarket ratios
Daily 23:30 IST: Goldilocks scraper → new PDFs/audio
Daily 23:45 IST: PDF extraction → Claude extraction → structured tables
Daily 23:50 IST: Outcome tracker checks active ideas
Monthly: Con-call download → ffmpeg → faster-whisper → Claude extraction
```

## 7. Dependencies & Prerequisites

| Dependency | Status | Action |
|---|---|---|
| PyMuPDF on EC2 | Installed | None |
| ffmpeg on EC2 | NOT installed | `sudo apt install -y ffmpeg` |
| faster-whisper on EC2 | NOT installed | `pip3 install faster-whisper` |
| Claude API key | In .env | Verify ANTHROPIC_API_KEY |
| Goldilocks credentials | In .env | Verified working |
| Playwright on EC2 | Installed | None |

## 8. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Goldilocks site structure changes | Scraper breaks | Graceful failure + alert, selectors are generic |
| Con-call temp URLs change format | Can't download | Re-scrape page, URLs are in HTML source |
| faster-whisper OOM on t3.large | Transcription fails | Use small model + int8 quant, or tiny model as fallback |
| Claude extraction quality low | Bad structured data | quality_score threshold 0.70, manual review for first batch |
| Hindi-English code-switch in con-calls | Poor transcription | faster-whisper handles multilingual well, language="hi" |

## 9. Rollout Plan

**Phase A (this build):** Content pipeline + extraction + outcome tracking + API
**Phase B (computation):** Stochastic, divergence, fibonacci, pivots, intermarket, bollinger width

Phase A delivers the unique Goldilocks value. Phase B replicates his methodology computationally. Both included in chunk plan, but Phase A chunks can ship independently.

## 10. Cost Estimate

| Item | One-time | Monthly |
|---|---|---|
| Claude extraction (backfill 79 docs) | ~$3 | — |
| Claude extraction (daily) | — | ~$1.50 |
| faster-whisper | $0 (open source) | $0 |
| ffmpeg | $0 (apt package) | $0 |
| Disk (videos + transcripts) | ~1 GB | ~100 MB |
| **Total** | **~$3** | **~$1.50** |
