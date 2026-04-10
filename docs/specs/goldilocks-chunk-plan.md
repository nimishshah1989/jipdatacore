# Goldilocks Intelligence Engine — Chunk Plan

## Build Order & Dependencies

```
C1 (DB Schema) ─────────────────────────────────┐
  ↓                                               │
  ├──→ C2 (PDF Extraction)  ──┐                   │
  ├──→ C3 (Video + Transcribe)──→ C5 (Claude) ──→ C7 (Outcomes) ──┐
  ├──→ C4 (HTML Extraction)  ──┘      ↓                            │
  │                                 C8 (API) ──→ C9 (Market Pulse) │
  └──→ C6 (New Computations) ──────────↗              ↓            │
                                              C10 (Pipeline Wiring)←┘
```

## Chunk Summary

| # | Name | Complexity | Depends | Key Files | Est. Lines |
|---|---|---|---|---|---|
| C1 | DB Schema + Models | Medium | — | alembic migration, app/models/goldilocks.py, modify qualitative.py | ~400 |
| C2 | PDF Text Extraction | Low | C1 | pdf_extractor.py, extract_goldilocks_pdfs.py | ~200 |
| C3 | Video Download + Transcription | High | C1 | transcriber.py, download_goldilocks_media.py, transcribe_goldilocks.py | ~350 |
| C4 | HTML Content Extraction | Medium | C1 | html_cleaner.py, extract_goldilocks_html.py | ~200 |
| C5 | Claude Structured Extraction | High | C1,C2,C3,C4 | goldilocks_extractor.py, run_goldilocks_extraction.py, test | ~500 |
| C6 | New Computations | High | C1 | oscillators.py, divergence.py, fibonacci.py, pivots.py, intermarket.py | ~700 |
| C7 | Outcome Tracking | Medium | C1,C5 | outcome_tracker.py, test | ~250 |
| C8 | Goldilocks API Endpoints | Medium | C1,C5 | goldilocks.py router, modify __init__.py | ~300 |
| C9 | Market Pulse Combined | Medium | C6,C8 | market_pulse.py service, modify market.py | ~300 |
| C10 | Pipeline Wiring | Medium | All | goldilocks_daily.py, modify scraper, modify registry | ~300 |

**Total: 10 chunks, ~29 files, ~3,500 lines**

## Parallel Execution Plan

```
Wave 1 (independent):  C1
Wave 2 (parallel):     C2 + C3 + C4 + C6
Wave 3 (needs text):   C5
Wave 4 (parallel):     C7 + C8
Wave 5 (integration):  C9
Wave 6 (wiring):       C10
```

## EC2 Setup (before C3)

Must be done manually on EC2 before the video transcription chunk:
```bash
sudo apt install -y ffmpeg
pip3 install --break-system-packages faster-whisper
```

## Specs Location

All chunk specs: `docs/specs/goldilocks-chunks/c{N}-*.md`
- c1-db-schema.md
- c2-pdf-extraction.md
- c3-video-transcription.md
- c4-html-extraction.md
- c5-claude-extraction.md
- c6-new-computations.md
- c7-outcome-tracking.md
- c8-api-endpoints.md
- c9-market-pulse.md
- c10-pipeline-wiring.md
