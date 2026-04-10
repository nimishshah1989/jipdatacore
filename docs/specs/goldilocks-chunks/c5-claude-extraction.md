# C5: Claude Structured Extraction

**Complexity:** High
**Dependencies:** C1, C2, C3, C4 (all documents must have raw_text populated before this runs)
**Status:** pending

## Files
- app/pipelines/qualitative/goldilocks_extractor.py (new — Goldilocks-specific Claude extraction)
- scripts/ingest/run_goldilocks_extraction.py (new — backfill script)
- app/pipelines/qualitative/claude_extract.py (modify — add Goldilocks tool definitions)
- tests/pipelines/qualitative/test_goldilocks_extractor.py (new)

## Context

The existing app/pipelines/qualitative/claude_extract.py uses Claude tool_use to extract
market views into de_qual_extracts. Read that file before building this chunk — reuse the
anthropic client init, the tool_use call pattern, the quality_score logic, and the
processing_status update flow. Do not duplicate infrastructure.

de_qual_extracts already captures: asset_class, entity_ref, direction, timeframe, conviction,
view_text, source_quote, quality_score. This chunk adds Goldilocks-specific structured extraction
on top of that base.

## What To Build

### goldilocks_extractor.py — Goldilocks-Specific Claude Extraction

Async module (used from async FastAPI context and async pipeline runners).
All DB writes: SQLAlchemy 2.0 async sessions with `async with session.begin():`.
All price/level values: Decimal, never float — convert Claude's numeric output via Decimal(str(value)).

**1. extract_trend_friend(document_id: UUID, raw_text: str, session: AsyncSession) -> bool**

System prompt to Claude:
```
You are a financial data extractor. Extract structured market data from this Goldilocks Research
Trend Friend report. Be precise with numeric values — copy exact numbers from the text.
If a value is not mentioned, return null. Do not hallucinate levels.
```

Claude tool schema:
```json
{
  "name": "extract_trend_friend",
  "description": "Extract Trend Friend daily market view data",
  "input_schema": {
    "type": "object",
    "required": ["report_date", "trend_direction"],
    "properties": {
      "report_date": {"type": "string", "description": "YYYY-MM-DD"},
      "nifty_close": {"type": ["number", "null"]},
      "nifty_support_1": {"type": ["number", "null"]},
      "nifty_support_2": {"type": ["number", "null"]},
      "nifty_resistance_1": {"type": ["number", "null"]},
      "nifty_resistance_2": {"type": ["number", "null"]},
      "bank_nifty_close": {"type": ["number", "null"]},
      "bank_nifty_support_1": {"type": ["number", "null"]},
      "bank_nifty_support_2": {"type": ["number", "null"]},
      "bank_nifty_resistance_1": {"type": ["number", "null"]},
      "bank_nifty_resistance_2": {"type": ["number", "null"]},
      "trend_direction": {"type": "string", "enum": ["upward", "downward", "sideways"]},
      "trend_strength": {"type": ["integer", "null"], "minimum": 1, "maximum": 5},
      "global_impact": {"type": ["string", "null"], "enum": ["positive", "negative", "neutral", null]},
      "headline": {"type": ["string", "null"], "description": "One-line summary of market view"},
      "overall_view": {"type": ["string", "null"], "description": "Full narrative paragraph"},
      "sectors": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "sector": {"type": "string"},
            "trend": {"type": ["string", "null"]},
            "outlook": {"type": ["string", "null"]},
            "rank": {"type": ["integer", "null"]}
          },
          "required": ["sector"]
        }
      }
    }
  }
}
```

Processing:
- Call Claude claude-3-5-haiku (cost efficiency) with max_tokens=1000
- Extract tool_use response → tool_input dict
- Convert all numeric values to Decimal(str(v)) before DB insert
- INSERT into de_goldilocks_market_view ON CONFLICT (report_date) DO UPDATE all columns
- For each sector in tool_input["sectors"]:
  INSERT into de_goldilocks_sector_view ON CONFLICT (report_date, sector) DO UPDATE
- Return True on success, False on Claude error or validation failure
- Compute quality_score: fraction of non-null fields / total expected fields

**2. extract_stock_idea(document_id: UUID, raw_text: str, session: AsyncSession) -> bool**

Claude tool schema:
```json
{
  "name": "extract_stock_idea",
  "description": "Extract stock recommendation details from Stock Bullet or Big Catch report",
  "input_schema": {
    "type": "object",
    "required": ["published_date", "symbol", "company_name", "idea_type", "stop_loss"],
    "properties": {
      "published_date": {"type": "string", "description": "YYYY-MM-DD"},
      "symbol": {"type": "string", "description": "NSE stock symbol e.g. RELIANCE"},
      "company_name": {"type": "string"},
      "idea_type": {"type": "string", "enum": ["stock_bullet", "big_catch"]},
      "entry_price": {"type": ["number", "null"], "description": "Single entry price if given"},
      "entry_zone_low": {"type": ["number", "null"]},
      "entry_zone_high": {"type": ["number", "null"]},
      "target_1": {"type": ["number", "null"]},
      "target_2": {"type": ["number", "null"]},
      "lt_target": {"type": ["number", "null"], "description": "Long-term target if given"},
      "stop_loss": {"type": "number"},
      "timeframe": {"type": ["string", "null"]},
      "rationale": {"type": ["string", "null"], "description": "Key technical reasoning"},
      "technical_params": {
        "type": ["object", "null"],
        "properties": {
          "ema_200": {"type": ["number", "null"]},
          "rsi_14": {"type": ["number", "null"]},
          "support_1": {"type": ["number", "null"]},
          "support_2": {"type": ["number", "null"]},
          "resistance_1": {"type": ["number", "null"]},
          "resistance_2": {"type": ["number", "null"]}
        }
      }
    }
  }
}
```

Processing:
- Use claude-3-5-haiku, max_tokens=800
- Convert all price values to Decimal(str(v))
- Check if idea already exists: SELECT id FROM de_goldilocks_stock_ideas
  WHERE document_id = %s — skip if exists (idempotent)
- INSERT into de_goldilocks_stock_ideas with status='active'
- Return True on success

**3. extract_sector_views(document_id: UUID, raw_text: str, session: AsyncSession) -> bool**

For Sector Trends and Fortnightly reports. Extracts multiple sector views.

Claude tool schema:
```json
{
  "name": "extract_sector_views",
  "description": "Extract sector analysis from Sector Trends or Fortnightly report",
  "input_schema": {
    "type": "object",
    "required": ["report_date", "sectors"],
    "properties": {
      "report_date": {"type": "string"},
      "sectors": {
        "type": "array",
        "items": {
          "type": "object",
          "required": ["sector"],
          "properties": {
            "sector": {"type": "string"},
            "trend": {"type": ["string", "null"]},
            "outlook": {"type": ["string", "null"]},
            "rank": {"type": ["integer", "null"]},
            "top_picks": {
              "type": "array",
              "items": {
                "type": "object",
                "properties": {
                  "symbol": {"type": "string"},
                  "resistance_levels": {"type": "array", "items": {"type": "number"}}
                }
              }
            }
          }
        }
      }
    }
  }
}
```

Processing:
- INSERT each sector into de_goldilocks_sector_view ON CONFLICT (report_date, sector) DO UPDATE

**4. extract_general_views(document_id: UUID, raw_text: str, report_type: str, session: AsyncSession) -> bool**

Reuse existing claude_extract.py tool schema and pattern to extract general market views
into de_qual_extracts. This runs for ALL document types as the "base layer" extraction.
Con-calls and Big Picture reports will be richest here.

- Call the existing general extraction function from claude_extract.py (do not duplicate)
- If the function signature needs document_id + raw_text, use it as-is
- The result goes into de_qual_extracts (existing table, existing logic)

### run_goldilocks_extraction.py — Backfill Script

Async script using asyncio.run() + SQLAlchemy async session.

**Cost guard:**
- Accept --max-docs argument (default: 200)
- Track count. Exit after max-docs processed with "Cost guard: limit reached" message.
- This prevents runaway API costs on first backfill

**Flow:**
1. Open async DB session
2. Query documents ready for extraction:
   SELECT id, report_type, raw_text, processing_status
   FROM de_qual_documents
   WHERE source_name = 'Goldilocks Research'
   AND raw_text IS NOT NULL
   AND LENGTH(raw_text) > 100
   AND processing_status = 'pending'
   ORDER BY created_at
   LIMIT :max_docs
3. For each document, dispatch based on report_type:
   - 'trend_friend' → extract_trend_friend() + extract_general_views()
   - 'stock_bullet', 'big_catch' → extract_stock_idea() + extract_general_views()
   - 'sector_trends', 'fortnightly' → extract_sector_views() + extract_general_views()
   - 'concall', 'sound_byte', 'big_picture', 'qa', 'snippet', 'usa_report' → extract_general_views() only
   - None/unclassified: log warning, set processing_status='skipped', continue
4. On success: UPDATE de_qual_documents SET processing_status='done', updated_at=NOW()
5. On any exception: UPDATE de_qual_documents SET processing_status='failed', updated_at=NOW()
   Log full traceback with document_id and report_type
6. Print summary: done, failed, skipped, cost_guard_hit

**Rate limiting:**
- Add asyncio.sleep(0.5) between Claude calls to avoid rate limit errors
- Do not parallelize Claude calls — process documents sequentially

### Modifications to claude_extract.py

- Add the Goldilocks tool definitions (trend_friend, stock_idea, sector_views) as module-level
  constants, importable by goldilocks_extractor.py
- Do NOT change existing extraction logic — only add new tool definitions

### tests/pipelines/qualitative/test_goldilocks_extractor.py

**Test: test_extract_trend_friend_happy_path**
- Mock anthropic client to return a canned tool_use response with all fields populated
- Call extract_trend_friend() with fixture raw_text
- Assert de_goldilocks_market_view has one row with correct values
- Assert de_goldilocks_sector_view has expected sector rows
- Assert all price values are Decimal instances (not float)

**Test: test_extract_trend_friend_partial_fields**
- Mock Claude returning only required fields (trend_direction), all others null
- Assert INSERT succeeds with NULLs, does not raise

**Test: test_extract_stock_idea_happy_path**
- Mock Claude returning full stock idea
- Assert de_goldilocks_stock_ideas row created with status='active'
- Assert entry_price is Decimal

**Test: test_extract_stock_idea_idempotent**
- Run extract_stock_idea() twice with same document_id
- Assert only one row in de_goldilocks_stock_ideas (second call is skipped)

**Test: test_extract_sector_views_multiple_sectors**
- Mock Claude returning 5 sectors
- Assert 5 rows in de_goldilocks_sector_view

**Test: test_run_extraction_cost_guard**
- Insert 10 pending documents into test DB
- Run run_goldilocks_extraction with --max-docs=3
- Assert only 3 documents processed (status='done'), 7 remain 'pending'

## Acceptance Criteria
- [ ] Trend Friend reports populate de_goldilocks_market_view with S/R levels, trend, strength
- [ ] Trend Friend sector sections populate de_goldilocks_sector_view (all sectors, not just top 3)
- [ ] Stock Bullet / Big Catch populate de_goldilocks_stock_ideas with all price levels as Decimal
- [ ] Con-call transcripts, Big Picture, Q&A → de_qual_extracts with direction/conviction/timeframe
- [ ] quality_score >= 0.70 mean across all Trend Friend extractions (validated after backfill)
- [ ] processing_status updated to 'done' or 'failed' for every attempted document
- [ ] Backfill handles all 79 documents (in batches respecting --max-docs guard)
- [ ] All numeric/price values stored as Decimal — never float in DB
- [ ] Claude called with claude-3-5-haiku (not Opus) for cost efficiency
- [ ] ON CONFLICT upsert: re-running does not create duplicate rows
- [ ] All 6 tests pass: `pytest tests/pipelines/qualitative/test_goldilocks_extractor.py -v`
- [ ] `ruff check . --select E,F,W` passes on all 3 modified/new files
