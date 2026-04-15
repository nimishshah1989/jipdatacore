# GAP-21 — Extend `/api/v1/instrument/{symbol}` deepdive with BSE sections

## Goal
GAP-15 delivers a deepdive endpoint with 7 sections (instrument/fundamentals/
price/technicals/risk/relative_strength/sector_peers/recent_news). This
chunk adds the new BSE-sourced sections so Atlas can render a
fund-manager-grade single-page dossier from one API call.

## New response sections

```
{
  ...existing 7 sections,
  "announcements": [
    {"dt": "2026-04-14T09:30:00+05:30", "headline": "...", "category": "Result",
     "subcategory": "Audited", "pdf": "..."},
    ...  // last 20 from de_bse_announcements
  ],
  "corp_actions": {
    "upcoming": [
      {"type": "Dividend", "amount_per_share": 10.0, "ex_date": "2026-05-15",
       "record_date": "2026-05-16"},
      ...  // ex_date >= today
    ],
    "recent": [
      {"type": "Bonus", "ratio": "1:1", "ex_date": "2026-01-20"},
      ...  // last 5
    ]
  },
  "result_calendar": {
    "next_result_date": "2026-05-10",
    "period": "Q4 FY26"
  },
  "shareholding": {
    "latest_quarter": "2026-03-31",
    "promoter_pct": 50.3, "promoter_pledged_pct": 0.0,
    "public_pct": 49.7, "fii_pct": 22.1, "dii_pct": 16.7,
    "mutual_funds_pct": 8.4, "retail_pct": 4.2,
    "qoq_deltas": {
      "promoter_pct": 0.0, "fii_pct": +1.2, "dii_pct": -0.4
    },
    "pledged_trend": [   // last 4 quarters
      {"quarter": "2025-06-30", "pledged_pct": 0.1},
      {"quarter": "2025-09-30", "pledged_pct": 0.05},
      {"quarter": "2025-12-31", "pledged_pct": 0.0},
      {"quarter": "2026-03-31", "pledged_pct": 0.0}
    ]
  },
  "insider_activity": {
    "last_30_days": {
      "net_value_cr": 2.5, "buy_count": 3, "sell_count": 1
    },
    "last_90_days": {"net_value_cr": 8.1, "buy_count": 7, "sell_count": 2},
    "recent_trades": [
      {"filer": "...", "category": "Promoter", "type": "Buy",
       "qty": 10000, "value_cr": 2.5, "date": "2026-04-10"},
      ...  // last 10
    ]
  },
  "sast_disclosures": [
    {"acquirer": "...", "pre_pct": 4.8, "post_pct": 5.2,
     "delta_pct": 0.4, "regulation": "Reg 7(1)", "date": "2026-04-01"},
    ...  // last 5 crossings
  ],
  "trading_flags": {
    "asm": false, "asm_stage": null,
    "gsm": false, "gsm_stage": null,
    "price_band_pct": 20.0,
    "trade_to_trade": false,
    "upper_circuit_hit_30d_count": 0,
    "lower_circuit_hit_30d_count": 0
  },
  "institutional_flow": {
    "last_30_days": {
      "bulk_net_cr": 12.3, "block_net_cr": 4.1, "total_net_cr": 16.4
    },
    "recent_deals": [
      {"date": "2026-04-14", "type": "bulk", "side": "Buy",
       "client": "...", "qty": 500000, "value_cr": 14.2},
      ...  // last 10
    ]
  }
}
```

## Scope

### Modifications
- `app/schemas/instrument_deepdive.py` — add 7 new pydantic sub-models
- `app/services/instrument_deepdive_service.py` — add 7 new async loader
  functions + wire into the main `asyncio.gather` call. Each loader is
  one SQL query, most hit `(instrument_id, date DESC) LIMIT N`.
- `tests/api/test_instrument_deepdive.py` — extend golden snapshots

### Performance target
- Still p50 ≤ 250ms (the new sections are cheap indexed lookups)
- All 14 sub-queries run concurrently via `asyncio.gather`

### Graceful degradation
- If GAP-18a/b/c tables don't exist yet or are empty, the new sections
  return `null` or `[]` — endpoint never 500s.
- Explicit `completeness_pct` in the `meta` section now factors in
  presence of BSE data (was just fundamentals before).

## Acceptance criteria
- [ ] `curl https://data.jslwealth.in/api/v1/instrument/RELIANCE` returns
  all 14 top-level sections (7 old + 7 new)
- [ ] Golden snapshot tests updated and passing
- [ ] Query plan review: no sub-query does a full table scan
  (EXPLAIN ANALYZE on all 7 new loaders)
- [ ] Response time p50 ≤ 250ms on prod
- [ ] Commit subject starts with `GAP-21`

## Steps for the inner session
1. Read current `instrument_deepdive_service.py` to understand the
   concurrent-loader pattern
2. Add 7 new loader functions returning typed pydantic models
3. Wire into the main gather call
4. Update response schema
5. Refresh golden fixtures for RELIANCE, TCS, HDFCBANK
6. EXPLAIN ANALYZE each new query — add indexes if missing
7. Commit

## Out of scope
- PDF attachment rendering
- Historical fundamentals section (GAP-17)
- Real-time announcement push (websocket) — polling is fine for Atlas

## Dependencies
- Upstream: GAP-15 (endpoint exists), GAP-18a, GAP-18b, GAP-18c
  (data tables exist)
- Downstream: Atlas UI
