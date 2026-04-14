# JIP Morningstar client uses the wrong endpoint — IND-C9 / IND-C10 blocker

**Date**: 2026-04-14
**Priority**: P1 (blocks MF indicators v2)
**Discovered during**: IND-C9 (fetching PurchaseMode for MF eligibility filter)

## Symptom
`app/pipelines/morningstar/client.py::fetch(id_type, identifier, datapoints)` builds URLs of the shape:

```
GET https://api.morningstar.com/v2/service/mf/{IdType}/{Identifier}?datapoints=...&accesscode=...
```

Every call returns **HTTP 404** with body:
```json
{"timestamp":1776179196530,"status":404,"error":"Not Found","path":"/v2/service/mf/FundId/F00000PDZV"}
```

Tested with multiple known-good `mstar_id`s (HDFC Flexi Cap Direct Growth = `F00000PDZV`, etc.) and both `id_type="FundId"` and `id_type="ISIN"`. All 404.

## Independent confirmation
`de_mf_master.primary_benchmark` is **0 / 13,380** populated even though `Benchmark` is one of the datapoints JIP's `fund_master.py` pipeline fetches. Expense ratio and category are populated, but those likely come from a different seed path (AMFI or an initial CSV import) — not from a working Morningstar client.

Conclusion: JIP's Morningstar client has never successfully fetched data via this endpoint pattern.

## Root cause
The mfpulse_reimagined repo uses a **completely different** endpoint structure. From `backend/app/services/morningstar_fetcher.py`:

```python
url = f"{API_BASE}/{api.hash}/universeid/{UNIVERSE_CODE}?accesscode={self._access_code}"
```

- Uses a `universeid/{UNIVERSE_CODE}` endpoint (bulk fetch for the whole fund universe)
- Each API variant has a separate `hash` (`MORNINGSTAR_HASH_HOLDINGS`, `MORNINGSTAR_HASH_PORTFOLIO_SUMMARY`, etc.) — listed in `backend/app/core/morningstar_config.py`
- No per-fund `/FundId/{id}` path at all

JIP's client was likely written against a hypothetical endpoint pattern that Morningstar never exposed for this API tier, or that was deprecated. Either way it has been broken since day one.

## Impact on the indicators v2 build
- **IND-C9** (purchase_mode bootstrap) — blocked. Cannot fetch `PurchaseMode` via the existing client.
- **IND-C10** (MF technical indicators) — blocked transitively. The eligibility filter needs `purchase_mode=1` (Regular plan), and the only source is Morningstar or mfpulse's DB.

## Workarounds (ordered by effort)

### Option A: Bootstrap from mfpulse's DB (quickest path to MF indicators)
`mfpulse_reimagined` already has `fund_master.purchase_mode` populated for the same `mstar_id` universe. A one-off SQL COPY from mfpulse's RDS into JIP's `de_mf_master.purchase_mode` unblocks IND-C10 immediately.
**Requires**: mfpulse RDS credentials (not currently in JIP's `.env`). User must provide.

### Option B: Rewrite JIP's Morningstar client to use the mfpulse bulk pattern
Copy `morningstar_fetcher.py` from mfpulse into `app/pipelines/morningstar/`, wire up the API hashes in settings, update `fund_master.py` to consume the bulk format. 1–2 days of work. Authoritative and persistent (weekly refreshes stay current).
**Requires**: the production Morningstar API hashes (one per datapoint group). User or ops team to provide.

### Option C: Defer MF indicators indefinitely
Ship indicators v2 without MF coverage. Equities, indices, ETFs, globals all work. MF can be added once either Option A or B is unblocked externally.
**Requires**: nothing. This is the default if the user doesn't act on A or B.

## Current state (as of this report)
- `alembic/versions/007_add_purchase_mode_to_mf_master.py` is committed and applied to production — column exists, just NULL
- `app/pipelines/morningstar/fund_master.py` extended to read `PurchaseMode` datapoint — ready to work the moment the client is fixed or the column is bootstrapped
- `scripts/fetch_purchase_mode_from_morningstar.py` written and ready — tested, but cannot succeed against the current client
- `app/computation/indicators_v2/runner.py` excludes MF from the daily pipeline — will automatically pick it up once an MF wrapper is added and registered

## Recommendation
Option C (defer) for now. Option A (mfpulse bootstrap) is the shortest path if the user wants MF coverage soon. Option B (proper rewrite) is the right medium-term fix but too big for the indicators v2 scope.
