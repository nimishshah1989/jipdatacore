# Index Source Inventory — 2026-04-15

## Summary

53 indices in `de_index_master` have ≤5 days of data in `de_index_prices`.
Before scraping, we checked all available sister-project databases for existing
historical data that could be ingested directly.

## Sources Checked

### 1. fie2 (SSH to 13.206.34.214)

- **Result**: INACCESSIBLE — SSH public-key authentication failed.
- No SSH key configured for this host from the current environment.
- Cannot query `fie2.index_prices` directly.

### 2. fie_v3 (RDS: fie-db.c7osw6q6kwmw.ap-south-1.rds.amazonaws.com)

- **Result**: AUTH FAILED — `fie_admin` password rejected; pg_hba.conf also
  blocks unencrypted connections from 13.206.34.214.
- Cannot query any tables.

### 3. mfpulse_reimagined (Docker container `mf-pulse` on localhost)

- **Result**: Tables exist (`index_master`, `index_daily`) but contain **zero rows**.
- `index_daily` schema: `index_id, price_date, close_price, return_1d..return_10y`
  — only close price, no OHLCV. Even if populated, would not satisfy our needs.

## Decision

All three sister-project sources are either inaccessible or empty.
Backfill proceeds via **niftyindices.com historical POST API** — the same
source already proven in `app/pipelines/indices/index_backfill.py`.

Fallback to NSE bhav-copy archive
(`archives.nseindia.com/content/indices/ind_close_all_<DDMMYYYY>.csv`) is
available if niftyindices.com rate-limits or returns empty for specific indices.

## Indices Requiring Backfill (< 250 days)

| index_code | index_name | category | current_rows |
|---|---|---|---|
| NIFTY FIN SERVICE | NIFTY FINANCIAL SERVICES | sectoral | 5 |
| NIFTY RAILWAYSPSU | NIFTY INDIA RAILWAYS PSU | thematic | 5 |
| NIFTY DIV OPPS 50 | NIFTY DIVIDEND OPPORTUNITIES 50 | strategy | 5 |
| NIFTY GROWSECT 15 | NIFTY GROWTH SECTORS 15 | strategy | 5 |
| NIFTY100 QUALTY30 | NIFTY100 QUALITY 30 | strategy | 5 |
| NIFTY50 PR 2X LEV | NIFTY50 PR 2X LEVERAGE | strategy | 5 |
| NIFTY50 TR 1X INV | NIFTY50 TR 1X INVERSE | strategy | 5 |
| NIFTY50 PR 1X INV | NIFTY50 PR 1X INVERSE | strategy | 5 |
| NIFTY50 DIV POINT | NIFTY50 DIVIDEND POINTS | strategy | 5 |
| NIFTY50 EQL WGT | NIFTY50 EQUAL WEIGHT | strategy | 5 |
| NIFTY INTERNET | NIFTY INDIA INTERNET | thematic | 5 |
| NIFTY MULTI MFG | NIFTY500 MULTICAP INDIA MANUFACTURING 50:30:20 | thematic | 5 |
| NIFTY MID SELECT | NIFTY MIDCAP SELECT | broad | 5 |
| NIFTY SME EMERGE | NIFTY SME EMERGE | thematic | 5 |
| NIFTY SMLCAP 100 | NIFTY SMALLCAP 100 | broad | 5 |
| NIFTY IPO | NIFTY IPO | thematic | 5 |
| NIFTY HOUSING | NIFTY HOUSING | thematic | 5 |
| NIFTY500 MULTICAP | NIFTY500 MULTICAP 50:25:25 | broad | 5 |
| NIFTY LARGEMID250 | NIFTY LARGEMIDCAP 250 | broad | 5 |
| NIFTY TOTAL MKT | NIFTY TOTAL MARKET | broad | 5 |
| NIFTY FPI 150 | NIFTY INDIA FPI 150 | broad | 5 |
| NIFTY FINSRV25 50 | NIFTY FINANCIAL SERVICES 25/50 | sectoral | 5 |
| NIFTY PVT BANK | NIFTY PRIVATE BANK | sectoral | 5 |
| NIFTY HEALTHCARE | NIFTY HEALTHCARE INDEX | sectoral | 5 |
| NIFTY CONSR DURBL | NIFTY CONSUMER DURABLES | sectoral | 5 |
| NIFTY OIL AND GAS | NIFTY OIL & GAS | sectoral | 5 |
| NIFTY100 EQL WGT | NIFTY100 EQUAL WEIGHT | strategy | 5 |
| NIFTY100 LOWVOL30 | NIFTY100 LOW VOLATILITY 30 | strategy | 5 |
| NIFTY200MOMENTM30 | NIFTY200 MOMENTUM 30 | strategy | 5 |
| INDIA VIX | INDIA VIX | thematic | 5 |
| NIFTY IND DEFENCE | NIFTY INDIA DEFENCE | thematic | 5 |
| NIFTY CONSUMPTION | NIFTY INDIA CONSUMPTION | thematic | 5 |
| NIFTY INFRA | NIFTY INFRASTRUCTURE | thematic | 5 |
| NIFTY100 LIQ 15 | NIFTY100 LIQUID 15 | thematic | 5 |
| NIFTY MID LIQ 15 | NIFTY MIDCAP LIQUID 15 | thematic | 5 |
| NIFTY SERV SECTOR | NIFTY SERVICES SECTOR | thematic | 5 |
| NIFTY100ESGSECLDR | NIFTY100 ESG SECTOR LEADERS | thematic | 5 |
| NIFTY IND DIGITAL | NIFTY INDIA DIGITAL | thematic | 5 |
| NIFTY MULTI INFRA | NIFTY500 MULTICAP INFRASTRUCTURE 50:30:20 | thematic | 5 |
| NIFTY INDIA MFG | NIFTY INDIA MANUFACTURING | thematic | 5 |
| NIFTY TATA 25 CAP | NIFTY INDIA CORPORATE GROUP INDEX - TATA GROUP 25% CAP | thematic | 5 |
| NIFTY MICROCAP250 | NIFTY MICROCAP 250 | broad | 31 |
| NIFTY50 TR 2X LEV | NIFTY50 TR 2X LEVERAGE | strategy | 32 |
| NIFTY MIDSML HLTH | NIFTY MIDSMALL HEALTHCARE | sectoral | 33 |
| NIFTY EV | NIFTY EV & NEW AGE AUTOMOTIVE | thematic | 33 |
| NIFTY CAPITAL MKT | NIFTY CAPITAL MARKETS | sectoral | 41 |
| NIFTY TRANS LOGIS | NIFTY TRANSPORTATION & LOGISTICS | thematic | 164 |

Also partial (< 250 days):
| NIFTY50 SHARIAH | NIFTY50 SHARIAH | thematic | 274 — barely above threshold but gaps likely |

### 7 Critical Sectoral Indices (need ≥ 2,000 days)

| index_code | current_rows | gap |
|---|---|---|
| NIFTY PHARMA | 1,139 | needs ~861 more |
| NIFTY REALTY | 1,880 | needs ~120 more |
| NIFTY PVT BANK | 5 | needs ~1,995 more |
| NIFTY OIL AND GAS | 5 | needs ~1,995 more |
| NIFTY HEALTHCARE | 5 | needs ~1,995 more |
| NIFTY CONSR DURBL | 5 | needs ~1,995 more |
| NIFTY FIN SERVICE | 5 | needs ~1,995 more |
