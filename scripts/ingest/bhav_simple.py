"""Simple BHAV re-ingestion for 2025-2026. Downloads and upserts directly."""
import psycopg2, httpx, asyncio, time, io, csv
from datetime import date, timedelta

DB = "postgresql://jip_admin:JipDataEngine2026Secure@jip-data-engine.ctay2iewomaj.ap-south-1.rds.amazonaws.com:5432/data_engine"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Referer": "https://www.nseindia.com/"}

async def main():
    t0 = time.time()
    conn = psycopg2.connect(DB)
    conn.autocommit = True
    cur = conn.cursor()
    
    # Load symbol map
    cur.execute("SELECT current_symbol, id FROM de_instrument")
    sym_map = {r[0]: str(r[1]) for r in cur.fetchall()}
    print(f"Symbol map: {len(sym_map)} instruments")
    
    # Get trading days for 2025-2026
    cur.execute("SELECT date FROM de_trading_calendar WHERE is_trading = TRUE AND date >= '2025-01-01' AND date <= '2026-04-06' ORDER BY date")
    trading_days = [r[0] for r in cur.fetchall()]
    print(f"Trading days: {len(trading_days)}")
    
    total_ingested = 0
    failed_dates = []
    
    async with httpx.AsyncClient(timeout=30) as client:
        for i, d in enumerate(trading_days):
            # URL format for recent data
            url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{d.strftime('%d%m%Y')}.csv"
            try:
                r = await client.get(url, headers=HEADERS)
                if r.status_code != 200:
                    failed_dates.append(d)
                    continue
                
                # Parse CSV
                reader = csv.DictReader(io.StringIO(r.text.strip()))
                rows = []
                for row in reader:
                    sym = (row.get("SYMBOL") or "").strip()
                    series = (row.get(" SERIES") or row.get("SERIES") or "").strip()
                    if not sym or series not in ("EQ", "BE", "BZ"):
                        continue
                    iid = sym_map.get(sym)
                    if not iid:
                        continue
                    try:
                        close = float(row.get(" CLOSE_PRICE") or row.get("CLOSE_PRICE") or row.get(" LAST_PRICE") or 0)
                        open_p = float(row.get(" OPEN_PRICE") or row.get("OPEN_PRICE") or 0)
                        high = float(row.get(" HIGH_PRICE") or row.get("HIGH_PRICE") or 0)
                        low = float(row.get(" LOW_PRICE") or row.get("LOW_PRICE") or 0)
                        volume = int(float(row.get(" TTL_TRD_QNTY") or row.get("TTL_TRD_QNTY") or 0))
                        if close <= 0:
                            continue
                        rows.append((d, iid, sym, open_p, high, low, close, close, open_p, high, low, volume, "validated"))
                    except (ValueError, TypeError):
                        continue
                
                if rows:
                    # Bulk upsert via COPY + staging
                    buf = io.StringIO()
                    for row in rows:
                        buf.write("\t".join(str(x) for x in row) + "\n")
                    buf.seek(0)
                    
                    cur.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_bhav (date DATE, instrument_id UUID, symbol VARCHAR, open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, close_adj NUMERIC, open_adj NUMERIC, high_adj NUMERIC, low_adj NUMERIC, volume BIGINT, data_status VARCHAR)")
                    cur.execute("TRUNCATE tmp_bhav")
                    cur.copy_from(buf, "tmp_bhav", columns=["date","instrument_id","symbol","open","high","low","close","close_adj","open_adj","high_adj","low_adj","volume","data_status"])
                    
                    cur.execute("""
                        INSERT INTO de_equity_ohlcv (date, instrument_id, symbol, open, high, low, close, close_adj, open_adj, high_adj, low_adj, volume, data_status)
                        SELECT * FROM tmp_bhav
                        ON CONFLICT (date, instrument_id) DO UPDATE SET
                            symbol=EXCLUDED.symbol, open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                            close=EXCLUDED.close, close_adj=EXCLUDED.close_adj, open_adj=EXCLUDED.open_adj,
                            high_adj=EXCLUDED.high_adj, low_adj=EXCLUDED.low_adj, volume=EXCLUDED.volume,
                            data_status=EXCLUDED.data_status, updated_at=NOW()
                    """)
                    total_ingested += len(rows)
                
                if (i+1) % 20 == 0:
                    elapsed = time.time() - t0
                    print(f"[{i+1}/{len(trading_days)}] {d} | {total_ingested:,} rows | {elapsed:.0f}s", flush=True)
                
                await asyncio.sleep(0.5)  # Rate limit
                
            except Exception as e:
                failed_dates.append(d)
                if len(failed_dates) <= 5:
                    print(f"  Failed {d}: {str(e)[:60]}")
    
    cur.execute("DROP TABLE IF EXISTS tmp_bhav")
    cur.close()
    conn.close()
    
    print(f"\n=== DONE in {time.time()-t0:.0f}s ===")
    print(f"Ingested: {total_ingested:,} rows")
    print(f"Failed dates: {len(failed_dates)}")

asyncio.run(main())
