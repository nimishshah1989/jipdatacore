import psycopg2, time

JIP = "postgresql://jip_admin:JipDataEngine2026Secure@jip-data-engine.ctay2iewomaj.ap-south-1.rds.amazonaws.com:5432/data_engine"
MP = "postgresql://fie_admin:Nimish1234@fie-db.c7osw6q6kwmw.ap-south-1.rds.amazonaws.com:5432/fie_v3?sslmode=require"

j = psycopg2.connect(JIP); j.autocommit = True; jc = j.cursor()
m = psycopg2.connect(MP); m.autocommit = True; mc = m.cursor()

t0 = time.time()

# 1. Stock price match — recent date
print("=== STOCK PRICE CROSS-CHECK ===")
test_date = "2026-03-27"

jc.execute(f"SELECT i.current_symbol, COALESCE(e.close_adj, e.close)::float FROM de_equity_ohlcv e JOIN de_instrument i ON i.id = e.instrument_id WHERE e.date = '{test_date}'")
jip_prices = {r[0]: r[1] for r in jc.fetchall()}

mc.execute(f"SELECT ticker, close::float FROM compass_stock_prices WHERE date = '{test_date}'")
mp_prices = {r[0]: r[1] for r in mc.fetchall()}

common = sorted(set(jip_prices) & set(mp_prices))
sample = common[:len(common)//2]  # 50%

match = close = mismatch = 0
mismatches = []
for sym in sample:
    jp = jip_prices[sym]; mp = mp_prices[sym]
    if mp and mp != 0:
        dev = abs(jp - mp) / mp * 100
        if dev < 0.1: match += 1
        elif dev < 2: close += 1
        else:
            mismatch += 1
            if len(mismatches) < 5:
                mismatches.append(f"  {sym}: JIP={jp:.2f} MP={mp:.2f} dev={dev:.1f}%")

total = match + close + mismatch
print(f"Date: {test_date}")
print(f"JIP stocks: {len(jip_prices)} | MP stocks: {len(mp_prices)} | Common: {len(common)} | Sampled: {len(sample)}")
print(f"Exact (<0.1%):  {match}/{total} ({match/total*100:.1f}%)")
print(f"Close (0.1-2%): {close}/{total}")
print(f"Mismatch (>2%): {mismatch}/{total}")
if mismatches:
    print("Sample mismatches:")
    for m_str in mismatches: print(m_str)

# 2. Historical date check
hist_date = "2025-06-30"
jc.execute(f"SELECT i.current_symbol, COALESCE(e.close_adj, e.close)::float FROM de_equity_ohlcv e JOIN de_instrument i ON i.id = e.instrument_id WHERE e.date = '{hist_date}'")
jip_h = {r[0]: r[1] for r in jc.fetchall()}
mc.execute(f"SELECT ticker, close::float FROM compass_stock_prices WHERE date = '{hist_date}'")
mp_h = {r[0]: r[1] for r in mc.fetchall()}
common_h = set(jip_h) & set(mp_h)
h_match = sum(1 for s in common_h if mp_h[s] != 0 and abs(jip_h[s] - mp_h[s]) / mp_h[s] * 100 < 0.1)
print(f"\nHistorical ({hist_date}): {h_match}/{len(common_h)} exact ({h_match/len(common_h)*100:.0f}%)")
print(f"  JIP: {len(jip_h)} stocks | MP: {len(mp_h)} stocks")

# 3. Sector RS comparison
print("\n=== SECTOR RS COMPARISON ===")
jc.execute("""
    SELECT entity_id, rs_composite::float FROM de_rs_scores 
    WHERE entity_type = 'sector' AND vs_benchmark = 'NIFTY 50'
    AND date = (SELECT MAX(date) FROM de_rs_scores WHERE entity_type = 'sector')
    ORDER BY rs_composite DESC
""")
jip_sectors = {r[0]: r[1] for r in jc.fetchall()}

mc.execute("""
    SELECT instrument_id, rs_score::float FROM compass_rs_scores 
    WHERE instrument_type = 'index'
    AND date = (SELECT MAX(date) FROM compass_rs_scores)
""")
mp_sectors = {r[0]: r[1] for r in mc.fetchall()}

SECTOR_MAP = {
    "Banking": "BANKNIFTY", "IT": "NIFTYIT", "Pharma": "NIFTYPHARMA",
    "Metal": "NIFTYMETAL", "Realty": "NIFTYREALTY", "FMCG": "NIFTYFMCG",
    "Energy": "NIFTYENERGY", "Media": "NIFTYMEDIA", "Automobile": "NIFTYAUTO",
    "Oil & Gas": "NIFTYOILGAS", "Healthcare": "NIFTYHEALTHCARE",
    "Infrastructure": "NIFTYINFRA", "Consumer Durables": "NIFTYCONSUMERDURABLES",
    "Consumption": "NIFTYCONSUMPTION", "Capital Markets": "NIFTYCAPITALMARKETS",
    "Financial Services": "NIFTYFINSERVICE", "Defence": "NIFTYINDIADEFENCE",
    "Tourism": "NIFTYINDIATOURISM", "Chemicals": "NIFTYCHEMICALS",
    "Digital": "NIFTYINDIGITAL",
}

print(f"{'Sector':<25} {'JIP RS':>8} {'MP RS':>8} {'Direction':>10}")
print("-" * 55)
match_dir = 0; total_comp = 0
for sector, mp_key in sorted(SECTOR_MAP.items()):
    jr = jip_sectors.get(sector)
    mr = mp_sectors.get(mp_key)
    if jr is not None and mr is not None:
        same = (jr > 0 and mr > 0) or (jr < 0 and mr < 0) or (abs(jr) < 2 and abs(mr) < 2)
        d = "SAME" if same else "DIFF"
        if same: match_dir += 1
        total_comp += 1
        print(f"{sector:<25} {jr:8.2f} {mr:8.2f} {d:>10}")
    elif jr is not None:
        print(f"{sector:<25} {jr:8.2f} {'--':>8}")
if total_comp > 0:
    print(f"\nDirection match: {match_dir}/{total_comp} ({match_dir/total_comp*100:.0f}%)")

# 4. Breadth comparison
print("\n=== BREADTH COMPARISON ===")
jc.execute("SELECT date, advance, decline, total_stocks FROM de_breadth_daily WHERE date = '2026-03-27'")
jr = jc.fetchone()
mc.execute("SELECT date, metric, count, total FROM breadth_daily WHERE date = '2026-03-27' LIMIT 5")
mr = mc.fetchall()
if jr:
    print(f"JIP: date={jr[0]} adv={jr[1]} dec={jr[2]} total={jr[3]}")
if mr:
    for r in mr:
        print(f"MP:  date={r[0]} metric={r[1]} count={r[2]} total={r[3]}")
else:
    print("MP: No breadth data for this date")

print(f"\n=== CROSS-VALIDATION COMPLETE in {time.time()-t0:.0f}s ===")

jc.close(); j.close(); mc.close(); m.close()
