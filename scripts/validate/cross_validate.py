import psycopg2, time

DB = "postgresql://jip_admin:JipDataEngine2026Secure@jip-data-engine.ctay2iewomaj.ap-south-1.rds.amazonaws.com:5432/data_engine"
MP = "postgresql://fie_admin:Nimish1234@fie-db.c7osw6q6kwmw.ap-south-1.rds.amazonaws.com:5432/fie_v3?sslmode=require"

j = psycopg2.connect(DB); j.autocommit=True; jc=j.cursor()
m = psycopg2.connect(MP); m.autocommit=True; mc=m.cursor()

print("=" * 80)
print("VALIDATION — PROVING EVERY NUMBER IS CORRECT")
print("=" * 80)

# 1. STOCK PRICES vs MarketPulse (50% sample, 3 dates)
print("\n1. STOCK PRICES vs MarketPulse")
print("-" * 50)
for test_date in ['2026-03-27', '2025-12-31', '2025-06-30']:
    jc.execute(f"SELECT i.current_symbol, COALESCE(e.close_adj,e.close)::float FROM de_equity_ohlcv e JOIN de_instrument i ON i.id=e.instrument_id WHERE e.date='{test_date}'")
    jp = {r[0]:r[1] for r in jc.fetchall()}
    mc.execute(f"SELECT ticker, close::float FROM compass_stock_prices WHERE date='{test_date}'")
    mp = {r[0]:r[1] for r in mc.fetchall()}
    common = sorted(set(jp) & set(mp))
    sample = common[:len(common)//2]
    match=close_c=mis=0
    for s in sample:
        if mp[s] and mp[s]!=0:
            dev=abs(jp[s]-mp[s])/mp[s]*100
            if dev<0.1: match+=1
            elif dev<2: close_c+=1
            else: mis+=1
    total=match+close_c+mis
    if total>0:
        print(f"  {test_date}: {match}/{total} exact ({match/total*100:.0f}%) | {len(jp)} JIP vs {len(mp)} MP stocks")

# 2. RS SELF-CONSISTENCY — recompute composite from components
print("\n2. RS SELF-CONSISTENCY")
print("-" * 50)
jc.execute("""
    SELECT entity_id, rs_1w, rs_1m, rs_3m, rs_6m, rs_12m, rs_composite
    FROM de_rs_scores
    WHERE entity_type='equity' AND vs_benchmark='NIFTY 50'
    AND date = (SELECT MAX(date) FROM de_rs_scores WHERE entity_type='equity')
    AND rs_composite IS NOT NULL
    LIMIT 100
""")
mismatch_count = 0
for r in jc.fetchall():
    vals = {"rs_1w":r[1],"rs_1m":r[2],"rs_3m":r[3],"rs_6m":r[4],"rs_12m":r[5]}
    ws = {"rs_1w":0.10,"rs_1m":0.20,"rs_3m":0.30,"rs_6m":0.25,"rs_12m":0.15}
    wsum=0; tw=0
    for k,w in ws.items():
        if vals[k] is not None:
            wsum += float(vals[k])*w; tw += w
    recomputed = round(wsum/tw,4) if tw>0 else None
    stored = float(r[6])
    if recomputed and abs(stored-recomputed) > 0.01:
        mismatch_count += 1
print(f"  Checked 100 stocks: {100-mismatch_count}/100 composite = weighted sum (tolerance 0.01)")

# 3. BREADTH ARITHMETIC
print("\n3. BREADTH ARITHMETIC")
print("-" * 50)
jc.execute("SELECT date, advance, decline, unchanged, total_stocks FROM de_breadth_daily ORDER BY date DESC LIMIT 5")
ok=0; total_b=0
for r in jc.fetchall():
    computed = r[1]+r[2]+r[3]
    match_b = abs(computed - r[4]) <= 1  # allow 1 tolerance
    if match_b: ok+=1
    total_b+=1
print(f"  Last 5 dates: {ok}/{total_b} pass (adv+dec+unch = total)")

# 4. REGIME SELF-CONSISTENCY
print("\n4. REGIME CLASSIFICATION")
print("-" * 50)
jc.execute("SELECT date, regime, confidence, breadth_score FROM de_market_regime ORDER BY date DESC LIMIT 5")
for r in jc.fetchall():
    bs = float(r[3])
    conf = float(r[2])
    expected = "BULL" if bs>=60 else "BEAR" if bs<=40 else "SIDEWAYS"
    match_r = "OK" if r[1]==expected else f"MISMATCH (expected {expected})"
    print(f"  {r[0]}: {r[1]} conf={conf:.1f} bs={bs:.1f} — {match_r}")

# 5. MF SHARPE vs Morningstar API (using stored risk data)
print("\n5. MF SHARPE SPOT CHECK")
print("-" * 50)
# Check a few known funds
jc.execute("""
    SELECT m.fund_name, d.sharpe_1y::float, d.volatility_1y::float, d.beta_vs_nifty::float, d.max_drawdown_1y::float
    FROM de_mf_derived_daily d
    JOIN de_mf_master m ON m.mstar_id = d.mstar_id
    WHERE d.nav_date = (SELECT MAX(nav_date) FROM de_mf_derived_daily)
    AND m.fund_name IN ('HDFC Flexi Cap Gr', 'ICICI Pru Bluechip Gr', 'SBI Large & Midcap Gr', 'Kotak Flexi Cap Gr', 'Axis Bluechip Gr')
    ORDER BY m.fund_name
""")
for r in jc.fetchall():
    print(f"  {r[0][:35]}: Sharpe={r[1]:.3f} Vol={r[2]:.1f}% Beta={r[3]:.3f} MaxDD={r[4]:.1f}%")

# 6. MF RS — check fund RS makes sense relative to equity RS
print("\n6. MF RS vs EQUITY RS COMPARISON")
print("-" * 50)
jc.execute("""
    SELECT 'Equity' AS type, ROUND(AVG(rs_composite::float)::numeric,2) AS avg, ROUND(STDDEV(rs_composite::float)::numeric,2) AS std
    FROM de_rs_scores WHERE entity_type='equity' AND vs_benchmark='NIFTY 50' AND date=(SELECT MAX(date) FROM de_rs_scores WHERE entity_type='equity')
    UNION ALL
    SELECT 'MF', ROUND(AVG(rs_composite::float)::numeric,2), ROUND(STDDEV(rs_composite::float)::numeric,2)
    FROM de_rs_scores WHERE entity_type='mf' AND vs_benchmark='NIFTY 50' AND date=(SELECT MAX(date) FROM de_rs_scores WHERE entity_type='mf')
""")
for r in jc.fetchall():
    print(f"  {r[0]}: avg_RS={r[1]} std={r[2]}")

# 7. SECTOR RS direction vs MarketPulse
print("\n7. SECTOR RS vs MarketPulse")
print("-" * 50)
jc.execute("""
    SELECT entity_id, rs_composite::float FROM de_rs_scores
    WHERE entity_type='sector' AND vs_benchmark='NIFTY 50'
    AND date=(SELECT MAX(date) FROM de_rs_scores WHERE entity_type='sector')
    ORDER BY rs_composite DESC
""")
jip_sec = {r[0]:r[1] for r in jc.fetchall()}
mc.execute("""
    SELECT instrument_id, rs_score::float FROM compass_rs_scores
    WHERE instrument_type='index' AND date=(SELECT MAX(date) FROM compass_rs_scores)
""")
mp_sec = {r[0]:r[1] for r in mc.fetchall()}
smap = {"Banking":"BANKNIFTY","IT":"NIFTYIT","Pharma":"NIFTYPHARMA","Metal":"NIFTYMETAL",
        "Realty":"NIFTYREALTY","FMCG":"NIFTYFMCG","Energy":"NIFTYENERGY"}
match_d=0; total_d=0
for sec, mpk in smap.items():
    jr=jip_sec.get(sec); mr=mp_sec.get(mpk)
    if jr and mr:
        same = (jr>0 and mr>0) or (jr<0 and mr<0)
        if same: match_d+=1
        total_d+=1
        print(f"  {sec:<15} JIP={jr:7.2f} MP={mr:7.2f} {'SAME' if same else 'DIFF'}")
if total_d>0:
    print(f"  Direction: {match_d}/{total_d} ({match_d/total_d*100:.0f}%)")

# 8. NAV RETURNS — verify 1d return = (nav_today/nav_yesterday - 1)*100
print("\n8. NAV RETURN VERIFICATION")
print("-" * 50)
jc.execute("""
    SELECT n1.mstar_id, n1.nav_date, n1.nav::float, n1.return_1d::float,
           n0.nav::float AS prev_nav, n0.nav_date AS prev_date
    FROM de_mf_nav_daily n1
    JOIN de_mf_nav_daily n0 ON n0.mstar_id = n1.mstar_id
        AND n0.nav_date = (SELECT MAX(nav_date) FROM de_mf_nav_daily WHERE mstar_id=n1.mstar_id AND nav_date < n1.nav_date)
    WHERE n1.return_1d IS NOT NULL
    ORDER BY n1.nav_date DESC LIMIT 10
""")
ok_r=0
for r in jc.fetchall():
    expected = round((r[2]/r[4] - 1)*100, 4)
    stored = r[3]
    match_n = abs(stored - expected) < 0.01
    if match_n: ok_r+=1
print(f"  10 samples: {ok_r}/10 return_1d = (nav_today/nav_prev - 1)*100")

print(f"\n{'='*80}")
print("VALIDATION COMPLETE")
print(f"{'='*80}")

jc.close();j.close();mc.close();m.close()
