import psycopg2

JIP = "postgresql://jip_admin:JipDataEngine2026Secure@jip-data-engine.ctay2iewomaj.ap-south-1.rds.amazonaws.com:5432/data_engine"
MP = "postgresql://fie_admin:Nimish1234@fie-db.c7osw6q6kwmw.ap-south-1.rds.amazonaws.com:5432/fie_v3?sslmode=require"

j = psycopg2.connect(JIP); j.autocommit = True; jc = j.cursor()
m = psycopg2.connect(MP); m.autocommit = True; mc = m.cursor()

def jq(sql):
    jc.execute(sql); return jc.fetchone()[0]
def mq(sql):
    mc.execute(sql); return mc.fetchone()[0]

print("=" * 90)
print("JIP DATA CORE — COMPLETE STATUS")
print("=" * 90)

tables_data = [
    ("de_equity_ohlcv", "date"),
    ("de_equity_technical_daily", "date"),
    ("de_rs_scores", "date"),
    ("de_breadth_daily", "date"),
    ("de_market_regime", "date"),
    ("de_mf_nav_daily", "nav_date"),
    ("de_mf_derived_daily", "nav_date"),
    ("de_mf_master", None),
    ("de_mf_holdings", None),
    ("de_mf_category_flows", None),
    ("de_instrument", None),
    ("de_index_prices", "date"),
    ("de_index_master", None),
    ("de_index_constituents", None),
    ("de_corporate_actions", None),
    ("de_global_prices", "date"),
    ("de_global_instrument_master", None),
    ("de_market_cap_history", None),
    ("de_trading_calendar", "date"),
]

for t, dc in tables_data:
    cnt = jq(f"SELECT COUNT(*) FROM {t}")
    if cnt == 0:
        print(f"{t:<45} EMPTY")
        continue
    dr = ""
    if dc:
        jc.execute(f"SELECT MIN({dc}), MAX({dc}), COUNT(DISTINCT {dc}) FROM {t}")
        row = jc.fetchone()
        dr = f" | {row[0]} to {row[1]} ({row[2]:,} days)"
    print(f"{t:<45} {cnt:>12,}{dr}")

# Technical columns
tech_cols = jq("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='de_equity_technical_daily'")
print(f"\nTechnical indicator columns: {tech_cols}")

# Fill rates
print("\n" + "=" * 90)
print("COMPUTED FIELDS — FILL RATES")
print("=" * 90)

checks = [
    ("de_equity_technical_daily", "sma_50"),
    ("de_equity_technical_daily", "ema_20"),
    ("de_equity_technical_daily", "rsi_14"),
    ("de_equity_technical_daily", "macd_line"),
    ("de_equity_technical_daily", "beta_nifty"),
    ("de_equity_technical_daily", "sharpe_1y"),
    ("de_equity_technical_daily", "sortino_1y"),
    ("de_equity_technical_daily", "max_drawdown_1y"),
    ("de_equity_technical_daily", "adx_14"),
    ("de_equity_technical_daily", "obv"),
    ("de_equity_technical_daily", "mfi_14"),
    ("de_equity_technical_daily", "rsi_7"),
    ("de_equity_technical_daily", "rsi_9"),
    ("de_equity_technical_daily", "rsi_21"),
    ("de_equity_technical_daily", "calmar_ratio"),
    ("de_equity_technical_daily", "bollinger_upper"),
    ("de_equity_technical_daily", "volatility_20d"),
    ("de_equity_technical_daily", "relative_volume"),
    ("de_equity_technical_daily", "delivery_vs_avg"),
    ("de_rs_scores", "rs_composite"),
    ("de_breadth_daily", "pct_above_200dma"),
    ("de_mf_derived_daily", "sharpe_1y"),
    ("de_mf_derived_daily", "derived_rs_composite"),
    ("de_mf_derived_daily", "beta_vs_nifty"),
    ("de_mf_derived_daily", "manager_alpha"),
    ("de_mf_nav_daily", "return_1d"),
    ("de_mf_nav_daily", "return_1y"),
    ("de_mf_nav_daily", "return_3y"),
    ("de_mf_nav_daily", "nav_52wk_high"),
    ("de_instrument", "sector"),
    ("de_instrument", "industry"),
]

for t, col in checks:
    total = jq(f"SELECT COUNT(*) FROM {t}")
    if col == "is_mapped":
        filled = jq(f"SELECT COUNT(*) FROM {t} WHERE {col} = TRUE")
    else:
        filled = jq(f"SELECT COUNT(*) FROM {t} WHERE {col} IS NOT NULL")
    pct = filled/total*100 if total > 0 else 0
    print(f"  {t}.{col:<25} {pct:5.1f}% ({filled:,}/{total:,})")

# Holdings
total = jq("SELECT COUNT(*) FROM de_mf_holdings")
mapped = jq("SELECT COUNT(*) FROM de_mf_holdings WHERE is_mapped = TRUE")
print(f"  de_mf_holdings.is_mapped          {mapped/total*100:5.1f}% ({mapped:,}/{total:,})")

# MarketPulse comparison
print("\n" + "=" * 90)
print("JIP vs MARKETPULSE")
print("=" * 90)

print(f"{'Data':<25} {'JIP':>15} {'MarketPulse':>15} {'JIP Advantage':>15}")
print("-" * 75)

comps = [
    ("Stock Prices", "SELECT COUNT(*) FROM de_equity_ohlcv", "SELECT COUNT(*) FROM compass_stock_prices"),
    ("Unique Stocks", "SELECT COUNT(DISTINCT instrument_id) FROM de_equity_ohlcv", "SELECT COUNT(DISTINCT ticker) FROM compass_stock_prices"),
    ("Index Prices", "SELECT COUNT(*) FROM de_index_prices", "SELECT COUNT(*) FROM index_prices"),
    ("Index Constituents", "SELECT COUNT(*) FROM de_index_constituents", "SELECT COUNT(*) FROM index_constituents"),
    ("MF NAV", "SELECT COUNT(*) FROM de_mf_nav_daily", "SELECT COUNT(*) FROM mf_nav_history"),
    ("Breadth", "SELECT COUNT(*) FROM de_breadth_daily", "SELECT COUNT(*) FROM breadth_daily"),
    ("RS Scores", "SELECT COUNT(*) FROM de_rs_scores", "SELECT COUNT(*) FROM compass_rs_scores"),
]
for label, jsql, msql in comps:
    jval = jq(jsql)
    mval = mq(msql)
    ratio = f"{jval/mval:.1f}x" if mval > 0 else "-"
    print(f"{label:<25} {jval:>15,} {mval:>15,} {ratio:>15}")

# Date ranges
jc.execute("SELECT MIN(date), MAX(date) FROM de_equity_ohlcv")
jr = jc.fetchone()
mc.execute("SELECT MIN(date), MAX(date) FROM compass_stock_prices")
mr = mc.fetchone()
print(f"{'Price Range':<25} {str(jr[0])+' to '+str(jr[1]):>31} {str(mr[0])+' to '+str(mr[1]):>31}")

# JIP exclusives
print("\n" + "=" * 90)
print("JIP EXCLUSIVE DATA (not in MarketPulse)")
print("=" * 90)

exclusives = [
    ("MF Master (Morningstar)", jq("SELECT COUNT(*) FROM de_mf_master")),
    ("MF Holdings (ISIN-resolved)", jq("SELECT COUNT(*) FROM de_mf_holdings WHERE is_mapped=TRUE")),
    ("MF Derived (Sharpe/Beta/etc)", jq("SELECT COUNT(*) FROM de_mf_derived_daily WHERE sharpe_1y IS NOT NULL")),
    ("NAV Returns (1d-10y)", jq("SELECT COUNT(*) FROM de_mf_nav_daily WHERE return_1d IS NOT NULL")),
    ("Corporate Actions", jq("SELECT COUNT(*) FROM de_corporate_actions")),
    ("Close Adj (split-adjusted)", jq("SELECT COUNT(*) FROM de_equity_ohlcv WHERE close_adj != close")),
    ("39 Technical Indicators", jq("SELECT COUNT(*) FROM de_equity_technical_daily WHERE rsi_14 IS NOT NULL")),
    ("Market Regime (19yr)", jq("SELECT COUNT(*) FROM de_market_regime")),
    ("Sector Mapping", jq("SELECT COUNT(*) FROM de_instrument WHERE sector IS NOT NULL")),
    ("MF Category Flows", jq("SELECT COUNT(*) FROM de_mf_category_flows")),
    ("Global Prices (81 tickers)", jq("SELECT COUNT(*) FROM de_global_prices")),
    ("Market Cap History", jq("SELECT COUNT(*) FROM de_market_cap_history")),
]
for label, val in exclusives:
    print(f"  {label:<40} {val:>12,}")

# Pending
print("\n" + "=" * 90)
print("REMAINING COMPUTATION TASKS")
print("=" * 90)
pending = [
    "Fund sector exposure (join holdings x instrument.sector, group by sector)",
    "Sector RS aggregation (avg stock RS per sector per date)",
    "RS daily summary (denormalized for fast dashboard queries)",
    "Cross-validation vs MarketPulse + Morningstar",
]
for p in pending:
    print(f"  - {p}")

print("\n" + "=" * 90)
print("NOT IN SCOPE (separate sprint)")
print("=" * 90)
not_in_scope = [
    "FII/DII institutional flows (de_institutional_flows)",
    "F&O summary (de_fo_summary)",
    "FRED macro data (de_macro_values)",
    "MF dividends (de_mf_dividends)",
    "Qualitative layer (RSS, Claude extraction)",
    "Client portfolios",
]
for p in not_in_scope:
    print(f"  - {p}")

jc.close(); j.close(); mc.close(); m.close()
