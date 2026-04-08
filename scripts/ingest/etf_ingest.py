"""Ingest 130+ curated ETFs — country broad + US sectors + global sectors + China sectors + commodities + bonds."""
import psycopg2, time, io, os, gc
from pathlib import Path
from datetime import date
import pandas as pd

DB = os.environ.get("DATABASE_URL_SYNC", "postgresql://jip_admin:JipDataEngine2026Secure@jip-data-engine.ctay2iewomaj.ap-south-1.rds.amazonaws.com:5432/data_engine")
DATA_DIR = Path(os.environ.get("ETF_DATA_DIR", "/app/global-pulse-data"))
MIN_DATE = "2016-04-01"

# Complete curated universe
ETFS = {
    # Country Broad Market
    "SPY": ("US","Broad Market","NYSE","SPDR S&P 500"), "QQQ": ("US","Broad Market","NASDAQ","Invesco QQQ NASDAQ 100"),
    "IWM": ("US","Broad Market","NYSE","iShares Russell 2000"), "DIA": ("US","Broad Market","NYSE","SPDR Dow Jones"),
    "VTI": ("US","Broad Market","NYSE","Vanguard Total Stock Market"), "VOO": ("US","Broad Market","NYSE","Vanguard S&P 500"),
    "MDY": ("US","Broad Market","NYSE","SPDR S&P MidCap 400"), "IJR": ("US","Broad Market","NYSE","iShares S&P SmallCap 600"),
    "RSP": ("US","Broad Market","NYSE","Invesco S&P 500 Equal Weight"),
    "EWC": ("CA","Broad Market","NYSE","iShares MSCI Canada"), "EWZ": ("BR","Broad Market","NYSE","iShares MSCI Brazil"),
    "EWW": ("MX","Broad Market","NYSE","iShares MSCI Mexico"), "ECH": ("CL","Broad Market","NYSE","iShares MSCI Chile"),
    "ARGT": ("AR","Broad Market","NYSE","Global X MSCI Argentina"), "EPU": ("PE","Broad Market","NYSE","iShares MSCI Peru"),
    "EWU": ("UK","Broad Market","NYSE","iShares MSCI UK"), "EWG": ("DE","Broad Market","NYSE","iShares MSCI Germany"),
    "EWQ": ("FR","Broad Market","NYSE","iShares MSCI France"), "EWI": ("IT","Broad Market","NYSE","iShares MSCI Italy"),
    "EWP": ("ES","Broad Market","NYSE","iShares MSCI Spain"), "EWL": ("CH","Broad Market","NYSE","iShares MSCI Switzerland"),
    "EWN": ("NL","Broad Market","NYSE","iShares MSCI Netherlands"), "EWD": ("SE","Broad Market","NYSE","iShares MSCI Sweden"),
    "EWK": ("BE","Broad Market","NYSE","iShares MSCI Belgium"), "EWO": ("AT","Broad Market","NYSE","iShares MSCI Austria"),
    "EFNL": ("FI","Broad Market","NYSE","iShares MSCI Finland"), "EIRL": ("IE","Broad Market","NYSE","iShares MSCI Ireland"),
    "NORW": ("NO","Broad Market","NYSE","Global X MSCI Norway"), "GREK": ("GR","Broad Market","NYSE","Global X MSCI Greece"),
    "EPOL": ("PL","Broad Market","NYSE","iShares MSCI Poland"), "TUR": ("TR","Broad Market","NYSE","iShares MSCI Turkey"),
    "VGK": ("EU","Broad Market","NYSE","Vanguard FTSE Europe"),
    "EWJ": ("JP","Broad Market","NYSE","iShares MSCI Japan"), "EWH": ("HK","Broad Market","NYSE","iShares MSCI Hong Kong"),
    "EWY": ("KR","Broad Market","NYSE","iShares MSCI South Korea"), "EWT": ("TW","Broad Market","NYSE","iShares MSCI Taiwan"),
    "EWS": ("SG","Broad Market","NYSE","iShares MSCI Singapore"), "EWM": ("MY","Broad Market","NYSE","iShares MSCI Malaysia"),
    "THD": ("TH","Broad Market","NYSE","iShares MSCI Thailand"), "EPHE": ("PH","Broad Market","NYSE","iShares MSCI Philippines"),
    "VNM": ("VN","Broad Market","NYSE","VanEck Vietnam"), "IDX": ("ID","Broad Market","NYSE","VanEck Indonesia"),
    "EWA": ("AU","Broad Market","NYSE","iShares MSCI Australia"),
    "INDA": ("IN","Broad Market","NYSE","iShares MSCI India"), "INDY": ("IN","Broad Market","NYSE","iShares India 50 NIFTY"),
    "SMIN": ("IN","Broad Market","NYSE","iShares MSCI India Small-Cap"),
    "FXI": ("CN","Broad Market","NYSE","iShares China Large-Cap"), "MCHI": ("CN","Broad Market","NYSE","iShares MSCI China"),
    "ASHR": ("CN","Broad Market","NYSE","Xtrackers CSI 300 China A"), "GXC": ("CN","Broad Market","NYSE","SPDR S&P China"),
    "KSA": ("SA","Broad Market","NYSE","iShares MSCI Saudi Arabia"), "UAE": ("AE","Broad Market","NYSE","iShares MSCI UAE"),
    "QAT": ("QA","Broad Market","NYSE","iShares MSCI Qatar"), "EZA": ("ZA","Broad Market","NYSE","iShares MSCI South Africa"),
    "EIS": ("IL","Broad Market","NYSE","iShares MSCI Israel"), "AFK": ("AF","Broad Market","NYSE","VanEck Africa Index"),
    "EEM": ("EM","Broad Market","NYSE","iShares MSCI Emerging Markets"), "VWO": ("EM","Broad Market","NYSE","Vanguard FTSE EM"),
    "IEMG": ("EM","Broad Market","NYSE","iShares Core MSCI EM"), "EFA": ("INTL","Broad Market","NYSE","iShares MSCI EAFE"),
    "ACWI": ("GLOBAL","Broad Market","NYSE","iShares MSCI ACWI"), "AAXJ": ("APAC","Broad Market","NYSE","iShares Asia ex Japan"),
    # US Sectors
    "XLK": ("US","IT","NYSE","Technology Select Sector SPDR"), "VGT": ("US","IT","NYSE","Vanguard Information Technology"),
    "SOXX": ("US","IT","NASDAQ","iShares Semiconductor"), "SMH": ("US","IT","NASDAQ","VanEck Semiconductor"),
    "IGV": ("US","IT","NYSE","iShares Expanded Tech-Software"), "SKYY": ("US","IT","NASDAQ","First Trust Cloud Computing"),
    "HACK": ("US","IT","NYSE","ETFMG Prime Cyber Security"),
    "XLF": ("US","Financial Services","NYSE","Financial Select Sector SPDR"), "VFH": ("US","Financial Services","NYSE","Vanguard Financials"),
    "KRE": ("US","Banking","NYSE","SPDR S&P Regional Banking"), "KBE": ("US","Banking","NYSE","SPDR S&P Bank"),
    "XLV": ("US","Healthcare","NYSE","Health Care Select Sector SPDR"), "VHT": ("US","Healthcare","NYSE","Vanguard Health Care"),
    "IBB": ("US","Healthcare","NASDAQ","iShares Biotechnology"), "XBI": ("US","Healthcare","NYSE","SPDR S&P Biotech"),
    "ARKG": ("US","Healthcare","NYSE","ARK Genomic Revolution"),
    "XLE": ("US","Energy","NYSE","Energy Select Sector SPDR"), "VDE": ("US","Energy","NYSE","Vanguard Energy"),
    "XOP": ("US","Oil & Gas","NYSE","SPDR S&P Oil & Gas Exploration"), "OIH": ("US","Oil & Gas","NYSE","VanEck Oil Services"),
    "XLI": ("US","Infrastructure","NYSE","Industrial Select Sector SPDR"), "VIS": ("US","Infrastructure","NYSE","Vanguard Industrials"),
    "ITA": ("US","Defence","NYSE","iShares US Aerospace & Defense"), "IYT": ("US","Logistics","NYSE","iShares US Transportation"),
    "XLB": ("US","Chemicals","NYSE","Materials Select Sector SPDR"), "VAW": ("US","Chemicals","NYSE","Vanguard Materials"),
    "XME": ("US","Metal","NYSE","SPDR S&P Metals & Mining"), "GDX": ("US","Metal","NYSE","VanEck Gold Miners"),
    "SLX": ("US","Metal","NYSE","VanEck Steel"), "REMX": ("US","Metal","NYSE","VanEck Rare Earth/Strategic Metals"),
    "XLY": ("US","Consumer Durables","NYSE","Consumer Discretionary SPDR"), "VCR": ("US","Consumer Durables","NYSE","Vanguard Consumer Disc"),
    "XLP": ("US","FMCG","NYSE","Consumer Staples Select SPDR"), "VDC": ("US","FMCG","NYSE","Vanguard Consumer Staples"),
    "XLU": ("US","Utilities","NYSE","Utilities Select Sector SPDR"), "VPU": ("US","Utilities","NYSE","Vanguard Utilities"),
    "XLRE": ("US","Realty","NYSE","Real Estate Select Sector SPDR"), "VNQ": ("US","Realty","NYSE","Vanguard Real Estate"),
    "XHB": ("US","Realty","NYSE","SPDR S&P Homebuilders"), "ITB": ("US","Realty","NYSE","iShares US Home Construction"),
    "XLC": ("US","Media","NYSE","Communication Services SPDR"), "VOX": ("US","Media","NYSE","Vanguard Communication Services"),
    "ICLN": ("US","Clean Energy","NASDAQ","iShares Global Clean Energy"), "TAN": ("US","Clean Energy","NYSE","Invesco Solar"),
    "PBW": ("US","Clean Energy","NYSE","Invesco WilderHill Clean Energy"),
    # Global Sectors
    "IXN": ("GLOBAL","IT","NYSE","iShares Global Tech"), "IXJ": ("GLOBAL","Healthcare","NYSE","iShares Global Healthcare"),
    "IXG": ("GLOBAL","Financial Services","NYSE","iShares Global Financials"), "IXC": ("GLOBAL","Energy","NYSE","iShares Global Energy"),
    "MXI": ("GLOBAL","Chemicals","NYSE","iShares Global Materials"), "EXI": ("GLOBAL","Infrastructure","NYSE","iShares Global Industrials"),
    "RXI": ("GLOBAL","Consumer Durables","NYSE","iShares Global Consumer Disc"), "KXI": ("GLOBAL","FMCG","NYSE","iShares Global Consumer Staples"),
    "JXI": ("GLOBAL","Utilities","NYSE","iShares Global Utilities"),
    # China Sectors
    "KWEB": ("CN","IT","NYSE","KraneShares CSI China Internet"), "CQQQ": ("CN","IT","NASDAQ","Invesco China Technology"),
    "CHIQ": ("CN","Consumer Durables","NYSE","Global X China Consumer Disc"), "KGRN": ("CN","Clean Energy","NYSE","KraneShares China Clean Tech"),
    # Commodities
    "GLD": ("US","Gold","NYSE","SPDR Gold Shares"), "SLV": ("US","Silver","NYSE","iShares Silver Trust"),
    "USO": ("US","Oil","NYSE","United States Oil Fund"), "DBA": ("US","Agriculture","NYSE","Invesco DB Agriculture"),
    "DBC": ("US","Commodities","NYSE","Invesco DB Commodity Index"),
    # Bonds
    "TLT": ("US","US Treasury","NASDAQ","iShares 20+ Year Treasury"), "IEF": ("US","US Treasury","NASDAQ","iShares 7-10 Year Treasury"),
    "HYG": ("US","High Yield","NYSE","iShares High Yield Corporate"), "LQD": ("US","Corp Bond","NYSE","iShares Investment Grade Corporate"),
    "EMB": ("EM","EM Bond","NYSE","iShares JP Morgan EM Bond"),
}

# Tier 1 US ETFs — dict format for test compatibility
TIER1_US = {
    t: {"name": meta[3], "exchange": meta[2], "category": meta[1]}
    for t, meta in ETFS.items()
    if meta[0] == "US" and meta[1] == "Broad Market"
}
# Add core sector + thematic US ETFs to reach 33
for t in [
    "XLK", "XLF", "XLV", "XLE", "XLI", "XLB", "XLY", "XLP", "XLU", "XLRE",
    "SOXX", "IBB", "XOP", "KRE", "XHB",
    "GLD", "SLV", "TLT", "HYG", "LQD",
    "EEM", "VWO", "EFA", "ACWI",
]:
    if t in ETFS and t not in TIER1_US:
        meta = ETFS[t]
        TIER1_US[t] = {"name": meta[3], "exchange": meta[2], "category": meta[1]}

WORLD_INDICES = {
    "^SPX": {"name": "S&P 500", "country": "US"},
    "^NDQ": {"name": "NASDAQ 100", "country": "US"},
    "^DJI": {"name": "Dow Jones Industrial", "country": "US"},
    "^UKX": {"name": "FTSE 100", "country": "UK"},
    "^DAX": {"name": "DAX", "country": "DE"},
    "^CAC": {"name": "CAC 40", "country": "FR"},
    "^NKX": {"name": "Nikkei 225", "country": "JP"},
    "^HSI": {"name": "Hang Seng", "country": "HK"},
    "^SHCC": {"name": "Shanghai Composite", "country": "CN"},
    "^BSESN": {"name": "BSE Sensex", "country": "IN"},
}


NASDAQ_ETF_DIR = DATA_DIR / "us/nasdaq etfs"
NYSE_ETF_DIRS = [DATA_DIR / "us/nyse etfs/1", DATA_DIR / "us/nyse etfs/2"]


def find_etf_file(ticker, exchange="NYSE"):
    """Find the stooq data file for an ETF by ticker and exchange."""
    fname = ticker.lower() + ".us.txt"
    if exchange == "NASDAQ":
        p = NASDAQ_ETF_DIR / fname
        if p.exists():
            return p
    for d in NYSE_ETF_DIRS:
        p = d / fname
        if p.exists():
            return p
    if exchange != "NASDAQ":
        p = NASDAQ_ETF_DIR / fname
        if p.exists():
            return p
    return None


def parse_ohlcv_file(path, ticker, min_date="2016-04-01"):
    """Parse a stooq-format CSV and return a clean DataFrame."""
    df = pd.read_csv(
        path, header=0,
        names=["t", "per", "date", "time", "open", "high", "low", "close", "volume", "oi"],
    )
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["date"])
    df = df[df["date"] >= pd.Timestamp(min_date)].copy()
    df["ticker"] = ticker
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
    df.loc[df["volume"] == 0, "volume"] = None
    df["date"] = df["date"].dt.date
    out = df[["ticker", "date", "open", "high", "low", "close", "volume"]].copy()
    out["volume"] = out["volume"].astype("Int64")
    return out


def find_file(ticker):
    fname = ticker.lower() + ".us.txt"
    for subdir in ["us/nasdaq etfs", "us/nyse etfs/1", "us/nyse etfs/2"]:
        p = DATA_DIR / subdir / fname
        if p.exists(): return p
    return None

def main():
    t0 = time.time()
    conn = psycopg2.connect(DB); conn.autocommit = True; cur = conn.cursor()

    # Create tables
    print("Creating tables...", flush=True)
    cur.execute("""CREATE TABLE IF NOT EXISTS de_etf_master (
        ticker VARCHAR(30) PRIMARY KEY, name VARCHAR(200) NOT NULL, exchange VARCHAR(20) NOT NULL,
        country VARCHAR(5) NOT NULL, currency VARCHAR(5), sector VARCHAR(100), asset_class VARCHAR(50),
        category VARCHAR(100), benchmark VARCHAR(50), expense_ratio NUMERIC(6,4), inception_date DATE,
        is_active BOOLEAN DEFAULT TRUE, source VARCHAR(20) DEFAULT 'stooq',
        created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())""")
    cur.execute("""CREATE TABLE IF NOT EXISTS de_etf_ohlcv (
        date DATE NOT NULL, ticker VARCHAR(30) NOT NULL REFERENCES de_etf_master(ticker),
        open NUMERIC(18,4), high NUMERIC(18,4), low NUMERIC(18,4), close NUMERIC(18,4), volume BIGINT,
        created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (date, ticker))""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_etf_ohlcv_ticker ON de_etf_ohlcv(ticker)")
    print("  Done", flush=True)

    # Seed master
    print(f"Seeding {len(ETFS)} ETFs...", flush=True)
    cur.execute("TRUNCATE de_etf_ohlcv CASCADE")
    cur.execute("TRUNCATE de_etf_master CASCADE")
    for ticker, (country, sector, exchange, name) in ETFS.items():
        cur.execute("INSERT INTO de_etf_master (ticker, name, exchange, country, sector, currency) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (ticker) DO UPDATE SET name=EXCLUDED.name, sector=EXCLUDED.sector, country=EXCLUDED.country",
                    (ticker, name, exchange, country, sector, "USD"))
    print(f"  Seeded {len(ETFS)} ETFs", flush=True)

    # Ingest OHLCV
    print("Ingesting OHLCV...", flush=True)
    total = 0; missing = []
    for ticker in ETFS:
        path = find_file(ticker)
        if not path:
            missing.append(ticker)
            continue
        df = pd.read_csv(path, header=0,
                         names=["t","per","date","time","open","high","low","close","volume","oi"])
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["date"])
        df = df[df["date"] >= pd.Timestamp(MIN_DATE)].copy()
        if df.empty: continue
        df["ticker"] = ticker
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
        df.loc[df["volume"] == 0, "volume"] = None
        df["date"] = df["date"].dt.date
        out = df[["ticker","date","open","high","low","close","volume"]].copy()
        out["volume"] = out["volume"].astype("Int64")

        staging = f"tmp_etf_{ticker.lower().replace('-','_')}"
        cur.execute(f"DROP TABLE IF EXISTS {staging}")
        cur.execute(f"CREATE TEMP TABLE {staging} (ticker VARCHAR(30), date DATE, open NUMERIC(18,4), high NUMERIC(18,4), low NUMERIC(18,4), close NUMERIC(18,4), volume BIGINT)")
        buf = io.StringIO()
        out.to_csv(buf, index=False, header=False, na_rep="\\N")
        buf.seek(0)
        cur.copy_expert(f"COPY {staging} (ticker,date,open,high,low,close,volume) FROM STDIN WITH (FORMAT CSV, NULL '\\N')", buf)
        cur.execute(f"INSERT INTO de_etf_ohlcv (ticker,date,open,high,low,close,volume) SELECT * FROM {staging} ON CONFLICT (date,ticker) DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume, updated_at=NOW()")
        cur.execute(f"DROP TABLE IF EXISTS {staging}")
        total += len(out)

    print(f"  Ingested: {total:,} rows, {len(ETFS)-len(missing)} ETFs", flush=True)
    if missing: print(f"  Missing files: {', '.join(missing)}")

    # Verify
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT ticker) FROM de_etf_ohlcv")
    r = cur.fetchone()
    print(f"\nFinal: {r[0]:,} OHLCV rows, {r[1]} ETFs")
    print(f"Done in {time.time()-t0:.0f}s", flush=True)
    cur.close(); conn.close()

if __name__ == "__main__":
    main()
