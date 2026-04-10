"""Seed de_etf_master with ~67 curated NSE India ETFs.

Run once before nse_etf_sync pipeline — satisfies FK constraint so
de_etf_ohlcv INSERTs succeed.  Safe to re-run: ON CONFLICT upsert.

Usage:
    python scripts/ingest/nse_etf_master.py
"""

import os

import psycopg2

_raw_db = os.environ.get(
    "DATABASE_URL_SYNC",
    "postgresql://jip_admin:JipDataEngine2026Secure@jip-data-engine.ctay2iewomaj.ap-south-1.rds.amazonaws.com:5432/data_engine",
)
# Strip SQLAlchemy dialect prefix for psycopg2 compatibility
DB = _raw_db.replace("postgresql+psycopg2://", "postgresql://").replace("postgresql+asyncpg://", "postgresql://")

# Format: "TICKER": ("country", "category", "exchange", "Full Name")
NSE_ETFS: dict[str, tuple[str, str, str, str]] = {
    # Broad Index (20)
    "NIFTYBEES": ("IN", "Broad Index", "NSE", "Nippon Nifty 50 BeES"),
    "JUNIORBEES": ("IN", "Broad Index", "NSE", "Nippon Nifty Next 50 BeES"),
    "SETFNIF50": ("IN", "Broad Index", "NSE", "SBI ETF Nifty 50"),
    "SETFNN50": ("IN", "Broad Index", "NSE", "SBI ETF Nifty Next 50"),
    "ICICINIFTY": ("IN", "Broad Index", "NSE", "ICICI Prudential Nifty 50 ETF"),
    "ICICINXT50": ("IN", "Broad Index", "NSE", "ICICI Prudential Nifty Next 50 ETF"),
    "UTINIFTETF": ("IN", "Broad Index", "NSE", "UTI Nifty 50 ETF"),
    "UTINEXT50": ("IN", "Broad Index", "NSE", "UTI Nifty Next 50 ETF"),
    "HDFCNIFETF": ("IN", "Broad Index", "NSE", "HDFC Nifty 50 ETF"),
    "KOTAKNIFTY": ("IN", "Broad Index", "NSE", "Kotak Nifty 50 ETF"),
    "MOM50": ("IN", "Broad Index", "NSE", "Motilal Oswal M50 ETF"),
    "MOM100": ("IN", "Broad Index", "NSE", "Motilal Oswal M100 ETF"),
    "LICNETFN50": ("IN", "Broad Index", "NSE", "LIC MF Nifty 50 ETF"),
    "MAN50ETF": ("IN", "Broad Index", "NSE", "Mirae Asset Nifty 50 ETF"),
    "MANXT50": ("IN", "Broad Index", "NSE", "Mirae Asset Nifty Next 50 ETF"),
    "ICICISENSX": ("IN", "Broad Index", "NSE", "ICICI Prudential Sensex ETF"),
    "HDFCSENETF": ("IN", "Broad Index", "NSE", "HDFC Sensex ETF"),
    "UTISENSETF": ("IN", "Broad Index", "NSE", "UTI Sensex ETF"),
    "CPSEETF": ("IN", "Broad Index", "NSE", "CPSE ETF"),
    "ICICIB22": ("IN", "Broad Index", "NSE", "Bharat 22 ETF"),
    # Banking & Financial (10)
    "BANKBEES": ("IN", "Banking & Financial", "NSE", "Nippon Bank BeES"),
    "SETFNIFBK": ("IN", "Banking & Financial", "NSE", "SBI ETF Nifty Bank"),
    "KOTAKBKETF": ("IN", "Banking & Financial", "NSE", "Kotak Banking ETF"),
    "PSUBNKBEES": ("IN", "Banking & Financial", "NSE", "Nippon PSU Bank BeES"),
    "KOTAKPSUBK": ("IN", "Banking & Financial", "NSE", "Kotak PSU Bank ETF"),
    "HBANKETF": ("IN", "Banking & Financial", "NSE", "HDFC Banking ETF"),
    "ICICIBANKN": ("IN", "Banking & Financial", "NSE", "ICICI Prudential Bank Nifty ETF"),
    "UTIBANKETF": ("IN", "Banking & Financial", "NSE", "UTI Bank ETF"),
    "SBIETFPB": ("IN", "Banking & Financial", "NSE", "SBI ETF Private Bank"),
    "NPBET": ("IN", "Banking & Financial", "NSE", "Tata Nifty Private Bank ETF"),
    # Sectoral (8)
    "INFRABEES": ("IN", "Sectoral", "NSE", "Nippon Infra BeES"),
    "SBIETFIT": ("IN", "Sectoral", "NSE", "SBI ETF IT"),
    "NETFIT": ("IN", "Sectoral", "NSE", "Nippon IT ETF"),
    "ICICITECH": ("IN", "Sectoral", "NSE", "ICICI Prudential IT ETF"),
    "NETFCONSUM": ("IN", "Sectoral", "NSE", "Nippon Consumption ETF"),
    "NETFDIVOPP": ("IN", "Sectoral", "NSE", "Nippon Dividend Opportunities ETF"),
    "SBIETFQLTY": ("IN", "Sectoral", "NSE", "SBI ETF Quality"),
    "NETFMID150": ("IN", "Sectoral", "NSE", "Nippon Nifty Midcap 150 ETF"),
    # Gold (8)
    "GOLDBEES": ("IN", "Gold", "NSE", "Nippon Gold BeES"),
    "SETFGOLD": ("IN", "Gold", "NSE", "SBI Gold ETF"),
    "KOTAKGOLD": ("IN", "Gold", "NSE", "Kotak Gold ETF"),
    "HDFCMFGETF": ("IN", "Gold", "NSE", "HDFC Gold ETF"),
    "ICICIGOLD": ("IN", "Gold", "NSE", "ICICI Prudential Gold ETF"),
    "GOLDSHARE": ("IN", "Gold", "NSE", "UTI Gold ETF"),
    "AXISGOLD": ("IN", "Gold", "NSE", "Axis Gold ETF"),
    "BSLGOLDETF": ("IN", "Gold", "NSE", "BSL Gold ETF"),
    # Silver (2)
    "SILVERBEES": ("IN", "Silver", "NSE", "Nippon Silver BeES"),
    "ICICISLVR": ("IN", "Silver", "NSE", "ICICI Prudential Silver ETF"),
    # Debt & Liquid (6)
    "LIQUIDBEES": ("IN", "Debt & Liquid", "NSE", "Nippon Liquid BeES"),
    "LIQUIDIETF": ("IN", "Debt & Liquid", "NSE", "ICICI Prudential Liquid ETF"),
    "GILT5YBEES": ("IN", "Debt & Liquid", "NSE", "Nippon Gilt 5Y BeES"),
    "NETFLTGILT": ("IN", "Debt & Liquid", "NSE", "Nippon Long Term Gilt ETF"),
    "SETF10GILT": ("IN", "Debt & Liquid", "NSE", "SBI 10Y Gilt ETF"),
    "LICNETFGSC": ("IN", "Debt & Liquid", "NSE", "LIC G-Sec Long Term ETF"),
    # Bharat Bond (4)
    "EBBETF0425": ("IN", "Bharat Bond", "NSE", "Bharat Bond ETF Apr 2025"),
    "EBBETF0430": ("IN", "Bharat Bond", "NSE", "Bharat Bond ETF Apr 2030"),
    "EBBETF0431": ("IN", "Bharat Bond", "NSE", "Bharat Bond ETF Apr 2031"),
    "EBBETF0433": ("IN", "Bharat Bond", "NSE", "Bharat Bond ETF Apr 2033"),
    # International (2)
    "HNGSNGBEES": ("IN", "International", "NSE", "Nippon Hang Seng BeES"),
    "MAFANG": ("IN", "International", "NSE", "Mirae Asset FANG+ ETF"),
    # Smart Beta (4)
    "ICICIALPLV": ("IN", "Smart Beta", "NSE", "ICICI Alpha Low Vol 30 ETF"),
    "ICICILOVOL": ("IN", "Smart Beta", "NSE", "ICICI Nifty Low Vol 30 ETF"),
    "KOTAKNV20": ("IN", "Smart Beta", "NSE", "Kotak NV20 ETF"),
    "ICICINV20": ("IN", "Smart Beta", "NSE", "ICICI NV20 ETF"),
    # Midcap (3)
    "ICICIMCAP": ("IN", "Midcap", "NSE", "ICICI Midcap Select ETF"),
    "ICICIM150": ("IN", "Midcap", "NSE", "ICICI Midcap 150 ETF"),
    "ICICI500": ("IN", "Midcap", "NSE", "ICICI S&P BSE 500 ETF"),
}


def main() -> None:
    """Upsert all NSE ETFs into de_etf_master."""
    conn = psycopg2.connect(DB)
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute("SELECT COUNT(*) FROM de_etf_master WHERE exchange = 'NSE'")
        rows_before: int = cur.fetchone()[0]
        print(f"de_etf_master NSE rows before: {rows_before}", flush=True)

        upsert_sql = """
            INSERT INTO de_etf_master
                (ticker, name, exchange, country, currency, sector, source)
            VALUES (%s, %s, %s, %s, 'INR', %s, 'bhav')
            ON CONFLICT (ticker) DO UPDATE SET
                name       = EXCLUDED.name,
                sector     = EXCLUDED.sector,
                exchange   = EXCLUDED.exchange,
                country    = EXCLUDED.country,
                currency   = 'INR',
                source     = 'bhav',
                updated_at = NOW()
        """

        for ticker, (country, category, exchange, name) in NSE_ETFS.items():
            cur.execute(upsert_sql, (ticker, name, exchange, country, category))

        cur.execute("SELECT COUNT(*) FROM de_etf_master WHERE exchange = 'NSE'")
        rows_after: int = cur.fetchone()[0]
        print(f"de_etf_master NSE rows after:  {rows_after}", flush=True)
        print(f"Upserted {len(NSE_ETFS)} NSE ETF definitions ({rows_after - rows_before} net new)", flush=True)

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
