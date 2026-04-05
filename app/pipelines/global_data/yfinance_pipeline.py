"""Global Pre-Market Pipeline — YFinance and FRED fetchers."""

from datetime import date, timedelta
import yfinance as yf
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
import httpx

from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.models.prices import DeGlobalPrices, DeMacroValues
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

# Track C and Global Universe defined in spec v1.9
YF_TICKERS = ["^GSPC", "^IXIC", "^DJI", "^FTSE", "^GDAXI", "^FCHI", "^N225", "^HSI", "000001.SS", "^AXJO", "EEM", "URTH"]
YF_MACRO = ["DX-Y.NYB", "CL=F", "BZ=F", "GC=F", "SI=F", "USDINR=X", "USDJPY=X", "EURUSD=X", "USDCNH=X"]


class GlobalMarketPipeline(BasePipeline):
    pipeline_name = "global_premarket"
    requires_trading_day = False  # Global markets have different holidays natively detected by yfinance

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        rows_inserted = 0
        
        # 1. Fetch Equities/Indices from YFinance
        # Using period='min' up to 5 days to handle weekend roll-over / timezone differences cleanly
        logger.info(f"Downloading {len(YF_TICKERS)} global tickers via yfinance...")
        df_equities = yf.download(YF_TICKERS, period="5d", interval="1d", progress=False)
        
        equity_records = []
        if not df_equities.empty:
            # yfinance returns multi-index columns if multiple tickers are provided
            for ticker in YF_TICKERS:
                ticker_data = df_equities.xs(ticker, axis=1, level=1, drop_level=True) if len(YF_TICKERS) > 1 else df_equities
                ticker_data = ticker_data.dropna()
                if ticker_data.empty:
                    continue
                
                # Get the latest row within the last 5 days
                latest_date_pd = ticker_data.index[-1]
                row = ticker_data.iloc[-1]
                
                equity_records.append({
                    "date": latest_date_pd.date(),
                    "ticker": ticker,
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": int(row["Volume"]) if "Volume" in row and not pd.isna(row["Volume"]) else 0
                })

        if equity_records:
            stmt = insert(DeGlobalPrices).values(equity_records)
            stmt = stmt.on_conflict_do_update(
                index_elements=["date", "ticker"],
                set_={
                    "open": stmt.excluded.open,
                    "high": stmt.excluded.high,
                    "low": stmt.excluded.low,
                    "close": stmt.excluded.close,
                    "volume": stmt.excluded.volume
                }
            )
            await session.execute(stmt)
            rows_inserted += len(equity_records)

        # 2. Fetch Macro Forex/Commodity from YFinance
        logger.info(f"Downloading {len(YF_MACRO)} macro tickers via yfinance...")
        df_macro = yf.download(YF_MACRO, period="5d", interval="1d", progress=False)
        
        macro_records = []
        if not df_macro.empty:
            for ticker in YF_MACRO:
                ticker_data = df_macro.xs(ticker, axis=1, level=1, drop_level=True) if len(YF_MACRO) > 1 else df_macro
                ticker_data = ticker_data.dropna()
                if ticker_data.empty:
                    continue
                
                latest_date_pd = ticker_data.index[-1]
                # For Macro, we grab the close value
                macro_records.append({
                    "date": latest_date_pd.date(),
                    "ticker": ticker,
                    "value": float(ticker_data.iloc[-1]["Close"])
                })
                
        if macro_records:
            stmt = insert(DeMacroValues).values(macro_records)
            stmt = stmt.on_conflict_do_update(
                index_elements=["date", "ticker"],
                set_={"value": stmt.excluded.value}
            )
            await session.execute(stmt)
            rows_inserted += len(macro_records)
            
        await session.commit()
        return ExecutionResult(rows_inserted, 0)
