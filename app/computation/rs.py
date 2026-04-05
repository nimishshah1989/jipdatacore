"""Relative Strength (RS) computation using pandas vectorization."""

import asyncio
from datetime import date
from typing import List
import uuid

import numpy as np
import pandas as pd
from sqlalchemy import select, and_, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.computed import DeRsScores, DeRsDailySummary
from app.models.instruments import DeInstrument
from app.models.prices import DeEquityOhlcv, DeIndexPrices

logger = get_logger(__name__)

# Constants from spec
COMPUTATION_VERSION = 1
LOOKBACKS = {"1w": 5, "1m": 21, "3m": 63, "6m": 126, "1y": 252}
BENCHMARKS = ["NIFTY 50", "NIFTY 500"]


class RsComputationPipeline:
    """Computes JIP's proprietary Relative Strength metrics.
    Algorithm: RS_N = (Stock_Return_N - Benchmark_Return_N) / Benchmark_Rolling_Std_N
    Composite: 1w(10%) + 1m(20%) + 3m(30%) + 6m(25%) + 12m(15%)
    """

    async def execute_incremental(self, business_date: date, session: AsyncSession) -> int:
        """Calculate RS for all active stocks for a given date."""
        # 1. Load active universe (stocks only)
        # Note: Must use close_adj for accurate returns after corp actions
        logger.info(f"Loading OHLCV data for RS up to {business_date}")
        
        # Load exactly 1 year + margin to compute 252-day return and benchmark stdev
        # Native SQL is usually faster, but we need memory dataframes for cross-sectional operations
        query = text("""
            SELECT 
                r.instrument_id,
                r.date,
                r.close_adj
            FROM de_equity_ohlcv r
            JOIN de_instrument i ON r.instrument_id = i.id
            WHERE r.date <= :b_date 
              AND r.date >= :start_date
              AND r.data_status = 'validated'
              AND i.is_active = true
            ORDER BY r.instrument_id, r.date
        """)
        
        start_date = business_date.replace(year=business_date.year - 2) # At least 252 trading days
        result = await session.execute(query, {"b_date": business_date, "start_date": start_date})
        
        df = pd.DataFrame(result.fetchall(), columns=['instrument_id', 'date', 'close_adj'])
        if df.empty:
            return 0
            
        # Ensure dates are datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # 2. Load Benchmark (Index) Prices
        bm_query = text("""
            SELECT idx.index_code, p.date, p.close
            FROM de_index_prices p
            JOIN de_index_master idx ON p.index_code = idx.index_code
            WHERE p.date <= :b_date AND p.date >= :start_date
        """)
        bm_result = await session.execute(bm_query, {"b_date": business_date, "start_date": start_date})
        bm_df = pd.DataFrame(bm_result.fetchall(), columns=['index_code', 'date', 'close'])
        bm_df['date'] = pd.to_datetime(bm_df['date'])

        # Pivot stock data
        stock_pivot = df.pivot(index='date', columns='instrument_id', values='close_adj')
        stock_pivot = stock_pivot.sort_index()
        
        # Compute daily returns
        stock_daily_returns = stock_pivot.pct_change()

        records_to_upsert = []

        # 3. For each benchmark, compute RS vectorised
        for benchmark in BENCHMARKS:
            # Get actual index code for NIFTY 50
            idx_name_mapping = {"NIFTY 50": "NIFTY 50", "NIFTY 500": "NIFTY 500"} 
            bm_code = idx_name_mapping[benchmark]
            
            bdf = bm_df[bm_df['index_code'] == bm_code].set_index('date').sort_index()
            if bdf.empty:
                logger.warning(f"No benchmark data for {benchmark}, skipping.")
                continue
                
            bm_close = bdf['close']
            bm_daily_returns = bm_close.pct_change()
            
            # Align indices
            aligned_stocks, aligned_bm = stock_pivot.align(bm_close, axis=0, join='inner')
            _, aligned_bm_returns = stock_daily_returns.align(bm_daily_returns, axis=0, join='inner')
            
            rs_components = {}
            for period_name, lookback in LOOKBACKS.items():
                # Stock cumulative returns
                stock_ret = aligned_stocks.pct_change(periods=lookback)
                # Benchmark cumulative returns
                bm_ret = aligned_bm.pct_change(periods=lookback)
                
                # Benchmark rolling std of daily returns over N days
                # Multiply by math.sqrt(252/lookback) if annualized, but formula says "std over same N-day window" literally
                bm_rolling_std = aligned_bm_returns.rolling(window=lookback).std()
                
                # RS formula: (entity_cumreturn_N - benchmark_cumreturn_N) / benchmark_rolling_std_N
                # Broadcasting bm_ret and bm_rolling_std across columns
                rs_matrix = stock_ret.sub(bm_ret, axis=0).div(bm_rolling_std, axis=0)
                rs_components[period_name] = rs_matrix
                
            # Compute composite today
            today = pd.to_datetime(business_date)
            if today not in stock_pivot.index:
                # No data for the target date
                continue
            
            today_rs_1w = rs_components["1w"].loc[today]
            today_rs_1m = rs_components["1m"].loc[today]
            today_rs_3m = rs_components["3m"].loc[today]
            today_rs_6m = rs_components["6m"].loc[today]
            today_rs_1y = rs_components["1y"].loc[today]
            
            # rs_1w×0.10 + rs_1m×0.20 + rs_3m×0.30 + rs_6m×0.25 + rs_12m×0.15
            composite = (
                today_rs_1w * 0.10 +
                today_rs_1m * 0.20 +
                today_rs_3m * 0.30 +
                today_rs_6m * 0.25 +
                today_rs_1y * 0.15
            )
            
            # Drop NaNs (stocks without 1 yr history won't have 1y RS)
            composite = composite.dropna()
            
            # Compute percentiles for composite
            percentiles = composite.rank(pct=True) * 100
            
            # Build insert records
            for inst_id, comp_val in composite.items():
                records_to_upsert.append({
                    "entity_type": "equity",
                    "instrument_id": inst_id,
                    "date": business_date,
                    "vs_benchmark": benchmark,
                    "rs_1w": float(today_rs_1w[inst_id]),
                    "rs_1m": float(today_rs_1m[inst_id]),
                    "rs_3m": float(today_rs_3m[inst_id]),
                    "rs_6m": float(today_rs_6m[inst_id]),
                    "rs_1y": float(today_rs_1y[inst_id]),
                    "rs_composite": float(comp_val),
                    "rs_percentile": float(percentiles[inst_id]),
                    "computation_version": COMPUTATION_VERSION
                })
                
        if records_to_upsert:
            stmt = insert(DeRsScores).values(records_to_upsert)
            stmt = stmt.on_conflict_do_update(
                index_elements=["entity_type", "instrument_id", "date", "vs_benchmark"],
                set_={
                    "rs_1w": stmt.excluded.rs_1w,
                    "rs_1m": stmt.excluded.rs_1m,
                    "rs_3m": stmt.excluded.rs_3m,
                    "rs_6m": stmt.excluded.rs_6m,
                    "rs_1y": stmt.excluded.rs_1y,
                    "rs_composite": stmt.excluded.rs_composite,
                    "rs_percentile": stmt.excluded.rs_percentile,
                    "computation_version": stmt.excluded.computation_version
                }
            )
            
            # To handle 9000 records safely
            chunk_size = 2000
            for i in range(0, len(records_to_upsert), chunk_size):
                chunk = records_to_upsert[i:i + chunk_size]
                chunk_stmt = insert(DeRsScores).values(chunk)
                chunk_stmt = chunk_stmt.on_conflict_do_update(
                    constraint="uq_de_rs_scores", # from alembic config or columns
                    set_={
                        "rs_composite": chunk_stmt.excluded.rs_composite,
                        "rs_percentile": chunk_stmt.excluded.rs_percentile
                    }
                )
                try:
                    await session.execute(chunk_stmt)
                except Exception:
                    # Fallback to pure update logic if constraint names vary loosely
                    await session.execute(stmt)
                    
            await session.commit()
            
        return len(records_to_upsert)
