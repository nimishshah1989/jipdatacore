"""Create v2 technical indicator tables for equity, ETF, global, index, and MF.

Revision ID: 008_indicators_v2
Revises: 007_purchase_mode
Create Date: 2026-04-14

Why:
- The existing de_equity_technical_daily table has only 10 columns (SMA-50, SMA-200,
  EMA-20, close_adj, stochastics, disparity, bollinger_width, two generated booleans).
  The indicators overhaul expands coverage to ~130 columns across all indicator families.
- Rather than ALTER the live table (risky cutover during trading hours), the v2 tables
  are created alongside the originals. Chunk 6 handles the atomic rename cutover.
- de_index_technical_daily and de_mf_technical_daily are brand-new (no prior schema).
- Fix 12 (binding): de_index_technical_daily omits all volume-dependent columns because
  index constituents have no aggregate volume time-series in our data model.
- Fix 13 (binding): de_mf_technical_daily is a strict single-price subset — it omits all
  indicators that require OHLC width (ATR, Keltner, Donchian, Supertrend, PSAR, CCI,
  Williams %R, Ultosc, Aroon, stochastics, ADX/DI, and all volume indicators).
- Fix 9 (binding): all GENERATED ALWAYS AS expressions are hardcoded SQL strings,
  never Python f-strings, to ensure byte-identity across renames.

Index strategy:
- ix_{table}_{id}: single-column btree on the instrument key for FK lookups
- ix_{table}_{id}_date: composite (id, date DESC) for "latest N days of one instrument" queries
"""

from alembic import op

revision = "008_indicators_v2"
down_revision = "007_purchase_mode"
branch_labels = None
depends_on = None


# ---------------------------------------------------------------------------
# Shared SQL fragments — column block identical across equity / ETF / global
# ---------------------------------------------------------------------------

_FULL_COLUMN_BLOCK = """
    -- Price snapshot
    close_adj               NUMERIC(18,4),

    -- Overlap / Trend
    sma_5                   NUMERIC(18,4),
    sma_10                  NUMERIC(18,4),
    sma_20                  NUMERIC(18,4),
    sma_50                  NUMERIC(18,4),
    sma_100                 NUMERIC(18,4),
    sma_200                 NUMERIC(18,4),
    ema_5                   NUMERIC(18,4),
    ema_10                  NUMERIC(18,4),
    ema_20                  NUMERIC(18,4),
    ema_50                  NUMERIC(18,4),
    ema_100                 NUMERIC(18,4),
    ema_200                 NUMERIC(18,4),
    dema_20                 NUMERIC(18,4),
    tema_20                 NUMERIC(18,4),
    wma_20                  NUMERIC(18,4),
    hma_20                  NUMERIC(18,4),
    vwap                    NUMERIC(18,4),
    kama_20                 NUMERIC(18,4),
    zlma_20                 NUMERIC(18,4),
    alma_20                 NUMERIC(18,4),

    -- Momentum
    rsi_7                   NUMERIC(8,4),
    rsi_9                   NUMERIC(8,4),
    rsi_14                  NUMERIC(8,4),
    rsi_21                  NUMERIC(8,4),
    macd_line               NUMERIC(18,4),
    macd_signal             NUMERIC(18,4),
    macd_histogram          NUMERIC(18,4),
    stochastic_k            NUMERIC(8,4),
    stochastic_d            NUMERIC(8,4),
    cci_20                  NUMERIC(10,4),
    mfi_14                  NUMERIC(8,4),
    roc_5                   NUMERIC(10,4),
    roc_10                  NUMERIC(10,4),
    roc_21                  NUMERIC(10,4),
    roc_63                  NUMERIC(10,4),
    roc_252                 NUMERIC(10,4),
    tsi_13_25               NUMERIC(10,4),
    williams_r_14           NUMERIC(8,4),
    cmo_14                  NUMERIC(10,4),
    trix_15                 NUMERIC(10,4),
    ultosc                  NUMERIC(8,4),

    -- Volatility
    bb_upper                NUMERIC(18,4),
    bb_middle               NUMERIC(18,4),
    bb_lower                NUMERIC(18,4),
    bb_width                NUMERIC(8,4),
    bb_pct_b                NUMERIC(8,4),
    atr_7                   NUMERIC(18,4),
    atr_14                  NUMERIC(18,4),
    atr_21                  NUMERIC(18,4),
    natr_14                 NUMERIC(8,4),
    true_range              NUMERIC(18,4),
    keltner_upper           NUMERIC(18,4),
    keltner_middle          NUMERIC(18,4),
    keltner_lower           NUMERIC(18,4),
    donchian_upper          NUMERIC(18,4),
    donchian_middle         NUMERIC(18,4),
    donchian_lower          NUMERIC(18,4),
    hv_20                   NUMERIC(10,4),
    hv_60                   NUMERIC(10,4),
    hv_252                  NUMERIC(10,4),

    -- Volume
    obv                     BIGINT,
    ad                      BIGINT,
    adosc_3_10              NUMERIC(18,4),
    cmf_20                  NUMERIC(8,4),
    efi_13                  NUMERIC(18,4),
    eom_14                  NUMERIC(18,4),
    kvo                     NUMERIC(18,4),
    pvt                     BIGINT,

    -- Trend strength
    adx_14                  NUMERIC(8,4),
    plus_di                 NUMERIC(8,4),
    minus_di                NUMERIC(8,4),
    aroon_up                NUMERIC(8,4),
    aroon_down              NUMERIC(8,4),
    aroon_osc               NUMERIC(8,4),
    supertrend_10_3         NUMERIC(18,4),
    supertrend_direction    SMALLINT,
    psar                    NUMERIC(18,4),

    -- Statistics
    zscore_20               NUMERIC(10,4),
    linreg_slope_20         NUMERIC(18,4),
    linreg_r2_20            NUMERIC(8,4),
    linreg_angle_20         NUMERIC(8,4),
    skew_20                 NUMERIC(10,4),
    kurt_20                 NUMERIC(10,4),

    -- Risk (empyrical)
    risk_sharpe_1y          NUMERIC(10,4),
    risk_sortino_1y         NUMERIC(10,4),
    risk_calmar_1y          NUMERIC(10,4),
    risk_max_drawdown_1y    NUMERIC(10,4),
    risk_beta_nifty         NUMERIC(10,4),
    risk_alpha_nifty        NUMERIC(10,4),
    risk_omega              NUMERIC(10,4),
    risk_information_ratio  NUMERIC(10,4),

    -- Derived booleans (GENERATED STORED) — Fix 9: byte-identical SQL strings
    above_50dma             BOOLEAN GENERATED ALWAYS AS (close_adj > sma_50) STORED,
    above_200dma            BOOLEAN GENERATED ALWAYS AS (close_adj > sma_200) STORED,
    above_20ema             BOOLEAN GENERATED ALWAYS AS (close_adj > ema_20) STORED,
    price_above_vwap        BOOLEAN GENERATED ALWAYS AS (close_adj > vwap) STORED,
    rsi_overbought          BOOLEAN GENERATED ALWAYS AS (rsi_14 > 70) STORED,
    rsi_oversold            BOOLEAN GENERATED ALWAYS AS (rsi_14 < 30) STORED,
    macd_bullish            BOOLEAN GENERATED ALWAYS AS (macd_line > macd_signal) STORED,
    adx_strong_trend        BOOLEAN GENERATED ALWAYS AS (adx_14 > 25) STORED,

    -- Audit
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
"""

# Fix 12: Index table column block — strip all volume-dependent columns.
# Removed: obv, ad, adosc_3_10, cmf_20, efi_13, eom_14, kvo, pvt, vwap,
#          price_above_vwap (references vwap), mfi_14.
_INDEX_COLUMN_BLOCK = """
    -- Price snapshot
    close_adj               NUMERIC(18,4),

    -- Overlap / Trend (vwap excluded — no volume for indices)
    sma_5                   NUMERIC(18,4),
    sma_10                  NUMERIC(18,4),
    sma_20                  NUMERIC(18,4),
    sma_50                  NUMERIC(18,4),
    sma_100                 NUMERIC(18,4),
    sma_200                 NUMERIC(18,4),
    ema_5                   NUMERIC(18,4),
    ema_10                  NUMERIC(18,4),
    ema_20                  NUMERIC(18,4),
    ema_50                  NUMERIC(18,4),
    ema_100                 NUMERIC(18,4),
    ema_200                 NUMERIC(18,4),
    dema_20                 NUMERIC(18,4),
    tema_20                 NUMERIC(18,4),
    wma_20                  NUMERIC(18,4),
    hma_20                  NUMERIC(18,4),
    kama_20                 NUMERIC(18,4),
    zlma_20                 NUMERIC(18,4),
    alma_20                 NUMERIC(18,4),

    -- Momentum (mfi_14 excluded — needs volume)
    rsi_7                   NUMERIC(8,4),
    rsi_9                   NUMERIC(8,4),
    rsi_14                  NUMERIC(8,4),
    rsi_21                  NUMERIC(8,4),
    macd_line               NUMERIC(18,4),
    macd_signal             NUMERIC(18,4),
    macd_histogram          NUMERIC(18,4),
    stochastic_k            NUMERIC(8,4),
    stochastic_d            NUMERIC(8,4),
    cci_20                  NUMERIC(10,4),
    roc_5                   NUMERIC(10,4),
    roc_10                  NUMERIC(10,4),
    roc_21                  NUMERIC(10,4),
    roc_63                  NUMERIC(10,4),
    roc_252                 NUMERIC(10,4),
    tsi_13_25               NUMERIC(10,4),
    williams_r_14           NUMERIC(8,4),
    cmo_14                  NUMERIC(10,4),
    trix_15                 NUMERIC(10,4),
    ultosc                  NUMERIC(8,4),

    -- Volatility
    bb_upper                NUMERIC(18,4),
    bb_middle               NUMERIC(18,4),
    bb_lower                NUMERIC(18,4),
    bb_width                NUMERIC(8,4),
    bb_pct_b                NUMERIC(8,4),
    atr_7                   NUMERIC(18,4),
    atr_14                  NUMERIC(18,4),
    atr_21                  NUMERIC(18,4),
    natr_14                 NUMERIC(8,4),
    true_range              NUMERIC(18,4),
    keltner_upper           NUMERIC(18,4),
    keltner_middle          NUMERIC(18,4),
    keltner_lower           NUMERIC(18,4),
    donchian_upper          NUMERIC(18,4),
    donchian_middle         NUMERIC(18,4),
    donchian_lower          NUMERIC(18,4),
    hv_20                   NUMERIC(10,4),
    hv_60                   NUMERIC(10,4),
    hv_252                  NUMERIC(10,4),

    -- Trend strength
    adx_14                  NUMERIC(8,4),
    plus_di                 NUMERIC(8,4),
    minus_di                NUMERIC(8,4),
    aroon_up                NUMERIC(8,4),
    aroon_down              NUMERIC(8,4),
    aroon_osc               NUMERIC(8,4),
    supertrend_10_3         NUMERIC(18,4),
    supertrend_direction    SMALLINT,
    psar                    NUMERIC(18,4),

    -- Statistics
    zscore_20               NUMERIC(10,4),
    linreg_slope_20         NUMERIC(18,4),
    linreg_r2_20            NUMERIC(8,4),
    linreg_angle_20         NUMERIC(8,4),
    skew_20                 NUMERIC(10,4),
    kurt_20                 NUMERIC(10,4),

    -- Risk (empyrical)
    risk_sharpe_1y          NUMERIC(10,4),
    risk_sortino_1y         NUMERIC(10,4),
    risk_calmar_1y          NUMERIC(10,4),
    risk_max_drawdown_1y    NUMERIC(10,4),
    risk_beta_nifty         NUMERIC(10,4),
    risk_alpha_nifty        NUMERIC(10,4),
    risk_omega              NUMERIC(10,4),
    risk_information_ratio  NUMERIC(10,4),

    -- Derived booleans — Fix 9: byte-identical. price_above_vwap excluded (vwap absent).
    above_50dma             BOOLEAN GENERATED ALWAYS AS (close_adj > sma_50) STORED,
    above_200dma            BOOLEAN GENERATED ALWAYS AS (close_adj > sma_200) STORED,
    above_20ema             BOOLEAN GENERATED ALWAYS AS (close_adj > ema_20) STORED,
    rsi_overbought          BOOLEAN GENERATED ALWAYS AS (rsi_14 > 70) STORED,
    rsi_oversold            BOOLEAN GENERATED ALWAYS AS (rsi_14 < 30) STORED,
    macd_bullish            BOOLEAN GENERATED ALWAYS AS (macd_line > macd_signal) STORED,
    adx_strong_trend        BOOLEAN GENERATED ALWAYS AS (adx_14 > 25) STORED,

    -- Audit
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
"""

# Fix 13: MF table strict subset — single-price indicators only.
# Excluded: all volume cols, vwap, price_above_vwap, mfi_14,
#           atr/natr/true_range, keltner, donchian, psar, supertrend,
#           cci_20, williams_r_14, ultosc, aroon family,
#           stochastic_k/d, adx_14/plus_di/minus_di, adx_strong_trend.
_MF_COLUMN_BLOCK = """
    -- Price snapshot (close_adj stores NAV for MF uniformity)
    close_adj               NUMERIC(18,4),

    -- Overlap / Trend — single-price only
    sma_5                   NUMERIC(18,4),
    sma_10                  NUMERIC(18,4),
    sma_20                  NUMERIC(18,4),
    sma_50                  NUMERIC(18,4),
    sma_100                 NUMERIC(18,4),
    sma_200                 NUMERIC(18,4),
    ema_5                   NUMERIC(18,4),
    ema_10                  NUMERIC(18,4),
    ema_20                  NUMERIC(18,4),
    ema_50                  NUMERIC(18,4),
    ema_100                 NUMERIC(18,4),
    ema_200                 NUMERIC(18,4),
    dema_20                 NUMERIC(18,4),
    tema_20                 NUMERIC(18,4),
    wma_20                  NUMERIC(18,4),
    hma_20                  NUMERIC(18,4),
    kama_20                 NUMERIC(18,4),
    zlma_20                 NUMERIC(18,4),
    alma_20                 NUMERIC(18,4),

    -- Momentum — single-price only
    rsi_7                   NUMERIC(8,4),
    rsi_9                   NUMERIC(8,4),
    rsi_14                  NUMERIC(8,4),
    rsi_21                  NUMERIC(8,4),
    macd_line               NUMERIC(18,4),
    macd_signal             NUMERIC(18,4),
    macd_histogram          NUMERIC(18,4),
    roc_5                   NUMERIC(10,4),
    roc_10                  NUMERIC(10,4),
    roc_21                  NUMERIC(10,4),
    roc_63                  NUMERIC(10,4),
    roc_252                 NUMERIC(10,4),
    tsi_13_25               NUMERIC(10,4),
    cmo_14                  NUMERIC(10,4),
    trix_15                 NUMERIC(10,4),

    -- Volatility — single-price only (BBands + HV)
    bb_upper                NUMERIC(18,4),
    bb_middle               NUMERIC(18,4),
    bb_lower                NUMERIC(18,4),
    bb_width                NUMERIC(8,4),
    bb_pct_b                NUMERIC(8,4),
    hv_20                   NUMERIC(10,4),
    hv_60                   NUMERIC(10,4),
    hv_252                  NUMERIC(10,4),

    -- Statistics
    zscore_20               NUMERIC(10,4),
    linreg_slope_20         NUMERIC(18,4),
    linreg_r2_20            NUMERIC(8,4),
    linreg_angle_20         NUMERIC(8,4),
    skew_20                 NUMERIC(10,4),
    kurt_20                 NUMERIC(10,4),

    -- Risk (empyrical)
    risk_sharpe_1y          NUMERIC(10,4),
    risk_sortino_1y         NUMERIC(10,4),
    risk_calmar_1y          NUMERIC(10,4),
    risk_max_drawdown_1y    NUMERIC(10,4),
    risk_beta_nifty         NUMERIC(10,4),
    risk_alpha_nifty        NUMERIC(10,4),
    risk_omega              NUMERIC(10,4),
    risk_information_ratio  NUMERIC(10,4),

    -- Derived booleans — only reference columns that exist in this table
    above_50dma             BOOLEAN GENERATED ALWAYS AS (close_adj > sma_50) STORED,
    above_200dma            BOOLEAN GENERATED ALWAYS AS (close_adj > sma_200) STORED,
    above_20ema             BOOLEAN GENERATED ALWAYS AS (close_adj > ema_20) STORED,
    rsi_overbought          BOOLEAN GENERATED ALWAYS AS (rsi_14 > 70) STORED,
    rsi_oversold            BOOLEAN GENERATED ALWAYS AS (rsi_14 < 30) STORED,
    macd_bullish            BOOLEAN GENERATED ALWAYS AS (macd_line > macd_signal) STORED,

    -- Audit
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
"""


def upgrade() -> None:
    # -------------------------------------------------------------------------
    # 1. de_equity_technical_daily_v2
    #    PK: (date, instrument_id UUID)
    #    FK: instrument_id -> de_instrument(id) ON DELETE CASCADE
    # -------------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE de_equity_technical_daily_v2 (
            date            DATE        NOT NULL,
            instrument_id   UUID        NOT NULL
                                REFERENCES de_instrument(id) ON DELETE CASCADE,
            {_FULL_COLUMN_BLOCK},
            PRIMARY KEY (date, instrument_id)
        )
    """)
    op.execute(
        "CREATE INDEX ix_de_equity_technical_daily_v2_instrument_id "
        "ON de_equity_technical_daily_v2 (instrument_id)"
    )
    op.execute(
        "CREATE INDEX ix_de_equity_technical_daily_v2_instrument_id_date "
        "ON de_equity_technical_daily_v2 (instrument_id, date DESC)"
    )

    # -------------------------------------------------------------------------
    # 2. de_etf_technical_daily_v2
    #    PK: (date, ticker VARCHAR(20))
    #    FK: ticker -> de_etf_master(ticker) ON DELETE CASCADE
    # -------------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE de_etf_technical_daily_v2 (
            date            DATE        NOT NULL,
            ticker          VARCHAR(20) NOT NULL
                                REFERENCES de_etf_master(ticker) ON DELETE CASCADE,
            {_FULL_COLUMN_BLOCK},
            PRIMARY KEY (date, ticker)
        )
    """)
    op.execute(
        "CREATE INDEX ix_de_etf_technical_daily_v2_ticker "
        "ON de_etf_technical_daily_v2 (ticker)"
    )
    op.execute(
        "CREATE INDEX ix_de_etf_technical_daily_v2_ticker_date "
        "ON de_etf_technical_daily_v2 (ticker, date DESC)"
    )

    # -------------------------------------------------------------------------
    # 3. de_global_technical_daily_v2
    #    PK: (date, ticker VARCHAR(30))
    #    FK: ticker -> de_global_instrument_master(ticker) ON DELETE CASCADE
    # -------------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE de_global_technical_daily_v2 (
            date            DATE        NOT NULL,
            ticker          VARCHAR(30) NOT NULL
                                REFERENCES de_global_instrument_master(ticker) ON DELETE CASCADE,
            {_FULL_COLUMN_BLOCK},
            PRIMARY KEY (date, ticker)
        )
    """)
    op.execute(
        "CREATE INDEX ix_de_global_technical_daily_v2_ticker "
        "ON de_global_technical_daily_v2 (ticker)"
    )
    op.execute(
        "CREATE INDEX ix_de_global_technical_daily_v2_ticker_date "
        "ON de_global_technical_daily_v2 (ticker, date DESC)"
    )

    # -------------------------------------------------------------------------
    # 4. de_index_technical_daily (NEW — greenfield, no v1)
    #    PK: (date, index_code VARCHAR(50))
    #    FK: index_code -> de_index_master(index_code) ON DELETE CASCADE
    #    Fix 12: volume-dependent columns omitted (obv, ad, adosc_3_10, cmf_20,
    #            efi_13, eom_14, kvo, pvt, vwap, price_above_vwap, mfi_14)
    # -------------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE de_index_technical_daily (
            date            DATE        NOT NULL,
            index_code      VARCHAR(50) NOT NULL
                                REFERENCES de_index_master(index_code) ON DELETE CASCADE,
            {_INDEX_COLUMN_BLOCK},
            PRIMARY KEY (date, index_code)
        )
    """)
    op.execute(
        "CREATE INDEX ix_de_index_technical_daily_index_code "
        "ON de_index_technical_daily (index_code)"
    )
    op.execute(
        "CREATE INDEX ix_de_index_technical_daily_index_code_date "
        "ON de_index_technical_daily (index_code, date DESC)"
    )

    # -------------------------------------------------------------------------
    # 5. de_mf_technical_daily (NEW — greenfield, no v1)
    #    PK: (nav_date, mstar_id VARCHAR(20))
    #    FK: mstar_id -> de_mf_master(mstar_id) ON DELETE CASCADE
    #    Fix 13: strict subset — single-price indicators only
    # -------------------------------------------------------------------------
    op.execute(f"""
        CREATE TABLE de_mf_technical_daily (
            nav_date        DATE        NOT NULL,
            mstar_id        VARCHAR(20) NOT NULL
                                REFERENCES de_mf_master(mstar_id) ON DELETE CASCADE,
            {_MF_COLUMN_BLOCK},
            PRIMARY KEY (nav_date, mstar_id)
        )
    """)
    op.execute(
        "CREATE INDEX ix_de_mf_technical_daily_mstar_id "
        "ON de_mf_technical_daily (mstar_id)"
    )
    op.execute(
        "CREATE INDEX ix_de_mf_technical_daily_mstar_id_date "
        "ON de_mf_technical_daily (mstar_id, nav_date DESC)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS de_mf_technical_daily")
    op.execute("DROP TABLE IF EXISTS de_index_technical_daily")
    op.execute("DROP TABLE IF EXISTS de_global_technical_daily_v2")
    op.execute("DROP TABLE IF EXISTS de_etf_technical_daily_v2")
    op.execute("DROP TABLE IF EXISTS de_equity_technical_daily_v2")
