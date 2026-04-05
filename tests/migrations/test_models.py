"""Unit tests for all SQLAlchemy models — table names, columns, constraints, FKs."""

from __future__ import annotations

import pytest
import sqlalchemy as sa

from app.db.base import Base
from app.models import (
    DeBreadthDaily,
    DeChampionTrades,
    DeClientKeys,
    DeClients,
    DeContributors,
    DeCorporateActions,
    DeDataAnomalies,
    DeEquityOhlcv,
    DeEquityTechnicalDaily,
    DeFoSummary,
    DeGlobalInstrumentMaster,
    DeGlobalPrices,
    DeIndexConstituents,
    DeIndexMaster,
    DeIndexPrices,
    DeInstitutionalFlows,
    DeInstrument,
    DeMacroMaster,
    DeMacroValues,
    DeMarketCapHistory,
    DeMarketRegime,
    DeMfCategoryFlows,
    DeMfDividends,
    DeMfHoldings,
    DeMfLifecycle,
    DeMfMaster,
    DeMfNavDaily,
    DeMigrationErrors,
    DeMigrationLog,
    DePiiAccessLog,
    DePipelineLog,
    DePortfolioHoldings,
    DePortfolioNav,
    DePortfolioRiskMetrics,
    DePortfolios,
    DePortfolioTransactions,
    DeQualDocuments,
    DeQualExtracts,
    DeQualOutcomes,
    DeQualSources,
    DeRecomputeQueue,
    DeRequestLog,
    DeRsDailySummary,
    DeRsScores,
    DeSourceFiles,
    DeSymbolHistory,
    DeSystemFlags,
    DeTradingCalendar,
    DeAdjustmentFactorsDaily,
)


# ---------------------------------------------------------------------------
# Helper: get column names from a model
# ---------------------------------------------------------------------------

def col_names(model_class) -> set[str]:
    return {c.name for c in model_class.__table__.columns}


def constraint_names(model_class) -> set[str]:
    return {c.name for c in model_class.__table__.constraints if c.name}


def fk_target_tables(model_class) -> set[str]:
    targets = set()
    for fk in model_class.__table__.foreign_keys:
        targets.add(fk.column.table.name)
    return targets


# ---------------------------------------------------------------------------
# Test: all models are registered with Base.metadata
# ---------------------------------------------------------------------------

class TestModelRegistration:
    def test_all_expected_tables_registered(self):
        registered = set(Base.metadata.tables.keys())
        expected = {
            "de_instrument", "de_market_cap_history", "de_symbol_history",
            "de_index_master", "de_index_constituents", "de_mf_master",
            "de_mf_lifecycle", "de_macro_master", "de_global_instrument_master",
            "de_contributors", "de_trading_calendar",
            "de_source_files", "de_pipeline_log", "de_system_flags",
            "de_migration_log", "de_migration_errors", "de_request_log",
            "de_equity_ohlcv", "de_corporate_actions", "de_adjustment_factors_daily",
            "de_recompute_queue", "de_data_anomalies",
            "de_mf_nav_daily", "de_mf_dividends",
            "de_index_prices", "de_global_prices", "de_macro_values",
            "de_institutional_flows", "de_mf_category_flows",
            "de_equity_technical_daily", "de_rs_scores", "de_rs_daily_summary",
            "de_market_regime", "de_breadth_daily", "de_fo_summary",
            "de_qual_sources", "de_qual_documents", "de_qual_extracts", "de_qual_outcomes",
            "de_clients", "de_client_keys", "de_pii_access_log",
            "de_portfolios", "de_portfolio_nav", "de_portfolio_transactions",
            "de_portfolio_holdings", "de_portfolio_risk_metrics",
            "de_champion_trades",
            "de_mf_holdings",
        }
        missing = expected - registered
        assert not missing, f"Tables not registered: {missing}"


# ---------------------------------------------------------------------------
# Test: instrument tables
# ---------------------------------------------------------------------------

class TestInstrumentModels:
    def test_de_instrument_table_name(self):
        assert DeInstrument.__tablename__ == "de_instrument"

    def test_de_instrument_required_columns(self):
        cols = col_names(DeInstrument)
        for c in ("id", "current_symbol", "created_at", "updated_at"):
            assert c in cols, f"Missing column: {c}"

    def test_de_instrument_current_symbol_unique(self):
        unique_cols = set()
        for uc in DeInstrument.__table__.constraints:
            if isinstance(uc, sa.UniqueConstraint):
                for col in uc.columns:
                    unique_cols.add(col.name)
        assert "current_symbol" in unique_cols

    def test_de_instrument_boolean_flags(self):
        cols = col_names(DeInstrument)
        for flag in ("nifty_50", "nifty_200", "nifty_500", "is_active", "is_suspended", "is_tradeable"):
            assert flag in cols

    def test_de_market_cap_history_check_constraint(self):
        names = constraint_names(DeMarketCapHistory)
        assert "chk_market_cap_category" in names

    def test_de_market_cap_history_fk_to_instrument(self):
        assert "de_instrument" in fk_target_tables(DeMarketCapHistory)

    def test_de_symbol_history_composite_pk(self):
        pk_cols = {c.name for c in DeSymbolHistory.__table__.primary_key.columns}
        assert pk_cols == {"instrument_id", "effective_date"}

    def test_de_index_master_check_constraint(self):
        names = constraint_names(DeIndexMaster)
        assert "chk_index_category" in names

    def test_de_index_constituents_fks(self):
        fk_targets = fk_target_tables(DeIndexConstituents)
        assert "de_index_master" in fk_targets
        assert "de_instrument" in fk_targets

    def test_de_mf_master_self_ref_fk(self):
        fk_targets = fk_target_tables(DeMfMaster)
        assert "de_mf_master" in fk_targets

    def test_de_mf_lifecycle_check_constraint(self):
        names = constraint_names(DeMfLifecycle)
        assert "chk_mf_lifecycle_event_type" in names

    def test_de_macro_master_check_constraints(self):
        names = constraint_names(DeMacroMaster)
        assert "chk_macro_source" in names
        assert "chk_macro_frequency" in names

    def test_de_global_instrument_master_check_constraint(self):
        names = constraint_names(DeGlobalInstrumentMaster)
        assert "chk_global_instrument_type" in names

    def test_de_contributors_unique_name(self):
        names = constraint_names(DeContributors)
        assert "uq_contributors_name" in names

    def test_de_trading_calendar_has_required_cols(self):
        cols = col_names(DeTradingCalendar)
        for c in ("date", "is_trading", "exchange", "created_at", "updated_at"):
            assert c in cols


# ---------------------------------------------------------------------------
# Test: pipeline tables
# ---------------------------------------------------------------------------

class TestPipelineModels:
    def test_de_source_files_unique_constraint(self):
        names = constraint_names(DeSourceFiles)
        assert "uq_source_files_dedup" in names

    def test_de_pipeline_log_status_check(self):
        names = constraint_names(DePipelineLog)
        assert "chk_pipeline_log_status" in names

    def test_de_pipeline_log_unique_run(self):
        names = constraint_names(DePipelineLog)
        assert "uq_pipeline_log_run" in names

    def test_de_system_flags_pk_is_key(self):
        pk_cols = {c.name for c in DeSystemFlags.__table__.primary_key.columns}
        assert "key" in pk_cols

    def test_de_migration_log_status_check(self):
        names = constraint_names(DeMigrationLog)
        assert "chk_migration_log_status" in names

    def test_de_migration_errors_fk_to_migration_log(self):
        assert "de_migration_log" in fk_target_tables(DeMigrationErrors)

    def test_de_request_log_has_required_cols(self):
        cols = col_names(DeRequestLog)
        for c in ("id", "request_id", "actor", "method", "endpoint", "status_code"):
            assert c in cols


# ---------------------------------------------------------------------------
# Test: price tables
# ---------------------------------------------------------------------------

class TestPriceModels:
    def test_de_equity_ohlcv_composite_pk(self):
        pk_cols = {c.name for c in DeEquityOhlcv.__table__.primary_key.columns}
        assert pk_cols == {"date", "instrument_id"}

    def test_de_equity_ohlcv_data_status_check(self):
        names = constraint_names(DeEquityOhlcv)
        assert "chk_equity_ohlcv_data_status" in names

    def test_de_equity_ohlcv_numeric_precision(self):
        col = DeEquityOhlcv.__table__.c["close"]
        assert isinstance(col.type, sa.Numeric)

    def test_de_equity_ohlcv_fk_source_files(self):
        assert "de_source_files" in fk_target_tables(DeEquityOhlcv)

    def test_de_equity_ohlcv_fk_pipeline_log(self):
        assert "de_pipeline_log" in fk_target_tables(DeEquityOhlcv)

    def test_de_corporate_actions_unique_constraint(self):
        names = constraint_names(DeCorporateActions)
        assert "uq_corporate_actions" in names

    def test_de_corporate_actions_action_type_check(self):
        names = constraint_names(DeCorporateActions)
        assert "chk_corp_action_type" in names

    def test_de_adjustment_factors_daily_composite_pk(self):
        pk_cols = {c.name for c in DeAdjustmentFactorsDaily.__table__.primary_key.columns}
        assert pk_cols == {"instrument_id", "date"}

    def test_de_recompute_queue_status_check(self):
        names = constraint_names(DeRecomputeQueue)
        assert "chk_recompute_queue_status" in names

    def test_de_recompute_queue_priority_check(self):
        names = constraint_names(DeRecomputeQueue)
        assert "chk_recompute_queue_priority" in names

    def test_de_data_anomalies_entity_type_check(self):
        names = constraint_names(DeDataAnomalies)
        assert "chk_data_anomaly_entity_type" in names

    def test_de_data_anomalies_severity_check(self):
        names = constraint_names(DeDataAnomalies)
        assert "chk_data_anomaly_severity" in names

    def test_de_mf_nav_daily_composite_pk(self):
        pk_cols = {c.name for c in DeMfNavDaily.__table__.primary_key.columns}
        assert pk_cols == {"nav_date", "mstar_id"}

    def test_de_mf_nav_daily_nav_check(self):
        names = constraint_names(DeMfNavDaily)
        assert "chk_mf_nav_positive" in names

    def test_de_mf_dividends_unique(self):
        names = constraint_names(DeMfDividends)
        assert "uq_mf_dividends" in names

    def test_de_mf_dividends_check(self):
        names = constraint_names(DeMfDividends)
        assert "chk_mf_div_positive" in names

    def test_de_index_prices_composite_pk(self):
        pk_cols = {c.name for c in DeIndexPrices.__table__.primary_key.columns}
        assert pk_cols == {"date", "index_code"}

    def test_de_global_prices_composite_pk(self):
        pk_cols = {c.name for c in DeGlobalPrices.__table__.primary_key.columns}
        assert pk_cols == {"date", "ticker"}

    def test_de_macro_values_composite_pk(self):
        pk_cols = {c.name for c in DeMacroValues.__table__.primary_key.columns}
        assert pk_cols == {"date", "ticker"}


# ---------------------------------------------------------------------------
# Test: flow tables
# ---------------------------------------------------------------------------

class TestFlowModels:
    def test_de_institutional_flows_composite_pk(self):
        pk_cols = {c.name for c in DeInstitutionalFlows.__table__.primary_key.columns}
        assert pk_cols == {"date", "category", "market_type"}

    def test_de_institutional_flows_has_net_flow(self):
        cols = col_names(DeInstitutionalFlows)
        assert "net_flow" in cols

    def test_de_mf_category_flows_composite_pk(self):
        pk_cols = {c.name for c in DeMfCategoryFlows.__table__.primary_key.columns}
        assert pk_cols == {"month_date", "category"}

    def test_de_institutional_flows_check_constraints(self):
        names = constraint_names(DeInstitutionalFlows)
        assert "chk_inst_flow_category" in names
        assert "chk_inst_flow_market_type" in names


# ---------------------------------------------------------------------------
# Test: computed tables
# ---------------------------------------------------------------------------

class TestComputedModels:
    def test_de_equity_technical_daily_has_generated_cols(self):
        cols = col_names(DeEquityTechnicalDaily)
        assert "above_50dma" in cols
        assert "above_200dma" in cols

    def test_de_rs_scores_composite_pk(self):
        pk_cols = {c.name for c in DeRsScores.__table__.primary_key.columns}
        assert pk_cols == {"date", "entity_type", "entity_id", "vs_benchmark"}

    def test_de_market_regime_check_constraints(self):
        names = constraint_names(DeMarketRegime)
        assert "chk_market_regime_type" in names
        assert "chk_market_regime_confidence" in names

    def test_de_breadth_daily_check_constraints(self):
        names = constraint_names(DeBreadthDaily)
        assert "chk_breadth_pct_200dma" in names
        assert "chk_breadth_pct_50dma" in names

    def test_de_fo_summary_has_pcr_columns(self):
        cols = col_names(DeFoSummary)
        assert "pcr_oi" in cols
        assert "pcr_volume" in cols


# ---------------------------------------------------------------------------
# Test: qualitative tables
# ---------------------------------------------------------------------------

class TestQualitativeModels:
    def test_de_qual_sources_unique_name(self):
        names = constraint_names(DeQualSources)
        assert "uq_qual_sources_name" in names

    def test_de_qual_sources_type_check(self):
        names = constraint_names(DeQualSources)
        assert "chk_qual_source_type" in names

    def test_de_qual_documents_unique_hash(self):
        names = constraint_names(DeQualDocuments)
        assert "uq_qual_doc_source_hash" in names

    def test_de_qual_documents_format_check(self):
        names = constraint_names(DeQualDocuments)
        assert "chk_qual_doc_format" in names

    def test_de_qual_documents_has_embedding_col(self):
        cols = col_names(DeQualDocuments)
        assert "embedding" in cols

    def test_de_qual_extracts_direction_check(self):
        names = constraint_names(DeQualExtracts)
        assert "chk_qual_extract_direction" in names

    def test_de_qual_extracts_quality_score_check(self):
        names = constraint_names(DeQualExtracts)
        assert "chk_qual_extract_quality" in names

    def test_de_qual_outcomes_fk_to_extracts(self):
        assert "de_qual_extracts" in fk_target_tables(DeQualOutcomes)


# ---------------------------------------------------------------------------
# Test: client tables
# ---------------------------------------------------------------------------

class TestClientModels:
    def test_de_clients_pk_is_client_id(self):
        pk_cols = {c.name for c in DeClients.__table__.primary_key.columns}
        assert "client_id" in pk_cols

    def test_de_clients_has_hash_columns(self):
        cols = col_names(DeClients)
        for c in ("pan_hash", "email_hash", "phone_hash"):
            assert c in cols

    def test_de_client_keys_composite_pk(self):
        pk_cols = {c.name for c in DeClientKeys.__table__.primary_key.columns}
        assert pk_cols == {"client_id", "key_version"}

    def test_de_portfolios_fk_to_clients(self):
        assert "de_clients" in fk_target_tables(DePortfolios)

    def test_de_portfolio_nav_check(self):
        names = constraint_names(DePortfolioNav)
        assert "chk_portfolio_nav_positive" in names

    def test_de_portfolio_transactions_type_check(self):
        names = constraint_names(DePortfolioTransactions)
        assert "chk_portfolio_txn_type" in names

    def test_de_portfolio_transactions_unique_constraint(self):
        names = constraint_names(DePortfolioTransactions)
        assert "uq_portfolio_transactions" in names

    def test_de_portfolio_holdings_weight_check(self):
        names = constraint_names(DePortfolioHoldings)
        assert "chk_portfolio_holdings_weight" in names

    def test_de_portfolio_risk_metrics_has_sharpe_alpha_beta(self):
        cols = col_names(DePortfolioRiskMetrics)
        for c in ("sharpe_ratio", "alpha", "beta", "max_drawdown"):
            assert c in cols


# ---------------------------------------------------------------------------
# Test: champion trades
# ---------------------------------------------------------------------------

class TestChampionModels:
    def test_de_champion_trades_direction_check(self):
        names = constraint_names(DeChampionTrades)
        assert "chk_champion_trade_direction" in names

    def test_de_champion_trades_stage_check(self):
        names = constraint_names(DeChampionTrades)
        assert "chk_champion_trade_stage" in names

    def test_de_champion_trades_source_ref_unique(self):
        names = constraint_names(DeChampionTrades)
        assert "uq_champion_trades_source_ref" in names


# ---------------------------------------------------------------------------
# Test: MF holdings
# ---------------------------------------------------------------------------

class TestHoldingsModels:
    def test_de_mf_holdings_unique_constraint(self):
        names = constraint_names(DeMfHoldings)
        assert "uq_mf_holdings" in names

    def test_de_mf_holdings_fk_to_mf_master(self):
        assert "de_mf_master" in fk_target_tables(DeMfHoldings)

    def test_de_mf_holdings_has_is_mapped(self):
        cols = col_names(DeMfHoldings)
        assert "is_mapped" in cols


# ---------------------------------------------------------------------------
# Test: column type validations (Numeric never Float for financial)
# ---------------------------------------------------------------------------

class TestFinancialColumnTypes:
    """Ensure all financial value columns are Numeric, never Float."""

    def _assert_numeric_not_float(self, model_class, col_name: str):
        col = model_class.__table__.c[col_name]
        assert isinstance(col.type, sa.Numeric), (
            f"{model_class.__tablename__}.{col_name} must be Numeric, got {type(col.type)}"
        )

    def test_equity_ohlcv_close_is_numeric(self):
        self._assert_numeric_not_float(DeEquityOhlcv, "close")

    def test_equity_ohlcv_open_is_numeric(self):
        self._assert_numeric_not_float(DeEquityOhlcv, "open")

    def test_mf_nav_daily_nav_is_numeric(self):
        self._assert_numeric_not_float(DeMfNavDaily, "nav")

    def test_adjustment_factor_is_numeric(self):
        self._assert_numeric_not_float(DeAdjustmentFactorsDaily, "cumulative_factor")

    def test_mf_dividends_per_unit_is_numeric(self):
        self._assert_numeric_not_float(DeMfDividends, "dividend_per_unit")

    def test_portfolio_nav_is_numeric(self):
        self._assert_numeric_not_float(DePortfolioNav, "nav")

    def test_mf_expense_ratio_is_numeric(self):
        self._assert_numeric_not_float(DeMfMaster, "expense_ratio")

    def test_rs_composite_is_numeric(self):
        self._assert_numeric_not_float(DeRsScores, "rs_composite")


# ---------------------------------------------------------------------------
# Test: created_at present on all tables (database convention)
# ---------------------------------------------------------------------------

class TestCreatedAtConvention:
    ALL_MODELS = [
        DeInstrument, DeMarketCapHistory, DeSymbolHistory, DeIndexMaster,
        DeIndexConstituents, DeMfMaster, DeMfLifecycle, DeMacroMaster,
        DeGlobalInstrumentMaster, DeContributors, DeTradingCalendar,
        DeSourceFiles, DePipelineLog, DeSystemFlags, DeMigrationLog,
        DeMigrationErrors, DeEquityOhlcv, DeCorporateActions,
        DeAdjustmentFactorsDaily, DeRecomputeQueue, DeDataAnomalies,
        DeMfNavDaily, DeMfDividends, DeIndexPrices, DeGlobalPrices,
        DeMacroValues, DeInstitutionalFlows, DeMfCategoryFlows,
        DeEquityTechnicalDaily, DeRsScores, DeRsDailySummary,
        DeMarketRegime, DeBreadthDaily, DeFoSummary,
        DeQualSources, DeQualDocuments, DeQualExtracts, DeQualOutcomes,
        DeClients, DeClientKeys, DePiiAccessLog, DePortfolios,
        DePortfolioNav, DePortfolioTransactions, DePortfolioHoldings,
        DePortfolioRiskMetrics, DeChampionTrades, DeMfHoldings, DeRequestLog,
    ]

    @pytest.mark.parametrize("model_class", ALL_MODELS)
    def test_created_at_column_exists(self, model_class):
        cols = col_names(model_class)
        assert "created_at" in cols, f"{model_class.__tablename__} missing created_at"

    @pytest.mark.parametrize("model_class", ALL_MODELS)
    def test_created_at_is_timezone_aware(self, model_class):
        col = model_class.__table__.c["created_at"]
        assert isinstance(col.type, sa.TIMESTAMP), (
            f"{model_class.__tablename__}.created_at must be TIMESTAMP"
        )
        assert col.type.timezone is True, (
            f"{model_class.__tablename__}.created_at must be timezone-aware"
        )
