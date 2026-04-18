"""Pipeline registry — maps names to classes and defines schedule groups.

Two registries:
  PIPELINE_REGISTRY  — pipeline_name str → BasePipeline subclass
  SCHEDULE_REGISTRY  — schedule group name → ordered list of pipeline names

DAG alias map handles the mismatch between DAG graph names
(e.g. "nse_bhav") and pipeline class names (e.g. "equity_bhav").
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from app.logging import get_logger

if TYPE_CHECKING:
    from app.pipelines.framework import BasePipeline

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Lazy import to avoid circular imports — each entry is (module_path, class_name)
# ---------------------------------------------------------------------------
_PIPELINE_CLASSES: dict[str, tuple[str, str]] = {
    # Equity
    "equity_bhav": ("app.pipelines.equity.bhav", "BhavPipeline"),
    "equity_delivery": ("app.pipelines.equity.delivery", "DeliveryPipeline"),
    "equity_eod": ("app.pipelines.equity.eod", "EodOrchestrator"),
    "equity_master_refresh": ("app.pipelines.equity.master_refresh", "MasterRefreshPipeline"),
    "equity_corporate_actions": ("app.pipelines.equity.corporate_actions", "CorporateActionsPipeline"),
    "market_cap_history": ("app.pipelines.equity.market_cap_history", "MarketCapHistoryPipeline"),
    "symbol_history": ("app.pipelines.equity.symbol_history", "SymbolHistoryPipeline"),
    "fo_bhavcopy": ("app.pipelines.equity.fo_bhavcopy", "FoBhavcopyPipeline"),
    "fo_ban_list": ("app.pipelines.equity.fo_ban", "FoBanListPipeline"),
    # Indices
    "index_prices": ("app.pipelines.indices.index_prices", "IndexPricePipeline"),
    "nse_indices": ("app.pipelines.indices.nse_indices", "NseIndicesPipeline"),
    "index_constituents": ("app.pipelines.indices.index_constituents", "IndexConstituentsPipeline"),
    "india_vix": ("app.pipelines.indices.vix", "IndiaVixPipeline"),
    # Mutual Funds
    "mf_eod": ("app.pipelines.mf.eod", "MfEodPipeline"),
    # Global Data
    "yfinance_global": ("app.pipelines.global_data.yfinance_pipeline", "YfinancePipeline"),
    "fred_macro": ("app.pipelines.global_data.fred_pipeline", "FredPipeline"),
    # Flows
    "fii_dii_flows": ("app.pipelines.flows.fii_dii", "FiiDiiFlowsPipeline"),
    "fo_summary": ("app.pipelines.flows.fo_summary", "FoSummaryPipeline"),
    "mf_category_flows": ("app.pipelines.flows.mf_category_flows", "MfCategoryFlowsPipeline"),
    "participant_oi": ("app.pipelines.flows.participant_oi", "ParticipantOiPipeline"),
    # Macro (RBI / CCIL / FBIL)
    "gsec_yields": ("app.pipelines.macro.gsec_yields", "GsecYieldsPipeline"),
    "rbi_fx_rates": ("app.pipelines.macro.rbi_fx", "RbiFxRatesPipeline"),
    "rbi_policy_rates": ("app.pipelines.macro.rbi_policy", "RbiPolicyRatesPipeline"),
    # Fundamentals (quarterly filings)
    "shareholding_pattern": ("app.pipelines.fundamentals.shareholding", "ShareholdingPatternPipeline"),
    # Morningstar
    "morningstar_fund_master": ("app.pipelines.morningstar.fund_master", "FundMasterPipeline"),
    "morningstar_holdings": ("app.pipelines.morningstar.holdings", "HoldingsPipeline"),
    "morningstar_risk": ("app.pipelines.morningstar.risk", "RiskPipeline"),
    # ETF
    "nse_etf_sync": ("app.pipelines.etf.nse_etf_sync", "NseEtfSyncPipeline"),
    "etf_prices": ("app.pipelines.etf.etf_prices", "EtfPricePipeline"),
    # Fundamentals
    "equity_fundamentals": ("app.pipelines.fundamentals.pipeline", "FundamentalsPipeline"),
    # Qualitative
    "qualitative_rss": ("app.pipelines.qualitative.rss", "RssPipeline"),
    "qualitative_goldilocks": ("app.pipelines.qualitative.playwright_goldilocks", "GoldilocksScraperPipeline"),
    "insider_trades": ("app.pipelines.qualitative.insider_trades", "InsiderTradesPipeline"),
    "bulk_block_deals": ("app.pipelines.qualitative.bulk_block_deals", "BulkBlockDealsPipeline"),
    # BSE
    "bse_filings": ("app.pipelines.bse.filings", "BseFilingsPipeline"),
    "bse_ownership": ("app.pipelines.bse.ownership", "BseOwnershipPipeline"),
}

# ---------------------------------------------------------------------------
# Indicators v2 runner — async function, not a BasePipeline subclass.
# Registered in COMPUTATION_SCRIPTS below.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# DAG alias map — translates DAG graph names → actual pipeline_name values
# The DAG executor uses short names; pipeline classes use prefixed names.
# ---------------------------------------------------------------------------
DAG_ALIAS: dict[str, str] = {
    "nse_bhav": "equity_bhav",
    "nse_corporate_actions": "equity_corporate_actions",
    "amfi_nav": "mf_eod",
    "mf_master": "equity_master_refresh",
    "morningstar_nav": "morningstar_fund_master",
    "morningstar_portfolio": "morningstar_holdings",
    # These already match — included for completeness
    "nse_indices": "nse_indices",
    "fii_dii_flows": "fii_dii_flows",
    "yfinance_global": "yfinance_global",
    "fred_macro": "fred_macro",
    "qualitative_rss": "qualitative_rss",
    "india_vix": "india_vix",
    "nse_etf_sync": "nse_etf_sync",
    "etf_prices": "etf_prices",
    "compute_indicators_v2": "compute_indicators_v2",
    "bse_filings": "bse_filings",
    # Atlas pipelines — names match pipeline_name
    "fo_bhavcopy": "fo_bhavcopy",
    "fo_ban_list": "fo_ban_list",
    "participant_oi": "participant_oi",
    "gsec_yields": "gsec_yields",
    "rbi_fx_rates": "rbi_fx_rates",
    "rbi_policy_rates": "rbi_policy_rates",
    "insider_trades": "insider_trades",
    "bulk_block_deals": "bulk_block_deals",
    "shareholding_pattern": "shareholding_pattern",
    "bse_ownership": "bse_ownership",
}

# Reverse map: pipeline_name → DAG alias (for lookups going the other direction)
_REVERSE_ALIAS: dict[str, str] = {v: k for k, v in DAG_ALIAS.items()}

# ---------------------------------------------------------------------------
# Schedule registry — maps schedule group → ordered pipeline names
# Uses the DAG alias names (the names the DAG executor understands)
# ---------------------------------------------------------------------------
SCHEDULE_REGISTRY: dict[str, list[str]] = {
    "pre_market": ["nse_bhav", "nse_corporate_actions", "nse_indices"],
    "t1_delivery": ["fii_dii_flows"],
    # NOTE: CronSchedule.default() in orchestrator/scheduler.py has a stale eod entry
    # that is missing india_vix, nse_etf_sync, and etf_prices. This SCHEDULE_REGISTRY
    # entry is the source of truth. The scheduler.py eod ScheduleEntry should be updated
    # to match, but that file is owned by the orchestration layer — update separately.
    "eod": [
        "nse_bhav", "nse_corporate_actions", "nse_indices",
        "fii_dii_flows", "amfi_nav", "yfinance_global", "fred_macro",
        "india_vix", "nse_etf_sync", "etf_prices",
        # Atlas additions — daily derivatives + ban list + macro + filings
        "fo_bhavcopy", "fo_ban_list", "participant_oi",
        "gsec_yields", "rbi_fx_rates",
        "insider_trades", "bulk_block_deals",
    ],
    # Weekend: no Indian equity, but global markets still trade
    "eod_weekend": [
        "yfinance_global", "fred_macro",
    ],
    # Late-evening AMFI catch-up — runs at 23:00 IST after AMFI publishes.
    # Same amfi_nav pipeline, just a later schedule slot so mf_nav lands
    # on the correct business_date instead of lagging by one day.
    "amfi_late": ["amfi_nav"],
    # Macro low-frequency (daily poll, writes rarely)
    "macro_daily": ["rbi_policy_rates"],
    # Quarterly filings — daily poll, new rows appear only after quarter-end + 21 days
    "filings_daily": ["shareholding_pattern"],
    "rs_computation": ["relative_strength"],
    "technicals": ["equity_technicals_sql", "equity_technicals_pandas"],
    "regime": ["market_breadth", "regime_detection"],
    "fund_metrics": ["mf_derived"],
    "global_data": ["yfinance_global"],
    "etf_global": ["etf_technicals", "etf_rs", "global_technicals", "global_rs"],
    "morningstar_weekly": ["morningstar_nav"],
    "fundamentals_weekly": ["equity_fundamentals"],
    "holdings_monthly": ["morningstar_portfolio", "mf_category_flows"],
    "bse_filings_daily": ["bse_filings"],
    "bse_ownership_weekly": ["bse_ownership"],
    "reconciliation": ["__reconciliation__"],
    "full_rs_rebuild": ["relative_strength"],
    # Nightly: validate → compute everything → goldilocks scrape
    # Full dependency-ordered pipeline (11 steps + goldilocks)
    # market_breadth runs breadth_regime.py which computes BOTH breadth AND regime
    # in a single run — no need for separate regime_detection step
    "nightly_compute": [
        "__validate_ohlcv__",
        "equity_technicals_sql",
        "equity_technicals_pandas",
        "relative_strength",
        "market_breadth",
        "sector_breadth",
        "mf_derived",
        "nse_etf_sync",
        "etf_technicals",
        "etf_rs",
        "global_technicals",
        "global_rs",
        "full_runner",
        # IND-C11: indicators v2 daily incremental run (equity/index/etf/global)
        # MF excluded — deferred to IND-C9/C10 completion
        "compute_indicators_v2",
        "__goldilocks_compute__",
    ],
}

# ---------------------------------------------------------------------------
# Computation scripts — standalone scripts that are NOT BasePipeline subclasses.
# These are called as subprocess via `python -m scripts.compute.<name>`.
# ---------------------------------------------------------------------------
COMPUTATION_SCRIPTS: dict[str, str] = {
    "equity_technicals_sql": "scripts.compute.technicals_sql",
    "equity_technicals_pandas": "scripts.compute.technicals_pandas",
    "relative_strength": "scripts.compute.rs_scores",
    "market_breadth": "scripts.compute.breadth_regime",
    "sector_breadth": "scripts.compute.sector_breadth",
    "regime_detection": "scripts.compute.breadth_regime",
    "mf_derived": "scripts.compute.fund_metrics",
    "etf_technicals": "scripts.compute.etf_technicals",
    "etf_rs": "scripts.compute.etf_rs",
    "global_technicals": "scripts.compute.global_technicals",
    "global_rs": "scripts.compute.global_rs",
    # Full runner — oscillators, pivots, intermarket, fibonacci, divergence
    # These steps ONLY exist in the in-process runner, not as standalone scripts
    "full_runner": "scripts.run_computations",
    # Indicators v2 — daily incremental runner (all asset classes except MF)
    # MF deferred: blocked on IND-C9 (purchase_mode bootstrap). P1 follow-up.
    "compute_indicators_v2": "app.computation.indicators_v2.runner",
}


# ---------------------------------------------------------------------------
# Special pipeline handlers — not BasePipeline subclasses, not computation
# scripts. These run inline SQL or subprocess chains.
# ---------------------------------------------------------------------------
SPECIAL_HANDLERS: set[str] = {
    "__validate_ohlcv__",
    "__goldilocks_compute__",
    "__reconciliation__",
}


def is_special_handler(name: str) -> bool:
    """Check if a name is a special inline handler."""
    return name in SPECIAL_HANDLERS


def resolve_name(name: str) -> str:
    """Resolve a DAG alias or schedule name to actual pipeline_name.

    Returns the name unchanged if it's already a valid pipeline name.
    """
    if name in _PIPELINE_CLASSES:
        return name
    if name in DAG_ALIAS:
        return DAG_ALIAS[name]
    return name


def get_pipeline(name: str) -> BasePipeline | None:
    """Instantiate a pipeline by name (or DAG alias).

    Returns None if the name is a computation script or unknown.
    """
    resolved = resolve_name(name)
    entry = _PIPELINE_CLASSES.get(resolved)
    if entry is None:
        return None

    module_path, class_name = entry
    try:
        import importlib
        mod = importlib.import_module(module_path)
        cls = getattr(mod, class_name)
        return cls()
    except Exception as exc:
        logger.error(
            "pipeline_import_failed",
            pipeline_name=name,
            resolved=resolved,
            module=module_path,
            error=str(exc),
        )
        return None


def is_computation_script(name: str) -> bool:
    """Check if a name refers to a standalone computation script."""
    return name in COMPUTATION_SCRIPTS


def get_computation_module(name: str) -> str | None:
    """Return the module path for a computation script."""
    return COMPUTATION_SCRIPTS.get(name)


def get_schedule(name: str) -> list[str] | None:
    """Return the pipeline list for a schedule group. None if unknown."""
    return SCHEDULE_REGISTRY.get(name)


def list_pipelines() -> list[str]:
    """Return all registered pipeline names."""
    return sorted(_PIPELINE_CLASSES.keys())


def list_schedules() -> dict[str, list[str]]:
    """Return all schedule groups with their pipeline lists."""
    return dict(SCHEDULE_REGISTRY)


def list_computation_scripts() -> list[str]:
    """Return all registered computation script names."""
    return sorted(COMPUTATION_SCRIPTS.keys())
