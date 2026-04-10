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
    "equity_bhav_copy": ("app.pipelines.equity.bhav_copy", "BhavCopyPipeline"),
    "equity_delivery": ("app.pipelines.equity.delivery", "DeliveryPipeline"),
    "equity_eod": ("app.pipelines.equity.eod", "EodOrchestrator"),
    "equity_master_refresh": ("app.pipelines.equity.master_refresh", "MasterRefreshPipeline"),
    "equity_corporate_actions": ("app.pipelines.equity.corporate_actions", "CorporateActionsPipeline"),
    "market_cap_history": ("app.pipelines.equity.market_cap_history", "MarketCapHistoryPipeline"),
    "symbol_history": ("app.pipelines.equity.symbol_history", "SymbolHistoryPipeline"),
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
    # Morningstar
    "morningstar_fund_master": ("app.pipelines.morningstar.fund_master", "FundMasterPipeline"),
    "morningstar_holdings": ("app.pipelines.morningstar.holdings", "HoldingsPipeline"),
    "morningstar_risk": ("app.pipelines.morningstar.risk", "RiskPipeline"),
    # Qualitative
    "qualitative_rss": ("app.pipelines.qualitative.rss", "RssPipeline"),
    "qualitative_goldilocks": ("app.pipelines.qualitative.playwright_goldilocks", "GoldilocksScraperPipeline"),
}

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
    "eod": [
        "nse_bhav", "nse_corporate_actions", "nse_indices",
        "fii_dii_flows", "amfi_nav", "yfinance_global", "fred_macro",
    ],
    "rs_computation": ["relative_strength"],
    "technicals": ["equity_technicals_sql", "equity_technicals_pandas"],
    "regime": ["market_breadth", "regime_detection"],
    "fund_metrics": ["mf_derived"],
    "global_data": ["yfinance_global"],
    "etf_global": ["etf_technicals", "etf_rs", "global_technicals", "global_rs"],
    "morningstar_weekly": ["morningstar_nav"],
    "holdings_monthly": ["morningstar_portfolio"],
    "reconciliation": ["__reconciliation__"],
    "full_rs_rebuild": ["relative_strength"],
    # Nightly: validate → compute everything → goldilocks scrape
    "nightly_compute": [
        "__validate_ohlcv__",
        "equity_technicals_sql",
        "relative_strength",
        "market_breadth",
        "regime_detection",
        "mf_derived",
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
    "regime_detection": "scripts.compute.breadth_regime",
    "mf_derived": "scripts.compute.fund_metrics",
    "etf_technicals": "scripts.compute.etf_technicals",
    "etf_rs": "scripts.compute.etf_rs",
    "global_technicals": "scripts.compute.global_technicals",
    "global_rs": "scripts.compute.global_rs",
}


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
