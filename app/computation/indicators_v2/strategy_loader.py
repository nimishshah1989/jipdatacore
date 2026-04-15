"""Load and cache per-asset-class pandas-ta-classic Strategy objects from strategy.yaml."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas_ta_classic as ta
import yaml

from app.logging import get_logger

logger = get_logger(__name__)

STRATEGY_YAML_PATH = Path(__file__).parent / "strategy.yaml"


def _load_raw_catalog() -> list[dict[str, Any]]:
    """Read and parse strategy.yaml. Returns the list of indicator entries."""
    with open(STRATEGY_YAML_PATH) as f:
        data = yaml.safe_load(f)
    return data["indicators"]


@lru_cache(maxsize=8)
def load_strategy_for_asset(asset_class: str, has_volume: bool) -> ta.Strategy:
    """Build a pandas_ta_classic.Strategy filtered to indicators that apply to this asset.

    Args:
        asset_class: one of {"equity", "etf", "index", "mf", "global"}
        has_volume: whether the asset's AssetSpec has a non-None volume_col.
                    Indicators marked requires_volume=true are excluded when False.

    Returns:
        A pandas_ta_classic.Strategy object ready to pass to df.ta.strategy(...)

    Cached per (asset_class, has_volume) pair — strategies are immutable, safe to reuse.
    """
    catalog = _load_raw_catalog()
    filtered = [
        entry for entry in catalog
        if asset_class in entry["applies_to"]
        and (has_volume or not entry.get("requires_volume", False))
    ]
    if not filtered:
        raise ValueError(
            f"No indicators apply to asset_class={asset_class!r}, has_volume={has_volume}"
        )

    # pandas-ta-classic Strategy takes a list of dicts with {kind, params...}
    ta_spec = [
        {"kind": e["kind"], **e["params"]}
        for e in filtered
    ]
    strategy = ta.Strategy(
        name=f"indicators_v2_{asset_class}",
        description=f"Auto-generated from strategy.yaml for {asset_class}",
        ta=ta_spec,
    )
    logger.info(
        "strategy_loaded",
        asset_class=asset_class,
        has_volume=has_volume,
        indicator_count=len(filtered),
    )
    return strategy


def get_rename_map(asset_class: str, has_volume: bool) -> dict[str, str]:
    """Return the {pandas_ta_output_name: schema_column_name} map for this asset."""
    catalog = _load_raw_catalog()
    rename: dict[str, str] = {}
    for entry in catalog:
        if asset_class not in entry["applies_to"]:
            continue
        if entry.get("requires_volume", False) and not has_volume:
            continue
        rename.update(entry["output_columns"])
    return rename


# Non-yaml-sourced columns present in every v2 table. Sources:
#   close_adj                  — price snapshot aliased from df["close"] by the engine
#   volatility_20d / _60d      — annualized log-return volatility (compute_hv_series)
#   hv_252                     — long-window HV (no v1 counterpart)
#   sharpe_1y / sortino_1y /
#     calmar_ratio /
#     max_drawdown_1y /
#     beta_nifty               — v1-compatible names (renamed from risk_* in mig 010)
#   risk_alpha_nifty /
#     risk_omega /
#     risk_information_ratio   — kept under risk_ prefix (no v1 counterpart)
_RISK_COLUMNS: frozenset[str] = frozenset(
    {
        "close_adj",
        # 1y risk — v1-compatible names (renamed via migration 010)
        "sharpe_1y",
        "sortino_1y",
        "calmar_ratio",
        "max_drawdown_1y",
        "beta_nifty",
        "risk_alpha_nifty",
        "risk_omega",
        "risk_information_ratio",
        "treynor_1y",
        "downside_risk_1y",
        # 3y risk (GAP-05)
        "sharpe_3y",
        "sortino_3y",
        "calmar_3y",
        "max_drawdown_3y",
        "beta_3y",
        "information_ratio_3y",
        "treynor_3y",
        "downside_risk_3y",
        # 5y risk (GAP-05)
        "sharpe_5y",
        "sortino_5y",
        "calmar_5y",
        "max_drawdown_5y",
        "beta_5y",
        "information_ratio_5y",
        "treynor_5y",
        "downside_risk_5y",
        # Volatility
        "volatility_20d",
        "volatility_60d",
        "hv_252",
        "volatility_3y",
        "volatility_5y",
    }
)


def get_schema_columns(asset_class: str, has_volume: bool) -> set[str]:
    """Return the set of schema column names this asset will produce.

    Combines pandas-ta columns from strategy.yaml with risk/HV columns
    from risk_metrics.py. ALL five asset classes' v2 tables include these
    risk/HV columns (confirmed in 008_indicators_v2_tables.py), so the
    addition is asset-class-independent.
    """
    yaml_cols = set(get_rename_map(asset_class, has_volume).values())
    return yaml_cols | _RISK_COLUMNS
