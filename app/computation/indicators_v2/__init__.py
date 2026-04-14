"""Indicators v2 — pandas-ta-classic + empyrical-reloaded.

Replaces the legacy hand-rolled ``app/computation/technicals.py`` formulae
with a battle-tested library stack. One generic engine runs across all
asset classes (equities, indices, ETFs, globals, MFs), driven by an
``AssetSpec`` dataclass and a versioned indicator catalog at
``strategy.yaml``.

Public entry points (populated as chunks land):

- ``assets.equity.compute_equity_indicators``
- ``assets.etf.compute_etf_indicators``
- ``assets.global_.compute_global_indicators``
- ``assets.index_.compute_index_indicators``
- ``assets.mf.compute_mf_indicators``

Risk metrics (Sharpe / Sortino / Calmar / max drawdown / beta / alpha)
are computed via empyrical-reloaded and land in the same tables under
``risk_*`` columns.

See ``docs/specs/indicators-prd.md`` and ``docs/specs/indicators-review-fixes.md``.
"""
