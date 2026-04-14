"""Unit tests for the ETF asset wrapper (IND-C7).

Purely structural — no DB connection needed.
Verifies ETF_SPEC is wired to the correct models, columns, and that
the public interface is importable and correctly typed.
"""

from __future__ import annotations


def test_etf_spec_columns_match_model() -> None:
    """ETF_SPEC OHLCV column names must exist as columns on DeEtfOhlcv."""
    from app.computation.indicators_v2.assets.etf import ETF_SPEC
    from app.models.etf import DeEtfOhlcv

    actual_cols = {c.key for c in DeEtfOhlcv.__table__.columns}

    def _check(col_spec: object) -> None:
        """Accept str or tuple[str, ...] — each entry must exist on the model."""
        if isinstance(col_spec, tuple):
            for name in col_spec:
                assert name in actual_cols, f"Column '{name}' not on DeEtfOhlcv"
        else:
            assert col_spec in actual_cols, f"Column '{col_spec}' not on DeEtfOhlcv"

    _check(ETF_SPEC.close_col)
    _check(ETF_SPEC.open_col)
    _check(ETF_SPEC.high_col)
    _check(ETF_SPEC.low_col)
    _check(ETF_SPEC.volume_col)


def test_etf_spec_output_model_is_v2() -> None:
    """ETF_SPEC must point at the v2 output table, not the legacy one."""
    from app.computation.indicators_v2.assets.etf import ETF_SPEC
    from app.models.indicators_v2 import DeEtfTechnicalDailyV2

    assert ETF_SPEC.output_model is DeEtfTechnicalDailyV2
    assert ETF_SPEC.output_model.__tablename__ == "de_etf_technical_daily_v2"


def test_etf_spec_id_column_is_ticker() -> None:
    """ETF_SPEC must use 'ticker' as id_column and 'date' as date_column."""
    from app.computation.indicators_v2.assets.etf import ETF_SPEC

    assert ETF_SPEC.id_column == "ticker"
    assert ETF_SPEC.date_column == "date"
