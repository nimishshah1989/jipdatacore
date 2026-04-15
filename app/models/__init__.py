"""Model registry — import all models so Alembic picks up metadata."""

from app.models.instruments import (  # noqa: F401
    DeContributors,
    DeGlobalInstrumentMaster,
    DeIndexConstituents,
    DeIndexMaster,
    DeInstrument,
    DeMacroMaster,
    DeMarketCapHistory,
    DeMfLifecycle,
    DeMfMaster,
    DeSymbolHistory,
    DeTradingCalendar,
)
from app.models.pipeline import (  # noqa: F401
    DeMigrationErrors,
    DeMigrationLog,
    DePipelineLog,
    DeRequestLog,
    DeSourceFiles,
    DeSystemFlags,
)
from app.models.prices import (  # noqa: F401
    DeAdjustmentFactorsDaily,
    DeCorporateActions,
    DeDataAnomalies,
    DeEquityOhlcv,
    DeGlobalPrices,
    DeIndexPrices,
    DeMacroValues,
    DeMfDividends,
    DeMfNavDaily,
    DeRecomputeQueue,
)
from app.models.flows import (  # noqa: F401
    DeInstitutionalFlows,
    DeMfCategoryFlows,
)
from app.models.computed import (  # noqa: F401
    DeBreadthDaily,
    DeEquityTechnicalDaily,
    DeFoSummary,
    DeMarketRegime,
    DeRsDailySummary,
    DeRsScores,
    DeSectorBreadthDaily,
)
from app.models.qualitative import (  # noqa: F401
    DeQualDocuments,
    DeQualExtracts,
    DeQualOutcomes,
    DeQualSources,
)
from app.models.clients import (  # noqa: F401
    DeClientKeys,
    DeClients,
    DePiiAccessLog,
    DePortfolioHoldings,
    DePortfolioNav,
    DePortfolioRiskMetrics,
    DePortfolios,
    DePortfolioTransactions,
)
from app.models.goldilocks import (  # noqa: F401
    DeDivergenceSignals,
    DeFibLevels,
    DeGoldilocksMarketView,
    DeGoldilocksSectorView,
    DeGoldilocksStockIdeas,
    DeIndexPivots,
    DeIntermarketRatios,
    DeOscillatorMonthly,
    DeOscillatorWeekly,
)
from app.models.champion import DeChampionTrades  # noqa: F401
from app.models.fundamentals import DeEquityFundamentals  # noqa: F401
from app.models.holdings import DeMfHoldings  # noqa: F401
from app.models.mf_derived import DeMfDerivedDaily  # noqa: F401
from app.models.indicators_v2 import (  # noqa: F401
    DeEquityTechnicalDailyV2,
    DeEtfTechnicalDailyV2,
    DeGlobalTechnicalDailyV2,
    DeIndexTechnicalDaily,
    DeMfTechnicalDaily,
)

__all__ = [
    # instruments
    "DeInstrument",
    "DeMarketCapHistory",
    "DeSymbolHistory",
    "DeIndexMaster",
    "DeIndexConstituents",
    "DeMfMaster",
    "DeMfLifecycle",
    "DeMacroMaster",
    "DeGlobalInstrumentMaster",
    "DeContributors",
    "DeTradingCalendar",
    # pipeline
    "DeSourceFiles",
    "DePipelineLog",
    "DeSystemFlags",
    "DeMigrationLog",
    "DeMigrationErrors",
    "DeRequestLog",
    # prices
    "DeEquityOhlcv",
    "DeCorporateActions",
    "DeAdjustmentFactorsDaily",
    "DeRecomputeQueue",
    "DeDataAnomalies",
    "DeMfNavDaily",
    "DeMfDividends",
    "DeIndexPrices",
    "DeGlobalPrices",
    "DeMacroValues",
    # flows
    "DeInstitutionalFlows",
    "DeMfCategoryFlows",
    # computed
    "DeEquityTechnicalDaily",
    "DeRsScores",
    "DeRsDailySummary",
    "DeMarketRegime",
    "DeBreadthDaily",
    "DeSectorBreadthDaily",
    "DeFoSummary",
    # qualitative
    "DeQualSources",
    "DeQualDocuments",
    "DeQualExtracts",
    "DeQualOutcomes",
    # clients
    "DeClients",
    "DeClientKeys",
    "DePiiAccessLog",
    "DePortfolios",
    "DePortfolioNav",
    "DePortfolioTransactions",
    "DePortfolioHoldings",
    "DePortfolioRiskMetrics",
    # goldilocks
    "DeGoldilocksMarketView",
    "DeGoldilocksSectorView",
    "DeGoldilocksStockIdeas",
    "DeOscillatorWeekly",
    "DeOscillatorMonthly",
    "DeDivergenceSignals",
    "DeFibLevels",
    "DeIndexPivots",
    "DeIntermarketRatios",
    # fundamentals
    "DeEquityFundamentals",
    # champion
    "DeChampionTrades",
    # holdings
    "DeMfHoldings",
    # mf derived
    "DeMfDerivedDaily",
    # indicators v2
    "DeEquityTechnicalDailyV2",
    "DeEtfTechnicalDailyV2",
    "DeGlobalTechnicalDailyV2",
    "DeIndexTechnicalDaily",
    "DeMfTechnicalDaily",
]
