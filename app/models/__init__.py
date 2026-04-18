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
    DeParticipantOi,
)
from app.models.computed import (  # noqa: F401
    DeBreadthDaily,
    DeEquityTechnicalDaily,
    DeFoBanList,
    DeFoBhavcopy,
    DeFoSummary,
    DeGsecYield,
    DeMarketRegime,
    DeRbiFxRate,
    DeRbiPolicyRate,
    DeRsDailySummary,
    DeRsScores,
    DeSectorBreadthDaily,
)
from app.models.qualitative import (  # noqa: F401
    DeBulkBlockDeal,
    DeInsiderTrade,
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
from app.models.holdings import DeMfHoldings, DeShareholdingPattern  # noqa: F401
from app.models.mf_derived import DeMfDerivedDaily  # noqa: F401
from app.models.bse import (  # noqa: F401
    DeBseAnnouncements,
    DeBseCorpActions,
    DeBseInsiderTrades,
    DeBsePledgeHistory,
    DeBseResultCalendar,
    DeBseSastDisclosures,
    DeBseShareholding,
)
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
    "DeParticipantOi",
    # computed
    "DeEquityTechnicalDaily",
    "DeRsScores",
    "DeRsDailySummary",
    "DeMarketRegime",
    "DeBreadthDaily",
    "DeSectorBreadthDaily",
    "DeFoSummary",
    "DeFoBhavcopy",
    "DeFoBanList",
    "DeGsecYield",
    "DeRbiFxRate",
    "DeRbiPolicyRate",
    # qualitative
    "DeQualSources",
    "DeQualDocuments",
    "DeQualExtracts",
    "DeQualOutcomes",
    "DeInsiderTrade",
    "DeBulkBlockDeal",
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
    "DeShareholdingPattern",
    # mf derived
    "DeMfDerivedDaily",
    # indicators v2
    "DeEquityTechnicalDailyV2",
    "DeEtfTechnicalDailyV2",
    "DeGlobalTechnicalDailyV2",
    "DeIndexTechnicalDaily",
    "DeMfTechnicalDaily",
    # bse
    "DeBseAnnouncements",
    "DeBseCorpActions",
    "DeBseInsiderTrades",
    "DeBsePledgeHistory",
    "DeBseResultCalendar",
    "DeBseSastDisclosures",
    "DeBseShareholding",
]
