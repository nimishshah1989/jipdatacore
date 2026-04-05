"""Post-ingestion validation for BHAV copy data — anomaly detection."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.models.prices import DeEquityOhlcv, DeCorporateActions
from app.pipelines.validation import AnomalyRecord

logger = get_logger(__name__)

# Price spike threshold: >20% change from previous close without a corporate action
PRICE_SPIKE_THRESHOLD = Decimal("0.20")

# Volume spike threshold: >10x 20-day average volume
VOLUME_SPIKE_MULTIPLIER = 10


class BhavValidator:
    """Anomaly detector for BHAV copy ingested data.

    Detects:
    - Negative values (critical)
    - Price range violations: high < low (critical)
    - Price spikes >20% from previous close without a corporate action (warning)
    - Volume spikes >10x 20-day average (info)
    """

    async def validate(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> list[AnomalyRecord]:
        """Run all anomaly checks for business_date equity data.

        Returns a list of AnomalyRecord objects for each detected anomaly.
        """
        anomalies: list[AnomalyRecord] = []

        # Check 1: Negative values (open/high/low/close < 0)
        negative_anomalies = await self._check_negative_values(business_date, session)
        anomalies.extend(negative_anomalies)

        # Check 2: Price range violations (high < low)
        range_anomalies = await self._check_price_range(business_date, session)
        anomalies.extend(range_anomalies)

        # Check 3: Price spikes >20% (no corporate action today)
        spike_anomalies = await self._check_price_spikes(business_date, session)
        anomalies.extend(spike_anomalies)

        # Check 4: Volume spikes >10x 20-day average
        volume_anomalies = await self._check_volume_spikes(business_date, session)
        anomalies.extend(volume_anomalies)

        logger.info(
            "bhav_validation_complete",
            business_date=business_date.isoformat(),
            total_anomalies=len(anomalies),
            negative_values=len(negative_anomalies),
            range_violations=len(range_anomalies),
            price_spikes=len(spike_anomalies),
            volume_spikes=len(volume_anomalies),
        )

        return anomalies

    async def _check_negative_values(
        self,
        business_date: date,
        session: AsyncSession,
    ) -> list[AnomalyRecord]:
        """Find rows with any of open/high/low/close < 0."""
        result = await session.execute(
            select(
                DeEquityOhlcv.instrument_id,
                DeEquityOhlcv.symbol,
                DeEquityOhlcv.open,
                DeEquityOhlcv.high,
                DeEquityOhlcv.low,
                DeEquityOhlcv.close,
            ).where(
                DeEquityOhlcv.date == business_date,
                sa.or_(
                    DeEquityOhlcv.open < 0,
                    DeEquityOhlcv.high < 0,
                    DeEquityOhlcv.low < 0,
                    DeEquityOhlcv.close < 0,
                ),
            )
        )
        rows = result.all()

        anomalies = []
        for row in rows:
            # Determine which field is negative
            negatives = []
            if row.open is not None and row.open < 0:
                negatives.append(f"open={row.open}")
            if row.high is not None and row.high < 0:
                negatives.append(f"high={row.high}")
            if row.low is not None and row.low < 0:
                negatives.append(f"low={row.low}")
            if row.close is not None and row.close < 0:
                negatives.append(f"close={row.close}")

            anomalies.append(
                AnomalyRecord(
                    entity_type="equity",
                    anomaly_type="negative_value",
                    severity="critical",
                    expected_range="All prices >= 0",
                    actual_value=", ".join(negatives),
                    instrument_id=row.instrument_id,
                )
            )

        if anomalies:
            logger.warning(
                "bhav_negative_values_found",
                count=len(anomalies),
                business_date=business_date.isoformat(),
            )

        return anomalies

    async def _check_price_range(
        self,
        business_date: date,
        session: AsyncSession,
    ) -> list[AnomalyRecord]:
        """Find rows where high < low (invalid price range)."""
        result = await session.execute(
            select(
                DeEquityOhlcv.instrument_id,
                DeEquityOhlcv.symbol,
                DeEquityOhlcv.high,
                DeEquityOhlcv.low,
            ).where(
                DeEquityOhlcv.date == business_date,
                DeEquityOhlcv.high.is_not(None),
                DeEquityOhlcv.low.is_not(None),
                DeEquityOhlcv.high < DeEquityOhlcv.low,
            )
        )
        rows = result.all()

        anomalies = [
            AnomalyRecord(
                entity_type="equity",
                anomaly_type="negative_value",
                severity="critical",
                expected_range="high >= low",
                actual_value=f"high={row.high}, low={row.low}",
                instrument_id=row.instrument_id,
            )
            for row in rows
        ]

        if anomalies:
            logger.warning(
                "bhav_price_range_violations",
                count=len(anomalies),
                business_date=business_date.isoformat(),
            )

        return anomalies

    async def _check_price_spikes(
        self,
        business_date: date,
        session: AsyncSession,
    ) -> list[AnomalyRecord]:
        """Detect price spikes >20% from previous close.

        Only flags as anomaly if there is NO corporate action on business_date
        for that instrument.
        """
        # Get instruments that had corporate actions today (exempt from spike check)
        ca_result = await session.execute(
            select(DeCorporateActions.instrument_id).where(
                DeCorporateActions.ex_date == business_date,
            )
        )
        exempt_instrument_ids = {row[0] for row in ca_result.all()}

        # Find today's data and yesterday's close in one query using window function
        stmt = text("""
            SELECT
                t.instrument_id,
                t.symbol,
                t.close AS today_close,
                LAG(t.close) OVER (
                    PARTITION BY t.instrument_id ORDER BY t.date
                ) AS prev_close
            FROM de_equity_ohlcv t
            WHERE t.date <= :business_date
              AND t.date >= :business_date - INTERVAL '10 days'
              AND t.close IS NOT NULL
              AND t.close > 0
        """)

        result = await session.execute(
            stmt, {"business_date": business_date}
        )
        rows = result.all()

        anomalies = []
        for row in rows:
            # Only check today's data
            if row.instrument_id in exempt_instrument_ids:
                continue
            if row.prev_close is None or row.prev_close <= 0:
                continue
            if row.today_close is None:
                continue

            prev_close = Decimal(str(row.prev_close))
            today_close = Decimal(str(row.today_close))

            pct_change = abs(today_close - prev_close) / prev_close
            if pct_change > PRICE_SPIKE_THRESHOLD:
                anomalies.append(
                    AnomalyRecord(
                        entity_type="equity",
                        anomaly_type="price_spike",
                        severity="warning",
                        expected_range=f"Within 20% of prev_close={prev_close}",
                        actual_value=f"close={today_close}, change={pct_change:.2%}",
                        instrument_id=row.instrument_id,
                    )
                )

        if anomalies:
            logger.info(
                "bhav_price_spikes_found",
                count=len(anomalies),
                business_date=business_date.isoformat(),
            )

        return anomalies

    async def _check_volume_spikes(
        self,
        business_date: date,
        session: AsyncSession,
    ) -> list[AnomalyRecord]:
        """Detect volume spikes >10x the 20-day rolling average."""
        stmt = text("""
            WITH recent AS (
                SELECT
                    instrument_id,
                    date,
                    volume,
                    AVG(volume) OVER (
                        PARTITION BY instrument_id
                        ORDER BY date
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) AS avg_vol_20d
                FROM de_equity_ohlcv
                WHERE date <= :business_date
                  AND date >= :business_date - INTERVAL '25 days'
                  AND volume IS NOT NULL
            )
            SELECT instrument_id, volume, avg_vol_20d
            FROM recent
            WHERE date = :business_date
              AND avg_vol_20d > 0
              AND volume > avg_vol_20d * :multiplier
        """)

        result = await session.execute(
            stmt,
            {
                "business_date": business_date,
                "multiplier": VOLUME_SPIKE_MULTIPLIER,
            },
        )
        rows = result.all()

        anomalies = [
            AnomalyRecord(
                entity_type="equity",
                anomaly_type="price_spike",  # Using price_spike as proxy (no volume_spike in enum)
                severity="info",
                expected_range=f"volume <= {VOLUME_SPIKE_MULTIPLIER}x 20-day avg",
                actual_value=f"volume={row.volume}, avg_20d={row.avg_vol_20d:.0f}",
                instrument_id=row.instrument_id,
            )
            for row in rows
        ]

        if anomalies:
            logger.info(
                "bhav_volume_spikes_found",
                count=len(anomalies),
                business_date=business_date.isoformat(),
            )

        return anomalies
