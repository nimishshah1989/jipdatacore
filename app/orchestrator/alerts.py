"""Alert management: Slack webhook + email notifications for SLA breaches and pipeline failures."""

from __future__ import annotations


import smtplib
from dataclasses import dataclass
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import httpx

from app.logging import get_logger
from app.orchestrator.sla import SLABreachEvent

logger = get_logger(__name__)


@dataclass
class AlertPayload:
    """Structured alert payload."""

    title: str
    message: str
    severity: str  # "info" | "warning" | "critical"
    pipeline: str | None = None
    business_date: str | None = None
    details: dict[str, Any] | None = None
    fired_at: datetime | None = None


class AlertManager:
    """Send alerts via Slack webhook and/or email.

    Usage:
        manager = AlertManager(slack_webhook_url="...", alert_email="admin@example.com")
        await manager.send_sla_breach(breach_event)
        await manager.send_pipeline_failure(pipeline_name, error, business_date)
    """

    SEVERITY_EMOJI = {
        "info": ":information_source:",
        "warning": ":warning:",
        "critical": ":fire:",
    }
    SEVERITY_COLOR = {
        "info": "#36a64f",
        "warning": "#ffa500",
        "critical": "#e01e5a",
    }

    def __init__(
        self,
        slack_webhook_url: str = "",
        alert_email: str = "",
        smtp_host: str = "localhost",
        smtp_port: int = 587,
        smtp_user: str = "",
        smtp_password: str = "",
        from_email: str = "noreply@jip-data-engine.internal",
    ) -> None:
        self._slack_webhook_url = slack_webhook_url
        self._alert_email = alert_email
        self._smtp_host = smtp_host
        self._smtp_port = smtp_port
        self._smtp_user = smtp_user
        self._smtp_password = smtp_password
        self._from_email = from_email

    async def send_slack(self, payload: AlertPayload) -> bool:
        """Post a Slack message via incoming webhook.

        Returns True on success, False on failure (never raises).
        """
        if not self._slack_webhook_url:
            logger.debug("slack_webhook_not_configured", skipping=True)
            return False

        emoji = self.SEVERITY_EMOJI.get(payload.severity, ":bell:")
        color = self.SEVERITY_COLOR.get(payload.severity, "#333333")
        fired_str = (
            payload.fired_at.strftime("%Y-%m-%d %H:%M:%S IST")
            if payload.fired_at
            else "unknown"
        )

        fields = [
            {"title": "Severity", "value": payload.severity.upper(), "short": True},
            {"title": "Fired At", "value": fired_str, "short": True},
        ]
        if payload.pipeline:
            fields.insert(0, {"title": "Pipeline", "value": payload.pipeline, "short": True})
        if payload.business_date:
            fields.insert(1, {"title": "Date", "value": payload.business_date, "short": True})
        if payload.details:
            for k, v in payload.details.items():
                fields.append({"title": k, "value": str(v), "short": True})

        slack_body = {
            "text": f"{emoji} *{payload.title}*",
            "attachments": [
                {
                    "color": color,
                    "text": payload.message,
                    "fields": fields,
                }
            ],
            "channel": "#jip-alerts",
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    self._slack_webhook_url,
                    json=slack_body,
                )
                if response.status_code == 200:
                    logger.info(
                        "slack_alert_sent",
                        title=payload.title,
                        severity=payload.severity,
                    )
                    return True
                else:
                    logger.error(
                        "slack_alert_failed",
                        status_code=response.status_code,
                        body=response.text[:200],
                    )
                    return False
        except Exception as exc:
            logger.error("slack_alert_exception", error=str(exc))
            return False

    def send_email_sync(self, payload: AlertPayload) -> bool:
        """Send an email alert (synchronous, for use in non-async contexts).

        Returns True on success, False on failure (never raises).
        """
        if not self._alert_email:
            logger.debug("email_not_configured", skipping=True)
            return False

        fired_str = (
            payload.fired_at.strftime("%Y-%m-%d %H:%M:%S IST")
            if payload.fired_at
            else "unknown"
        )

        html_body = f"""
        <html><body>
        <h2 style="color: {'red' if payload.severity == 'critical' else 'orange'}">
            JIP Data Engine Alert: {payload.title}
        </h2>
        <p><strong>Severity:</strong> {payload.severity.upper()}</p>
        <p><strong>Fired At:</strong> {fired_str}</p>
        {'<p><strong>Pipeline:</strong> ' + payload.pipeline + '</p>' if payload.pipeline else ''}
        {'<p><strong>Date:</strong> ' + payload.business_date + '</p>' if payload.business_date else ''}
        <p><strong>Message:</strong></p>
        <pre>{payload.message}</pre>
        </body></html>
        """

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"[JIP Alert] {payload.severity.upper()}: {payload.title}"
            msg["From"] = self._from_email
            msg["To"] = self._alert_email
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(self._smtp_host, self._smtp_port) as server:
                if self._smtp_user and self._smtp_password:
                    server.starttls()
                    server.login(self._smtp_user, self._smtp_password)
                server.sendmail(self._from_email, [self._alert_email], msg.as_string())

            logger.info(
                "email_alert_sent",
                title=payload.title,
                to=self._alert_email,
            )
            return True
        except Exception as exc:
            logger.error("email_alert_exception", error=str(exc))
            return False

    async def send_alert(self, payload: AlertPayload) -> None:
        """Send alert via all configured channels."""
        if payload.fired_at is None:
            from datetime import timezone
            payload.fired_at = datetime.now(tz=timezone.utc)

        await self.send_slack(payload)
        # Email is synchronous; run in executor for async safety
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.send_email_sync, payload)

    async def send_sla_breach(self, breach: SLABreachEvent) -> None:
        """Send alert for an SLA breach event."""
        payload = AlertPayload(
            title=f"SLA Breach: {breach.pipeline_name}",
            message=(
                f"Pipeline `{breach.pipeline_name}` missed its SLA deadline.\n"
                f"Deadline: {breach.deadline_ist.strftime('%H:%M IST')}\n"
                f"Current status: {breach.current_status}\n"
                f"Business date: {breach.business_date.isoformat()}"
            ),
            severity=breach.severity,
            pipeline=breach.pipeline_name,
            business_date=breach.business_date.isoformat(),
            details={
                "deadline_ist": breach.deadline_ist.strftime("%Y-%m-%d %H:%M IST"),
                "current_status": breach.current_status,
            },
            fired_at=breach.detected_at,
        )
        await self.send_alert(payload)

    async def send_pipeline_failure(
        self,
        pipeline_name: str,
        error: str,
        business_date: str,
        severity: str = "critical",
    ) -> None:
        """Send alert for a pipeline execution failure."""
        payload = AlertPayload(
            title=f"Pipeline Failed: {pipeline_name}",
            message=f"Pipeline `{pipeline_name}` failed on {business_date}.\n\nError:\n{error[:500]}",
            severity=severity,
            pipeline=pipeline_name,
            business_date=business_date,
            details={"error_snippet": error[:200]},
        )
        await self.send_alert(payload)

    async def send_reconciliation_failure(
        self,
        check_name: str,
        details: str,
        business_date: str,
        severity: str = "warning",
    ) -> None:
        """Send alert for a reconciliation check failure."""
        payload = AlertPayload(
            title=f"Reconciliation Failed: {check_name}",
            message=f"Reconciliation check `{check_name}` failed on {business_date}.\n\n{details}",
            severity=severity,
            business_date=business_date,
            details={"check": check_name},
        )
        await self.send_alert(payload)
