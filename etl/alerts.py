"""
==============================================================================
Observability & Monitoring: Failure Alerts & Logging
==============================================================================
Module: etl.alerts
Description:
    Provides production-grade alerting and structured logging for data pipelines
    and Airflow orchestration. Supports Slack Webhook alerts, SMTP/Simulated
    email notifications, and Airflow task failure callbacks (on_failure_callback).

Class Comments:
    In Production Lakehouse Architectures:
    - Silent failures cause data staleness and SLA breaches in downstream BI.
    - Automated failure notifications route stack traces, task context, and
      execution metadata to on-call engineering teams immediately.
    - Decoupled alerting allows seamless fallback to mock/local logging when
      external webhooks or SMTP servers are unreachable.
==============================================================================
"""

import json
import logging
import os
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional
import urllib.request
import urllib.error

from etl.config import AlertConfig

# Configure structured logging
logger = logging.getLogger("lakehouse.alerts")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


class AlertManager:
    """Manages multi-channel failure alerts and observability telemetry."""

    def __init__(self, config: Optional[AlertConfig] = None):
        self.config = config or AlertConfig()
        self.slack_url = self.config.slack_webhook_url
        self.smtp_server = self.config.email_smtp_server
        self.alert_email = self.config.alert_email
        self.enabled = self.config.enabled

    def send_slack_alert(
        self,
        message: str,
        title: str = "🚨 Pipeline Alert",
        level: str = "ERROR",
        details: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Send an alert notification to a Slack channel via incoming webhook.

        Args:
            message: Core alert description or error summary.
            title: Title header for the Slack message card.
            level: Severity level ('INFO', 'WARNING', 'ERROR', 'CRITICAL').
            details: Optional dictionary of metadata key-values.

        Returns:
            bool: True if sent or simulated successfully, False on error.
        """
        color_map = {
            "INFO": "#2eb886",
            "WARNING": "#daa038",
            "ERROR": "#a30200",
            "CRITICAL": "#5a0000",
        }
        color = color_map.get(level.upper(), "#a30200")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        fields = [
            {"title": "Severity", "value": level.upper(), "short": True},
            {"title": "Timestamp", "value": timestamp, "short": True},
        ]

        if details:
            for k, v in details.items():
                fields.append({"title": str(k).replace("_", " ").title(), "value": str(v), "short": True})

        payload = {
            "text": f"{title}: {message}",
            "attachments": [
                {
                    "color": color,
                    "title": title,
                    "text": message,
                    "fields": fields,
                    "footer": "Lakehouse Observability Engine",
                    "ts": int(datetime.now(timezone.utc).timestamp()),
                }
            ],
        }

        logger.info(f"[ALERT:SLACK] {level} - {title}: {message}")

        if not self.slack_url or not self.enabled:
            logger.info("[ALERT:SLACK] Webhook URL not configured or alerts disabled; recorded alert to log.")
            return True

        try:
            req = urllib.request.Request(
                self.slack_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.status in (200, 204)
        except urllib.error.URLError as e:
            logger.error(f"[ALERT:SLACK] Failed to dispatch Slack webhook: {e}")
            return False

    def send_email_alert(
        self,
        subject: str,
        body: str,
        recipients: Optional[List[str]] = None,
        is_html: bool = False,
    ) -> bool:
        """
        Dispatch an email notification via SMTP or structured log simulation.

        Args:
            subject: Email subject line.
            body: Plain text or HTML email body.
            recipients: List of target email addresses (defaults to alert_email).
            is_html: Flag indicating HTML formatting.

        Returns:
            bool: True if dispatched or simulated successfully.
        """
        target_recipients = recipients or ([self.alert_email] if self.alert_email else [])
        logger.info(f"[ALERT:EMAIL] Subject: {subject} | To: {target_recipients}")

        if not target_recipients or not self.smtp_server or not self.enabled:
            logger.info("[ALERT:EMAIL] SMTP server or recipients not configured; recorded alert to log.")
            return True

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"lakehouse-alerts@{os.getenv('HOSTNAME', 'localhost')}"
            msg["To"] = ", ".join(target_recipients)

            subtype = "html" if is_html else "plain"
            msg.attach(MIMEText(body, subtype))

            with smtplib.SMTP(self.smtp_server, 25, timeout=5) as server:
                server.sendmail(msg["From"], target_recipients, msg.as_string())
            return True
        except Exception as e:
            logger.error(f"[ALERT:EMAIL] Failed to dispatch email alert: {e}")
            return False

    def handle_pipeline_failure(
        self,
        pipeline_name: str,
        stage: str,
        error: Exception,
        batch_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Generic handler to record and broadcast pipeline execution failures.
        """
        error_msg = f"Pipeline '{pipeline_name}' failed at stage '{stage}': {str(error)}"
        details = {
            "pipeline": pipeline_name,
            "stage": stage,
            "error_type": type(error).__name__,
        }
        if batch_id:
            details["batch_id"] = batch_id
        if context:
            details.update(context)

        logger.error(f"[PIPELINE FAILURE] {error_msg}", exc_info=True)
        self.send_slack_alert(
            message=str(error),
            title=f"💥 {pipeline_name} Failure: {stage}",
            level="CRITICAL",
            details=details,
        )
        self.send_email_alert(
            subject=f"[URGENT] {pipeline_name} Failure at {stage}",
            body=f"{error_msg}\n\nDetails:\n{json.dumps(details, indent=2)}",
        )


# Singleton instance
default_alert_manager = AlertManager()


def send_slack_alert(
    message: str,
    title: str = "🚨 Pipeline Alert",
    level: str = "ERROR",
    details: Optional[Dict[str, Any]] = None,
) -> bool:
    """Helper shortcut for default alert manager Slack alert."""
    return default_alert_manager.send_slack_alert(message, title, level, details)


def send_email_alert(
    subject: str,
    body: str,
    recipients: Optional[List[str]] = None,
) -> bool:
    """Helper shortcut for default alert manager email alert."""
    return default_alert_manager.send_email_alert(subject, body, recipients)


def airflow_task_failure_alert(context: Dict[str, Any]) -> None:
    """
    Airflow task failure callback hook (on_failure_callback).

    Extracts execution context (dag_id, task_id, execution_date, exception,
    log_url, try_number) and dispatches alerts to Slack and Email.

    Usage in DAG:
        default_args = {
            'on_failure_callback': airflow_task_failure_alert,
            ...
        }
    """
    dag_id = context.get("dag", None)
    dag_id_str = dag_id.dag_id if dag_id else str(context.get("dag_id", "unknown_dag"))
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else str(context.get("task_id", "unknown_task"))
    execution_date = str(context.get("execution_date", datetime.now(timezone.utc)))
    exception = context.get("exception", "No exception stack trace available")
    log_url = getattr(task_instance, "log_url", "N/A") if task_instance else "N/A"
    try_number = getattr(task_instance, "try_number", 1) if task_instance else 1

    title = f"🚨 Airflow Task Failed: {dag_id_str}.{task_id}"
    message = f"Task `{task_id}` in DAG `{dag_id_str}` encountered an unhandled exception."

    details = {
        "dag_id": dag_id_str,
        "task_id": task_id,
        "try_number": try_number,
        "execution_date": execution_date,
        "exception": str(exception)[:300],
        "log_url": log_url,
    }

    logger.error(f"[AIRFLOW FAILURE] {dag_id_str}.{task_id} failed: {exception}")

    default_alert_manager.send_slack_alert(
        message=message,
        title=title,
        level="CRITICAL",
        details=details,
    )

    email_body = (
        f"<h2>Airflow Task Failure Alert</h2>"
        f"<p><b>DAG:</b> {dag_id_str}<br>"
        f"<b>Task:</b> {task_id}<br>"
        f"<b>Try:</b> {try_number}<br>"
        f"<b>Execution Date:</b> {execution_date}</p>"
        f"<h3>Exception:</h3><pre>{exception}</pre>"
        f"<p><a href='{log_url}'>View Task Logs in Airflow UI</a></p>"
    )

    default_alert_manager.send_email_alert(
        subject=f"[AIRFLOW FAILURE] {dag_id_str} - {task_id}",
        body=email_body,
        is_html=True,
    )
