"""
==============================================================================
Unit & Mock Tests: Observability & Alerting Engine
==============================================================================
Module: tests.test_alerts
Description:
    Validates Slack Webhook alert dispatch, SMTP/Simulated email notifications,
    pipeline failure handlers, and Airflow task failure callback hooks.
==============================================================================
"""

from unittest.mock import MagicMock, patch
from etl.alerts import (
    AlertManager,
    airflow_task_failure_alert,
)
from etl.config import AlertConfig


class TestAlerts:
    """Test suite for pipeline alerting and failure observability."""

    def test_alert_manager_initialization(self):
        """Verify AlertManager initializes with custom config values."""
        cfg = AlertConfig(
            slack_webhook_url="https://hooks.slack.com/services/TEST/TOKEN",
            email_smtp_server="smtp.test.com",
            alert_email="alerts@lakehouse.internal",
            enabled=True,
        )
        mgr = AlertManager(cfg)
        assert mgr.slack_url == "https://hooks.slack.com/services/TEST/TOKEN"
        assert mgr.smtp_server == "smtp.test.com"
        assert mgr.alert_email == "alerts@lakehouse.internal"
        assert mgr.enabled is True

    def test_send_slack_alert_disabled_or_empty_url(self):
        """Verify alert returns True (graceful log-only fallback) when unconfigured."""
        mgr = AlertManager(AlertConfig(enabled=False))
        res = mgr.send_slack_alert("Test Alert", level="WARNING")
        assert res is True

    @patch("urllib.request.urlopen")
    def test_send_slack_alert_http_dispatch(self, mock_urlopen):
        """Verify outgoing HTTP POST payload when Slack webhook is configured and active."""
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        mgr = AlertManager(
            AlertConfig(
                slack_webhook_url="https://hooks.slack.com/services/TEST/123",
                enabled=True,
            )
        )
        res = mgr.send_slack_alert(
            message="Micro-batch failed",
            title="Spark Stream Failure",
            level="ERROR",
            details={"batch_id": "b-42", "partition": "TWS"},
        )
        assert res is True
        mock_urlopen.assert_called_once()

    def test_send_email_alert_fallback(self):
        """Verify email alert logs and returns True when SMTP server is not set."""
        mgr = AlertManager(AlertConfig(enabled=False))
        res = mgr.send_email_alert("Test Subject", "Test Body")
        assert res is True

    def test_handle_pipeline_failure(self):
        """Verify handle_pipeline_failure logs and formats error details without crashing."""
        mgr = AlertManager(AlertConfig(enabled=False))
        try:
            raise ValueError("Corrupt Parquet file detected")
        except ValueError as err:
            mgr.handle_pipeline_failure(
                pipeline_name="stream_processor",
                stage="iceberg_commit",
                error=err,
                batch_id="batch-123",
            )

    @patch("etl.alerts.default_alert_manager.send_slack_alert")
    @patch("etl.alerts.default_alert_manager.send_email_alert")
    def test_airflow_task_failure_alert_hook(self, mock_email, mock_slack):
        """Verify Airflow task failure callback extracts context dict and alerts."""
        mock_dag = MagicMock()
        mock_dag.dag_id = "iceberg_table_maintenance"

        mock_ti = MagicMock()
        mock_ti.task_id = "bin_pack_compaction"
        mock_ti.log_url = "http://airflow:8080/log?task_id=bin_pack_compaction"
        mock_ti.try_number = 2

        context = {
            "dag": mock_dag,
            "task_instance": mock_ti,
            "execution_date": "2026-10-13T12:00:00Z",
            "exception": ValueError("S3 connection timeout"),
        }

        airflow_task_failure_alert(context)

        mock_slack.assert_called_once()
        mock_email.assert_called_once()
        slack_call = mock_slack.call_args[1]
        assert "bin_pack_compaction" in slack_call["title"]
        assert slack_call["level"] == "CRITICAL"
        assert slack_call["details"]["dag_id"] == "iceberg_table_maintenance"
