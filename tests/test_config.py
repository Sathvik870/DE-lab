"""
==============================================================================
Unit Tests: Programmatic Configuration (etl.config)
==============================================================================
Module: tests.test_config
Description:
    Tests the configuration management dataclasses, environment overrides,
    and connection parameter conversions.

Class Comments:
    Pipeline as Code necessitates testing the configuration layer to ensure
    environment variables are parsed cleanly and invalid settings don't
    reach database or message broker connectors.
==============================================================================
"""

import os
from unittest.mock import patch
import pytest

from etl.config import (
    DatabaseConfig,
    KafkaConfig,
    AirflowConfig,
    PipelineSettings,
    db_config,
    kafka_config,
    airflow_config,
    pipeline_settings,
)


class TestConfigUnit:
    """Unit tests for configuration dataclasses."""

    def test_default_database_config(self):
        """Verify default database configuration values."""
        config = DatabaseConfig()
        assert config.port == 5432
        assert config.name == os.getenv("DB_NAME", "earbuds_db")

        params = config.to_dict()
        assert isinstance(params, dict)
        assert "host" in params
        assert "port" in params
        assert "database" in params

    def test_database_config_env_overrides(self):
        """Verify environment variables override defaults."""
        with patch.dict(os.environ, {
            "DB_HOST": "prod-db.internal",
            "DB_PORT": "5433",
            "DB_NAME": "prod_earbuds",
            "DB_USER": "prod_user",
            "DB_PASSWORD": "secret_password",
        }):
            config = DatabaseConfig()
            assert config.host == "prod-db.internal"
            assert config.port == 5433
            assert config.name == "prod_earbuds"
            assert config.user == "prod_user"
            assert config.password == "secret_password"

    def test_kafka_and_airflow_config(self):
        """Verify Kafka and Airflow configurations instantiate properly."""
        k_cfg = KafkaConfig()
        assert "localhost" in k_cfg.server or "earbuds-kafka" in k_cfg.server
        assert k_cfg.topic == "earbuds_events"

        a_cfg = AirflowConfig()
        assert a_cfg.dag_id == "earbuds_rough_to_staging"
        assert a_cfg.username == "admin"

    def test_pipeline_settings(self):
        """Verify pipeline settings instantiate and parse integer parameters."""
        p_cfg = PipelineSettings()
        assert p_cfg.batch_size > 0
        assert p_cfg.log_level in {"INFO", "DEBUG", "WARN", "ERROR"}

