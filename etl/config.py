"""
==============================================================================
Pipeline as Code: Configuration Management (Phase I & II Lakehouse)
==============================================================================
Module: etl.config
Description:
    Centralized, programmatic configuration management for the Earbuds ETL
    pipeline, real-time Kafka streams, Apache Iceberg Lakehouse, and Trino.

Class Comments:
    In Data Engineering and DataOps, configurations must NEVER be hardcoded
    within transformation or loading scripts. By decoupling configuration
    from execution logic, we adhere to the 12-Factor App methodology,
    enabling seamless pipeline transitions across Local, Staging, and Production
    environments without code modifications.
==============================================================================
"""

import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# Load variables from .env file if available
load_dotenv()


@dataclass(frozen=True)
class DatabaseConfig:
    """PostgreSQL database connectivity parameters."""
    host: Optional[str] = None
    port: Optional[int] = None
    name: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None

    def __post_init__(self):
        object.__setattr__(self, 'host', self.host or os.getenv('DB_HOST', 'localhost'))
        object.__setattr__(self, 'port', self.port or int(os.getenv('DB_PORT', '5432')))
        object.__setattr__(self, 'name', self.name or os.getenv('DB_NAME', 'earbuds_db'))
        object.__setattr__(self, 'user', self.user or os.getenv('DB_USER', 'postgres'))
        object.__setattr__(
            self,
            'password',
            self.password if self.password is not None else os.getenv('DB_PASSWORD', ''),
        )

    def to_dict(self) -> dict:
        """Return parameters dictionary for psycopg2.connect."""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.name,
            'user': self.user,
            'password': self.password,
        }


@dataclass(frozen=True)
class KafkaConfig:
    """Apache Kafka broker configuration."""
    server: Optional[str] = None
    topic: Optional[str] = None
    stream_topic: Optional[str] = None
    dlq_topic: Optional[str] = None

    def __post_init__(self):
        object.__setattr__(self, 'server', self.server or os.getenv('KAFKA_SERVER', 'localhost:9092'))
        object.__setattr__(self, 'topic', self.topic or os.getenv('KAFKA_TOPIC', 'earbuds_events'))
        object.__setattr__(self, 'stream_topic', self.stream_topic or os.getenv('KAFKA_STREAM_TOPIC', 'earbuds_stream'))
        object.__setattr__(self, 'dlq_topic', self.dlq_topic or os.getenv('KAFKA_DLQ_TOPIC', 'earbuds_dlq'))


@dataclass(frozen=True)
class LakehouseConfig:
    """MinIO and Apache Iceberg REST Catalog configuration."""
    minio_endpoint: Optional[str] = None
    minio_access_key: Optional[str] = None
    minio_secret_key: Optional[str] = None
    bucket: Optional[str] = None
    iceberg_rest_uri: Optional[str] = None
    warehouse: Optional[str] = None
    catalog_name: Optional[str] = None
    database_name: Optional[str] = None

    def __post_init__(self):
        object.__setattr__(
            self,
            'minio_endpoint',
            self.minio_endpoint or os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
        )
        object.__setattr__(
            self,
            'minio_access_key',
            self.minio_access_key or os.getenv('MINIO_ACCESS_KEY', 'admin'),
        )
        object.__setattr__(
            self,
            'minio_secret_key',
            self.minio_secret_key or os.getenv('MINIO_SECRET_KEY', 'admin123'),
        )
        object.__setattr__(self, 'bucket', self.bucket or os.getenv('MINIO_BUCKET', 'lakehouse'))
        object.__setattr__(
            self,
            'iceberg_rest_uri',
            self.iceberg_rest_uri or os.getenv('ICEBERG_REST_URI', 'http://localhost:8181'),
        )
        object.__setattr__(
            self,
            'warehouse',
            self.warehouse or os.getenv('ICEBERG_WAREHOUSE', 's3a://lakehouse/'),
        )
        object.__setattr__(
            self,
            'catalog_name',
            self.catalog_name or os.getenv('ICEBERG_CATALOG_NAME', 'lakehouse'),
        )
        object.__setattr__(
            self,
            'database_name',
            self.database_name or os.getenv('ICEBERG_DATABASE_NAME', 'earbuds'),
        )


@dataclass(frozen=True)
class TrinoConfig:
    """Trino distributed query engine parameters."""
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    catalog: Optional[str] = None
    schema: Optional[str] = None

    def __post_init__(self):
        object.__setattr__(self, 'host', self.host or os.getenv('TRINO_HOST', 'localhost'))
        object.__setattr__(self, 'port', self.port or int(os.getenv('TRINO_PORT', '8085')))
        object.__setattr__(self, 'user', self.user or os.getenv('TRINO_USER', 'admin'))
        object.__setattr__(self, 'catalog', self.catalog or os.getenv('TRINO_CATALOG', 'iceberg'))
        object.__setattr__(self, 'schema', self.schema or os.getenv('TRINO_SCHEMA', 'earbuds'))


@dataclass(frozen=True)
class AlertConfig:
    """Monitoring and failure alert configuration."""
    slack_webhook_url: Optional[str] = None
    email_smtp_server: Optional[str] = None
    alert_email: Optional[str] = None
    enabled: Optional[bool] = None

    def __post_init__(self):
        object.__setattr__(
            self,
            'slack_webhook_url',
            self.slack_webhook_url or os.getenv('SLACK_WEBHOOK_URL', ''),
        )
        object.__setattr__(
            self,
            'email_smtp_server',
            self.email_smtp_server or os.getenv('SMTP_SERVER', ''),
        )
        object.__setattr__(
            self,
            'alert_email',
            self.alert_email or os.getenv('ALERT_EMAIL', ''),
        )
        object.__setattr__(
            self,
            'enabled',
            self.enabled if self.enabled is not None else (os.getenv('ALERTS_ENABLED', 'false').lower() == 'true'),
        )


@dataclass(frozen=True)
class AirflowConfig:
    """Airflow orchestrator API configuration."""
    url: Optional[str] = None
    dag_id: Optional[str] = None
    maintenance_dag_id: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    def __post_init__(self):
        object.__setattr__(self, 'url', self.url or os.getenv('AIRFLOW_URL', 'http://127.0.0.1:8080'))
        object.__setattr__(
            self,
            'dag_id',
            self.dag_id or os.getenv('AIRFLOW_DAG_ID', 'earbuds_rough_to_staging'),
        )
        object.__setattr__(
            self,
            'maintenance_dag_id',
            self.maintenance_dag_id or os.getenv('AIRFLOW_MAINTENANCE_DAG_ID', 'iceberg_table_maintenance'),
        )
        object.__setattr__(self, 'username', self.username or os.getenv('AIRFLOW_USERNAME', 'admin'))
        object.__setattr__(
            self,
            'password',
            self.password if self.password is not None else os.getenv('AIRFLOW_PASSWORD', 'admin123'),
        )


@dataclass(frozen=True)
class PipelineSettings:
    """ETL Pipeline operational parameters."""
    temp_dir: Optional[str] = None
    batch_size: Optional[int] = None
    log_level: Optional[str] = None

    def __post_init__(self):
        object.__setattr__(
            self,
            'temp_dir',
            self.temp_dir or os.getenv('ETL_TEMP_DIR', '/opt/airflow/logs/etl_tmp'),
        )
        object.__setattr__(self, 'batch_size', self.batch_size or int(os.getenv('BATCH_SIZE', '500')))
        object.__setattr__(self, 'log_level', self.log_level or os.getenv('LOG_LEVEL', 'INFO'))


# Global configuration singleton instances
db_config = DatabaseConfig()
kafka_config = KafkaConfig()
lakehouse_config = LakehouseConfig()
trino_config = TrinoConfig()
alert_config = AlertConfig()
airflow_config = AirflowConfig()
pipeline_settings = PipelineSettings()
