"""
==============================================================================
Pipeline as Code: Configuration Management
==============================================================================
Module: etl.config
Description:
    Centralized, programmatic configuration management for the Earbuds ETL
    pipeline and associated services. Reads from environment variables with
    sensible defaults for local development and containerized production.

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

    def __post_init__(self):
        object.__setattr__(self, 'server', self.server or os.getenv('KAFKA_SERVER', 'localhost:9092'))
        object.__setattr__(self, 'topic', self.topic or os.getenv('KAFKA_TOPIC', 'earbuds_events'))


@dataclass(frozen=True)
class AirflowConfig:
    """Airflow orchestrator API configuration."""
    url: Optional[str] = None
    dag_id: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    def __post_init__(self):
        object.__setattr__(self, 'url', self.url or os.getenv('AIRFLOW_URL', 'http://127.0.0.1:8080'))
        object.__setattr__(
            self,
            'dag_id',
            self.dag_id or os.getenv('AIRFLOW_DAG_ID', 'earbuds_rough_to_staging'),
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
airflow_config = AirflowConfig()
pipeline_settings = PipelineSettings()
