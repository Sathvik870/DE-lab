"""
==============================================================================
Lakehouse Layer: Apache Iceberg DDL & Table Management
==============================================================================
Module: spark_streaming.iceberg_sink
Description:
    Manages Apache Iceberg table schemas, partitioning strategies, and snapshot
    inspection for the Earbuds Lakehouse platform.

Class Comments:
    Lakehouse Open Table Format Design Principles:
    1. Partition Strategy: Partitioning by `category` prunes irrelevant files
       during analytical queries in Trino, minimizing scan overhead.
    2. ACID Compliance: Every micro-batch write creates an atomic, immutable
       snapshot in the Iceberg metadata tree.
    3. Time-Travel & Snapshot Isolation: Queries can travel back in time to
       any historical snapshot ID or timestamp without locking the table.
==============================================================================
"""

import os
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession


CURATED_PRODUCTS_DDL = """
CREATE TABLE IF NOT EXISTS lakehouse.earbuds.curated_products (
    event_id STRING,
    event_type STRING,
    product_id STRING,
    url STRING,
    title STRING,
    price DOUBLE,
    mrp DOUBLE,
    discount DOUBLE,
    rating DOUBLE,
    rating_count BIGINT,
    category STRING,
    brand STRING,
    savings_amount DOUBLE,
    description STRING,
    highlights STRING,
    event_time TIMESTAMP,
    event_date DATE
)
USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'history.expire.max-snapshot-age-ms' = '604800000',
    'write.target-file-size-bytes' = '134217728'
)
"""

QUARANTINE_DLQ_DDL = """
CREATE TABLE IF NOT EXISTS lakehouse.earbuds.quarantine (
    event_id STRING,
    product_id STRING,
    title STRING,
    price DOUBLE,
    mrp DOUBLE,
    rating DOUBLE,
    discount DOUBLE,
    error_reason STRING,
    raw_timestamp STRING,
    rejected_at TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
)
"""


def initialize_lakehouse_tables(spark: SparkSession) -> None:
    """
    Create Iceberg namespace and tables in the REST Catalog if they do not exist.
    """
    print("[ICEBERG INIT] Ensuring namespace 'lakehouse.earbuds' exists...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.earbuds")

    print("[ICEBERG INIT] Creating table 'lakehouse.earbuds.curated_products'...")
    spark.sql(CURATED_PRODUCTS_DDL)

    print("[ICEBERG INIT] Creating Dead-Letter Queue table 'lakehouse.earbuds.quarantine'...")
    spark.sql(QUARANTINE_DLQ_DDL)
    print("[ICEBERG INIT] Lakehouse tables initialized successfully.")


def get_table_snapshots(spark: SparkSession, table_name: str = "lakehouse.earbuds.curated_products") -> List[Dict[str, Any]]:
    """
    Query historical Iceberg snapshot metadata to support Time-Travel inspections.
    """
    try:
        snapshots_df = spark.sql(f"SELECT committed_at, snapshot_id, parent_id, operation, summary FROM {table_name}.snapshots ORDER BY committed_at DESC")
        return [row.asDict() for row in snapshots_df.collect()]
    except Exception as e:
        print(f"[ICEBERG SNAPSHOTS] Warning: could not retrieve snapshots for {table_name}: {e}")
        return []


def query_time_travel(
    spark: SparkSession,
    table_name: str = "lakehouse.earbuds.curated_products",
    as_of_timestamp: Optional[str] = None,
    snapshot_id: Optional[int] = None,
) -> Any:
    """
    Execute a time-travel query against an Iceberg table at a specific point in time or snapshot.
    """
    if snapshot_id is not None:
        query = f"SELECT * FROM {table_name} VERSION AS OF {snapshot_id}"
    elif as_of_timestamp is not None:
        query = f"SELECT * FROM {table_name} TIMESTAMP AS OF '{as_of_timestamp}'"
    else:
        query = f"SELECT * FROM {table_name}"

    print(f"[ICEBERG TIME-TRAVEL] Executing: {query}")
    return spark.sql(query)

