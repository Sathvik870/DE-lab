"""
==============================================================================
Unit & Schema Tests: Lakehouse Streaming & Iceberg Table Sinks
==============================================================================
Module: tests.test_lakehouse_streaming
Description:
    Validates PySpark canonical event schemas, transformation validation rules,
    DLQ partitioning logic, and Apache Iceberg DDL specifications.
==============================================================================
"""

from spark_streaming.stream_processor import EARBUDS_EVENT_SCHEMA
from spark_streaming.iceberg_sink import CURATED_PRODUCTS_DDL, QUARANTINE_DLQ_DDL


class TestLakehouseStreaming:
    """Tests schema enforcement contracts and Iceberg table definitions."""

    def test_canonical_event_schema_fields(self):
        """Verify the PySpark StructType schema defines all required event attributes."""
        field_names = EARBUDS_EVENT_SCHEMA.fieldNames()
        expected_fields = [
            "event_id",
            "event_type",
            "product_id",
            "url",
            "title",
            "price",
            "mrp",
            "discount",
            "rating",
            "rating_count",
            "category",
            "brand",
            "description",
            "highlights",
            "timestamp",
        ]
        for field in expected_fields:
            assert field in field_names, f"Missing field in canonical schema: {field}"

    def test_canonical_schema_data_types(self):
        """Verify data types match strict lakehouse standards (DOUBLE, BIGINT, STRING)."""
        field_dict = {f.name: f.dataType.simpleString() for f in EARBUDS_EVENT_SCHEMA.fields}
        assert field_dict["price"] == "double"
        assert field_dict["mrp"] == "double"
        assert field_dict["rating"] == "double"
        assert field_dict["rating_count"] == "bigint"
        assert field_dict["title"] == "string"
        assert field_dict["category"] == "string"

    def test_curated_iceberg_ddl_partitioning_and_format(self):
        """Verify Iceberg DDL defines category partitioning and Parquet storage."""
        assert "CREATE TABLE IF NOT EXISTS lakehouse.earbuds.curated_products" in CURATED_PRODUCTS_DDL
        assert "USING iceberg" in CURATED_PRODUCTS_DDL
        assert "PARTITIONED BY (category)" in CURATED_PRODUCTS_DDL
        assert "'write.format.default' = 'parquet'" in CURATED_PRODUCTS_DDL
        assert "'write.target-file-size-bytes' = '134217728'" in CURATED_PRODUCTS_DDL

    def test_quarantine_dlq_ddl_error_tracking(self):
        """Verify Quarantine DLQ table includes error diagnostic columns."""
        assert "CREATE TABLE IF NOT EXISTS lakehouse.earbuds.quarantine" in QUARANTINE_DLQ_DDL
        assert "error_reason STRING" in QUARANTINE_DLQ_DDL
        assert "rejected_at TIMESTAMP" in QUARANTINE_DLQ_DDL
        assert "USING iceberg" in QUARANTINE_DLQ_DDL
