"""
==============================================================================
Unit & Integration Tests: Trino Lakehouse Client & Benchmarks
==============================================================================
Module: tests.test_trino_client
Description:
    Validates Trino connection configuration, query execution timing,
    fallback telemetry, and real-time KPI aggregations.
==============================================================================
"""

from unittest.mock import MagicMock, patch

from analytics.trino_client import TrinoLakehouseClient, get_trino_client


class TestTrinoClient:
    """Test suite for Trino SQL client and serving layer analytics."""

    def setup_method(self):
        self.client = TrinoLakehouseClient(
            host="localhost",
            port=8085,
            user="test_admin",
            catalog="iceberg",
            schema="earbuds",
        )

    def test_client_configuration(self):
        """Verify client fields are properly initialized."""
        assert self.client.host == "localhost"
        assert self.client.port == 8085
        assert self.client.user == "test_admin"
        assert self.client.catalog == "iceberg"
        assert self.client.schema == "earbuds"

    def test_fallback_query_execution_when_offline(self):
        """Verify client returns graceful fallback dict when Trino is offline."""
        result = self.client.execute_query("SELECT 1")
        assert isinstance(result, dict)
        assert "status" in result
        assert "elapsed_ms" in result
        assert result["elapsed_ms"] >= 0.0

    @patch.object(TrinoLakehouseClient, "_get_connection")
    def test_successful_live_query_execution(self, mock_conn):
        """Verify query execution timing and parsing with mocked DBAPI cursor."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("TWS", 450, 2499.0)]
        mock_cursor.description = [("category", None), ("count", None), ("avg_price", None)]

        mock_conn_instance = MagicMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn.return_value = mock_conn_instance

        res = self.client.execute_query("SELECT category, count(*), avg(price) FROM curated_products GROUP BY category")

        assert res["status"] == "success"
        assert res["row_count"] == 1
        assert res["columns"] == ["category", "count", "avg_price"]
        assert res["rows"] == [("TWS", 450, 2499.0)]
        assert res["source"] == "trino_live"
        assert "elapsed_ms" in res

    def test_get_realtime_kpis_structure(self):
        """Verify KPI method returns all required dashboard metrics."""
        kpis = self.client.get_realtime_kpis()
        assert "total_products" in kpis
        assert "avg_price" in kpis
        assert "avg_rating" in kpis
        assert "active_snapshots" in kpis

    def test_get_category_aggregates_structure(self):
        """Verify category breakdown returns a non-empty list of categories."""
        aggregates = self.client.get_category_aggregates()
        assert isinstance(aggregates, list)
        assert len(aggregates) > 0
        first = aggregates[0]
        assert "category" in first
        assert "count" in first
        assert "avg_price" in first

    def test_get_snapshot_history_structure(self):
        """Verify snapshot history returns immutable commit logs."""
        snapshots = self.client.get_snapshot_history(limit=5)
        assert isinstance(snapshots, list)
        assert len(snapshots) > 0
        first = snapshots[0]
        assert "snapshot_id" in first
        assert "committed_at" in first
        assert "operation" in first

    def test_get_latency_benchmark_structure(self):
        """Verify pre- and post-compaction benchmark metrics are returned."""
        benchmark = self.client.get_latency_benchmark()
        assert "pre_compaction" in benchmark
        assert "post_compaction" in benchmark
        assert benchmark["pre_compaction"]["latency_ms"] > benchmark["post_compaction"]["latency_ms"]
        assert "improvement_pct" in benchmark
        assert benchmark["improvement_pct"] > 50.0

    def test_singleton_accessor(self):
        """Verify get_trino_client returns a valid client singleton instance."""
        singleton = get_trino_client()
        assert isinstance(singleton, TrinoLakehouseClient)
