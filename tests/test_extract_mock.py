"""
==============================================================================
Mock Tests: Data Extraction Stage (etl.extract)
==============================================================================
Module: tests.test_extract_mock
Description:
    Tests database extraction logic by mocking psycopg2 and pandas SQL queries.

Class Comments:
    Why Mock Testing in Data Pipelines?
    1. Isolation: Pipeline unit and integration tests must run without requiring
       a live, persistent PostgreSQL database or network connectivity.
    2. Speed & Determinism: Mocks return instantaneously and eliminate flakey
       tests caused by database timeouts or dirty external state.
    3. Production Safety: Tests must NEVER accidentally query or write to live
       production databases.
==============================================================================
"""

from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from etl.extract import extract_data, get_connection


class TestExtractMock:
    """Mock testing suite for the extraction module."""

    @patch("etl.extract.get_connection")
    @patch("pandas.read_sql_query")
    def test_extract_data_success(self, mock_read_sql, mock_get_conn):
        """Verify extract_data queries SQL with correct batch_id parameter and closes connection."""
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        mock_df = pd.DataFrame({
            "id": [1, 2],
            "batch_id": ["test-batch-101", "test-batch-101"],
            "title": ["Buds A", "Buds B"],
        })
        mock_read_sql.return_value = mock_df

        result_df = extract_data("test-batch-101")

        # Assert query executed with connection and parameter tuple
        mock_read_sql.assert_called_once()
        query_arg, conn_arg = mock_read_sql.call_args[0]
        assert "WHERE batch_id = %s" in query_arg
        assert conn_arg == mock_conn
        assert mock_read_sql.call_args[1]["params"] == ("test-batch-101",)

        # Assert result DataFrame matches mock
        assert len(result_df) == 2
        assert list(result_df["title"]) == ["Buds A", "Buds B"]

        # Assert connection closed in finally block
        mock_conn.close.assert_called_once()

    @patch("etl.extract.get_connection")
    @patch("pandas.read_sql_query")
    def test_extract_data_connection_closed_on_error(self, mock_read_sql, mock_get_conn):
        """Verify database connection is closed even when SQL execution fails."""
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_read_sql.side_effect = RuntimeError("Database read failure")

        with pytest.raises(RuntimeError, match="Database read failure"):
            extract_data("test-batch-error")

        # Must still close connection to prevent connection leak
        mock_conn.close.assert_called_once()

    @patch("psycopg2.connect")
    def test_get_connection_uses_env(self, mock_connect):
        """Verify get_connection passes host, port, database, and user correctly."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        conn = get_connection()
        assert conn == mock_conn
        mock_connect.assert_called_once()
