"""
==============================================================================
Mock Tests: Data Loading Stage (etl.loader)
==============================================================================
Module: tests.test_loader_mock
Description:
    Validates database loading logic, deduplication algorithms, and transaction
    rollback mechanisms using mocked psycopg2 connections and cursors.

Class Comments:
    Data Loading Mock Testing:
    Loading into analytical storage requires bulletproof idempotency and ACID
    transaction safety. By mocking the database cursor, we verify that:
    1. In-batch duplicate URLs are pruned before issuing SQL commands.
    2. Staging table collisions are resolved (skipping already ingested URLs).
    3. Transactions are committed on success and rolled back on SQL failures.
    4. Database connections are closed cleanly in all execution paths.
==============================================================================
"""

from unittest.mock import MagicMock, patch
import pandas as pd
import psycopg2
import pytest

from etl.loader import load_data


class TestLoaderMock:
    """Mock testing suite for the loader module."""

    @patch("etl.loader.get_connection")
    def test_load_data_empty_df_returns_zero(self, mock_get_conn):
        """Verify passing empty DataFrame returns 0 immediately without touching database."""
        empty_df = pd.DataFrame()
        result = load_data(empty_df, "batch-empty")

        assert result == 0
        mock_get_conn.assert_not_called()

    @patch("etl.loader.get_connection")
    def test_load_data_success_new_records(self, mock_get_conn, sample_clean_df):
        """Verify new unique records are inserted and transaction is committed."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        # Mock existing URLs in staging as empty set
        mock_cur.fetchall.return_value = []

        inserted = load_data(sample_clean_df, "batch-new-001")

        assert inserted == 2
        # Verify 1 SELECT query + 2 INSERT queries executed
        assert mock_cur.execute.call_count == 3
        # Verify transaction committed
        mock_conn.commit.assert_called_once()
        mock_conn.rollback.assert_not_called()
        # Verify resources closed
        mock_cur.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("etl.loader.get_connection")
    def test_load_data_intra_batch_deduplication(self, mock_get_conn):
        """Verify duplicate URLs arriving within the same batch are deduplicated."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        mock_cur.fetchall.return_value = []

        # Create DataFrame with 2 identical URLs
        duplicate_df = pd.DataFrame({
            "url": [
                "https://example.com/item-1",
                "https://example.com/item-1",  # duplicate in batch
            ],
            "title": ["Item 1 First", "Item 1 Second"],
            "price": [100.0, 100.0],
            "mrp": [200.0, 200.0],
            "discount": [50.0, 50.0],
            "rating": [4.0, 4.0],
            "rating_count": [10.0, 10.0],
            "description": ["Desc", "Desc"],
            "highlights": ["High", "High"],
            "image_urls": ["[]", "[]"],
            "video_urls": ["[]", "[]"],
        })

        inserted = load_data(duplicate_df, "batch-intra-dup")

        # Only 1 unique item should be inserted
        assert inserted == 1
        # 1 SELECT + 1 INSERT
        assert mock_cur.execute.call_count == 2
        mock_conn.commit.assert_called_once()

    @patch("etl.loader.get_connection")
    def test_load_data_inter_batch_deduplication(self, mock_get_conn):
        """Verify records already present in staging table are filtered out."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        # Mock staging table as already containing item-1
        mock_cur.fetchall.return_value = [("https://example.com/item-1",)]

        df = pd.DataFrame({
            "url": [
                "https://example.com/item-1",  # already exists in staging
                "https://example.com/item-2",  # new item
            ],
            "title": ["Item 1", "Item 2"],
            "price": [100.0, 200.0],
            "mrp": [200.0, 300.0],
            "discount": [50.0, 33.3],
            "rating": [4.0, 4.5],
            "rating_count": [10.0, 20.0],
            "description": ["Desc 1", "Desc 2"],
            "highlights": ["High 1", "High 2"],
            "image_urls": ["[]", "[]"],
            "video_urls": ["[]", "[]"],
        })

        inserted = load_data(df, "batch-inter-dup")

        # Only item-2 should be inserted
        assert inserted == 1
        # 1 SELECT + 1 INSERT
        assert mock_cur.execute.call_count == 2
        mock_conn.commit.assert_called_once()

    @patch("etl.loader.get_connection")
    def test_load_data_all_existing_commits_without_inserts(self, mock_get_conn):
        """Verify that when all incoming records exist in staging, 0 inserts occur."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        mock_cur.fetchall.return_value = [("https://example.com/item-1",)]

        df = pd.DataFrame({
            "url": ["https://example.com/item-1"],
            "title": ["Item 1"],
            "price": [100.0],
            "mrp": [200.0],
            "discount": [50.0],
            "rating": [4.0],
            "rating_count": [10.0],
            "description": ["Desc 1"],
            "highlights": ["High 1"],
            "image_urls": ["[]"],
            "video_urls": ["[]"],
        })

        inserted = load_data(df, "batch-all-dup")

        assert inserted == 0
        # Only the SELECT query ran
        assert mock_cur.execute.call_count == 1
        mock_conn.commit.assert_called_once()

    @patch("etl.loader.get_connection")
    def test_load_data_rollback_on_database_error(self, mock_get_conn, sample_clean_df):
        """Verify transaction is rolled back when SQL insertion fails."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        # Fetchall succeeds for existing URLs
        mock_cur.fetchall.return_value = []
        # Simulate failure during INSERT
        mock_cur.execute.side_effect = [None, psycopg2.DatabaseError("Disk full or constraint violation")]

        with pytest.raises(psycopg2.DatabaseError, match="Disk full or constraint violation"):
            load_data(sample_clean_df, "batch-failure")

        # Verify rollback was called to preserve ACID consistency
        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()
        mock_cur.close.assert_called_once()
        mock_conn.close.assert_called_once()
