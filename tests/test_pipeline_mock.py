"""
==============================================================================
Mock Tests: End-to-End Pipeline Integration (etl.pipeline)
==============================================================================
Module: tests.test_pipeline_mock
Description:
    Integration testing for run_pipeline coordinating Extract, Transform,
    Validate, and Load with mocked database I/O.

Class Comments:
    Mocking external boundaries while running real transformations and validators
    provides high-fidelity integration testing. We test the pipeline's control
    flow, data handover between stages, and early-termination conditions
    without any external database dependencies.
==============================================================================
"""

from unittest.mock import patch
import pandas as pd

from etl.pipeline import run_pipeline


class TestPipelineMock:
    """Mock integration test suite for the pipeline coordinator."""

    @patch("etl.pipeline.extract_data")
    def test_run_pipeline_empty_batch(self, mock_extract):
        """Verify pipeline handles empty batches gracefully with early termination."""
        mock_extract.return_value = pd.DataFrame()

        metrics = run_pipeline("batch-empty-001")

        assert metrics["status"] == "empty"
        assert metrics["extracted"] == 0
        assert metrics["loaded"] == 0
        mock_extract.assert_called_once_with("batch-empty-001")

    @patch("etl.pipeline.load_data")
    @patch("etl.pipeline.extract_data")
    def test_run_pipeline_full_flow(self, mock_extract, mock_load, sample_raw_dirty_df):
        """Verify full ETL execution: raw data -> transform -> validate -> load."""
        mock_extract.return_value = sample_raw_dirty_df
        # Mock loader returning count of inserted records
        mock_load.return_value = 3

        metrics = run_pipeline("batch-flow-001")

        assert metrics["status"] == "success"
        assert metrics["extracted"] == len(sample_raw_dirty_df)
        assert metrics["valid"] > 0
        assert metrics["rejected"] > 0
        assert metrics["loaded"] == 3

        # Verify load was called with the valid DataFrame subset and batch_id
        mock_load.assert_called_once()
        passed_df, passed_batch_id = mock_load.call_args[0]
        assert passed_batch_id == "batch-flow-001"
        assert len(passed_df) == metrics["valid"]
