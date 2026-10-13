"""
==============================================================================
Orchestration Tests: Iceberg Table Maintenance DAG Integrity & Structure
==============================================================================
Module: tests.test_iceberg_maintenance_dag
Description:
    Validates the Airflow Iceberg maintenance DAG file for syntax errors,
    task definitions (compaction, snapshot expiry, vacuuming), pipeline order,
    and failure callbacks.
==============================================================================
"""

import ast
import os
import pytest

DAG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "airflow", "dags", "iceberg_table_maintenance_dag.py")
)


class TestIcebergMaintenanceDagIntegrity:
    """Validates Iceberg Table Maintenance Airflow DAG structure and tasks."""

    def test_maintenance_dag_file_exists(self):
        """Verify the maintenance DAG file exists on the filesystem."""
        assert os.path.isfile(DAG_PATH), f"DAG file missing at {DAG_PATH}"

    def test_maintenance_dag_ast_syntax(self):
        """Verify the DAG file contains valid, parseable Python code without syntax errors."""
        with open(DAG_PATH, "r", encoding="utf-8") as f:
            source = f.read()

        tree = ast.parse(source, filename="iceberg_table_maintenance_dag.py")
        assert tree is not None

    def test_maintenance_dag_defines_required_tasks(self):
        """Verify DAG defines compaction, snapshot expiry, vacuuming, and health checks."""
        with open(DAG_PATH, "r", encoding="utf-8") as f:
            source = f.read()

        expected_tasks = [
            'task_id="lakehouse_health_check"',
            'task_id="bin_pack_compaction"',
            'task_id="expire_snapshots"',
            'task_id="remove_orphan_files"',
            'task_id="pipeline_post_maintenance_summary"',
        ]
        for task in expected_tasks:
            assert task in source, f"Missing maintenance task: {task}"

        # Verify sequential execution order
        assert "t1_health_check >> t2_compaction >> t3_expire_snapshots >> t4_vacuum >> t5_summary" in source

    def test_maintenance_dag_configures_failure_callback(self):
        """Verify the DAG attaches on_failure_callback to trigger failure alerts."""
        with open(DAG_PATH, "r", encoding="utf-8") as f:
            source = f.read()

        assert "on_failure_callback" in source
        assert "airflow_task_failure_alert" in source

    def test_maintenance_dag_bag_loading_if_airflow_installed(self):
        """If apache-airflow is present, validate DAG loading via DagBag."""
        try:
            from airflow.models import DagBag

            dag_bag = DagBag(dag_folder=os.path.dirname(DAG_PATH), include_examples=False)
            assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

            dag = dag_bag.get_dag(dag_id="iceberg_table_maintenance")
            assert dag is not None, "DAG 'iceberg_table_maintenance' not found in DagBag"

            task_ids = set(dag.task_dict.keys())
            expected_ids = {
                "lakehouse_health_check",
                "bin_pack_compaction",
                "expire_snapshots",
                "remove_orphan_files",
                "pipeline_post_maintenance_summary",
            }
            assert task_ids == expected_ids
        except ImportError:
            pytest.skip("apache-airflow not installed in current environment; AST syntax verified.")
