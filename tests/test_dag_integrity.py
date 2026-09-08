"""
==============================================================================
Orchestration Tests: Airflow DAG Integrity & Structure
==============================================================================
Module: tests.test_dag_integrity
Description:
    Validates the Airflow DAG file for syntax errors, import correctness,
    task definitions, and pipeline order.

Class Comments:
    In Data Engineering, DAG Integrity Testing is a critical CI/CD gate.
    A syntax error, broken import, or cyclical dependency in an Airflow DAG file
    will crash the Airflow DAG processor and prevent ALL pipelines from running.
    Automating DAG syntax and structural checks in CI catches these issues
    before deployment to the orchestration cluster.
==============================================================================
"""

import ast
import os
import pytest

DAG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "airflow", "dags", "earbuds_etl_dag.py")
)


class TestDagIntegrity:
    """Validates Airflow DAG file integrity and structure."""

    def test_dag_file_exists(self):
        """Verify the Airflow DAG file exists on the filesystem."""
        assert os.path.isfile(DAG_PATH), f"DAG file missing at {DAG_PATH}"

    def test_dag_python_syntax_and_ast(self):
        """Verify the DAG file contains valid, parseable Python code without syntax errors."""
        with open(DAG_PATH, "r", encoding="utf-8") as f:
            source_code = f.read()

        # Parse Abstract Syntax Tree (AST) - raises SyntaxError if invalid
        tree = ast.parse(source_code, filename="earbuds_etl_dag.py")
        assert tree is not None

    def test_dag_defines_required_tasks(self):
        """Verify the DAG source code defines all four required ETL stages."""
        with open(DAG_PATH, "r", encoding="utf-8") as f:
            source_code = f.read()

        # Ensure task IDs and stages are explicitly present
        expected_tasks = [
            'task_id="extract"',
            'task_id="transform"',
            'task_id="validate"',
            'task_id="load"',
        ]
        for task in expected_tasks:
            assert task in source_code, f"Missing expected task definition: {task}"

        # Ensure pipeline execution order extract >> transform >> validate >> load
        assert "extract >> transform >> validate >> load" in source_code

    def test_dag_object_if_airflow_installed(self):
        """If apache-airflow is present in the environment, validate DAGBag loading."""
        try:
            from airflow.models import DagBag

            dag_bag = DagBag(dag_folder=os.path.dirname(DAG_PATH), include_examples=False)

            assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

            dag = dag_bag.get_dag(dag_id="earbuds_rough_to_staging")
            assert dag is not None, "DAG 'earbuds_rough_to_staging' not found in DagBag"

            task_ids = set(dag.task_dict.keys())
            assert task_ids == {"extract", "transform", "validate", "load"}

        except ImportError:
            pytest.skip("apache-airflow not installed in current environment; AST syntax verification passed.")
