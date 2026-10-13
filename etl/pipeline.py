"""
==============================================================================
ETL Pipeline: End-to-End Orchestration Runner
==============================================================================
Module: etl.pipeline
Description:
    Coordinates the four discrete stages of the Earbuds Data Pipeline:
    Extract -> Transform -> Validate -> Load.
    Includes structured logging and automated Slack/Email alert integration
    for pipeline latency and failure handling.

Class Comments:
    In modern Data Engineering:
    - Pipelines are composed of single-responsibility stages.
    - Each stage transforms data contractually and returns measurable metrics
      (rows extracted, valid rows, rejected rows, loaded rows).
    - By decoupling orchestration from specific engines (e.g. running via CLI,
      Airflow PythonOperator, or CI automated test runners), pipelines remain
      portable, modular, and easy to unit and mock test.
    - Phase II integrates structured logging, failure hooks, and multi-channel
      alerting to meet enterprise observability standards.
==============================================================================
"""

import logging
import sys
import time
from typing import Dict, Any

from etl.extract import extract_data
from etl.transform import transform_data
from etl.validator import validate_data
from etl.loader import load_data
from etl.alerts import default_alert_manager

logger = logging.getLogger("lakehouse.etl.pipeline")


def run_pipeline(batch_id: str) -> Dict[str, Any]:
    """
    Execute the full ETL pipeline sequentially for a specified batch ID.

    Stages:
    1. Extract: Pull raw records for batch_id from `earbuds_rough`.
    2. Transform: Sanitize numeric fields and normalize text strings.
    3. Validate: Partition data into valid and rejected sets based on business rules.
    4. Load: Deduplicate and atomically insert valid records into `earbuds_staging`.

    Args:
        batch_id (str): Unique batch identifier.

    Returns:
        Dict[str, Any]: Execution metrics summary.
    """
    start_time = time.time()
    print('=' * 60)
    print('EARBUDS ETL PIPELINE EXECUTION')
    print(f'BATCH ID: {batch_id}')
    print('=' * 60)
    logger.info(f"Initiating ETL pipeline execution for batch: {batch_id}")

    try:
        # STAGE 1: EXTRACT
        raw_df = extract_data(batch_id)

        if raw_df.empty:
            print('[PIPELINE] No raw records found for batch. Terminating pipeline.')
            logger.warning(f"Batch '{batch_id}' contains 0 records; early termination.")
            return {
                'batch_id': batch_id,
                'status': 'empty',
                'extracted': 0,
                'valid': 0,
                'rejected': 0,
                'loaded': 0,
                'duration_seconds': round(time.time() - start_time, 3),
            }

        # STAGE 2: TRANSFORM
        transformed_df = transform_data(raw_df)

        # STAGE 3: VALIDATE
        valid_df, rejected_df = validate_data(transformed_df)

        # STAGE 4: LOAD
        inserted_count = load_data(valid_df, batch_id)

        duration = round(time.time() - start_time, 3)
        metrics = {
            'batch_id': batch_id,
            'status': 'success',
            'extracted': len(raw_df),
            'valid': len(valid_df),
            'rejected': len(rejected_df),
            'loaded': inserted_count,
            'duration_seconds': duration,
        }

        print('=' * 60)
        print('PIPELINE EXECUTION COMPLETED')
        print(f"Extracted : {metrics['extracted']}")
        print(f"Valid     : {metrics['valid']}")
        print(f"Rejected  : {metrics['rejected']}")
        print(f"Loaded    : {metrics['loaded']}")
        print(f"Duration  : {duration}s")
        print('=' * 60)
        logger.info(f"Batch '{batch_id}' successfully loaded {inserted_count}/{len(raw_df)} records in {duration}s")

        return metrics

    except Exception as exc:
        duration = round(time.time() - start_time, 3)
        logger.error(f"Pipeline failed for batch '{batch_id}' after {duration}s: {exc}", exc_info=True)
        default_alert_manager.handle_pipeline_failure(
            pipeline_name="earbuds_batch_pipeline",
            stage="pipeline_execution",
            error=exc,
            batch_id=batch_id,
            context={"duration_seconds": duration},
        )
        raise exc


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python -m etl.pipeline <batch_id>')
        sys.exit(1)

    target_batch_id = sys.argv[1]
    run_pipeline(target_batch_id)
