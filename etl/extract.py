"""
==============================================================================
ETL Pipeline: Data Extraction Stage
==============================================================================
Module: etl.extract
Description:
    Extracts raw ingested product batches from the PostgreSQL `earbuds_rough` table.

Class Comments:
    In batch-oriented Data Engineering architectures, pipeline runs should be
    partitioned by immutable batch identifiers (`batch_id`). This ensures:
    1. Deterministic Replayability: Any historic pipeline run can be reprocessed
       without affecting unrelated batches.
    2. Zero Race Conditions: Concurrently ingested batches can be processed
       in isolation without table locks or partial reads.
    3. Testability via Mocks: Extraction can be easily mocked in CI environments
       by intercepting database connection pools and cursor queries.
==============================================================================
"""

import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """
    Establish a connection to the PostgreSQL database using environment configuration.
    """
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', '5432')),
        database=os.getenv('DB_NAME', 'earbuds_db'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', ''),
    )


def extract_data(batch_id: str) -> pd.DataFrame:
    """
    Extract all raw records associated with a specific batch ID from `earbuds_rough`.

    Args:
        batch_id (str): Unique UUID identifying the ingestion batch.

    Returns:
        pd.DataFrame: Raw product records.
    """
    print('=' * 60)
    print(f'[EXTRACT] Reading earbuds_rough for batch_id: {batch_id}')
    print('=' * 60)

    conn = get_connection()

    try:
        query = """
            SELECT
                id,
                url,
                title,
                price,
                mrp,
                discount,
                rating,
                rating_count,
                description,
                highlights,
                specifications,
                image_urls,
                video_urls,
                batch_id,
                uploaded_at
            FROM earbuds_rough
            WHERE batch_id = %s
        """

        df = pd.read_sql_query(
            query,
            conn,
            params=(batch_id,),
        )

        print(f'[EXTRACT] Retrieved {len(df)} records for batch {batch_id}.')
        return df

    finally:
        conn.close()
        print('[EXTRACT] Database connection closed.')
