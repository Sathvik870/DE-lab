"""
==============================================================================
ETL Pipeline: Data Loading Stage
==============================================================================
Module: etl.loader
Description:
    Loads validated records into the primary analytical storage table
    (`earbuds_staging`) using idempotent, transaction-safe operations.

Class Comments:
    In enterprise Data Engineering, the Load phase must satisfy three critical
    DataOps criteria:
    1. Idempotency: Re-running a batch must never produce duplicate entries in
       analytical tables. Even if upstream sends the same products twice,
       unique natural keys (product URL) are used to eliminate duplicates.
    2. Two-Tier Deduplication:
       - Intra-Batch Deduplication: Drops duplicate URLs within the incoming payload.
       - Inter-Batch Deduplication: Checks existing URLs in `earbuds_staging` to
         prevent re-inserting already cataloged items.
    3. ACID Transaction Isolation: All inserts within a batch are committed
       atomically. Any failure triggers an automatic `conn.rollback()`, ensuring
       the warehouse is never left in a corrupted or partially-loaded state.
==============================================================================
"""

import os
from typing import Set
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


def load_data(valid_df: pd.DataFrame, batch_id: str) -> int:
    """
    Load validated DataFrame into `earbuds_staging` with deduplication and rollback safety.

    Args:
        valid_df (pd.DataFrame): DataFrame containing validated records.
        batch_id (str): Unique UUID of the batch being processed.

    Returns:
        int: Total number of newly inserted records.
    """
    print('[LOAD] Preparing to load data into earbuds_staging...')

    if valid_df.empty:
        print('[LOAD] No valid records to load. Skipping database insertion.')
        return 0

    conn = get_connection()

    try:
        cur = conn.cursor()

        # STAGE 1: INTRA-BATCH DEDUPLICATION
        before_duplicates = len(valid_df)
        deduped_df = valid_df.drop_duplicates(subset=['url'], keep='first').copy()
        duplicate_in_batch = before_duplicates - len(deduped_df)

        print(f'[LOAD] Duplicate records eliminated inside batch: {duplicate_in_batch}')

        # STAGE 2: INTER-BATCH DEDUPLICATION
        cur.execute("""
            SELECT url
            FROM earbuds_staging
            WHERE url IS NOT NULL
        """)

        existing_urls: Set[str] = {row[0] for row in cur.fetchall()}
        print(f'[LOAD] Existing unique URLs in staging table: {len(existing_urls)}')

        already_in_staging = deduped_df[deduped_df['url'].isin(existing_urls)]
        records_to_insert = deduped_df[~deduped_df['url'].isin(existing_urls)].copy()

        print(f'[LOAD] Records skipped (already in staging): {len(already_in_staging)}')
        print(f'[LOAD] New unique records to insert: {len(records_to_insert)}')

        if records_to_insert.empty:
            print('[LOAD] Zero new records to insert. Committing empty transaction.')
            conn.commit()
            return 0

        # STAGE 3: ATOMIC BATCH INSERTION
        insert_query = """
            INSERT INTO earbuds_staging (
                url,
                title,
                price,
                mrp,
                discount,
                rating,
                rating_count,
                description,
                highlights,
                image_urls,
                video_urls,
                batch_id
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        inserted_count = 0
        for _, row in records_to_insert.iterrows():
            cur.execute(
                insert_query,
                (
                    row.get('url'),
                    row.get('title'),
                    row.get('price'),
                    row.get('mrp'),
                    row.get('discount'),
                    row.get('rating'),
                    row.get('rating_count'),
                    row.get('description'),
                    row.get('highlights'),
                    row.get('image_urls'),
                    row.get('video_urls'),
                    batch_id,
                ),
            )
            inserted_count += 1

        conn.commit()

        print('=' * 60)
        print(f'[LOAD] Successfully inserted {inserted_count} new records into earbuds_staging.')
        print(f'[LOAD] Batch ID               : {batch_id}')
        print(f'[LOAD] In-batch duplicates    : {duplicate_in_batch}')
        print(f'[LOAD] Cross-batch duplicates : {len(already_in_staging)}')
        print(f'[LOAD] Net inserted records   : {inserted_count}')
        print('=' * 60)

        return inserted_count

    except Exception as exc:
        conn.rollback()
        print(f'[LOAD] Database error occurred. Transaction successfully rolled back: {exc}')
        raise

    finally:
        cur.close()
        conn.close()
        print('[LOAD] Database cursor and connection closed.')
