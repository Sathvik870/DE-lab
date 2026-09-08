from datetime import datetime
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from etl.extract import extract_data
from etl.transform import transform_data
from etl.validator import validate_data
from etl.loader import load_data
import pandas as pd

# ============================================================
# TEMP DATA DIRECTORY
# ============================================================

TEMP_DIR = "/opt/airflow/logs/etl_tmp"

os.makedirs(TEMP_DIR, exist_ok=True)


# ============================================================
# HELPER
# ============================================================

def get_batch_id(context):

    dag_run = context["dag_run"]

    batch_id = dag_run.conf.get("batch_id")

    if not batch_id:
        raise ValueError(
            "batch_id was not provided to the DAG"
        )

    return batch_id


# ============================================================
# STAGE 1 - EXTRACT
# ============================================================

def extract_stage(**context):

    batch_id = get_batch_id(context)

    print("=" * 60)
    print("ETL STAGE 1 - EXTRACT")
    print(f"BATCH ID: {batch_id}")
    print("=" * 60)

    df = extract_data(batch_id)

    print(
        f"[EXTRACT] Retrieved {len(df)} records."
    )

    output_path = os.path.join(
        TEMP_DIR,
        f"{batch_id}_extract.pkl"
    )

    df.to_pickle(output_path)

    print(
        f"[EXTRACT] Saved temporary data: {output_path}"
    )


# ============================================================
# STAGE 2 - TRANSFORM
# ============================================================

def transform_stage(**context):

    batch_id = get_batch_id(context)

    print("=" * 60)
    print("ETL STAGE 2 - TRANSFORM")
    print(f"BATCH ID: {batch_id}")
    print("=" * 60)

    input_path = os.path.join(
        TEMP_DIR,
        f"{batch_id}_extract.pkl"
    )

    if not os.path.exists(input_path):
        raise FileNotFoundError(
            f"Extract output not found: {input_path}"
        )

    df = pd.read_pickle(input_path)

    transformed_df = transform_data(df)

    output_path = os.path.join(
        TEMP_DIR,
        f"{batch_id}_transform.pkl"
    )

    transformed_df.to_pickle(output_path)

    print(
        f"[TRANSFORM] Saved transformed data: "
        f"{output_path}"
    )


# ============================================================
# STAGE 3 - VALIDATE
# ============================================================

def validate_stage(**context):

    batch_id = get_batch_id(context)

    print("=" * 60)
    print("ETL STAGE 3 - VALIDATE")
    print(f"BATCH ID: {batch_id}")
    print("=" * 60)

    input_path = os.path.join(
        TEMP_DIR,
        f"{batch_id}_transform.pkl"
    )

    if not os.path.exists(input_path):
        raise FileNotFoundError(
            f"Transform output not found: {input_path}"
        )

    df = __import__("pandas").read_pickle(
        input_path
    )

    valid_df, rejected_df = validate_data(df)

    print(
        f"[VALIDATE] Valid records: "
        f"{len(valid_df)}"
    )

    print(
        f"[VALIDATE] Rejected records: "
        f"{len(rejected_df)}"
    )

    # Save only valid records for LOAD
    output_path = os.path.join(
        TEMP_DIR,
        f"{batch_id}_valid.pkl"
    )

    valid_df.to_pickle(output_path)

    # Save rejected records separately
    rejected_path = os.path.join(
        TEMP_DIR,
        f"{batch_id}_rejected.pkl"
    )

    rejected_df.to_pickle(rejected_path)

    print(
        f"[VALIDATE] Valid data saved: "
        f"{output_path}"
    )

    print(
        f"[VALIDATE] Rejected data saved: "
        f"{rejected_path}"
    )


# ============================================================
# STAGE 4 - LOAD
# ============================================================

def load_stage(**context):

    batch_id = get_batch_id(context)

    print("=" * 60)
    print("ETL STAGE 4 - LOAD")
    print(f"BATCH ID: {batch_id}")
    print("=" * 60)

    input_path = os.path.join(
        TEMP_DIR,
        f"{batch_id}_valid.pkl"
    )

    if not os.path.exists(input_path):
        raise FileNotFoundError(
            f"Validation output not found: {input_path}"
        )

    valid_df = pd.read_pickle(input_path)

    inserted = load_data(
        valid_df,
        batch_id
    )

    print(
        f"[LOAD] Inserted records: {inserted}"
    )

    # --------------------------------------------------------
    # Cleanup temporary files
    # --------------------------------------------------------

    files_to_remove = [
        f"{batch_id}_extract.pkl",
        f"{batch_id}_transform.pkl",
        f"{batch_id}_valid.pkl",
        f"{batch_id}_rejected.pkl",
    ]

    for filename in files_to_remove:

        path = os.path.join(
            TEMP_DIR,
            filename
        )

        if os.path.exists(path):
            os.remove(path)

            print(
                f"[CLEANUP] Removed {filename}"
            )

    print("=" * 60)
    print("ETL PIPELINE COMPLETED")
    print(f"BATCH ID: {batch_id}")
    print(f"INSERTED: {inserted}")
    print("=" * 60)


# ============================================================
# AIRFLOW DAG
# ============================================================

with DAG(
    dag_id="earbuds_rough_to_staging",

    description=(
        "Process earbuds_rough data "
        "into earbuds_staging"
    ),

    start_date=datetime(2026, 1, 1),

    schedule=None,

    catchup=False,

    tags=[
        "earbuds",
        "etl",
    ],

) as dag:

    # ========================================================
    # 1. EXTRACT
    # ========================================================

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_stage,
    )

    # ========================================================
    # 2. TRANSFORM
    # ========================================================

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_stage,
    )

    # ========================================================
    # 3. VALIDATE
    # ========================================================

    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_stage,
    )

    # ========================================================
    # 4. LOAD
    # ========================================================

    load = PythonOperator(
        task_id="load",
        python_callable=load_stage,
    )

    # ========================================================
    # PIPELINE ORDER
    # ========================================================

    extract >> transform >> validate >> load