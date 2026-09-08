from fastapi import (
    FastAPI,
    UploadFile,
    File,
    HTTPException,
)

from fastapi.middleware.cors import CORSMiddleware

import asyncio
import math
import os
import uuid
from io import BytesIO

import pandas as pd
import psycopg2
import requests

from dotenv import load_dotenv


load_dotenv()

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


AIRFLOW_URL = os.getenv(
    "AIRFLOW_URL",
    "http://127.0.0.1:8080",
)

AIRFLOW_DAG_ID = os.getenv(
    "AIRFLOW_DAG_ID",
    "earbuds_rough_to_staging",
)

AIRFLOW_USERNAME = os.getenv(
    "AIRFLOW_USERNAME",
    "admin",
)

AIRFLOW_PASSWORD = os.getenv(
    "AIRFLOW_PASSWORD",
    "admin123",
)


latest_batch_id = None
latest_pipeline_status = None
latest_pipeline_message = None


def get_connection():
    return psycopg2.connect(
        host=os.getenv(
            "DB_HOST",
            "localhost",
        ),
        port=int(
            os.getenv(
                "DB_PORT",
                "5432",
            )
        ),
        database=os.getenv(
            "DB_NAME",
            "earbuds_db",
        ),
        user=os.getenv(
            "DB_USER",
            "postgres",
        ),
        password=os.getenv(
            "DB_PASSWORD",
        ),
    )


def safe_float(value, default=0.0):
    if value is None:
        return default

    try:
        value = float(value)

        if not math.isfinite(value):
            return default

        return value

    except (
        ValueError,
        TypeError,
    ):
        return default


def get_airflow_token():

    response = requests.post(
        f"{AIRFLOW_URL}/auth/token",
        json={
            "username": AIRFLOW_USERNAME,
            "password": AIRFLOW_PASSWORD,
        },
        timeout=10,
    )

    print(
        f"[AIRFLOW AUTH] Status: "
        f"{response.status_code}"
    )

    if response.status_code not in (
        200,
        201,
    ):
        raise Exception(
            f"Airflow authentication failed: "
            f"{response.status_code} - "
            f"{response.text}"
        )

    data = response.json()

    token = data.get("access_token")

    if not token:
        raise Exception(
            "Airflow authentication succeeded "
            "but no access token was returned."
        )

    print(
        "[AIRFLOW AUTH] JWT token received"
    )

    return token


def trigger_airflow(batch_id):

    token = get_airflow_token()

    url = (
        f"{AIRFLOW_URL}/api/v2/dags/"
        f"{AIRFLOW_DAG_ID}/dagRuns"
    )

    payload = {
        "logical_date": None,
        "conf": {
            "batch_id": str(batch_id),
        },
    }

    response = requests.post(
        url,
        json=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        timeout=10,
    )

    print(
        f"[AIRFLOW] Trigger status: "
        f"{response.status_code}"
    )

    print(
        f"[AIRFLOW] Response: "
        f"{response.text}"
    )

    if response.status_code not in (
        200,
        201,
    ):
        raise Exception(
            f"Airflow DAG trigger failed: "
            f"{response.status_code} - "
            f"{response.text}"
        )

    data = response.json()

    dag_run_id = data.get("dag_run_id")

    if not dag_run_id:
        raise Exception(
            "Airflow accepted the request "
            "but did not return a DAG run ID."
        )

    print(
        f"[AIRFLOW] DAG triggered successfully"
    )

    print(
        f"[AIRFLOW] DAG Run ID: "
        f"{dag_run_id}"
    )

    return dag_run_id, token


async def wait_for_airflow(
    dag_run_id,
    token,
):

    headers = {
        "Authorization": f"Bearer {token}",
    }

    max_attempts = 120

    for attempt in range(max_attempts):

        response = requests.get(
            f"{AIRFLOW_URL}/api/v2/dags/"
            f"{AIRFLOW_DAG_ID}/dagRuns/"
            f"{dag_run_id}",
            headers=headers,
            timeout=10,
        )

        if response.status_code != 200:
            raise Exception(
                f"Failed to get Airflow DAG status: "
                f"{response.status_code} - "
                f"{response.text}"
            )

        data = response.json()

        state = data.get("state")

        print(
            f"[AIRFLOW] Attempt {attempt + 1}/"
            f"{max_attempts} "
            f"State: {state}"
        )

        if state == "success":

            print(
                "[AIRFLOW] Pipeline completed successfully"
            )

            return True

        if state in (
            "failed",
            "upstream_failed",
        ):

            print(
                "[AIRFLOW] Pipeline failed"
            )

            return False

        await asyncio.sleep(2)

    raise Exception(
        "Airflow pipeline timeout: "
        "DAG did not finish within "
        "the expected time."
    )


def get_eda(df):
    print("[EDA] Calculating EDA from uploaded CSV...")

    total_rows = len(df)

    numeric_df = df.copy()

    def clean_numeric(value):
        if pd.isna(value):
            return 0

        value = str(value).strip()

        if not value:
            return 0

        value = value.replace("₹", "")
        value = value.replace(",", "")
        value = value.replace("%", "")

        if value.lower() in [
            "nan",
            "none",
            "null",
            "na",
            "n/a",
            "invalid",
        ]:
            return 0

        try:

            number = float(value)

            if math.isnan(number) or math.isinf(number):
                return 0

            return number

        except (ValueError, TypeError):

            return 0

    numeric_columns = [
        "price",
        "mrp",
        "discount",
        "rating",
        "rating_count_text",
    ]

    for column in numeric_columns:
        if column in numeric_df.columns:
            numeric_df[column] = (
                numeric_df[column]
                .apply(clean_numeric)
            )

    average_price = (
        numeric_df["price"].mean()
        if "price" in numeric_df.columns
        else 0
    )

    average_discount = (
        numeric_df["discount"].mean()
        if "discount" in numeric_df.columns
        else 0
    )

    average_rating = (
        numeric_df["rating"].mean()
        if "rating" in numeric_df.columns
        else 0
    )

    average_rating_count = (
        numeric_df["rating_count_text"].mean()
        if "rating_count_text" in numeric_df.columns
        else 0
    )

    total_cells = (
        df.shape[0] * df.shape[1]
    )

    missing_cells = int(
        df.isna().sum().sum()
    )

    empty_string_cells = int(
        (
            df.astype(str)
            .apply(
                lambda column:
                column.str.strip().eq("").sum()
            )
            .sum()
        )
    )

    missing_cells += empty_string_cells

    missing_values_percent = (
        (missing_cells / total_cells) * 100
        if total_cells > 0
        else 0
    )

    duplicate_titles = 0

    if "title" in df.columns:

        duplicate_titles = int(
            df["title"]
            .dropna()
            .duplicated()
            .sum()
        )

    result = {
        "total_rows": int(total_rows),

        "average_price": safe_float(
            average_price
        ),

        "average_discount": safe_float(
            average_discount
        ),

        "average_rating": safe_float(
            average_rating
        ),

        "average_rating_count": safe_float(
            average_rating_count
        ),

        "missing_values_percent": safe_float(
            missing_values_percent
        ),

        "duplicate_titles": int(
            duplicate_titles
        ),
    }

    print(
        f"[EDA] CSV Rows: "
        f"{result['total_rows']} | "
        f"Avg Price: "
        f"{result['average_price']} | "
        f"Avg Rating: "
        f"{result['average_rating']} | "
        f"Avg Discount: "
        f"{result['average_discount']}% | "
        f"Avg Rating Count: "
        f"{result['average_rating_count']} | "
        f"Missing: "
        f"{result['missing_values_percent']}%"
    )

    return result

def get_business_analytics():
    print("[BUSINESS] Fetching staging business analytics...")

    conn = get_connection()

    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT
                COUNT(*),
                AVG(price),
                AVG(mrp),
                AVG(discount),
                AVG(rating),
                COALESCE(SUM(rating_count), 0),
                AVG(rating_count)
            FROM earbuds_staging
            WHERE price IS NOT NULL
        """)

        (
            total_products,
            average_selling_price,
            average_mrp,
            average_discount,
            average_rating,
            total_rating_count,
            average_rating_count,
        ) = cur.fetchone()

        cur.execute("""
            SELECT
                price,
                rating,
                rating_count,
                discount
            FROM earbuds_staging
            WHERE
                price IS NOT NULL
                OR rating IS NOT NULL
                OR rating_count IS NOT NULL
                OR discount IS NOT NULL
        """)

        rows = cur.fetchall()

        products = []

        for row in rows:
            price, rating, rating_count, discount = row

            products.append({
                "price": safe_float(price),
                "rating": safe_float(rating),
                "rating_count": safe_float(rating_count),
                "discount": safe_float(discount),
            })

        cur.close()

        return {
            "kpis": {
                "total_products": int(
                    total_products or 0
                ),

                "average_selling_price": safe_float(
                    average_selling_price
                ),

                "average_mrp": safe_float(
                    average_mrp
                ),

                "average_discount": safe_float(
                    average_discount
                ),

                "average_rating": safe_float(
                    average_rating
                ),

                "total_rating_count": safe_float(
                    total_rating_count
                ),

                "average_rating_count": safe_float(
                    average_rating_count
                ),
            },

            "products": products,
        }

    finally:
        conn.close()


def get_analytics():

    print(
        "[DB] Fetching processed analytics..."
    )

    conn = get_connection()

    try:

        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                COUNT(*),
                AVG(price),
                AVG(rating),
                AVG(discount)
            FROM earbuds_staging
            """
        )

        (
            total,
            avg_price,
            avg_rating,
            avg_discount,
        ) = cur.fetchone()

        cur.execute(
            """
            SELECT
                title,
                price,
                rating,
                discount
            FROM earbuds_staging
            WHERE rating IS NOT NULL
            ORDER BY rating DESC
            LIMIT 10
            """
        )

        rows = cur.fetchall()

        top_products = []

        for row in rows:

            top_products.append(
                {
                    "title": row[0],
                    "price": safe_float(row[1]),
                    "rating": safe_float(row[2]),
                    "discount": safe_float(row[3]),
                }
            )

        cur.close()

        return {
            "total_products": int(
                total or 0
            ),

            "average_price": safe_float(
                avg_price
            ),

            "average_rating": safe_float(
                avg_rating
            ),

            "average_discount": safe_float(
                avg_discount
            ),

            "top_products": top_products,
        }

    finally:

        conn.close()


@app.get("/")
def root():

    return {
        "message":
            "Earbuds Analytics API is running"
    }


@app.get("/analytics")
def analytics():

    return get_analytics()


@app.get("/pipeline-status")
def pipeline_status():

    return {
        "batch_id": latest_batch_id,
        "status": latest_pipeline_status,
        "message": latest_pipeline_message,
    }

@app.get("/business-analytics")
def business_analytics():
    return get_business_analytics()


@app.post("/upload")
async def upload_csv(
    file: UploadFile = File(...)
):

    global latest_batch_id
    global latest_pipeline_status
    global latest_pipeline_message

    if not file.filename:

        raise HTTPException(
            status_code=400,
            detail="Filename is required",
        )

    if not file.filename.lower().endswith(
        ".csv"
    ):

        raise HTTPException(
            status_code=400,
            detail="Only CSV files are allowed",
        )

    try:

        contents = await file.read()

        df = pd.read_csv(
            BytesIO(contents)
        )

        print(
            f"[UPLOAD] Receiving file: "
            f"{file.filename}"
        )

        print(
            f"[UPLOAD] CSV contains "
            f"{len(df)} records"
        )

        eda_data = get_eda(df)

        required_columns = [
            "url",
            "title",
            "price",
            "mrp",
            "discount",
            "rating",
            "rating_count_text",
            "description",
            "highlights",
            "specifications",
            "image_urls",
            "video_urls",
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing_columns:

            raise HTTPException(
                status_code=400,
                detail={
                    "message":
                        "CSV is missing required columns",

                    "missing_columns":
                        missing_columns,
                },
            )

        batch_id = str(
            uuid.uuid4()
        )

        print(
            f"[UPLOAD] Batch ID: "
            f"{batch_id}"
        )

        conn = get_connection()

        try:

            cur = conn.cursor()

            insert_query = """
                INSERT INTO earbuds_rough (
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
                    batch_id
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
            """

            inserted = 0

            for _, row in df.iterrows():

                cur.execute(
                    insert_query,
                    (
                        row.get("url"),
                        row.get("title"),
                        row.get("price"),
                        row.get("mrp"),
                        row.get("discount"),
                        row.get("rating"),
                        row.get(
                            "rating_count_text"
                        ),
                        row.get(
                            "description"
                        ),
                        row.get(
                            "highlights"
                        ),
                        row.get(
                            "specifications"
                        ),
                        row.get(
                            "image_urls"
                        ),
                        row.get(
                            "video_urls"
                        ),
                        batch_id,
                    ),
                )

                inserted += 1

            conn.commit()

            cur.close()

            print(
                f"[UPLOAD] Inserted "
                f"{inserted} records into "
                f"earbuds_rough"
            )

            print(
                "[UPLOAD] Database insertion successful"
            )

        except Exception:

            conn.rollback()

            raise

        finally:

            conn.close()

        latest_batch_id = batch_id

        latest_pipeline_status = "running"

        latest_pipeline_message = (
            "CSV uploaded. "
            "Airflow pipeline is running..."
        )

        print(
            "[AIRFLOW] Triggering DAG: "
            f"{AIRFLOW_DAG_ID}"
        )

        print(
            f"[AIRFLOW] Batch ID: "
            f"{batch_id}"
        )

        try:

            dag_run_id, token = trigger_airflow(
                batch_id
            )

            pipeline_success = (
                await wait_for_airflow(
                    dag_run_id,
                    token,
                )
            )

            if pipeline_success:

                latest_pipeline_status = (
                    "success"
                )

                latest_pipeline_message = (
                    "Pipeline completed successfully"
                )

                print(
                    "[PIPELINE] "
                    "ETL completed successfully"
                )

                return {
                    "status": "success",

                    "pipeline_status":
                        "completed",

                    "message": (
                        "CSV uploaded and "
                        "ETL pipeline completed "
                        "successfully"
                    ),

                    "filename":
                        file.filename,

                    "records_inserted":
                        inserted,

                    "batch_id":
                        batch_id,

                    "airflow_dag":
                        AIRFLOW_DAG_ID,

                    "dag_run_id":
                        dag_run_id,

                    "eda": 
                        eda_data,
                }

            latest_pipeline_status = "error"

            latest_pipeline_message = (
                "Airflow pipeline failed"
            )

            print(
                "[PIPELINE] "
                "ETL pipeline failed"
            )

            raise HTTPException(
                status_code=500,
                detail={
                    "message": (
                        "CSV was uploaded, "
                        "but the ETL pipeline failed"
                    ),

                    "batch_id":
                        batch_id,

                    "dag_run_id":
                        dag_run_id,
                },
            )

        except HTTPException:

            raise

        except Exception as error:

            latest_pipeline_status = "error"

            latest_pipeline_message = (
                f"Airflow pipeline failed: "
                f"{error}"
            )

            print(
                f"[AIRFLOW ERROR] {error}"
            )

            raise HTTPException(
                status_code=500,
                detail={
                    "message": (
                        "CSV was uploaded to "
                        "PostgreSQL, but the "
                        "Airflow pipeline failed"
                    ),

                    "batch_id":
                        batch_id,

                    "error":
                        str(error),
                },
            )

    except HTTPException:

        raise

    except Exception as error:

        latest_pipeline_status = "error"

        latest_pipeline_message = str(error)

        print(
            f"[UPLOAD ERROR] {error}"
        )

        raise HTTPException(
            status_code=500,
            detail=str(error),
        )