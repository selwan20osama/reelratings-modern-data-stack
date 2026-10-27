import os
import subprocess
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "reelratings",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}


def verify_raw_data_in_snowflake():
    import snowflake.connector

    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse="COMPUTE_WH",
        database="REELRATINGS_DB",
        schema="RAW",
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM RAW_MOVIES")
            row_count = cursor.fetchone()[0]
            print(f"Snowflake RAW_MOVIES rows: {row_count}", flush=True)
    finally:
        conn.close()


def run_dbt_and_log_output():
    command = [
        "/home/airflow/.local/bin/dbt",
        "run",
        "--project-dir", "/opt/airflow/dbt/my_dbt_project",
        "--profiles-dir", "/opt/airflow/dbt_profiles",
        "--target-path", "/tmp/dbt-target",
        "--log-path", "/tmp/dbt-logs",
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )

    print("=== DBT OUTPUT ===", flush=True)
    print(result.stdout, flush=True)
    print(result.stderr, flush=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"dbt failed with exit code {result.returncode}"
        )

    print("DBT completed successfully.", flush=True)


with DAG(
    dag_id="reelratings_pipeline_v2",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["reelratings", "snowflake", "dbt"],
) as dag:

    verify_raw_task = PythonOperator(
        task_id="verify_raw_snowflake_data",
        python_callable=verify_raw_data_in_snowflake,
    )

    dbt_transform_task = PythonOperator(
        task_id="run_dbt_models",
        python_callable=run_dbt_and_log_output,
    )

    verify_raw_task >> dbt_transform_task
