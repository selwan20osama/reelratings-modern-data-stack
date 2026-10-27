from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'omar',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='sql_operator',
    default_args=default_args,
    start_date=datetime(2024, 10, 16),
    schedule='* * * * *',
    catchup=False
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='dw_postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50)
            );
        """
    )