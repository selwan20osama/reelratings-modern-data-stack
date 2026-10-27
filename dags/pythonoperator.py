from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'omar',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def hello():
    print('Hello WORLD..............')

with DAG(
    dag_id='python_dag',
    default_args=default_args,
    schedule='* * * * *',
    start_date=datetime(2024, 10, 16),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='greating',
        python_callable=hello
    )