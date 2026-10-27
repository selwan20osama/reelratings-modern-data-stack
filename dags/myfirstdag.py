from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'omar',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='my_dag',
    default_args=default_args,
    schedule='*/1 * * * *',
    catchup=False
) as dag:

    start = BashOperator(
        task_id='start_task',
        bash_command='echo "Starting DAG......"'
    )

    processing = BashOperator(
        task_id='processing_data',
        bash_command='echo "ETL Processing...."'
    )

    end = BashOperator(
        task_id='end_task',
        bash_command='echo "Completed ETL Task...."'
    )

    start >> processing >> end