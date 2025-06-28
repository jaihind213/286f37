from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def say_hello():
    import time
    print("before sleep")
    time.sleep(60)
    print("Hello, world!")


with DAG(
    dag_id="hello_world",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",  # Run once per day
    catchup=False,               # Don't backfill missed runs
    tags=["example"],
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello_task",
        python_callable=say_hello,
    )
