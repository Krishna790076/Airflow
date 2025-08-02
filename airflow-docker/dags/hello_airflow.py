from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello():
    print("Hello, Airflow!")


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule='@daily',  # ✅ updated here
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    task1 = PythonOperator(
        task_id='say_hello_task',
        python_callable=say_hello,
    )

    task1
