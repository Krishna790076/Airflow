from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='databricks_notebook_trigger2',
    start_date=datetime(2023, 1, 1),
    # schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
) as dag:

    notebook_task = DatabricksSubmitRunOperator(
        task_id='run_notebook',
        databricks_conn_id='databricks_default',
        existing_cluster_id='0717-060649-o4oiwe2g-v2n',
        notebook_task={
            'notebook_path': '/Workspace/Users/devansharma0909@gmail.com/Untitled Notebook 2025-07-23 12:38:32'
        },
    )
