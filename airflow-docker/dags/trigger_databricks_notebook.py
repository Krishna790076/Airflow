from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
}

with DAG('databricks_notebook_trigger',
         start_date=datetime(2023, 1, 1),
         #  schedule='@daily',
         catchup=False,
         default_args=default_args) as dag:

    notebook_task = DatabricksSubmitRunOperator(
        task_id='run_notebook',
        databricks_conn_id='databricks',
        new_cluster={
            'spark_version': '16.4.x-scala2.12',  # Match what you use in Databricks
            # Or whatever is allowed in your workspace
            'node_type_id': 'Standard_D4s_v3',
            'num_workers': 1
        },
        notebook_task={
            'notebook_path': '/Workspace/Users/devansharma0909@gmail.com/Untitled Notebook 2025-07-23 12:38:32'  # <-- Replace this
        }
    )

# This is optional, just to clarify the task dependencies
notebook_task
