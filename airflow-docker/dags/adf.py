from airflow import DAG
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="trigger_adf_pipeline_dag",
    default_args=default_args,
    # schedule_interval=None,  # Run manually or customize
    catchup=False,
    tags=["azure", "adf"],
) as dag:

    run_adf_pipeline = AzureDataFactoryRunPipelineOperator(
        task_id="run_adf_pipeline",
        pipeline_name="Webapicall",             # ✅ Replace with real ADF pipeline name
        # ✅ This must match your connection ID
        azure_data_factory_conn_id="azure_data_factory_conn",
        factory_name="devanshdf",                    # ✅ e.g., 'adf-prod-pipeline'
        resource_group_name="RgModerndbdev",           # ✅ e.g., 'rg-data-prod'
        # Optional: pass pipeline params
        parameters={}
    )
