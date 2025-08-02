from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
# from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    # 'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['admin@company.com']  # Update with your email
}

# DAG definition
dag = DAG(
    'databricks_notebook_dag',
    default_args=default_args,
    description='Run Databricks notebook with Airflow - Creates cluster automatically',
    # schedule_interval='@daily',  # Run daily at midnight
    catchup=False,
    tags=['databricks', 'etl', 'data-processing']
)

# Databricks notebook configuration - Creates new cluster automatically
notebook_task = {
    'notebook_task': {
        # Update with your notebook path
        'notebook_path': '/Workspace/Users/krishnajaipur23@gmail.com/testing',
        'base_parameters': {
            'env': 'prod',
            'date': '{{ ds }}',  # Airflow execution date (YYYY-MM-DD)
            'batch_id': '{{ run_id }}',  # Unique run identifier
            'dag_run_id': '{{ dag_run.run_id }}'
        }
    },
    # Auto-creates and terminates cluster for each run
    'new_cluster': {
        'spark_version': '13.3.x-scala2.12',  # Latest stable version
        'node_type_id': 'i3.xlarge',  # Change based on your cloud provider
        'num_workers': 2,  # Adjust based on your workload
        'spark_conf': {
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.adaptive.skewJoin.enabled': 'true'
        },
        'custom_tags': {
            'Environment': 'Production',
            'Project': 'DataPipeline',
            'ManagedBy': 'Airflow'
        }
    },
    'timeout_seconds': 3600,  # 1 hour timeout
    'max_retries': 1
}

# For AWS (uncomment and update if using AWS)
# notebook_task['new_cluster']['aws_attributes'] = {
#     'zone_id': 'us-west-2a',
#     'instance_profile_arn': 'arn:aws:iam::YOUR_ACCOUNT:instance-profile/databricks-instance-profile'
# }

# For Azure (uncomment and update if using Azure)
# notebook_task['new_cluster']['azure_attributes'] = {
#     'availability': 'ON_DEMAND_AZURE',
#     'first_on_demand': 1,
#     'spot_bid_max_price': -1
# }

# For GCP (uncomment and update if using GCP)
# notebook_task['new_cluster']['gcp_attributes'] = {
#     'use_preemptible_executors': False,
#     'google_service_account': 'your-service-account@your-project.iam.gserviceaccount.com'
# }

# Main task to run the Databricks notebook
run_databricks_notebook = DatabricksSubmitRunOperator(
    task_id='run_databricks_notebook',
    databricks_conn_id='databricks_default',  # Airflow connection ID
    json=notebook_task,
    dag=dag
)

# Function to check job status and log results


def check_databricks_job_status(**context):
    """Check if the Databricks job completed successfully and log details"""
    hook = DatabricksHook(databricks_conn_id='databricks_default')

    # Get the run_id from the previous task
    run_id = context['task_instance'].xcom_pull(
        task_ids='run_databricks_notebook')

    if run_id:
        run_info = hook.get_run_state(run_id)
        print(f"Job run ID: {run_id}")
        print(f"Job state: {run_info}")

        if run_info['state']['life_cycle_state'] == 'TERMINATED':
            if run_info['state']['result_state'] == 'SUCCESS':
                print("✅ Databricks notebook executed successfully!")

                # Get additional run details
                run_details = hook.get_run_page_url(run_id)
                print(f"Run details URL: {run_details}")

                return {
                    'status': 'SUCCESS',
                    'run_id': run_id,
                    'run_url': run_details
                }
            else:
                error_msg = f"❌ Job failed with result: {run_info['state']['result_state']}"
                if 'state_message' in run_info['state']:
                    error_msg += f"\nError message: {run_info['state']['state_message']}"
                raise Exception(error_msg)
        else:
            raise Exception(
                f"Job in unexpected state: {run_info['state']['life_cycle_state']}")
    else:
        raise Exception("Could not retrieve run_id from previous task")


# Task to check job completion
check_job_status = PythonOperator(
    task_id='check_job_status',
    python_callable=check_databricks_job_status,
    dag=dag
)

# Email notification on success
success_email = EmailOperator(
    task_id='send_success_email',
    to=['admin@company.com'],  # Update with your email
    subject='✅ Databricks Notebook Completed Successfully - {{ ds }}',
    html_content="""
    <h3>Databricks Notebook Executed Successfully</h3>
    <p><strong>Execution Date:</strong> {{ ds }}</p>
    <p><strong>Run ID:</strong> {{ run_id }}</p>
    <p><strong>DAG:</strong> {{ dag.dag_id }}</p>
    <p><strong>Task:</strong> {{ task.task_id }}</p>
    <p><strong>Notebook Path:</strong> /Users/your-username/your-notebook</p>
    
    <p>The cluster was automatically created and terminated for this run.</p>
    
    <p>Check the Airflow logs for more details.</p>
    """,
    dag=dag
)

# Optional: Add a simple data validation task


def validate_notebook_output(**context):
    """
    Add your data validation logic here
    This could check if expected tables were created, row counts, etc.
    """
    print("🔍 Running data validation checks...")

    # Example validation (customize based on your needs)
    # You could check if certain tables exist, validate row counts, etc.

    # For now, just return success
    print("✅ Data validation completed successfully")
    return "VALIDATION_PASSED"


data_validation = PythonOperator(
    task_id='validate_data',
    python_callable=validate_notebook_output,
    dag=dag
)

# Set up task dependencies - Simple linear flow
run_databricks_notebook >> check_job_status >> data_validation >> success_email
