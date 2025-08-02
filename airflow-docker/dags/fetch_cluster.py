from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
import requests
from datetime import datetime
import socket


def get_cluster_id(**context):
    try:
        # Get connection details
        conn = BaseHook.get_connection('databricks_default')

        # Validate connection
        if not conn.password:
            raise AirflowException(
                "Databricks token is not configured in the connection. Please set the password field with your Databricks personal access token.")

        # Handle different possible host formats
        host = conn.host
        if host.endswith('/'):
            host = host[:-1]

        # Ensure we don't have double /api/
        if '/api/' in host:
            base_url = host.split('/api/')[0]
        else:
            base_url = host

        print(f"Base URL: {base_url}")

        # Test DNS resolution
        try:
            hostname = base_url.replace('https://', '').replace('http://', '')
            socket.gethostbyname(hostname)
            print(f"DNS resolution successful for {hostname}")
        except socket.gaierror as e:
            print(f"DNS resolution failed for {hostname}: {e}")
            raise AirflowException(
                f"Cannot resolve hostname {hostname}. Please check if the Databricks workspace URL is correct and accessible from your network.")

        headers = {
            "Authorization": f"Bearer {conn.password}",
            "Content-Type": "application/json"
        }

        # Try the API call
        api_url = f"{base_url}/api/2.0/clusters/list"
        print(f"Full API URL: {api_url}")

        response = requests.get(api_url, headers=headers, timeout=30)

        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            clusters = data.get("clusters", [])

            print(f"Found {len(clusters)} clusters")

            for cluster in clusters:
                print(f"Cluster Name: {cluster.get('cluster_name', 'N/A')}")
                print(f"Cluster ID: {cluster.get('cluster_id', 'N/A')}")
                print(f"State: {cluster.get('state', 'N/A')}")
                print("-" * 50)

            return clusters
        elif response.status_code == 401:
            print("Authentication failed. Please check your Databricks token.")
            raise AirflowException(
                "Authentication failed. Please verify your Databricks personal access token.")
        elif response.status_code == 403:
            print("Access denied. Please check your permissions.")
            raise AirflowException(
                "Access denied. Please verify your Databricks token has the necessary permissions.")
        else:
            print(f"Error Response: {response.text}")
            raise AirflowException(
                f"API call failed with status {response.status_code}: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
        raise AirflowException(f"Request failed: {e}")
    except Exception as e:
        print(f"General Error: {e}")
        raise AirflowException(f"Unexpected error: {e}")


def test_connection(**context):
    """Test the Databricks connection before trying to fetch clusters"""
    try:
        conn = BaseHook.get_connection('databricks_default')

        print(f"Connection Host: {conn.host}")
        print(f"Connection Type: {conn.conn_type}")
        print(f"Has Password (token): {bool(conn.password)}")

        if not conn.password:
            raise AirflowException(
                "No token configured in databricks_default connection")

        # Test basic connectivity
        host = conn.host.rstrip('/')
        test_url = f"{host}/api/2.0/clusters/list"

        headers = {
            "Authorization": f"Bearer {conn.password}",
            "Content-Type": "application/json"
        }

        response = requests.get(test_url, headers=headers, timeout=10)

        if response.status_code == 200:
            print("✅ Connection test successful!")
            return True
        else:
            print(
                f"❌ Connection test failed: {response.status_code} - {response.text}")
            return False

    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False


dag = DAG(
    'get_databricks_cluster_id',
    start_date=datetime(2024, 1, 1),
    # schedule_interval=None,
    catchup=False,
    description='Fetch Databricks cluster information',
    tags=['databricks', 'clusters']
)

test_connection_task = PythonOperator(
    task_id='test_connection',
    python_callable=test_connection,
    dag=dag
)

get_cluster_task = PythonOperator(
    task_id='get_cluster_id',
    python_callable=get_cluster_id,
    dag=dag
)

# Set task dependencies
test_connection_task >> get_cluster_task
