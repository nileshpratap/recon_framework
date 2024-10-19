from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from recon_framework.framework.engines.jdbc import JDBCConnector  
from recon_framework.framework.engines.athena import AthenaConnector  
import boto3
import json

# Define DAG arguments
args = {"provide_context": True}

# JSON config for both source and destination
json_config = {
    "source": {
        "name": "JDBC_source1",
        "secret_key": "jdbc_secret_key",  # Add the secret key
        "table_name": "source_table",
        "primary_key": "CLAIM_ID,VERSION_NO"
    },
    "destination": {
        "name": "Athena_destination",
        "database": "athena_db",
        "s3_output": "s3://bucket/path/",
        "table_name": "destination_table"
    }
}

# 1: Fetch record count from JDBC source
def fetch_jdbc_count(**kwargs):
    source_details = json_config['source']
    secret_key = source_details['secret_key']

    # Fetch secret from AWS Secrets Manager
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')
    get_secret_value_response = client.get_secret_value(SecretId=secret_key)
    secret = json.loads(get_secret_value_response['SecretString'])

    # Initialize JDBCConnector with the fetched secret
    connector = JDBCConnector(secret=secret)
    
    table_name = source_details['table_name']
    
    # Fetch total count from the JDBC source
    count = connector.get_total_count(table_name)
    print(f"JDBC Source Count: {count}")
    return count

# 2: Fetch record count from Athena destination
def fetch_athena_count(**kwargs):
    dest_details = json_config['destination']
    table_name = dest_details['table_name']
    
    athena_connector = AthenaConnector(
        database=dest_details['database'],
        s3_output=dest_details['s3_output']
    )
    
    count = athena_connector.get_total_count(table_name)
    print(f"Athena Destination Count: {count}")
    return count

def compare_counts(ti, **kwargs):
    jdbc_count = ti.xcom_pull(task_ids='fetch_jdbc_count')
    athena_count = ti.xcom_pull(task_ids='fetch_athena_count')
    
    if jdbc_count == athena_count:
        print(f"Record count matches: {jdbc_count}")
    else:
        print(f"Discrepancy! JDBC: {jdbc_count}, Athena: {athena_count}")

with DAG('jdbc_athena_count_comparison_dag',
         schedule_interval=None,
         default_args=args,
         start_date=datetime(2024, 9, 11),
         catchup=False) as dag:

    # Task to fetch count from JDBC source
    fetch_jdbc_count_task = PythonOperator(
        task_id='fetch_jdbc_count',
        python_callable=fetch_jdbc_count
    )

    # Task to fetch count from Athena destination
    fetch_athena_count_task = PythonOperator(
        task_id='fetch_athena_count',
        python_callable=fetch_athena_count
    )

    #compare the counts
    compare_task = PythonOperator(
        task_id='compare_counts',
        python_callable=compare_counts,
        provide_context=True
    )

    fetch_jdbc_count_task >> fetch_athena_count_task >> compare_task
