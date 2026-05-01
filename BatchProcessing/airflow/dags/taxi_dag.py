from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from taxi_pipeline import run_taxi_pipeline

default_args = {
    'owner': 'student',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nyc_taxi_batch_processing',
    default_args=default_args,
    schedule_interval='@once', 
    catchup=False
) as dag:

    process_data = PythonOperator(
        task_id='process_yellow_taxi_data',
        python_callable=run_taxi_pipeline,
        op_kwargs={
            'local_input_path':'./input/yellow_tripdata_2025-01.parquet',
            'local_output_path': './output/processed_taxi_data.parquet',
            'azure_conn_str': 'YOUR_AZURE_CONNECTION_STRING',
            'container_name': 'taxi-data'
        }
    )