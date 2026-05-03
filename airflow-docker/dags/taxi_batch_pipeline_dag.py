from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/opt/airflow/project"
BATCH_ROOT = os.path.join(PROJECT_ROOT, "BatchProcessing")

# Make the pipeline module importable in the container.
sys.path.insert(0, BATCH_ROOT)

from taxi_pipeline import run_taxi_pipeline

INPUT_PATH = os.path.join(BATCH_ROOT, "input", "yellow_tripdata_2025-01.parquet")
OUTPUT_PATH = os.path.join(BATCH_ROOT, "output", "yellow_tripdata_2025-01_processed.parquet")


default_args = {
    "owner": "student",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="taxi_batch_pipeline",
    default_args=default_args,
    schedule=timedelta(minutes=20),
    start_date=datetime(2026, 5, 4),
    catchup=False,
    tags=["batch", "taxi"],
) as dag:
    PythonOperator(
        task_id="process_yellow_taxi_data",
        python_callable=run_taxi_pipeline,
        op_kwargs={
            "local_input_path": INPUT_PATH,
            "local_output_path": OUTPUT_PATH,
            "azure_conn_str": "",
            "container_name": "",
        },
    )
