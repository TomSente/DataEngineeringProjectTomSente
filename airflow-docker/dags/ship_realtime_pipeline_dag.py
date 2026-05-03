from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/opt/airflow/project"
REALTIME_ROOT = os.path.join(PROJECT_ROOT, "RealtimeProcessing")

# Make the pipeline module importable in the container.
sys.path.insert(0, REALTIME_ROOT)

from ship_pipeline import run_ship_pipeline

INPUT_PATH = os.path.join(REALTIME_ROOT, "input", "api_ships.csv")
OUTPUT_PATH = os.path.join(REALTIME_ROOT, "output", "api_ships.csv")
API_URL = "https://api.star-citizen.wiki/api/shipmatrix/vehicles?page[size]=100"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ship_realtime_pipeline",
    default_args=default_args,
    description="Real-time ship data pipeline",
    schedule=timedelta(minutes=5),
    start_date=datetime(2025, 5, 4),
    catchup=False,
    tags=["realtime", "ship"],
) as dag:
    PythonOperator(
        task_id="process_ship_data",
        python_callable=run_ship_pipeline,
        op_kwargs={
            "local_input_path": INPUT_PATH,
            "local_output_path": OUTPUT_PATH,
            "azure_conn_str": "",
            "container_name": "",
            "api_url": API_URL,
        },
    )
