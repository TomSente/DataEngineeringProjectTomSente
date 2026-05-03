from datetime import timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

PROJECT_ROOT = "/opt/airflow/project"
REALTIME_ROOT = os.path.join(PROJECT_ROOT, "RealtimeProcessing")

# Make the pipeline module importable in the container.
sys.path.insert(0, REALTIME_ROOT)

from ship_pipeline import run_ship_pipeline

INPUT_PATH = os.path.join(REALTIME_ROOT, "input", "api_ships.csv")
OUTPUT_PATH = os.path.join(REALTIME_ROOT, "output", "api_ships_processed.csv")


def input_file_exists(file_path):
    return os.path.isfile(file_path)

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
    schedule=None,
    catchup=False,
    tags=["realtime", "ship"],
) as dag:
    wait_for_ship_file = PythonSensor(
        task_id="wait_for_api_ships_file",
        python_callable=input_file_exists,
        op_kwargs={"file_path": INPUT_PATH},
        poke_interval=5,
        mode="poke",
    )

    process_ship_data_task = PythonOperator(
        task_id="process_ship_data",
        python_callable=run_ship_pipeline,
        op_kwargs={
            "local_input_path": INPUT_PATH,
            "local_output_path": OUTPUT_PATH,
            "azure_conn_str": "",
            "container_name": "",
        },
    )

    wait_for_ship_file >> process_ship_data_task




