from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from reader import read_data
from validator import validate_data, backup_validate
from processor import process_data
from writer import write_local, write_azure

def process_ship_file(**context):
    input_folder = context['params']['input_folder']
    output_folder = context['params']['output_folder']
    azure_conn_str = context['params']['azure_conn_str']
    container_name = context['params']['container_name']
    for fname in os.listdir(input_folder):
        if fname.endswith(('.csv', '.xlsx')):
            file_path = os.path.join(input_folder, fname)
            df = read_data(file_path)
            df = validate_data(df)
            df = process_data(df)
            df = backup_validate(df)
            output_path = os.path.join(output_folder, fname.replace('.xlsx', '.csv'))
            write_local(df, output_path)
            write_azure(output_path, azure_conn_str, container_name)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ship_realtime_pipeline',
    default_args=default_args,
    description='Real-time ship data pipeline',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 4, 20),
    catchup=False,
)

process_task = PythonOperator(
    task_id='process_ship_file',
    python_callable=process_ship_file,
    provide_context=True,
    params={
        'input_folder': '/path/to/input',
        'output_folder': '/path/to/output',
        'azure_conn_str': 'YOUR_AZURE_CONNECTION_STRING',
        'container_name': 'YOUR_CONTAINER_NAME',
    },
    dag=dag,
)
