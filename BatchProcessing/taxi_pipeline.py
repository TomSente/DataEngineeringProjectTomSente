
# Import split modules
from reader import read_taxi_data
from validator import validate_taxi_data, backup_validate
from processor import process_taxi_data
from writer import write_local, write_azure

def run_taxi_pipeline(local_input_path, local_output_path, azure_conn_str, container_name):
    # --- READER ---
    df = read_taxi_data(local_input_path)

    # --- VALIDATOR ---
    df = validate_taxi_data(df)

    # --- PROCESSOR ---
    df = process_taxi_data(df)

    # --- BACK-UP VALIDATOR ---
    df = backup_validate(df)

    # --- WRITER ---
    write_local(df, local_output_path)
    # write_azure(local_output_path, azure_conn_str, container_name)
    return "Success"

# For testing without airflow
if __name__ == "__main__":
    input_folder = "./input/yellow_tripdata_2025-01.parquet"
    output_folder = "./output/yellow_tripdata_2025-01_processed.parquet"
    file_name = "api_ships.csv"
    azure_conn_str = ""
    container_name = ""
    run_taxi_pipeline(input_folder, output_folder, azure_conn_str, container_name)