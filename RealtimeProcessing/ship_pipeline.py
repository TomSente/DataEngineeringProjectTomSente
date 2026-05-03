import os

from reader import fetch_and_save_data, read_ship_data
from validator import validate_ship_data, backup_validate
from processor import process_ship_data
from writer import write_local, write_azure

def run_ship_pipeline(local_input_path, local_output_path, azure_conn_str, container_name):

    # --- READER ---
    df = read_ship_data(local_input_path)

    # --- VALIDATOR ---
    df = validate_ship_data(df)

    # --- PROCESSOR ---
    df = process_ship_data(df)

    # --- BACK-UP VALIDATOR ---
    df = backup_validate(df)

    # --- WRITER ---
    write_local(df, local_output_path)
    # write_azure(local_output_path, azure_conn_str, container_name)
    return "Success"
