import os
import time
import requests
import pandas as pd
from reader import read_data,fetch_and_save_data
from validator import validate_data, backup_validate
from processor import process_data
from writer import write_local, write_azure

def fetch_and_save_api_data(api_url, input_folder, file_name):
    """
    Fetch data from the API and save as a CSV file in the input folder.
    """
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    # If the API returns a dict with a key containing the list, adjust as needed
    if isinstance(data, dict) and "data" in data:
        data = data["data"]
    
    df = pd.DataFrame(data)
    print("DataFrame columns:", list(df.columns))
    file_path = os.path.join(input_folder, file_name).replace("\\", "/")
    df.to_csv(file_path, index=False)
    print(f"Fetched API data and saved to {file_path}")

def monitor_folder(input_folder, output_folder, azure_conn_str, container_name, poll_interval=5):
    processed_files = set()
    while True:
        for fname in os.listdir(input_folder):
            if fname.endswith(('.csv', '.xlsx')) and fname not in processed_files:
                file_path = os.path.join(input_folder, fname).replace("\\", "/")
                print(f"Processing new file: {file_path}")
                df = read_data(file_path)
                df = validate_data(df)
                df = process_data(df)
                df = backup_validate(df)
                output_path = os.path.join(output_folder, fname.replace('.xlsx', '.csv'))
                write_local(df, output_path)
                # write_azure(output_path, azure_conn_str, container_name)
                processed_files.add(fname)
        time.sleep(poll_interval)

if __name__ == "__main__":
    input_folder = "D:\Education\AcademieJaar_2025_2026\Semester_2\DATAENG\DataEngineeringProjectTomSente\RealtimeProcessing\input"
    output_folder = "D:\Education\AcademieJaar_2025_2026\Semester_2\DATAENG\DataEngineeringProjectTomSente\RealtimeProcessing\output"
    api_url = "https://api.star-citizen.wiki/api/shipmatrix/vehicles"
    file_name = "api_ships.csv"
    azure_conn_str = ""
    container_name = ""
    fetch_and_save_api_data(api_url, input_folder,file_name)
    monitor_folder(input_folder, output_folder, azure_conn_str, container_name)