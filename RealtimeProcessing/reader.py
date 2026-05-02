import pandas as pd
import requests
import os

def fetch_and_save_data(api_url, download_path):
    """Fetches data from API and saves as CSV."""
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data)
    os.makedirs(os.path.dirname(download_path), exist_ok=True)
    df.to_csv(download_path, index=False)
    return download_path

def read_data(file_path):
    """Reads CSV or Excel file into DataFrame."""
    print("Filepath: ", file_path)
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.xlsx'):
        return pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file type")
