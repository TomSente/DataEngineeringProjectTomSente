import pandas as pd
import requests
import os

def fetch_and_save_data(api_url, download_path):
    """Fetches data from API and saves as CSV."""
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    # If the API returns a dict with a key containing the list, adjust as needed
    if isinstance(data, dict) and "data" in data:
        data = data["data"]
    df = pd.DataFrame(data)
    print("DataFrame columns:", list(df.columns))
    os.makedirs(os.path.dirname(download_path), exist_ok=True)
    df.to_csv(download_path, index=False)
    print(f"Fetched API data and saved to {download_path}")
    return download_path

def read_ship_data(file_path):
    """Reads CSV or Excel file into DataFrame."""
    print("Filepath: ", file_path)
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.xlsx'):
        return pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file type")
