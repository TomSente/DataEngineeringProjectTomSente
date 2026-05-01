import pandas as pd

def read_taxi_data(local_input_path):
    """Reads taxi data from a parquet file."""
    print("Reading Complete")
    return pd.read_parquet(local_input_path)
