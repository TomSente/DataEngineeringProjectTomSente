import pandas as pd

def read_taxi_data(input_path):
    """Reads taxi data from a parquet file."""
    return pd.read_parquet(input_path)
