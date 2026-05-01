import os

import pandas as pd

def read_taxi_data(local_input_path):
    """Reads taxi data from a parquet file."""
    if not os.path.exists(local_input_path):
        raise FileNotFoundError(f'Input file not found: {local_input_path}')

    print("Reading Complete")
    return pd.read_parquet(local_input_path)
