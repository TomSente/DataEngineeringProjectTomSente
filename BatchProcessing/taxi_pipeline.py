import pandas as pd
import numpy as np
import os
from azure.storage.blob import BlobServiceClient

def run_taxi_pipeline(input_path, local_output_path, azure_conn_str, container_name):
    # --- READER ---
    df = pd.read_parquet(input_path)
    
    # --- VALIDATOR ---
    mandatory_cols = [
        'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
        'trip_distance', 'PULocationID', 'DOLocationID', 
        'payment_type', 'fare_amount', 'total_amount'
    ]
    
    # Drop rows missing mandatory data
    initial_count = len(df)
    df = df.dropna(subset=mandatory_cols)
    
    # Logical Validation: Trip distance and fare must be positive
    df = df[(df['trip_distance'] >= 0) & (df['total_amount'] >= 0)]
    print(f"Validation complete. Rows removed: {initial_count - len(df)}")

    # --- PROCESSOR ---
    # Drop columns
    df = df.drop(columns=['VendorID', 'store_and_fwd_flag', 'RatecodeID'], errors='ignore')

    # Calculations
    df['trip_duration_minutes'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
    
    # Handle division by zero for speed
    df['average_speed_mph'] = np.where(
        df['trip_duration_minutes'] > 0, 
        df['trip_distance'] / (df['trip_duration_minutes'] / 60), 
        0
    )
    
    # Pickup details
    df['pickup_year'] = df['tpep_pickup_datetime'].dt.year
    df['pickup_month'] = df['tpep_pickup_datetime'].dt.month
    
    # Revenue per mile
    df['revenue_per_mile'] = np.where(
        df['trip_distance'] > 0, 
        df['total_amount'] / df['trip_distance'], 
        0
    )

    # Binning/Categorization
    df['trip_distance_category'] = pd.cut(
        df['trip_distance'], bins=[-1, 2, 10, np.inf], labels=['Short', 'Medium', 'Long']
    )
    df['fare_category'] = pd.cut(
        df['fare_amount'], bins=[-1, 20, 50, np.inf], labels=['Low', 'Medium', 'High']
    )

    # Time of Day Logic
    def get_time_of_day(hour):
        if 5 <= hour < 12: return 'Morning'
        elif 12 <= hour < 17: return 'Afternoon'
        elif 17 <= hour < 21: return 'Evening'
        else: return 'Night'

    df['trip_time_of_day'] = df['tpep_pickup_datetime'].dt.hour.apply(get_time_of_day)

    # --- BACK-UP VALIDATOR ---
    # Ensure no infinity values were created during division
    df.replace([np.inf, -np.inf], 0, inplace=True)

    # --- WRITER ---
    # 1. Local Write
    os.makedirs(os.path.dirname(local_output_path), exist_ok=True)
    df.to_parquet(local_output_path, index=False)

    # 2. Azure Write
    blob_service_client = BlobServiceClient.from_connection_string(azure_conn_str)
    blob_client = blob_service_client.get_blob_client(
        container=container_name, 
        blob=os.path.basename(local_output_path)
    )
    with open(local_output_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
    return "Success"