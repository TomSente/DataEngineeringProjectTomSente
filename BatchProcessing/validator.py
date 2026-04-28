import numpy as np

def validate_taxi_data(df):
    """Validates taxi data for mandatory columns and logical checks."""
    mandatory_cols = [
        'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
        'trip_distance', 'PULocationID', 'DOLocationID', 
        'payment_type', 'fare_amount', 'total_amount'
    ]
    initial_count = len(df)
    df = df.dropna(subset=mandatory_cols)
    df = df[(df['trip_distance'] >= 0) & (df['total_amount'] >= 0)]
    print(f"Validation complete. Rows removed: {initial_count - len(df)}")
    return df

def backup_validate(df):
    """Back-up validation to replace inf values with 0."""
    df.replace([np.inf, -np.inf], 0, inplace=True)
    return df
