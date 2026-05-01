import pandas as pd


MANDATORY_COLUMNS = [
    'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
    'trip_distance', 'PULocationID', 'DOLocationID',
    'payment_type', 'fare_amount', 'total_amount'
]

DATETIME_COLUMNS = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

def validate_taxi_data(df):
    """Validates taxi data for mandatory columns and logical checks."""
    if df is None:
        raise ValueError('Input DataFrame is None.')

    missing_columns = [column for column in MANDATORY_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(f'Missing mandatory columns: {missing_columns}')

    df = df.copy()
    for column in DATETIME_COLUMNS:
        df[column] = pd.to_datetime(df[column], errors='coerce')

    initial_count = len(df)
    df = df.dropna(subset=MANDATORY_COLUMNS)
    df = df[
        (df['tpep_dropoff_datetime'] >= df['tpep_pickup_datetime'])
        & (df['trip_distance'] >= 0)
        & (df['fare_amount'] >= 0)
        & (df['total_amount'] >= 0)
        & (df['passenger_count'] >= 0)
    ]

    if df.empty:
        raise ValueError('Validation removed all rows. Check the upstream input data.')

    print(f"Validation Complete: Rows removed: {initial_count - len(df)}")
    return df

def backup_validate(df):
    """Sanity-check that no infinite values remain in numeric columns."""
    df = df.copy()
    numeric_df = df.select_dtypes(include='number')
    if not numeric_df.empty and numeric_df.isin([float('inf'), float('-inf')]).any().any():
        raise ValueError('Infinite values detected after processing.')

    print("Backup Validation Complete")
    return df
