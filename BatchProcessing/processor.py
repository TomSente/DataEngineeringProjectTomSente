import numpy as np
import pandas as pd

def process_taxi_data(df):
    """Processes taxi data: drops columns, calculates features, categorizes, and bins."""
    df = df.drop(columns=['VendorID', 'store_and_fwd_flag', 'RatecodeID'], errors='ignore')
    df['trip_duration_minutes'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
    df['average_speed_mph'] = np.where(
        df['trip_duration_minutes'] > 0, 
        df['trip_distance'] / (df['trip_duration_minutes'] / 60), 
        0
    )
    df['pickup_year'] = df['tpep_pickup_datetime'].dt.year
    df['pickup_month'] = df['tpep_pickup_datetime'].dt.month
    df['revenue_per_mile'] = np.where(
        df['trip_distance'] > 0, 
        df['total_amount'] / df['trip_distance'], 
        0
    )
    df['trip_distance_category'] = pd.cut(
        df['trip_distance'], bins=[-1, 2, 10, np.inf], labels=['Short', 'Medium', 'Long']
    )
    df['fare_category'] = pd.cut(
        df['fare_amount'], bins=[-1, 20, 50, np.inf], labels=['Low', 'Medium', 'High']
    )
    df['trip_time_of_day'] = df['tpep_pickup_datetime'].dt.hour.apply(get_time_of_day)
    return df

def get_time_of_day(hour):
    if 5 <= hour < 12:
        return 'Morning'
    elif 12 <= hour < 17:
        return 'Afternoon'
    elif 17 <= hour < 21:
        return 'Evening'
    else:
        return 'Night'
