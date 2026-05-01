import numpy as np
import pandas as pd

def process_taxi_data(df):
    # Removal of VendorID, store_and_fwd_flag and RatecodeID columns
    df = df.drop(columns=['VendorID', 'store_and_fwd_flag', 'RatecodeID'], errors='ignore')
    print("Processor: Successfull Removal of Columns: VendorID, store_and_fwd_flag, RatecodeID")

    # Creation of trip_duration_minutes column
    df['trip_duration_minutes'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60

    # Creation of average_speed column
    df['average_speed_mph'] = np.where(
        df['trip_duration_minutes'] > 0, 
        df['trip_distance'] / (df['trip_duration_minutes'] / 60), 
        0
    )

    # Creation of pickup_year column
    df['pickup_year'] = df['tpep_pickup_datetime'].dt.year

    # Creation of pickup_month column
    df['pickup_month'] = df['tpep_pickup_datetime'].dt.month

    # Creation of revenue_per_mile column
    df['revenue_per_mile'] = np.where(
        df['trip_distance'] > 0, 
        df['total_amount'] / df['trip_distance'], 
        0
    )

    # Creation of trip_distance_category column
    df['trip_distance_category'] = pd.cut(
        df['trip_distance'], bins=[-1, 2, 10, np.inf], labels=['Short', 'Medium', 'Long']
    )

    # Creation of fare_category column
    df['fare_category'] = pd.cut(
        df['fare_amount'], bins=[-1, 20, 50, np.inf], labels=['Low', 'Medium', 'High']
    )

    # Creation of trip_time_of_day column
    df['trip_time_of_day'] = df['tpep_pickup_datetime'].dt.hour.apply(get_time_of_day)

    print("Processing Complete")
    return df


# Helper function to determine time of day
def get_time_of_day(hour):
    if 5 <= hour < 12:
        return 'Morning'
    elif 12 <= hour < 17:
        return 'Afternoon'
    elif 17 <= hour < 21:
        return 'Evening'
    else:
        return 'Night'
