import pandas as pd



MANDATORY_COLUMNS = [
    'id', 'chassis_id', 'name',
    'slug', 'sizes', 'dimension',
    'mass', 'cargo_capacity', 'crew','speed','agility','foci',
    'production_status','type','description','size','size','pledge_url','skus','manufacturer','loaner','link','updated_at'

]

DATETIME_COLUMNS = ['updated_at']



def validate_ship_data(df):
    """Validates ship data for mandatory columns and logical checks."""
    if df is None:
        raise ValueError('Input DataFrame is None.')

    missing_columns = [column for column in MANDATORY_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(f'Missing mandatory columns: {missing_columns}')

    df = df.copy()
    # Parsing valid date string to timestamps, invalid or empty becomes missing values.
    for column in DATETIME_COLUMNS:
        df[column] = pd.to_datetime(df[column], errors='coerce')


    initial_count = len(df)
    df = df.dropna(subset=MANDATORY_COLUMNS)
    df = df[
        (df['mass'] >= 0)
        & (df['cargo_capacity'] >= 0)
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
