import pandas as pd

def process_data(df):
    """Creates extra columns and removes duplicates."""
    # Example extra columns
    print("DataFrame columns:", list(df.columns))
    print("First row of DataFrame:\n", df.head(1))
    df['crew_min'] = df['crew'].apply(lambda x: x.get('min') if isinstance(x, dict) else None)
    df['crew_max'] = df['crew'].apply(lambda x: x.get('max') if isinstance(x, dict) else None)
    df['crew_range'] = df['crew_max'] - df['crew_min']
    df['is_cargo'] = df['cargo_capacity'] > 0
    df['speed_ratio'] = df['afterburner_speed'] / df['scm_speed'].replace(0, 1)
    df = df.drop_duplicates()
    return df
