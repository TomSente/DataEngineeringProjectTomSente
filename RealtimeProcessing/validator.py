def validate_data(df):
    print("Validate print columns:",df.columns)
    """Validates each column based on custom rules."""
    # Example rules, adjust as needed for your dataset
    rules = {
        'name': lambda x: isinstance(x, str) and len(x) > 0,
        'manufacturer': lambda x: isinstance(x, str) and len(x) > 0,
        'size': lambda x: x in ['Small', 'Medium', 'Large', 'Capital'],
        'length': lambda x: x > 0,
        'mass': lambda x: x >= 0,
        'cargo_capacity': lambda x: x >= 0,
        'crew_min': lambda x: x >= 0,
        'crew_max': lambda x: x >= 0,
        'scm_speed': lambda x: x >= 0,
        'afterburner_speed': lambda x: x >= 0,
    }
    for col, rule in rules.items():
        if col in df.columns:
            df = df[df[col].apply(rule)]
    return df

def backup_validate(df):
    """Back-up validation: fill NA and replace inf values."""
    df = df.fillna(0)
    df.replace([float('inf'), float('-inf')], 0, inplace=True)
    return df
