import pandas as pd
import ast

def parse(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return {}
    return {}

def process_ship_data(df):
    """Creates extra columns and removes duplicates."""
    print("DataFrame columns:", list(df.columns))
    df['crew_min'] = df['crew'].apply(lambda x: parse(x).get("min"))
    df['crew_max'] = df['crew'].apply(lambda x: parse(x).get("max"))
    df['crew_range'] = df['crew_max'] - df['crew_min']
    df['can_carry_cargo'] = df['cargo_capacity'] > 0

    df["production_status"] = df["production_status"].apply(lambda x: parse(x).get("en_EN"))

    df["production_note"] = df["production_note"].apply(lambda x: parse(x).get("en_EN"))

    df["type"] = df["type"].apply(lambda x: parse(x).get("en_EN"))

    df["description"] = df["description"].apply(lambda x: parse(x).get("en_EN"))

    df["size"] = df["size"].apply(lambda x: parse(x).get("en_EN"))

    df["manufacturer_code"] = df["manufacturer"].apply(lambda x: parse(x).get("code"))

    df["manufacturer_name"] = df["manufacturer"].apply(lambda x: parse(x).get("name"))

    df["foci"] = df["foci"].apply(lambda x: parse(x)[0].get("en_EN"))

    df["loaner"] = df["loaner"].apply(lambda x: parse(x)[0].get("name") if len(parse(x))>0 else None)

    df["speed"] = df["speed"].apply(lambda x: parse(x).get("scm"))

    df.rename(columns={"msrp":"manufacturer_suggested_retail_price","speed":"space_combat_maneuvering_speed"},inplace=True)

    df = df.drop(columns = ["manufacturer","agility"])

    print("DataFrame columns:", list(df.columns))
    return df
