import pandas as pd


def normalize_visual_crossing_data(df: pd.DataFrame, city_name: str) -> pd.DataFrame:
    """
    Normalizes the DataFrame data from Visual Crossing to a consistent format.
    Adds the city column, and date related columns.
    """
    df['date'] = pd.to_datetime(df['datetime'])
    df['city'] = city_name
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year

    return df

