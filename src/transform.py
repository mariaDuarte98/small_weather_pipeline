import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import OUTPUT_PARQUET, OUTPUT_TRANSFORMED

logger = logging.getLogger(__name__)


def load_raw() -> pd.DataFrame:
    """Read the full raw Parquet dataset for transform over the complete history."""
    try:
        return pq.read_table(OUTPUT_PARQUET).to_pandas()
    except Exception as e:
        logger.warning(f"Could not load raw data: {e}")
        return pd.DataFrame()


def compute_daily_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Temperature anomaly = daily average temp minus that city's mean over the dataset."""
    city_means = df.groupby("city")["temp"].transform("mean")
    df = df.copy()
    df["temp_anomaly"] = df["temp"] - city_means
    return df


def compute_rolling_avg(df: pd.DataFrame, window: int = 7) -> pd.DataFrame:
    """7-day rolling average of daily average temp per city, ordered by date."""
    df = df.copy().sort_values(["city", "date"])
    df["temp_rolling_7d"] = (
        df.groupby("city")["temp"]
        .transform(lambda s: s.rolling(window, min_periods=1).mean())
    )
    return df


def transform_weather(df: pd.DataFrame) -> pd.DataFrame:
    """Run both transform steps; return empty DataFrame if input is None or empty."""
    if df is None or df.empty:
        logger.warning("No data to transform.")
        return pd.DataFrame()
    df = compute_daily_anomalies(df)
    df = compute_rolling_avg(df)
    return df


def save_transformed(df: pd.DataFrame) -> None:
    """Write transformed DataFrame to partitioned Parquet; no-op on empty input."""
    if df is None or df.empty:
        logger.warning("No transformed data to save.")
        return
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=OUTPUT_TRANSFORMED,
        partition_cols=["city", "year", "month"],
        existing_data_behavior="overwrite_or_ignore",
    )
    logger.info(f"Transformed data ({len(df)} rows) saved to {OUTPUT_TRANSFORMED}.")
