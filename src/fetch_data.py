import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from src.config import OUTPUT_PARQUET, CITIES, API_KEY
from src.utils.weather_utils import normalize_visual_crossing_data
from datetime import timedelta, datetime
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch_weather(city_name: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Fetches historical weather data for a location using the Visual Crossing API.
    The API is called based on the city name.
    """

    if not start_date or not end_date:
        logger.error(
            f"start date time or end time is missing: start -> {start_date} end -> {end_date}"
        )
        return

    logger.info(
        f"Fetching historical data for {city_name} from {start_date.date()} to {end_date.date()}..."
    )

    url = (
        f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        f"{city_name}/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?unitGroup=metric&key={API_KEY}&contentType=json" # TO DO: &include=hours
    )

    try:
        response = requests.get(url)
        response.raise_for_status()
        historical_data = response.json()

        daily_data = historical_data.get('days', [])
        if not daily_data:
            logger.warning(f"No daily data found for {city_name}.")
            return

        df = pd.DataFrame(daily_data)
        df = normalize_visual_crossing_data(df, city_name)
        df_normalized = df.drop_duplicates(subset=["datetime"])

        return df_normalized

    except requests.exceptions.RequestException as e:
        logger.error(
            f"Error fetching data from Visual Crossing for {city_name}: {e}")
        return pd.DataFrame()
    
def fetch_historical_weather(start_date_str: str, end_date_str: str) -> pd.DataFrame:
    """
    Fetches historical data for all defined cities.
    """
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    df_list = []
    for city in CITIES:
        city_df = fetch_weather(city['name'], start_date, end_date)
        if not city_df.empty:
            df_list.append(city_df)

    if not df_list:
        logger.warning("No data fetched. Returning an empty DataFrame.")
        return pd.DataFrame()
    
    return pd.concat(df_list, ignore_index=True)

def save_data(df: pd.DataFrame):
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(table,
                            root_path=OUTPUT_PARQUET,
                            partition_cols=['city', 'year', 'month'],
                            existing_data_behavior='overwrite_or_ignore')
    logger.info(f"Data for {len(df)} saved successfully.")

if __name__ == '__main__':
    df = fetch_historical_weather("2024-09-01", "2024-10-01")
    save_data(df)

    


