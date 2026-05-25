from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.fetch_data import fetch_historical_weather, save_data
from src.transform import load_raw, transform_weather, save_transformed

start_date = datetime(2025, 8, 2)

default_args = {
    'owner': 'weather_voyager',
    'depends_on_past': False,
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def fetch_and_save(start_date_str: str, end_date_str: str) -> None:
    """Fetch historical weather for all cities and save raw Parquet output."""
    df = fetch_historical_weather(start_date_str, end_date_str)
    save_data(df)


def run_transform() -> None:
    """Load the full raw dataset, transform it, and save the result."""
    df = load_raw()
    transformed = transform_weather(df)
    save_transformed(transformed)


with DAG(
        'fetch_weather_data',
        default_args=default_args,
        schedule='@daily',
        catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_save_weather',
        python_callable=fetch_and_save,
        op_args=["{{ prev_ds }}", "{{ ds }}"],
    )

    transform_task = PythonOperator(
        task_id='transform_weather',
        python_callable=run_transform,
    )

    fetch_task >> transform_task
