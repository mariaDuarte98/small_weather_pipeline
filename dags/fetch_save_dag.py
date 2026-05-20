from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.fetch_data import fetch_historical_weather, save_data

start_date = datetime(2025, 8, 2)

default_args = {
    'owner': 'weather_voyager',
    'depends_on_past': False,
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def fetch_and_save(start_date_str: str, end_date_str: str) -> None:
    df = fetch_historical_weather(start_date_str, end_date_str)
    save_data(df)


with DAG(
        'fetch_weather_data',
        default_args=default_args,
        schedule='@daily',
        catchup=False,
) as dag:

    PythonOperator(
        task_id='fetch_and_save_weather',
        python_callable=fetch_and_save,
        op_args=["{{ prev_ds }}", "{{ ds }}"],
    )
