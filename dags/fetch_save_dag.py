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

with DAG(
        'fetch_weather_data',
        default_args=default_args,
        schedule='@daily',
        catchup=False,
) as dag:

    task_fetch_historical = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_historical_weather,
        op_args=["{{ prev_ds }}", "{{ ds }}"])

    task_train = PythonOperator(
        task_id='save_data',
        python_callable=save_data,
        op_args=[task_fetch_historical.output]
    )


    task_fetch_historical >> task_train
