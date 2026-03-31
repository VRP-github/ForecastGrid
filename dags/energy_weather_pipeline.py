from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta

from src.ingestion.eia_api import fetch_eia_demand
from src.ingestion.weather_api import fetch_weather_data
from src.processing.merger import merge_and_validate_data
from src.features.feature_engineering import build_energy_features

default_args = {
    'owner': 'vp',
    'depends_on_past': True,
    'start_date': datetime(2023, 10, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='daily_energy_demand_pipline',
    default_args=default_args,
    description='Fetches live grid demand and weather, merges, and engineers features',
    schedule_interval='@daily',
    catchup=False,
    tags=['energy', 'live_api', 'feature_engineering'],
) as dag:

    start_pipeline = EmptyOperator(task_id='start')

    fetch_energy_task = PythonOperator(
        task_id='fetch_eia_demand',
        python_callable=fetch_eia_demand,
        op_kwargs={'grid_region': 'TEXAS'}
    )

    fetch_weather_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
        op_kwargs={'lat': 31.96, 'lon':-99.90}
    )

    merge_validate_task = PythonOperator(
        task_id='merge_and_validate',
        python_callable=merge_and_validate_data
    )

    feature_engineering_task = PythonOperator(
        task_id='engineer_features',
        python_callable=build_energy_features
    )

    end_pipeline = EmptyOperator(task_id='end')

    start_pipeline >> [fetch_energy_task, fetch_weather_task]
    [fetch_energy_task, fetch_weather_task] >> merge_validate_task
    merge_validate_task >> feature_engineering_task >> end_pipeline
