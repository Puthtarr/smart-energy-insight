from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'piyaphat',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='smart_energy_ingestion',
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    schedule_interval='0 6 * * *',  # รันทุกวัน 06:00
    catchup=False
) as dag:

    ingest_fuel = BashOperator(
        task_id='ingest_fuel',
        bash_command='python3 ingestion/fuel/fuel_pipeline_runner.py'
    )

    ingest_water = BashOperator(
        task_id='ingest_water',
        bash_command='python3 ingestion/water/water_pipeline_runner.py'
    )

    ingest_electric = BashOperator(
        task_id='ingest_electric',
        bash_command='python3 ingestion/electric/elect_pipeline_runner.py'
    )

    ingest_fuel >> ingest_water >> ingest_electric
