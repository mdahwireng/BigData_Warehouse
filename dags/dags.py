from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from create_schema import create_schema
from stations import fill_stations
from weekday import fill_weekdays
from traffic import populate_traff

DAG_CONFIG = {
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email': ['kaaymyke@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '0 0/1 0 ? * * *',
}

with DAG("my_wh_dag", # Dag id
    default_args=DAG_CONFIG,
    catchup=False,
    schedule_interval='@once'
) as dag:
    # Tasks are implemented under the dag object
    create_database = PythonOperator(
        task_id="Create database",
        python_callable= create_schema
    )
    fill_stations_tbl = PythonOperator(
    task_id="populate station table",
    python_callable= fill_stations
    )
    fill_weekdays_tbl= PythonOperator(
    task_id="populate weekdays table",
    python_callable= fill_weekdays
    )
    fill_traffic_tbl= PythonOperator(
    task_id="populate traffic table",
    python_callable= populate_traff
    )
    create_database >> [fill_stations_tbl, fill_weekdays_tbl] >> fill_traffic_tbl