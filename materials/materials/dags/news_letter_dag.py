from airflow import DAG
from datetime import datetime, time, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.latest_only import LatestOnlyOperator


default_args={
    'start_date': days_ago(2)
}

with DAG('news_letter_dag', default_args=default_args, schedule_interval='@daily', catchup=True) as dag:

    getting_data = DummyOperator(
        task_id='getting_data'
    )

    creating_email = DummyOperator(
        task_id='creating_email'
    )

    is_latest = LatestOnlyOperator(
        task_id='is_latest'
    )

    sending_newsletter = DummyOperator(
        task_id='sending_newsletter'
    )

    getting_data >> creating_email >> is_latest >> sending_newsletter
