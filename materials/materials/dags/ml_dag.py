from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator
import yaml


default_args={
    'start_date': datetime(2022, 5, 9)
}

def _check_holidays(ds):
    with open('dags/files/days_off.yml', 'r') as f:
        days_off = set(yaml.load(f, Loader=yaml.FullLoader))
        if ds not in days_off:
            return 'process'
    return 'stop'

    # accuracy = 1
    # if accuracy >= 1:
    #     return ['accurate', 'top_accurate']
    # return 'inaccurate'


with DAG('ml_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    # training_ml = DummyOperator(
    #     task_id='training_ml'
    # )

    check_holidays = BranchPythonOperator(
        task_id='check_holidays',
        python_callable=_check_holidays
    )

    process = DummyOperator(task_id='process')
    cleaning_stock = DummyOperator(task_id='cleaning_task')

    stop = DummyOperator(task_id='stop')


    # accurate = DummyOperator(
    #     task_id='accurate'
    # )

    # top_accurate = DummyOperator(
    #     task_id='top_accurate'
    # )

    # inaccurate = DummyOperator(
    #     task_id='inaccurate'
    # )
    
    # publish_ml = DummyOperator(task_id='publish_ml', trigger_rule='none_failed_or_skipped')
    
    # training_ml >> check_accuracy >> [accurate, inaccurate, top_accurate] >> publish_ml

    check_holidays >>  [process, stop]
    process >> cleaning_stock