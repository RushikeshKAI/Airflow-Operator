from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator


def _check_accuracy():
    accuracy = 1
    if accuracy >= 1:
        return ['accurate', 'top_accurate']
    return 'inaccurate'

with DAG('ml_dag', start_date=datetime(2022, 5, 9), schedule_interval='@daily', catchup=False) as dag:

    training_ml = DummyOperator(
        task_id='training_ml'
    )

    check_accuracy = BranchPythonOperator(
        task_id='check_accuracy',
        python_callable=_check_accuracy
    )

    accurate = DummyOperator(
        task_id='accurate'
    )

    top_accurate = DummyOperator(
        task_id='top_accurate'
    )

    inaccurate = DummyOperator(
        task_id='inaccurate'
    )
    
    publish_ml = DummyOperator(task_id='publish_ml', trigger_rule='none_failed_or_skipped')
    
    training_ml >> check_accuracy >> [accurate, inaccurate, top_accurate] >> publish_ml
