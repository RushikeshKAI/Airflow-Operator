from airflow import DAG
from datetime import datetime, time, timedelta
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator


default_args={
    'start_date': datetime(2022, 5, 9)
}

def _is_monday(execution_date):
    return execution_date.weekday == 0

with DAG('short_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    task_a = DummyOperator(
        task_id='task_a'
    )


    task_b = DummyOperator(
        task_id='task_b'
    )

    task_c = DummyOperator(
        task_id='task_c'
    )

    task_d = DummyOperator(
        task_id='task_d'
    )

    is_monday = ShortCircuitOperator(
        task_id='is_monday',
        python_callable=_is_monday
    )

    task_a >> task_b >> task_c >> is_monday >> task_d
