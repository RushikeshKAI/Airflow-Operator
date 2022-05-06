from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


default_args = {
    "email":["rushi-vm94@yopmail.com"],
    "email_on_retry":True,
    "email_on_failure":False,
}

def _my_func(execution_date):
    if execution_date.day == 9:
        raise ValueError("Error")

with DAG('my_dag_v_1_0_0', default_args=default_args, start_date=datetime(2022, 5, 6), schedule_interval='@daily', dagrun_timeout=40, catchup=True) as dag:

    task_a = BashOperator(
        owner='a',
        task_id='task_a',
        bash_command='echo "task_a" && sleep 10'
    )

    task_b = BashOperator(
        owner='b',
        task_id='task_b',
        retries=3,
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=10),
        bash_command='echo "{{ti.try_number}} && exit 0"'
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=_my_func,
        depends_on_past=True
    )

    task_a >> task_b >> task_c
