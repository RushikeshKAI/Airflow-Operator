from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import cross_downstream


default_args = {
    "email":["rushi-vm94@yopmail.com"],
    "email_on_retry":True,
    "email_on_failure":False,
}

def _my_func(execution_date):
    if execution_date.day == 9:
        raise ValueError("Error")


with DAG('my_dag_v_1_0_0', default_args=default_args, start_date=datetime(2022, 5, 6), schedule_interval='@daily', catchup=False) as dag:

    extract_a = BashOperator(
        owner='extract_a',
        task_id='extract_a',
        bash_command='echo "task_a" && sleep 10',
        wait_for_downstream=True
    )

    extract_b = BashOperator(
        owner='extract_b',
        task_id='extract_b',
        bash_command='echo "task_a" && sleep 10',
        wait_for_downstream=True
    )

    process_a = BashOperator(
        owner='process_a',
        task_id='process_a',
        retries=3,
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=10),
        bash_command='echo "{{ti.try_number}} && sleep 20"'
    )

    process_b = BashOperator(
        owner='process_b',
        task_id='process_b',
        retries=3,
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=10),
        bash_command='echo "{{ti.try_number}} && sleep 20"'
    )

    process_c = BashOperator(
        owner='process_c',
        task_id='process_c',
        retries=3,
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=10),
        bash_command='echo "{{ti.try_number}} && sleep 20"'
    )

    store = PythonOperator(
        task_id='store',
        python_callable=_my_func,
        depends_on_past=True
    )

    cross_downstream([extract_a, extract_b], [process_a, process_b, process_c]) >> store
    # [process_a, process_b, process_c] >> store
