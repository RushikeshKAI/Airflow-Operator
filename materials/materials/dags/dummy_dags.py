from airflow.operators.dummy import DummyOperator
from airflow import DAG


default_args={
    'start_date': datetime(2022, 5, 9)
}

with DAG("dummy_dag", default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    task_a = DummyOperator(task_id='task_a')
    task_b = DummyOperator(task_id='task_b')
    task_c = DummyOperator(task_id='task_c')
    task_d = DummyOperator(task_id='task_d')

    dummy = DummyOperator(
        task_id='dummy'
    )

    task_a >> dummy >> task_b
    task_c >> task_d

