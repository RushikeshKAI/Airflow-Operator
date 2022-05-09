from airflow import DAG
from datetime import datetime, time
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.weekday import WeekDay


default_args={
    'start_date': datetime(2022, 5, 9)
}


with DAG('branch_week_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    task_a = DummyOperator(
        task_id='task_a'
    )

    task_b = DummyOperator(
        task_id='task_b'
    )

    is_wednesday = BranchDayOfWeekOperator(
        task_id='wednesday',
        follow_task_ids_if_true=['task_c'],
        follow_task_ids_if_false=['end'],
        weekday=WeekDay.WEDNESDAY,
        use_task_execution_day=True
    )

    end = DummyOperator(
        task_id='end'
    )

    task_c = DummyOperator(
        task_id='task_c'
    )

    task_a >> task_b >> is_wednesday >> [task_c, end]
