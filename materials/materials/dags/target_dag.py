from airflow import DAG
from datetime import datetime, time
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.operators.datetinchme import BranchDateTimeOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor


default_args={
    'start_date': datetime(2022, 5, 9)
}


with DAG('target_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    waiting_end_parent_dag = ExternalTaskSensor(
        task_id='waiting_end_parent_dag',
        external_dag_id='parent_dag',
        external_task_id='end'
    )

    process_ml = BashOperator(
        task_id='process_ml',
        bash_command="echo 'processing_ml'"


    )

    waiting_end_parent_dag >> process_ml 


    # is_in_time_frame = BranchDateTimeOperator(
    #     task_id='is_in_time_frame',
    #     sql='sql/CREATE_TABLE_PARTNERS.sql',
    #     postgres_conn_id='postgres',
    #     follow_task_ids_if_true=['move_forward'],
    #     follow_task_ids_if_false=['end'],
    #     target_upper=time(1, 0, 0),
    #     target_lower=time(2, 0, 0),
    # )

    # target_upper = DummyOperator(
    #     task_id='target_upper'
    # )

    # target_lower = DummyOperator(
    #     task_id='target_lower'
    # )

    # is_in_time_frame >> [target_upper, target_lower]
