from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.sql import BranchSQLOperator


default_args={
    'start_date': datetime(2022, 5, 9)
}


with DAG('branch_sql_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        sql='sql/CREATE_TABLE_PARTNERS.sql',
        postgres_conn_id='postgres'
    )

    insert_data = PostgresOperator(
        task_id='insert_data',
        sql='sql/INSERT_INTO_PARTNERS.sql',
        postgres_conn_id='postgres'
    )

    choose_task = BranchSQLOperator(
        task_id='choose_task',
        sql='SELECT COUNT(1) FROM partners WHERE partner_status=TRUE',
        follow_task_ids_if_true=['process'],
        follow_task_ids_if_false=['notif_email', 'notif_slack'],
        conn_id='postgres'
    )

    process = DummyOperator(
        task_id= 'process'
    )

    notif_email = DummyOperator(
        task_id='notif_email'
    )

    notif_slack = DummyOperator(
        task_id='notif_slack'
    )

    create_table >> insert_data >> choose_task >> [process, notif_email, notif_slack]
