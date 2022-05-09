from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


class CustomPostgresOpearator(PostgresOperator):
    template_fields = ('sql', 'filename')

def _function_():
    return "my_file"

with DAG('my_postgres_dag', start_date=datetime(2022, 5, 8), schedule_interval='@daily', catchup=False) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='sql/CREATE_TABLE_MY_TABLE.sql'
    )

    my_task = PythonOperator(
        task_id='my_task',
        python_callable=_function_
    )

    store = PostgresOperator(
        task_id='store',
        postgres_conn_id='postgres',
        sql=[
            'sql/INSERT_INTO_MY_TABLE.sql',
            'SELECT * FROM my_table'
        ],

        parameters={
            'filename': '{{ ti.xcom_pull(task_ids=["my_task"])[0] }}'
        }
    )


