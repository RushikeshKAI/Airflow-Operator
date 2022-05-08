from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator


# def _process(path, filename, **context):
#     print(f"{path}/{filename} - {context['ds']}")


with DAG('my_bash_dag', start_date=datetime(2022, 5, 8), schedule_interval='@daily', catchup=False) as dag:

    execute_command = BashOperator(
        task_id='execute_command',
        bash_command="scripts/commands.sh",
        skip_exit_code=10,
        env = {
            "my_api": "api_key_aws"
        }
    )


