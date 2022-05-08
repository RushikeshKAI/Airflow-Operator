from airflow import DAG, variable
from datetime import datetime
from airflow.operators.python import PythonOperator


def _process(path, filename, **context):
    print(f"{path}/{filename} - {context['ds']}")


with DAG('my_python_dag', start_date=datetime(2022, 5, 8), schedule_interval='@daily', catchup=False) as dag:

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=_process,

        # Accessing dynamic var values
        op_kwargs=variable.get("my_settings", deserialize_json=True)

        # Accessing variables values through Jinja
        # op_kwargs={
        #     "path": '{{var.value.path}}', 
        #     "filename": '{{var.value.filename}}'
        #     }
    )


