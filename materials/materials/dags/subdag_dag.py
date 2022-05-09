from airflow import DAG
from datetime import datetime, time
from airflow.operators.dummy import DummyOperator   
from airflow.operators.bash import BashOperator


def subdag_factory(parent_dag_id, subdag_dag_id, default_args)
    with DAG(f'{parent_dag_id}.{subdag_dag_id}', default_args=default_args) as dag:
    
        training_a = BashOperator(task_id='training_a', bash_command="echo {{ dag_run.conf['output] }}")
        training_b = BashOperator(task_id='training_b', bash_command="echo 'training_b")
        training_c = BashOperator(task_id='training_c', bash_command="echo 'training_c")

    return dag
