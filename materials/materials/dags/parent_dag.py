from airflow import DAG
from datetime import datetime, time
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from subdag_dag import subdag_factory
from task_group import training_groups
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args={
    'start_date': datetime(2022, 5, 9)
}


with DAG('parent_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "start"'
    )

    group_training_tasks = training_groups()

    # process_ml = TriggerDagRunOperator(
    #     task_id='process_ml',
    #     trigger_dag_id='{{ var.value.my_dag_id }}',
    #     conf={"path": '/opt/airflow/ml'},
    #     execution_date="{{ ds }}",
    #     reset_dag_run=True,
    #     wait_for_completion=True,
    #     poke_interval=60
    # )

    end = DummyOperator(
        task_id='end',
        bash_command='echo "end"'
    )

    start >> group_training_tasks >> end
