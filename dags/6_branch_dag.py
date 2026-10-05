import time
from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.sdk import DAG


def decide_which_path():
    if int(time.time()) % 2 == 0:
        return "even_path_task"
    else:
        return "odd_path_task"


with DAG(
    dag_id="branch_operator_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    default_args={"retries": 1},
    catchup=False,
):
    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=decide_which_path,
    )

    even_path_task = EmptyOperator(task_id="even_path_task")
    odd_path_task = EmptyOperator(task_id="odd_path_task")

    branch_task >> [even_path_task, odd_path_task]
