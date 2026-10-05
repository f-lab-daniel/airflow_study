from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Label, task_group

with DAG(
    dag_id="task_group",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    default_args={"retries": 1},
    catchup=False,
):
    @task_group(default_args={"retries": 3})
    def group1():
        """This docstring will become the tooltip for the TaskGroup."""
        task1 = EmptyOperator(task_id="task1")
        task2 = BashOperator(task_id="task2",
                             bash_command="echo Hello World!",
                             retries=2)
        print(task1.retries)  # 3
        print(task2.retries)  # 2

    task3 = EmptyOperator(task_id="task3")

    group1() >> Label("When completed") >> task3
