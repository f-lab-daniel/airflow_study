from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, TriggerRule

with DAG(
    dag_id="trigger_rule_example",
    start_date=datetime(2026, 1, 1),
    schedule=None,
):
    task1 = EmptyOperator(task_id="task1")
    task2 = EmptyOperator(task_id="task2")

    task3 = EmptyOperator(
        task_id="task3",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    [task1, task2] >> task3
