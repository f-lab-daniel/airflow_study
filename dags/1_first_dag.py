from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def print_hello():
    print("Hello Airflow!")


# Define the DAG
with DAG(
    dag_id="sample_dag",
    description="A simple DAG",
    schedule="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
):
    # Task 1: Print "Hello Airflow!"
    task1 = PythonOperator(task_id="print_hello_task", python_callable=print_hello)

    # Task 2: Empty task
    task2 = EmptyOperator(task_id="empty_task")

    # Define the task dependencies
    task1 >> task2
