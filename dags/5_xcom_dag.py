import random
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def generate_number(**context):
    number = random.randint(0, 100)
    print(f"Generated number: {number}")
    # push the integer
    context["ti"].xcom_push(key="random_number", value=number)
    # # push the json
    # context["ti"].xcom_push(key="random_number", value={"v": number})


def read_number(ti):
    number = ti.xcom_pull(key="random_number", task_ids="generate_number_task")
    print(f"Received number: {number}")


with DAG(
    dag_id="xcom_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    default_args={"retries": 1},
    catchup=False,
):
    # PythonOperator passes context values (ti, ds, ...) that match the callable's arguments
    generate_number_task = PythonOperator(
        task_id="generate_number_task",
        python_callable=generate_number,
    )

    read_number_task = PythonOperator(
        task_id="read_number_task",
        python_callable=read_number,
    )

    generate_number_task >> read_number_task
