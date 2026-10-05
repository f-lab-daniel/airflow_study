from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def print_logical_date(logical_date, ds, **kwargs):
    print(f"kwargs: {kwargs}")
    print(f"ds: {ds}")
    print(f"logical_date: {logical_date}")


def sum_numbers1(*args):
    total = 0
    for val in args:
        total += val
    return total


def sum_numbers2(**kwargs):
    dag_run = kwargs["dag_run"]
    print(dag_run.conf)
    args = dag_run.conf.get("numbers", [])
    total = 0
    for val in args:
        total += val
    return total


with DAG(
    dag_id="templating",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    default_args={"retries": 1},
    catchup=False,
    render_template_as_native_obj=True,  # Render templates using Jinja NativeEnvironment
):
    print_logical_date_task = PythonOperator(
        task_id="print_logical_date_task",
        python_callable=print_logical_date,
    )

    print_days = BashOperator(
        task_id="print_days",
        bash_command="echo Days since {{ ds_nodash }}",
    )

    # trigger with conf: {"numbers": [1, 2, 3]}
    sum_numbers_task = PythonOperator(
        task_id="sum_numbers1",
        python_callable=sum_numbers1,
        op_args="{{ dag_run.conf.get('numbers', []) }}",
    )

    sum_numbers_task2 = PythonOperator(
        task_id="sum_numbers2",
        python_callable=sum_numbers2,
    )
