from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def print_logical_date(logical_date, ds, data_interval_start, data_interval_end, macros, **kwargs):
    print(f"kwargs: {kwargs}")
    print(f"ds: {ds}")
    print(f"logical_date: {logical_date}")
    print(f"data_interval: {data_interval_start} ~ {data_interval_end}")

    # logical_date 기준 2일 전 (처리 대상 날짜를 상대적으로 계산)
    print(f"logical_date - 2 days: {logical_date - timedelta(days=2)}")
    print(f"ds - 2 days: {macros.ds_add(ds, -2)}")


with DAG(
    dag_id="print_logical_date",
    # start_date: 스케줄이 시작되는 기준 시점 (처리 대상 날짜와는 무관)
    start_date=datetime(2026, 1, 1),
    schedule="*/2 * * * *",
    default_args={"retries": 1},
    catchup=False,
):
    print_logical_date_task = PythonOperator(
        task_id="print_logical_date_task",
        python_callable=print_logical_date,
    )

    # 템플릿에서 logical_date 기준 2일 전 날짜 사용
    print_two_days_ago = BashOperator(
        task_id="print_two_days_ago",
        bash_command="echo ds={{ ds }} two_days_ago={{ macros.ds_add(ds, -2) }}",
    )

    print_logical_date_task >> print_two_days_ago
