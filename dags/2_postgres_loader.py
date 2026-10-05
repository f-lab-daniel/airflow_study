from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.sdk import DAG

default_args = {
    "owner": "airflow",
}

# Define the DAG
with DAG(
    dag_id="postgres_loader",
    description="PostgreSQL Loader Example",
    default_args=default_args,
    schedule="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
):
    # Task: Execute a SQL query on the connection's database
    postgres_task = SQLExecuteQueryOperator(
        task_id="execute_sql_query",
        conn_id="my_postgres_connection",
        sql="""
            INSERT INTO sample_table (key, value)
            VALUES ('hello', 'world')
        """,
    )

    # Sensor: wait until a row with key='hello1' exists
    sql_sensor = SqlSensor(
        task_id="wait_for_condition",
        conn_id="my_postgres_connection",
        sql="SELECT COUNT(*) FROM sample_table WHERE key='hello1'",
        mode="reschedule",
        poke_interval=5,
    )

    postgres_confirm_task = SQLExecuteQueryOperator(
        task_id="execute_sql_confirm_query",
        conn_id="my_postgres_connection",
        sql="""
            INSERT INTO sample_table (key, value)
            VALUES ('sensor', 'confirmed')
        """,
    )

    postgres_task >> sql_sensor >> postgres_confirm_task
