from datetime import datetime

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sdk import DAG

# Define the DAG
with DAG(
    dag_id="spark_submit_sample",
    description="A simple DAG",
    schedule="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
):
    submit = SparkKubernetesOperator(
        task_id="spark_pi_submit",
        namespace="spark-operator",
        application_file="sample.yaml",
        kubernetes_conn_id="eks-flab",
        do_xcom_push=True,
    )
