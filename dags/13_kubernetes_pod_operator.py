from datetime import datetime

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

# Kubernetes에 배포된 Airflow에서만 동작합니다. (k8s/README.md 참고)
# 태스크마다 지정한 이미지로 Pod를 새로 띄워 실행하고, 끝나면 Pod를 지웁니다.


def print_result(ti):
    result = ti.xcom_pull(task_ids="produce_xcom_pod")
    print(f"Result from pod: {result}")


with DAG(
    dag_id="kubernetes_pod_operator",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
):
    # Airflow 이미지와 상관없이 원하는 이미지로 작업을 실행
    hello_pod = KubernetesPodOperator(
        task_id="hello_pod",
        name="hello-pod",
        image="alpine:3.20",
        cmds=["sh", "-c"],
        arguments=["echo Hello from $(hostname); cat /etc/os-release | head -2"],
        in_cluster=True,          # Airflow가 실행 중인 클러스터에 Pod 생성
        get_logs=True,            # Pod 로그를 태스크 로그로 가져옴
        on_finish_action="delete_pod",
    )

    # Pod에서 /airflow/xcom/return.json에 쓴 값을 XCom으로 전달
    produce_xcom_pod = KubernetesPodOperator(
        task_id="produce_xcom_pod",
        name="produce-xcom-pod",
        image="alpine:3.20",
        cmds=["sh", "-c"],
        arguments=['mkdir -p /airflow/xcom && echo \'{"rows": 42}\' > /airflow/xcom/return.json'],
        in_cluster=True,
        do_xcom_push=True,
        on_finish_action="delete_pod",
    )

    print_result_task = PythonOperator(
        task_id="print_result",
        python_callable=print_result,
    )

    hello_pod >> produce_xcom_pod >> print_result_task
