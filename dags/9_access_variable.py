from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable


def _access_variable():
    print("printing variable info....")
    # one request per variable
    print(Variable.get("var1"))
    print(Variable.get("var2"))

    # recommended way: group values into one JSON variable
    var_dict = Variable.get("var_dict", deserialize_json=True)
    print(var_dict["var1"])
    print(var_dict["var2"])
    print("ended")


# dag with context manager
with DAG(
    dag_id="access_variable",
    start_date=datetime(2026, 1, 1),
    schedule=None,
):
    task_extract_data_op = PythonOperator(
        task_id="extract_data_op",
        python_callable=_access_variable,
    )
