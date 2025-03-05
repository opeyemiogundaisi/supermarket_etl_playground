from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'ope joseph',
    'retries': 5,
    'retry_delay':timedelta(minutes=2)
}

def greet(name,age):
    print("My name is", name, "and I am", age, "years old.")

with DAG(
    default_args=default_args,
    dag_id = 'python__dag_v2',
    description = 'first_python_operator',
    start_date= datetime(2025,3,3),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'task_1',
        python_callable=greet,
        op_kwargs={'name':'Johnson','age':'3'}
    )

    task1