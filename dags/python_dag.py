import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 12),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "generate_transactions",
    default_args=default_args,
    description="A DAG to generate and save dummy transaction data",
    schedule_interval="0 12 * * *",
    catchup=False,
)

script_path = "/opt/airflow/dags/generate_transactions.py" 

run_script = BashOperator(
    task_id="run_generate_transactions",
    bash_command=f"python {script_path}",
    dag=dag,
)

run_script
