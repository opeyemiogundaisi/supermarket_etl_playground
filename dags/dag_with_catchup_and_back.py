from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'ope joseph',
    'retries': 5,
    'retry_delay':timedelta(minutes=2)
}

with DAG(
    dag_id='dag_catchup_back_v_since_1st_feb',
    default_args= default_args,
    description='First dag',
    start_date=datetime(2025,3,1),
    schedule_interval='@daily',
    catchup=True
) as dag:
    task1_cyclic=BashOperator(
        task_id= 'task1',
        bash_command = "echo hello world"
    )
    task1_cyclic
    
