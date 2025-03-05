from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'ope joseph',
    'retries': 5,
    'retry_delay':timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_crontab_sample_since_feb17_withcatchup',
    default_args= default_args,
    description='First dag',
    start_date=datetime(2025,2,17),
    schedule_interval='0 15 * * 2',
    catchup=True

) as dag:
    task1_cyclic=BashOperator(
        task_id= 'task1',
        bash_command = "echo hello world"
    )
    task1_cyclic
    
