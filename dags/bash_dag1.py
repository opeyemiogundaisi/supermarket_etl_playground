from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args= {
    'owner': 'ope joseph',
    'retries': 5,
    'retry_delay':timedelta(minutes=2)
}


with DAG(
    dag_id='dag_1_v2',
    default_args= default_args,
    description='First dag',
    start_date=datetime(2025,3,3,2),
    schedule_interval='@daily'
) as dag:
    task1= BashOperator(
        task_id= 'task1',
        bash_command= "echo Hello,i am task 1"
    )
    task2= BashOperator(
        task_id= 'task2',
        bash_command= "echo Hello,i am task 2, i will run after 1"
    )
    task3= BashOperator(
        task_id= 'task3',
        bash_command= "echo Hello,i am task 3, also run after task 1 as my downstream"
    )
    #Method 1
    #task1.set_downstream(task2)
    #task1.set_downstream(task3)

    #Method 2
    task1 >> task2
    task1 >> task3