from datetime import datetime,timedelta

from airflow.decorators import dag, task

default_args = {
    'owner':'ope joseph',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
     dag_id='dag_with_taskflow_api_v_rerun_from3rd', 
     default_args=default_args,
     start_date=datetime(2025,3,3),
     schedule_interval='@daily')
def hello_world_etl():
    @task()
    def get_name():
        return "Jerry"
    
    @task()
    def get_age():
        return 19
    
    @task()
    def greet(name, age):
        print("My name is", name,"i am", age,"years old")

    name = get_name()
    age = get_age()
    greet(name=name, age=age)

greet_dag = hello_world_etl()