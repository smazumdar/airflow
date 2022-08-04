from datetime import datetime
import time
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_hello():
    for x in range(5):
        time.sleep(120)
    return True

dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval='45 11,12,13 * * *',
          start_date=datetime(2022, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

hello_operator
