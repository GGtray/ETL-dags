from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_hello():
    return 'Hello world-7!'

dag = DAG('hello_world-7', description='Simple tutorial DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

start_point = DummyOperator(task_id='start_point', retries=3, dag=dag)

layer_1_1 = PythonOperator(task_id='layer_1_1', python_callable=print_hello, dag=dag)
layer_1_2 = PythonOperator(task_id='layer_1_2', python_callable=print_hello, dag=dag)
layer_1_3 = PythonOperator(task_id='layer_1_3', python_callable=print_hello, dag=dag)
layer_1_4 = PythonOperator(task_id='layer_1_4', python_callable=print_hello, dag=dag)
layer_1_5 = PythonOperator(task_id='layer_1_5', python_callable=print_hello, dag=dag)
layer_1_6 = PythonOperator(task_id='layer_1_6', python_callable=print_hello, dag=dag)
layer_1_7 = PythonOperator(task_id='layer_1_7', python_callable=print_hello, dag=dag)

layer_2_1 = PythonOperator(task_id='layer_2_1', python_callable=print_hello, dag=dag)
layer_2_2 = PythonOperator(task_id='layer_2_2', python_callable=print_hello, dag=dag)
layer_2_3 = PythonOperator(task_id='layer_2_3', python_callable=print_hello, dag=dag)
layer_2_4 = PythonOperator(task_id='layer_3_4', python_callable=print_hello, dag=dag)
layer_2_5 = PythonOperator(task_id='layer_4_5', python_callable=print_hello, dag=dag)
layer_2_6 = PythonOperator(task_id='layer_5_6', python_callable=print_hello, dag=dag)
layer_2_7 = PythonOperator(task_id='layer_6_7', python_callable=print_hello, dag=dag)

start_point >> layer_1_1
start_point >> layer_1_2
start_point >> layer_1_3
start_point >> layer_1_4
start_point >> layer_1_5
start_point >> layer_1_6
start_point >> layer_1_7

layer_1_1 >> layer_2_1
layer_1_1 >> layer_2_2
layer_1_1 >> layer_2_3
layer_1_1 >> layer_2_4
layer_1_1 >> layer_2_5
layer_1_1 >> layer_2_6
layer_1_1 >> layer_2_7




