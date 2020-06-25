from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def some_work():
    return 'just did some work!'


dag = DAG('lizi-auto-scale-etl-1', description='DAG for auto-scale testing',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

start_point = DummyOperator(task_id='start_point', retries=3, dag=dag)
end_point = DummyOperator(task_id='end_point', retries=3, dag=dag)

layer_1 = PythonOperator(task_id='extract_from_s3_1', python_callable=some_work, dag=dag)
layer_2 = PythonOperator(task_id='extract_from_s3_1', python_callable=some_work, dag=dag)
layer_3 = PythonOperator(task_id='extract_from_s3_1', python_callable=some_work, dag=dag)
layer_4 = PythonOperator(task_id='extract_from_s3_1', python_callable=some_work, dag=dag)
layer_5 = PythonOperator(task_id='extract_from_s3_2', python_callable=some_work, dag=dag)

layer_6 = PythonOperator(task_id='transfer_1', python_callable=some_work, dag=dag)

layer_7 = PythonOperator(task_id='transfer_2', python_callable=some_work, dag=dag)
layer_8 = PythonOperator(task_id='transfer_2', python_callable=some_work, dag=dag)
layer_9 = PythonOperator(task_id='transfer_2', python_callable=some_work, dag=dag)
layer_10 = PythonOperator(task_id='transfer_2', python_callable=some_work, dag=dag)
layer_11 = PythonOperator(task_id='transfer_2', python_callable=some_work, dag=dag)

layer_12 = PythonOperator(task_id='transfer_3', python_callable=some_work, dag=dag)
layer_13 = PythonOperator(task_id='transfer_3', python_callable=some_work, dag=dag)
layer_14 = PythonOperator(task_id='transfer_3', python_callable=some_work, dag=dag)

layer_15 = PythonOperator(task_id='insert_to_redshift_1', python_callable=some_work, dag=dag)
layer_16 = PythonOperator(task_id='insert_to_redshift_2', python_callable=some_work, dag=dag)

start_point >> layer_1
start_point >> layer_2
start_point >> layer_3
start_point >> layer_4

layer_1 >> layer_6
layer_2 >> layer_6
layer_3 >> layer_6
layer_4 >> layer_6

layer_6 >> layer_7
layer_6 >> layer_8
layer_6 >> layer_9
layer_6 >> layer_10
layer_6 >> layer_11

layer_7 >> layer_15
layer_8 >> layer_15
layer_9 >> layer_15
layer_10 >> layer_15
layer_11 >> layer_15

start_point >> layer_5

layer_5 >> layer_12
layer_5 >> layer_13
layer_5 >> layer_14

layer_12 >> layer_16
layer_13 >> layer_16
layer_14 >> layer_16

layer_15 >> end_point
layer_16 >> end_point
