from datetime import datetime
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


dag = DAG(
    dag_id='autoscaling-test-0',
    # owner='lizi zhang',
    schedule_interval='0 12 * * *',
    start_date=datetime(2017, 3, 20),
    dagrun_timeout=timedelta(minutes=5),
)

start_point = DummyOperator(task_id='etl_start_point', retries=3, dag=dag)

task_1_1 = BashOperator(
    task_id='task_1_1',
    bash_command='sleep 30;echo HI>/home/airflow/logs/count_hi.txt',
    dag=dag,
)

task_1_2 = BashOperator(
    task_id='task_2_1',
    bash_command='sleep 30;echo HI>/home/airflow/logs/count_hi.txt',
    dag=dag,
)

start_point >> task_1_1
start_point >> task_1_2
# start_point >> layer_1_4
# start_point >> layer_1_5
# start_point >> layer_1_6
# start_point >> layer_1_7
