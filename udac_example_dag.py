from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from plugins.stage_redshift import StageToRedshiftOperator
from create_tables import *

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now() - timedelta(seconds=1),
    'schedule_interval': None,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': True
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          catchup=False,
          max_active_runs=10
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# start-date:   'log_data/2018/11/2018-11-01-events.json'
# end-date:     'log_data/2018/11/2018-11-30-events.json'



end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


start_operator >>  end_operator
