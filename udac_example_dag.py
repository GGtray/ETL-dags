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
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# start-date:   'log_data/2018/11/2018-11-01-events.json'
# end-date:     'log_data/2018/11/2018-11-30-events.json'


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="public.staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials_root",
    s3_bucket="s3_bucket",
    s3_key="log_data",
    # s3_key='log_data/2018/11/{}-{}-{}-events.json',
    region="us-west-2",
    create_table=staging_events_table_create,
    format="json 's3://udacity-dend/log_json_path.json'",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="public.staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials_root",
    s3_bucket="s3_bucket",
    s3_key="song_data",
    region="us-west-2",
    create_table=staging_songs_table_create,
    format="json 'auto'",
    provide_context=True
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift >> end_operator
