from airflow.operators.redshift_to_s3_operator import RedshiftToS3Transfer
from datetime import datetime, timedelta
from airflow import DAG

default_args = {
  'owner': 'me',
  'start_date': datetime.today(),
  'max_active_runs': 1,
}

dag = DAG(dag_id='redshift_S3',
  default_args=default_args,
  schedule_interval="@once",
  catchup=False
)

unload_to_S3 = RedshiftToS3Transfer(
  task_id='unload_to_S3',
  schema='schema_name',
  table='table_name',
  s3_bucket='bucket_name',
  s3_key='s3_key',
  redshift_conn_id='redshift',
  aws_conn_id='my_s3_conn',
  dag=dag
)
