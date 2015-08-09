"""
Airflow DAG for copying files from one S3 location to another.
"""
__author__ = 'Robb Wagoner (@robbwagoner)'

from airflow import DAG
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from airflow.models import Connection
from datetime import datetime, timedelta

default_args = {
    'owner': 'robb',
    'depends_on_past': True,
    'start_date': datetime(2015, 6, 1),
    'email': ['robb@pandastrike.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_interval': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'schedule_interval': timedelta(1),
    # 'end_date': datetime(2016, 1, 1),
}


dag = DAG('s3copy', default_args=default_args)

t1 = S3FileTransformOperator(
    task_id='get_avro_input',
    source_s3_key='s3://{{params.bucket}}/v2.2/{{params.cust_id}}/'
                      '{{macros.ds_format(ds,"%Y-%m-%d","%Y/%m/%d")}}/{{params.avro}}',
    dest_s3_key='s3://{{params.bucket}}/pipeline_root/{{macros.ds_format(ds,"%Y-%m-%d","%Y/%m/%d")}}/{{params.avro}}',
    transform_script='scripts/s3_transform',
    dest_s3_conn_id='s3',
    source_s3_conn_id='s3',
    params={
        'cust_id': '1032156711',
        'bucket': 'neon-tracker-logs-test-hadoop',
        'avro': 'merged.clicklog.1433134145102.avro'
    },
    dag=dag
)

