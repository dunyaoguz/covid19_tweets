from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.operators.postgres_operator import PostgresOperator

from dotenv import load_dotenv
load_dotenv()

data_lake_name = os.environ['data_lake_name']

default_args = {
    'owner': 'dunya_oguz',
    'start_date': datetime(2020, 3, 7),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['dunyaoguz13@gmail.com'],
    'email_on_retry': False
}

dag = DAG('covid_tweets_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

copy_tweets = S3ToRedshiftTransfer(
    task_id='Copy_tweets',
    dag=dag,
    s3_bucket=f'{data_lake_name}/processed',
    s3_key='tweets.csv',
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials',
    schema='public',
    table='tweets')

copy_users = S3ToRedshiftTransfer(
    task_id='Copy_users',
    dag=dag,
    s3_bucket=f'{data_lake_name}/processed',
    s3_key='twitter_users.csv',
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials',
    schema='public',
    table='users',
    copy_options='csv')

copy_covid_data = S3ToRedshiftTransfer(
    task_id='Copy_covid_data',
    dag=dag,
    s3_bucket=f'{data_lake_name}/processed',
    s3_key='case_data.csv',
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials',
    schema='public',
    table='staging_cases',
    copy_options='csv')

copy_covid_data = S3ToRedshiftTransfer(
    task_id='Copy_covid_data',
    dag=dag,
    s3_bucket=f'{data_lake_name}/processed/',
    s3_key='case_data.csv',
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials',
    schema='public',
    table='staging_cases',
    copy_options=('csv'))

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_tables

create_tables >> copy_tweets
create_tables >> copy_users
create_tables >> copy_covid_data

copy_tweets >> end_operator
copy_users >> end_operator
copy_covid_data >> end_operator
