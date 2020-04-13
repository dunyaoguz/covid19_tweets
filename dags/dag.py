from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
# from airflow.operators.custom_plugins import S3ToRedshiftOperator

from copy_csv_from_s3 import S3ToRedshiftOperator

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

create_tables = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql='create_tables.sql'
)

copy_tweets = S3ToRedshiftOperator(
    task_id='Copy_tweets',
    dag=dag,
    s3_bucket=f'{data_lake_name}/processed',
    s3_key='tweets.csv',
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    schema='public',
    table='tweets')

copy_users = S3ToRedshiftOperator(
    task_id='Copy_users',
    dag=dag,
    s3_bucket=f'{data_lake_name}/processed',
    s3_key='twitter_users.csv',
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    schema='public',
    table='users')

copy_covid_data = S3ToRedshiftOperator(
    task_id='Copy_covid_data',
    dag=dag,
    s3_bucket=f'{data_lake_name}/processed',
    s3_key='case_data.csv',
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    schema='public',
    table='staging_cases')

load_tables = PostgresOperator(
    task_id='Load_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql='load_tables.sql'
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_tables

create_tables >> copy_tweets
create_tables >> copy_users
create_tables >> copy_covid_data

copy_tweets >> load_tables
copy_users >> load_tables
copy_covid_data >> load_tables

load_tables >> end_operator
