from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.custom_plugins import S3ToRedshiftOperator, HasRowsOperator, ContextCheckOperator

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

check_tweet_records = HasRowsOperator(
    task_id='Check_tweet_records',
    dag=dag,
    redshift_conn_id='redshift',
    schema='public',
    table='tweets')

check_user_records = HasRowsOperator(
    task_id='Check_user_records',
    dag=dag,
    redshift_conn_id='redshift',
    schema='public',
    table='users')

check_users_completeness = ContextCheckOperator(
    task_id='Check_users_completeness',
    dag=dag,
    redshift_conn_id='redshift',
    query='''SELECT COUNT(*) FROM public.tweets
             WHERE created_by NOT IN (SELECT DISTINCT user_id FROM public.users)''',
    expected_result=0)

check_covid19_records = HasRowsOperator(
    task_id='Check_covid_records',
    dag=dag,
    redshift_conn_id='redshift',
    schema='public',
    table='covid19_stats')

check_country_records = HasRowsOperator(
    task_id='Check_country_records',
    dag=dag,
    redshift_conn_id='redshift',
    schema='public',
    table='country_stats')

check_covid_records_completeness = ContextCheckOperator(
    task_id='Check_covid_records_completeness',
    dag=dag,
    redshift_conn_id='redshift',
    query='''SELECT COUNT(*) FROM public.covid19_stats
             WHERE country NOT IN (SELECT DISTINCT country FROM public.country_stats)''',
    expected_result=0)

# OPERATOR PIPELINE
create_tables >> copy_tweets
create_tables >> copy_users
create_tables >> copy_covid_data

copy_tweets >> load_tables
copy_users >> load_tables
copy_covid_data >> load_tables

load_tables >> check_tweet_records
load_tables >> check_user_records
load_tables >> check_covid19_records
load_tables >> check_country_records

check_tweet_records >> check_users_completeness
check_user_records >> check_users_completeness

check_covid19_records >> check_covid_records_completeness
check_country_records >> check_covid_records_completeness
