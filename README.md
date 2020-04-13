# COVID19 Tweets

## Purpose and Scope

Over the past few weeks, COVID19 has dramatically impacted all realms of public life - from where we go to how we interact with one another - everywhere in the world. In North America, while the growing spread of the novel coronavirus was widely known since the beginning of February, its extreme repercussions begun in the week of March 9th, after the suspension of the NBA season and Trump's ban on European travel on March 12. The crisis came into full force in the following week in Canada and USA, leading to a stock market crash, closure of borders to all foreign nationals, mass lay offs and severe social distancing measures. 

The purpose of this project is to allow analysts to examine public discourse via Twitter regarding COVID19 in a period of 7 days leading upto, including and immediately following March 12 - the point at which the crisis truly and fully erupted in North America - and how it related to the growing number of cases and deaths across the world. Some questions that can be tackled with the data gathered in the project are as follows:

* What was the relationship between number of tweets and number of cases in different countries?
* How did the volume of coronavirus related tweets and interactions with coronavirus related tweets change after March 12 in different countries? 
* Do countries with higher number of cases per capita experience higher number of deaths per capita? 
* What was the relationship between number of users publishing tweets, and the number in deaths and countries across different countries?

## Data Sources

1. Ids of COVID19 related tweets 
    - Source: [Georgia State University](https://zenodo.org/record/3749360#.XpSyuS0ZPfY)
    - Size: 1,636,081 rows
    - Update frequency: Daily 

2. Data on the geographic distribution of COVID-19 cases worldwide
    - Source: [EU Open Data Portal](https://data.europa.eu/euodp/en/data/dataset/covid-19-coronavirus-data/resource/55e8f966-d5c8-438e-85bc-c7a5a26f4863)
    - Size: 10,538 rows
    - Update frequency: Daily 

## Data Model

![ERD](images/erd.png)

## Directory

* `hydrate.py`:
* `redshift_conn.py`:
* `staging_transform.py`:
* `dags/dag.py`:
* `dags/create_tables.sql`:
* `dags/load_tables.sql`:
* `plugins/operators/copy_data_from_s3.py`:
* `plugins/operators/check_row_count.py`:
* `plugins/operators/check_context.py`:

## ETL Process and Data Pipeline

1. Source data was downloaded locally 
2. Tweet ids were hydrated 
3. Preprocessing was performed on hydrated tweet data and covid19 data, including de-duplication, null imputation and data type correction. Processed data was uploaded on S3 
4. Redshift cluster was instantiated 
5. Staging, fact and dimension tables were created in Redshift 
6. Processed data was extracted from the S3 bucket and loaded on appropriate tables in Redshift 
7. Data quality checks were performed to ensure that loaded data was complete, accurate and consistent

Steps 5-7 are visualised in the following DAG:

![DAG](images/pipeline.png)

## Quick Start

1. `git clone https://github.com/dunyaoguz/covid19_tweets`
2. `cd covid19_tweets`
3. Obtain a Twitter developer account and get API keys. Create a .env file with your consumer key, consumer secret, twitter access and twitter secret keys
4. Run `python hydrate.py`
5. Create an IAM user on your AWS account with full S3 and Redshift access. Add your AWS secret access key and AWS access key on your .env file
6. Run `python redshift_conn.py`
7. Instantiate Airflow with the `airflow scheduler` and `airflow webserver` commands
8. Create connections on Airflow to your Redshift data warehouse (conn type=postgres) and AWS credentials (conn type=aws)
9. Move the dags and plugins folder to your Airflow home directory
10. Turn on covid_tweets_dag

## Potential Scenarios
