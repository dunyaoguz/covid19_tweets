from dotenv import load_dotenv, find_dotenv
import os

import boto3
import pandas as pd

from datetime import datetime
from io import StringIO, BytesIO

pd.set_option('display.float_format', lambda x: '%.3f' % x)
load_dotenv()

aws_access = os.environ['aws_access']
aws_secret = os.environ['aws_secret']
data_lake_name = os.environ['data_lake_name']

s3 = boto3.client('s3',
                  region_name='us-west-2',
                  aws_access_key_id=aws_access,
                  aws_secret_access_key=aws_secret)

def get_objects():
    '''Get list objects from data lake'''
    objects = s3.list_objects(Bucket=data_lake_name)['Contents']

    case_data = [object['Key'] for object in objects if 'COVID-19' in object['Key']]
    tweet_data = [object['Key'] for object in objects if 'covid_tweets' in object['Key'] and 'users' not in object['Key']]
    twitter_user_data = [object['Key'] for object in objects if 'users_GSU' in object['Key']]

    return {'case_data': case_data, 'tweet_data': tweet_data, 'twitter_user_data': twitter_user_data}

def process_tweet_data(bucket_contents):
    '''Perform data quality check and formatting fixes for tweet data'''

    df = pd.DataFrame({'tweet_id': [], 'created_at': [], 'full_text': [], 'created_by': [], 'language': [],
                       'retweet_count': [], 'favorite_count': [], 'in_reply_to_status_id': [], 'in_reply_to_user_id': []})

    retval = {}
    for tweet_data in bucket_contents['tweet_data']:
        print(tweet_data)
        object = s3.get_object(Bucket= data_lake_name, Key= tweet_data)
        data = pd.read_csv(object['Body'], lineterminator='\n')

        retval[tweet_data] = data.shape[0]
        df = df.append(data, sort=False)

    # check number of null values, drop rows that have nulls in certain columns and impute nulls
    print('---------------------------------------------')
    print('---------------- NULL VALUES ----------------')
    print('---------------------------------------------')
    print(df.isna().sum())
    for col in ['language', 'created_by', 'full_text']:
        df.drop(df[df[col].isna()].index, inplace=True)
    df[['retweet_count', 'favorite_count']] = df[['retweet_count', 'favorite_count']].fillna(0)

    # check for duplicates
    print('---------------------------------------------')
    print('------------- DUPLICATE TWEETS --------------')
    print('---------------------------------------------')
    print(df.duplicated().sum())
    df.drop_duplicates(inplace=True)

    # check for number of tweets that weren't able to be hydrated
    print('---------------------------------------------')
    print('-------------- UNFOUND TWEETS ---------------')
    print('---------------------------------------------')
    original_tweet_counts = pd.read_csv('tweet_count.csv')
    for _, row in original_tweet_counts.iterrows():
        print('file:', row.file.split('/')[1])
        try:
            print('percent hydrated:', retval[row.file.split('/')[1]] / row.number_of_tweets)
        except:
            pass

    # check if the data types are correct
    print('---------------------------------------------')
    print('---------------- DATA TYPES -----------------')
    print('---------------------------------------------')
    print(df.head())
    print(df.dtypes)

    # remove milli seconds
    df.created_at = df.created_at.str[0:19]

    # fix dates
    dt_converter = lambda x: datetime.strptime('2020 ' + x.strip(), '%Y %a %b %d %X')
    df['created_at'] = df['created_at'].apply(dt_converter)

    # fix integer columns
    df['tweet_id'] = df['tweet_id'].apply(int)
    df['created_by'] = df['created_by'].apply(int)
    df['retweet_count'] = df['retweet_count'].apply(int)
    df['favorite_count'] = df['favorite_count'].apply(int)

    df['in_reply_to_user_id'] = df['in_reply_to_user_id'].fillna(0).apply(int)
    df['in_reply_to_status_id'] = df['in_reply_to_status_id'].fillna(0).apply(int)
    df[['in_reply_to_status_id', 'in_reply_to_user_id']] = df[['in_reply_to_status_id', 'in_reply_to_user_id']].replace({0:None})

    return df

def process_user_data(bucket_contents):
    '''Perform data quality check and formatting fixes for twitter user data'''

    df = pd.DataFrame({'user_id': [], 'name': [], 'screen_name': [], 'location': [], 'description': [], 'verified': [],
                       'followers_count': [], 'friends_count': [], 'statuses_count': [], 'created_at': []})

    for user_data in bucket_contents['twitter_user_data']:
        object = s3.get_object(Bucket= data_lake_name, Key= user_data)
        data = pd.read_csv(object['Body'], lineterminator='\n')
        df = df.append(data, sort=False)

    # check for duplicates
    print('---------------------------------------------')
    print('------------- DUPLICATE USERS ---------------')
    print('---------------------------------------------')
    print(df.duplicated().sum())
    df.drop_duplicates(inplace=True)

    # check number of null values, drop rows that have nulls in certain columns
    print('---------------------------------------------')
    print('---------------- NULL VALUES ----------------')
    print('---------------------------------------------')
    print(df.isna().sum())

    for col in ['user_id', 'created_at', 'verified', 'statuses_count', 'friends_count', 'followers_count']:
        df.drop(df[df[col].isna()].index, inplace=True)

    # check if the data types are correct
    print('---------------------------------------------')
    print('---------------- DATA TYPES -----------------')
    print('---------------------------------------------')
    print(df.head())
    print(df.dtypes)

    df['user_id'] = df['user_id'].apply(int)

    bool_parser = lambda x: True if x == 1 else False
    df['verified'] = df['verified'].apply(bool_parser)

    df['followers_count'] = df['followers_count'].apply(int)
    df['friends_count'] = df['friends_count'].apply(int)
    df['statuses_count'] = df['statuses_count'].apply(int)

    # remove milli seconds
    df.created_at = df.created_at.str[0:19]

    # fix dates
    dt_converter = lambda x: datetime.strptime('2020 ' + x.strip(), '%Y %a %b %d %X')
    df['created_at'] = df['created_at'].apply(dt_converter)

    return df

def process_case_data(bucket_contents):
    '''Perform data quality check and formatting fixes for data on covid cases'''

    object = s3.get_object(Bucket= data_lake_name, Key= bucket_contents['case_data'][0])
    df = pd.read_csv(object['Body'])

    # check for duplicates
    print('---------------------------------------------')
    print('------------- DUPLICATE USERS ---------------')
    print('---------------------------------------------')
    print(df.duplicated().sum())
    df.drop_duplicates(inplace=True)

    # check number of null values, drop rows that have nulls in certain columns
    print('---------------------------------------------')
    print('---------------- NULL VALUES ----------------')
    print('---------------------------------------------')
    print(df.isna().sum())

    # check if the data types are correct
    print('---------------------------------------------')
    print('---------------- DATA TYPES -----------------')
    print('---------------------------------------------')
    print(df.head())
    print(df.dtypes)

    # fix dates
    df['dateRep'] = pd.to_datetime(df['dateRep'], format='%m/%d/%y')

    return df

def file_upload(df, key):
    '''Upload a given dataframe on s3 using the key in the argument'''

    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=data_lake_name, Key=key)

def main():
    bucket_contents = get_objects()

    tweets = process_tweet_data(bucket_contents)
    users = process_user_data(bucket_contents)
    cases = process_case_data(bucket_contents)

    file_upload(tweets, 'processed/tweets.csv')
    file_upload(users, 'processed/twitter_users.csv')
    file_upload(cases, 'processed/case_data.csv')

if __name__ == "__main__":
    main()
