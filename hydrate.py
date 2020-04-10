#!/usr/bin/env python3

import gzip
import json

from twarc import Twarc
from pathlib import Path

from dotenv import load_dotenv
import os

import re
import boto3

from io import StringIO

import pandas as pd
pd.set_option('display.float_format', lambda x: '%.3f' % x)

load_dotenv()

consumer_key = os.environ['consumer_key']
consumer_secret = os.environ['consumer_secret']
twitter_access = os.environ['twitter_access']
twitter_secret = os.environ['twitter_secret']
aws_access = os.environ['aws_access']
aws_secret = os.environ['aws_secret']
data_lake_name = os.environ['data_lake_name']

twarc = Twarc(consumer_key, consumer_secret, twitter_access, twitter_secret)

s3 = boto3.resource('s3',
                  region_name='us-west-2',
                  aws_access_key_id=aws_access,
                  aws_secret_access_key=aws_secret)

def get_tweet_count():
    '''Computes how many COVID-related tweets were published in a day'''

    tweet_count = pd.DataFrame({'file': [], 'number_of_tweets': []})

    for path in Path('tweet_ids').iterdir():
        if path.name.endswith('.csv'):

            with open(path, 'r') as f:
                tweet_ids = f.read()
                tweet_ids = tweet_ids.split('\n')

                tweet_ids.remove('')
                no_tweets = len(tweet_ids)

            tweet_count = tweet_count.append(pd.DataFrame({'file': [path], 'number_of_tweets': [no_tweets]}))

    tweet_count.to_csv('tweet_count.csv', index=False)

def extract_tweet_info(tweet, df):
    '''Extracts information related to a given tweet'''

    tweet_df = pd.DataFrame({'tweet_id': [tweet['id']], 'created_at': [tweet['created_at']],
                             'full_text': [tweet['full_text']], 'created_by': [tweet['user']['id']],
                             'language': [tweet['lang']], 'retweet_count': [tweet['retweet_count']],
                             'favorite_count': [tweet['favorite_count']], 'in_reply_to_status_id': [tweet['in_reply_to_status_id']],
                             'in_reply_to_user_id': [tweet['in_reply_to_user_id']]})

    df = df.append(tweet_df, sort=False)
    return df

def extract_user_info(user, df):
    '''Extracts information related to the user that published a given tweet'''

    user_df = pd.DataFrame({'user_id': [user['id']], 'name': [user['name']],
                            'screen_name': [user['screen_name']], 'location': [user['location']],
                            'description': [user['description']], 'followers_count': [user['followers_count']],
                            'friends_count': [user['friends_count']], 'statuses_count': [user['statuses_count']],
                            'created_at': [user['created_at']], 'verified': [user['verified']]})

    df = df.append(user_df, sort=False)
    return df

def hydrate(id, tweets_df, users_df):
    '''Hydrates (i.e. gets the complete details for) a given tweet id'''

    print('Hydrating {}'.format(id))

    try:
        tweet = twarc.tweet(id)
        tweets_df = extract_tweet_info(tweet, tweets_df)
        users_df = extract_user_info(tweet['user'], users_df)

    except:
        print(f'Couldn\'t hydrate tweet no {id}')

    return tweets_df, users_df

def main(df, file):

    users_df = pd.DataFrame({'user_id': [], 'name': [], 'screen_name': [], 'location': [], 'description': [], 'followers_count': [],
                             'friends_count': [], 'statuses_count': [], 'created_at': [], 'verified': []})
    tweets_df = pd.DataFrame({'tweet_id': [], 'created_at': [], 'full_text': [], 'created_by': [], 'language': [],
                              'retweet_count': [], 'favorite_count': [], 'in_reply_to_status_id': [], 'in_reply_to_user_id': []})

    count = 0
    for id in df.tweet_id.tolist():
        tweets_df, users_df = hydrate(id, tweets_df, users_df)

        count += 1
        print(f'Processed {count} tweets')

        csv_buffer = StringIO()
        users_df.to_csv(csv_buffer)
        s3.Object(data_lake_name, f'users_{file}').put(Body=csv_buffer.getvalue())

        csv_buffer = StringIO()
        tweets_df.to_csv(csv_buffer)
        s3.Object(data_lake_name, f'{file}').put(Body=csv_buffer.getvalue())

if __name__ == "__main__":
    # get_tweet_count()

    data_dirs = []
    for path in Path('tweet_ids/').iterdir():
        if path.name.endswith('.csv'):
            data_dirs.append(path.name)

    for file in data_dirs:
        df = pd.read_csv(f'tweet_ids/{file}')
        main(df, file)
