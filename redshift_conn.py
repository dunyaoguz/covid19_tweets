import pandas as pd
import boto3
import json
from dotenv import load_dotenv, find_dotenv
import os
from time import sleep
import psycopg2

load_dotenv()
KEY = os.environ['AWS_KEY']
SECRET = os.environ['AWS_SECRET']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']

iam = boto3.client('iam',
                   region_name='us-west-2',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   )

redshift = boto3.client('redshift',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

def create_role():
    """
    Creates an IAM role with s3 read access.
    """
    iam.create_role(RoleName='airflow_project_s3_access', AssumeRolePolicyDocument=json.dumps({'Statement': [{'Action': 'sts:AssumeRole',
                                                                                                          'Effect': 'Allow',
                                                                                                          'Principal': {'Service': 'redshift.amazonaws.com'}}],
                                                                                           'Version': '2012-10-17'}))
    # attach s3 read access policy to airflow_project_s3_access
    iam.attach_role_policy(RoleName='airflow_project_s3_access', PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

def create_cluster(ROLE_ARN):
    """
    Creates a Redshift cluster with 4 dc2.large type nodes and the specified IAM role.
    """
    redshift.create_cluster(ClusterType='multi-node',
                            NodeType='dc2.large',
                            NumberOfNodes=4,
                            # Identifiers & Credentials
                            DBName='dev',
                            ClusterIdentifier='redshift-cluster-1',
                            MasterUsername=DB_USER,
                            MasterUserPassword=DB_PASSWORD,
                            # Roles
                            IamRoles=[ROLE_ARN]
    )

def check_status(status):
    """
    Checks whether the cluster has the desired status.
    """
    try:
        while redshift.describe_clusters(ClusterIdentifier='redshift-cluster-1')['Clusters'][0]['ClusterStatus'] != status:
            print('{} cluster'.format(redshift.describe_clusters(ClusterIdentifier='redshift-cluster-1')['Clusters'][0]['ClusterStatus']))
            sleep(15)
        print('cluster is {}'.format(redshift.describe_clusters(ClusterIdentifier='redshift-cluster-1')['Clusters'][0]['ClusterStatus']))
    except:
        print('cluster is deleted')

def check_connection(ENDPOINT, PORT):
    """
    Checks if a connection can be made to the Redshift cluster that was created.
    """
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(ENDPOINT, 'dev', DB_USER, DB_PASSWORD, PORT))
        cur = conn.cursor()
        cur.execute("CREATE TABLE test (test_id INTEGER)")
        cur.execute("SELECT * FROM test")
        conn.close()
        print('connection to cluster is successful')
    except Exception as e:
        print('something went wrong, can not connect to cluster')

def reset():
    """
    Deletes Redshift cluster.
    """
    redshift.delete_cluster(ClusterIdentifier='redshift-cluster-1',
                            SkipFinalClusterSnapshot=True)

def main():
    ROLE_ARN = iam.get_role(RoleName='dwh_project_s3_access')['Role']['Arn']
    print(f'ARN: {ROLE_ARN}')

    # uncomment code below to create cluster
    # create_cluster(ROLE_ARN)
    # check_status('available')

    ENDPOINT = redshift.describe_clusters(ClusterIdentifier='redshift-cluster-1')['Clusters'][0]['Endpoint']['Address']
    PORT = redshift.describe_clusters(ClusterIdentifier='redshift-cluster-1')['Clusters'][0]['Endpoint']['Port']
    print(f'HOST: {ENDPOINT}')
    print(f'PORT: {PORT}')

    check_connection(ENDPOINT, PORT)

    # uncomment the code below to delete the cluster
    reset()
    check_status('deleted')

if __name__ == "__main__":
    main()
