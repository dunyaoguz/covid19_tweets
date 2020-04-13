from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class S3ToRedshiftOperator(BaseOperator):
    """Extracts CSV data from S3 and loads it onto the specified table in the given Redshift cluster"""

    ui_color = '#F9AC1D'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials = "",
                 schema="",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 *args, **kwargs):

        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):

        # define aws creds and redshift conn
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info('Copying data from s3 to Redshift')
        redshift.run("""COPY {}.{}
                        FROM 's3://{}/{}'
                        CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
                        REGION 'us-west-2'
                        CSV
                        IGNOREHEADER 1""".format(self.schema,
                                                 self.table,
                                                 self.s3_bucket,
                                                 self.s3_key,
                                                 credentials.access_key,
                                                 credentials.secret_key))
