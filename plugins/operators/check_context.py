from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class ContextCheckOperator(BaseOperator):
    """Runs checks on the data that exists within a table to confirm accuracy and/or completeness"""

    ui_color = '#39B54A'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 query = "",
                 expected_result = "",
                 *args, **kwargs):

        super(ContextCheckOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.expected_result = expected_result

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info('Running context check...')
        records = redshift.get_records(self.query)

        if records[0][0] != self.expected_result:
            raise ValueError(f"Context check has failed. {records[0][0]} does not equal {self.expected_result}.")
        else:
            self.log.info("Context check passed")
