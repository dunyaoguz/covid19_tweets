from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class HasRowsOperator(BaseOperator):
    """Check the number of records that exist in a given table"""

    ui_color = '#009FFD'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 schema="",
                 table = "",
                 *args, **kwargs):

        super(HasRowsOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.schema = schema
        self.table = table

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info('Fetching records from table...')
        records = redshift.get_records(f'SELECT COUNT(*) FROM {self.schema}.{self.table}')

        if records[0][0] == 0:
            raise ValueError(f'No records exist on table {self.table}.')
        else:
            self.log.info(f'''Table {self.table} has {records[0][0]} records.
                              Row count check has been completed succesfully.''')
