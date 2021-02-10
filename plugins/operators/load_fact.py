from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="redshift",
                 sql_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement

    def execute(self, context):
        self.log.info('Starting LoadFactOperator for table ' + self.table)
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Initialized PostgresHook with Redshift credentials')
        
        sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql_statement)
        self.log.info('Formatted SQL statement into INSERT command. Running INSERT command in Redshift')
        
        redshift.run(sql_statement)
        self.log.info('Redshift INSERT command complete for table ' + self.table)