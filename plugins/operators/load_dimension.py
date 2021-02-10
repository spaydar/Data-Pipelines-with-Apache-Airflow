from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="redshift",
                 sql_statement="",
                 truncate_insert_on=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.truncate_insert_on = truncate_insert_on

    def execute(self, context):
        self.log.info('Starting LoadDimensionOperator for table ' + table)
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Initialized PostgresHook with Redshift credentials')
        
        if self.truncate_insert_on:
            self.log.info('Truncate-insert mode is on. Deleting all rows from table ' + self.table + ' before insertion')
            sql_statement = 'DELETE FROM %s' % self.table
            redshift.run(sql_statement)
            self.log.info('Redshift DELETE command complete')
        else:
            self.log.info('Truncate-insert mode is off. No records will be deleted before insertion')
        
        sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql_statement)
        self.log.info('Formatted SQL statement into INSERT command. Running INSERT command in Redshift')
        
        redshift.run(sql_statement)
        self.log.info('Redshift INSERT command complete for table ' + self.table)