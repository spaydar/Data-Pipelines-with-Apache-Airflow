from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table_names=[],
                 redshift_conn_id="redshift",
                 sql_queries_dict={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_names = table_names
        self.redshift_conn_id = redshift_conn_id
        self.sql_queries_dict = sql_queries_dict

    def execute(self, context):
        self.log.info('Starting DataQualityOperator. Checking against the following tables: ' + str(table_names))
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Initialized PostgresHook with Redshift credentials')
        
        for table in self.table_names:
            
            for sql_query in self.sql_queries_dict:
                
                result = redshift.run(sql_query.format(table))
                
                if self.sql_queries_dict[sql_query](result):
                    
                    self.log.info("Table " + table + " passed test associated with query " + sql_query)
                    
                else:
                    
                    raise ValueError("Table " + table + " failed test associated with query " + sql_query + ". Result: " + str(result))
                    
        self.log.info('DataQualityOperator complete')