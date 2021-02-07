from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql_template = """
        COPY {}
        FROM {}
        ACCESS_KEY_ID {}
        SECRET_ACCESS_KEY {}
        JSON {}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 table="",
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 s3_bucket="",
                 s3_key="",
                 json_settings="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_settings = json_settings

    def execute(self, context):
        self.log.info('Beginning StageToRedshiftOperator for table ' + table)





