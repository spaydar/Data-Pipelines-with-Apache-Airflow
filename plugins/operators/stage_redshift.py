from airflow.contrib.hooks.aws_hook import AwsHook
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
                 table="",
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 s3_bucket="",
                 s3_key="",
                 json_settings="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_settings = json_settings

    def execute(self, context):
        self.log.info('Starting StageToRedshiftOperator for table ' + self.table)
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Initialized PostgresHook with Redshift credentials')
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info('Retrieved AWS credentials via AwsHook')
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        copy_command = StageToRedshiftOperator.copy_sql_template.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_settings
        )
        self.log.info('Formatted S3 path for read and SQL COPY command. Running COPY command in Redshift')
        
        redshift.run(copy_command)
        self.log.info('Redshift COPY command complete for table ' + self.table)


