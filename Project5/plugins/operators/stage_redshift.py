from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    SQL_COPY_QUERY= """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON {}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id= '',
                 aws_credentials_id= '',
                 s3_bucket= '',
                 s3_key= '',
                 rs_table= '',
                 json_path= '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id= redshift_conn_id
        self.aws_credentials_id= aws_credentials_id
        self.s3_bucket= s3_bucket
        self.s3_key= s3_key
        self.rs_table= rs_table
        self.json_path= json_path
        
    def execute(self, context):
        #self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook= AwsHook(self.aws_credentials_id)
        aws_credentials= aws_hook.get_credential()
        redshift_hook= PostgresHook(postgres_conn_id= self.redshift_conn_id)
        
        # Clean all table that exist from the last session
        redshift.run(f'delete from {self.rs_table}')
        
        # Get render_key to determine S3 bucket target location
        render_key= self.s3_key.format(**context)
        s3_save_path= f's3://{self.s3_bucket}/{render_key}'

        copy_sql= StageToRedshiftOperator.SQL_COPY_QUERY.format(
            self.rs_table,
            s3_save_path,
            aws_credentials.access_key,
            aws_credentials.secret_key,
            self.json_path
        )
        
        redshift_hook.run(copy_sql)

