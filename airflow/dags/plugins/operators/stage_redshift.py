from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    #template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT as json '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 create_table="",
                 copy_json_option="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.create_table=create_table
        self.aws_credentials_id = aws_credentials_id
        self.copy_json_option = copy_json_option

        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        self.log.info('connect to redshift')        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('check and delete if table exist') 
        redshift.run("DROP TABLE IF EXISTS {}".format(self.table))
        
        self.log.info('create table')
        redshift.run(self.create_table)
        
        self.log.info("Copying data from s3 to table in Redshfit")
        s3_key_output = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, s3_key_output)
        
        self.log.info("print credentials {}".format(credentials))
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.copy_json_option
        )
        redshift.run(formatted_sql)
        
        





