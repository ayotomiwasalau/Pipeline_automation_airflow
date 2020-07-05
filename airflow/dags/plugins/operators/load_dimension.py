from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    load_dimdata_sql = """
    INSERT INTO {} {}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 create_table="",
                 load_table="",
                 drop_if_exist=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.create_table = create_table
        self.load_table = load_table
        self.drop_if_exist = drop_if_exist

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Delete table if exist otherwise append')
        if self.drop_if_exist == True:
            redshift.run("DROP TABLE IF EXISTS {}".format(self.table))
        
        self.log.info('Create table')
        redshift.run(self.create_table)
        
        self.log.info('load data into the table')
        formatted_load_data_sql = LoadDimensionOperator.load_dimdata_sql.format(
            self.table, self.load_table
        )
        redshift.run(formatted_load_data_sql)
                      
