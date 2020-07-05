from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    load_factdata_sql = """
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

        super(LoadFactOperator, self).__init__(*args, **kwargs)
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
        self.log.info('Clear the destination if table exist')
        
        if self.drop_if_exist == True:
            redshift.run("DROP TABLE IF EXISTS {}".format(self.table))
        
        self.log.info('create table if table not exist')
        redshift.run(self.create_table)
                
        self.log.info('load fact table')
        formatted_facts_sql = LoadFactOperator.load_factdata_sql.format(
            self.table,
            self.load_table
        )
        redshift.run(formatted_facts_sql)