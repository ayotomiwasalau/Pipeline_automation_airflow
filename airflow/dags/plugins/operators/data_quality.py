from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 #tables=[],
                 sql_tests=[],
                 expected_results = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        #self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        self.sql_tests = sql_tests
        self.expected_results = expected_results
        

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
           
        if len(self.sql_tests) != len(self.expected_results):
            raise ValueError('Tests and expected results do not match in lengths')


        for i in range(len(self.sql_tests)):
            res = redshift_hook.get_first(self.sql_tests[i])
            if res[0] != self.expected_results[i]:
                raise ValueError('Test {} failed'.format(i))
            else:
                self.log.info("Test {} passed".format(i))
        
#         for table in self.tables:
#             self.log.info('Starting data quality check for {} in Redshift'.format(table))
#             records = redshift_hook.get_records("SELECT COUNT(*) FROM {}".format(table))
#             if len(records) < 1 or len(records[0]) < 1:
#                 raise ValueError("Data quality check failed. {} returned no results".format(table))
#             num_records = records[0][0]
#             if num_records < 1:
#                 raise ValueError("Data quality check failed. {} contained 0 rows".format(table))
#             logging.info("Data quality on table {} check passed with {} records".format(table, records[0][0]))        
                
                
         
        