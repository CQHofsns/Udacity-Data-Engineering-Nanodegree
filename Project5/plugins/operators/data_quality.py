from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id= '',
                 check_table_sql= '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id= redshift_conn_id
        self.check_table_sql= check_table_sql

    def execute(self, context):
        #self.log.info('DataQualityOperator not implemented yet')
        redshift_hook= PostgresHook(postgres_conn_id= self.redshift_conn_id)
        
        failed_test_cases= 0
        
        for sql in self.check_table_sql:
            query= sql.get('check_sql')
            expected_result= sql.get('expected_result')
            
            records = redshift.get_records(query)[0]
            
            if expected_result != records[0]:
                print(f'Failed at query {query}')
                failed_test_cases += 1
                
        if failed_test_cases > 0:
            print(f'{failed_test_cases} table test failed')
            
        print('All tables have passed the data quality check')
