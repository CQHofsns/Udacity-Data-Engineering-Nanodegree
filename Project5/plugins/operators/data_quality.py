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
                 check_table_list= '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id= redshift_conn_id
        self.check_table_list= check_table_list

    def execute(self, context):
        #self.log.info('DataQualityOperator not implemented yet')
        redshift_hook= PostgresHook(postgres_conn_id= self.redshift_conn_id)
        
        for check_table in self.check_table_list:
            print('Begin Data Quality evaluation process...')
            records= redshift_hook.get_records(f'SELECT COUNT(*) FROM {check_table}')
            if len(records) < 1 or len(records[0]) < 1:
                print(f' ---> [ERROR]: Get no return records in {check_table} -->  Data Quality check failed.')
            
            number_records= records[0][0]
            if number_records < 1:
                print(f' ---> [ERROR]: {check_table} return 0 rows -->  Data Quality check failed.')
                
            print(f' ---> [INFO]: {check_table} pass the Data Quality check')