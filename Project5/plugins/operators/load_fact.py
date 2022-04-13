from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    INSERT_FACT_TABLE_SQL= """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id= '',
                 fact_table= '',
                 load_fact_sql= '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id= redshift_conn_id
        self.fact_table= fact_table
        self.load_fact_sql= load_fact_sql

    def execute(self, context):
        #self.log.info('LoadFactOperator not implemented yet')
        redshift_hook= PostgresHook(postgres_conn_id= self.redshift_conn_id)
        
        fact_table_load_sql= LoadFactOperator.INSERT_FACT_TABLE_SQL.format(
            self.fact_table,
            self.load_fact_sql
        )
        
        redshift_hook.run(fact_table_load_sql)