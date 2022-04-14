from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    INSERT_DIMENSION_TABLE_SQL= """
        TRUNCATE TABLE {};
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id= '',
                 dimension_table= '',
                 load_dimension_sql= '',
                 append= '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id= redshift_conn_id
        self.dimension_table= dimension_table
        self.load_dimension_sql= load_dimension_sql
        self.append= append
        
    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        
        redshift_hook= PostgresHook(postgres_conn_id= self.redshift_conn_id)
        
        if self.append== 'no':
            redshift_hook.run('DELETE FROM {}'.format(self.dimension_table))
        
        elif self.append== 'yes':
            dimension_table_load_sql= LoadDimensionOperator.INSERT_DIMENSION_TABLE_SQL.format(
                self.dimension_table,
                self.dimension_table,
                self.load_dimension_sql
            )
        
            redshift_hook.run(dimension_table_load_sql)
