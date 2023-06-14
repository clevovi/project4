from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="", 
                 sql_query="", 
                 table="",
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        #self.log.info('LoadFactOperator not implemented yet')
        #redshift_hook.run(self.sql_query)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("load data to redshift")
        
        redshift.run(self.sql_query)