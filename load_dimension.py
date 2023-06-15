from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="", 
                 sql_query="", 
                 table="",
                 load_mode="truncate_insert",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.load_mode = load_mode


    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.load_mode == "truncate_insert":
           self.log.info("Clearing table")
           redshift_hook.run(f"TRUNCATE {self.table}")
        redshift_hook.run(self.sql_query)
