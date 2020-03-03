from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    load_fact_table = """
            INSERT INTO {}
            {}
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_query = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.insert_query = insert_query

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Load data to the fact table: {self.table}")
        
        insert_table = LoadFactOperator.load_fact_table.format(
            self.table,
            self.insert_query
        )
        
        redshift.run(insert_table)
        
        self.log.info(f"Completed loading data from staging tables to the {self.table}")
        
