from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    load_dimension_table = """
            INSERT INTO {}
            {}
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_query = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.insert_query = insert_query

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Clearing data from dimension table before loading in new data: {self.table}" )
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info(f"Load data to the dimension table: {self.table}")
        
        insert_table = LoadDimensionOperator.load_dimension_table.format(
            self.table,
            self.insert_query
        )
        
        redshift.run(insert_table)
        
        self.log.info(f"Completed loading data from staging tables to the {self.table}")
