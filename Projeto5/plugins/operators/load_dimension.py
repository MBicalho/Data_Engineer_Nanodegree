from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 myTable,
                 conn_id,
                 sqlQuery,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.myTable = myTable
        self.sqlQuery = sqlQuery

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
            
        redshit.run(f"INSERT INTO {self.myTable} {self.sqlQuery}")
        
        self.log.info(f"Insert done! into {self.myTable}")