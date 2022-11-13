from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 myTable=[],
                 conn_id
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.myTable = myTable

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        for myTable in self.myTable:
            table = redshit.get_records(f"SELECT TOP 10 * FROM {myTable}")
            
            if (len(table) <= 0):
                raise ValueError (f"The table {myTable} returns with none result")
                
            self.log.info(f"The table data quality passed")