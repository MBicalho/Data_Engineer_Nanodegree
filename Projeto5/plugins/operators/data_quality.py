from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 myTable=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.myTable = myTable

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        for myTables in self.myTable:
            table = redshit.get_records(f"SELECT COUNT(*) FROM {myTables}")
            
            if (len(table) <= 0 or len(table[0]) <= 0):
                raise ValueError (f"The table {myTables} returns with none result")
                
            
            num = table[0][0]
            
            if (num == 0):
                raise ValueError (f"Data Quality {myTables} failed!")
                
            self.log.info(f"The table data quality passed")
