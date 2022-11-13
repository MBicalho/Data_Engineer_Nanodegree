from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 myTable,
                 bucket,
                 path,
                 json,
                 aws_secret,
                 aws_key,
                 aws_region,
                 conn_id
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.mytable = table
        self.bucket = bucket
        self.path = path
        self.json = json
        self.aws_secret = aws_secret
        self.aws_key = aws_key
        self.aws_region = aws_region
        self.conn_id = conn_id

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')

        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        #delete data for the table
        
        redshift.run(f"TRUNCATE TABLE {self.myTable}")
        self.log.info(f"Deleting data from the table {self.myTable} done!")
        
        redshift.run(f"COPY {self.myTable}
                        FROM 's3://{self.bucket}/{self.path}'   \
                        ACCESS_KEY_ID '{self.aws_key}'          \
                        SECRET_ACCESS_KEY '{self.aws_secret}'   \
                        REGION '{self.aws_region}'              \
                        JSON '{self.json}'
                     "
        self.log.info(f"Copy {self.myTable} done!")
        )




