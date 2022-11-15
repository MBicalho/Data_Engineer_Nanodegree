from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 myTable,
                 bucket,
                 key,
                 path,
                 aws_credentials,
                 conn_id,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.myTable = myTable
        self.bucket = bucket
        self.key = key
        self.path = path
        self.aws_credentials = aws_credentials
        self.conn_id = conn_id

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        hook = AwsHook(self.aws_credentials)
        credentials = hook.get_credentials()
        
        #delete data for the table
        
        redshift.run(f"TRUNCATE TABLE {self.myTable}")
        self.log.info(f"Deleting data from the table {self.myTable} done!")
        
        #Formating the files
        
        self.key = self.key.format(**context)
        
        redshift.run(f"COPY {self.myTable}  \
                        FROM 's3://{self.bucket}/{self.key}'   \
                        ACCESS_KEY_ID '{credentials.access_key}'          \
                        SECRET_ACCESS_KEY '{credentials.secret_key}'   \
                        FORMAT AS JSON '{self.path}'"
                    )
        self.log.info(f"Copy {self.myTable} done!")




