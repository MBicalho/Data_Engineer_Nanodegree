from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Matheus Bicalho',
    'start_date': datetime.now(),
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'catchup': False
}

dag = DAG('sparkfy_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id = 'redshift',
    myTable = 'staging_events',
    bucket = 'udacity-dend',
    key = 'log_data',
    aws_credentials = 'aws_credentials',
    path = 's3://udacity-dend/log_json_path.json'
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    conn_id = 'redshift',
    myTable = 'staging_songs',
    bucket = 'udacity-dend',
    key = 'song_data',
    aws_credentials = 'aws_credentials',
    path = 'auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id = 'redshift',
    myTable = 'songplays',
    sqlQuery = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id = 'redshift',
    myTable = 'users',
    sqlQuery = SqlQueries.user_table_insert,
    truncate = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id = 'redshift',
    myTable = 'songs',
    sqlQuery = SqlQueries.song_table_insert,
    truncate = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id = 'redshift',
    myTable = 'artists',
    sqlQuery = SqlQueries.artist_table_insert,
    truncate = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id = 'redshift',
    myTable = 'time',
    sqlQuery = SqlQueries.time_table_insert,
    truncate = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id = 'redshift',
    myTable = ['songplays', 'songs', 'artists', 'users', 'time'],
    dq_checks = [{ 'check_sql': 'SELECT COUNT(*) FROM '}, 
        { 'check_sql': 'SELECT DISTINCT * FROM '}]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator                                                                      \
    >> [stage_events_to_redshift, stage_songs_to_redshift]                          \
    >> load_songplays_table                                                         \
    >> [load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table, load_user_dimension_table]   \
    >> run_quality_checks                                                           \
    >> end_operator
