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
    'start_date': datetime.now,
    'email_on_failure': false,
    'email_on_retry': false,
    'depends_on_past': false,
    'retries': 3;
    'retry_delay': timedelta(minutes = 5),
    'catchup': false
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id = 'redshift',
    myTable = 'staging_events'
    bucket = 'udacity-dend'
    key = 'log_data'
    aws_credentials = 'aws_credentials',
    json = 's3://udacity-dend/log_json_path.json'
    aws_region = 'us-west-2',
    provide_context = True
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    conn_id = 'redshift',
    myTable = 'staging_songs'
    bucket = 'udacity-dend'
    key = 'song_data'
    aws_credentials = 'aws_credentials',
    json = 'auto'
    aws_region = 'us-west-2',
    provide_context = True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id = 'redshift',
    myTable = 'songplays',
    sqlQuery = SqlQueries.songplay_table_insert,
    provide_context = True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id = 'redshift',
    myTable = 'users',
    sqlQuery = SqlQueries.user_table_insert,
    provide_context = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id = 'redshift',
    myTable = 'songs',
    sqlQuery = SqlQueries.song_table_insert,
    provide_context = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id = 'redshift',
    myTable = 'artists'
    sqlQuery = SqlQueries.artist_table_insert,
    provide_context = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id = 'redshift',
    myTable = 'time'
    sqlQuery = SqlQueries.time_table_insert,
    provide_context = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id = 'redshift',
    myTable = ['songplays', 'songs', 'artists', 'users', 'time'],
    provide_context = True
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [
    stage_events_to_redshift, 
    stage_songs_to_redshift
                            ] >> [
                                     load_songplays_table,
                                     load_user_dimension_table,
                                     load_song_dimension_table,
                                     load_artist_dimension_table,
                                     load_time_dimension_table
                                        ] >> [
    run_quality_checks
                       ]   >> [
end_operator  
]
