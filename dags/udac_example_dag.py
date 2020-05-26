from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'demilade',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False
}
start_date = datetime.utcnow()
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs = 3
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_events_table = PostgresOperator(
    task_id="create_events_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql = "create_tables.sql" 
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'staging_events',
    s3_path = "s3://udacity-dend/log_data",   # The log data (JSON data) objects don't correspond directly to staging events column names. 
    json_path = "s3://udacity-dend/log_json_path.json", # As a result, A JSONPath file is used to map the JSON elements to columns.
    file_format = "JSON",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'staging_songs',
    s3_path = "s3://udacity-dend/song_data/A/A/A",
    json_path = "auto", ## The song data (JSON data) objects correspond directly to staging songs column names. 
    file_format = "JSON"  # Hence, Copy From JSON using the ‘auto’ format option.   
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = 'songplays',
    redshift_conn_id = "redshift",
    insert_query = SqlQueries.songplay_table_insert
    
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = 'users',
    redshift_conn_id = "redshift",
    insert_query = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table = 'songs',
    redshift_conn_id = "redshift",
    insert_query = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table = 'artists',
    redshift_conn_id = "redshift",
    insert_query = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table = 'time_table',
    redshift_conn_id = "redshift",
    insert_query = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table = ["songplays", "users", "songs", "artists", "time_table"],
    redshift_conn_id = "redshift",
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Airflow task dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
