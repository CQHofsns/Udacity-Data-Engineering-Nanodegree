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
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id= 'Stage_events',
    dag= dag,
    redshift_conn_id= 'redshift',
    aws_credentials= 'aws_credentials',
    s3_bucket= 'udacity-dend',
    s3_key= 'log_data',
    rs_table= 'staging_events',
    json_path= 's3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id= 'redshift',
    aws_credentials= 'aws_credentials',
    s3_bucket= 'udacity-dend',
    s3_key= 'song_data/A/A/A',
    rs_table= 'staging_songs',
    json_path= 'auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id= 'redshift',
    fact_table= 'songplays',
    load_fact_sql= SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id= 'redshift',
    dimension_table= 'users',
    load_dimension_sql= SqlQueries.user_table_insert 
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id= 'redshift',
    dimension_table= 'songs',
    load_dimension_sql= SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id= 'redshift',
    dimension_table= 'artists',
    load_dimension_sql= SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id= 'redshift',
    dimension_table= 'time',
    load_dimension_sql= SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id= 'redshift',
    check_table_list= ['songplays', 'users', 'artists', 'songs', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Create DAG flows

# First stage: Staging phase
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

# Second stage: Insert staging data into fact songplays table
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# Third stage: Insert data from fact songplays table into 4 dimension tables
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# Fourth stage: Data Quality check
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# Final stage: Ending phase
run_quality_checks >> end_operator