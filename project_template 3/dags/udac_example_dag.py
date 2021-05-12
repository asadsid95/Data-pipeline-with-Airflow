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
    'owner': 'Asad Siddiqui',   
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1, 0, 0, 0, 0),
    'end_date': datetime(2018, 12, 1, 0, 0, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='DAG for Data Engineering Project 5 - To load from S3 and transform data in Redshift with Airflow',          
          schedule_interval='@daily',
          max_active_runs=1
        )

table_names=('songplays','artists','users','songs','time')

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials="aws_credentials",
    redshift_conn_id='redshift',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials="aws_credentials",
    redshift_conn_id='redshift',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    aws_credentials='aws_credentials',
    redshift_conn_id='redshift',
    table='songplays',
    sql_statement=SqlQueries.songplay_table_insert
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    sql_statement=SqlQueries.user_table_insert,
    truncate='Y'

)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    sql_statement=SqlQueries.song_table_insert,
    truncate='Y'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    sql_statement=SqlQueries.artist_table_insert,
    truncate='Y'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    sql_statement=SqlQueries.time_table_insert,
    truncate='Y'

)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table_names=table_names,
    quality_checks=[
    {'check_sql':'SELECT count(*) FROM users WHERE userid IS null', 'expected_result':0},
    {'check_sql':'SELECT count(*) FROM songs WHERE songid IS null', 'expected_result':0},
    {'check_sql':'SELECT count(*) FROM artists WHERE artistid IS null', 'expected_result':0},
    {'check_sql':'SELECT count(*) FROM time WHERE start_time IS null', 'expected_result':0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task dependencies 

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
