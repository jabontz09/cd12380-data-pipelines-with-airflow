from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.secrets.metastore import MetastoreBackend
import logging
import pendulum
import os
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():
    metastoreBackend = MetastoreBackend()
    aws_connection = metastoreBackend.get_connection("aws_credentials")
    bucket = Variable.get("s3_bucket")
    s3_region = Variable.get("s3_region")

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table = 'staging_events',
        redshift_conn_id = 'redshift',
        aws_user = aws_connection.login,
        aws_pw = aws_connection.password,
        s3_path = f"s3://{bucket}/log_data",
        region = s3_region,
        json = f"s3://{bucket}/log_json_path.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table = 'staging_songs',
        redshift_conn_id = 'redshift',
        aws_user = aws_connection.login,
        aws_pw = aws_connection.password,
        s3_path = f"s3://{bucket}/song-data",
        region = s3_region,
        json = "auto"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id = 'redshift',
        table = 'songplays',
        select_statement = SqlQueries.songplay_table_insert,
        append_only = False

    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id = 'redshift',
        table = 'users',
        select_statement = SqlQueries.user_table_insert,
        append_only = False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id = 'redshift',
        table = 'songs',
        select_statement = SqlQueries.song_table_insert,
        append_only = False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id = 'redshift',
        table = 'artists',
        select_statement = SqlQueries.artist_table_insert,
        append_only = False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id = 'redshift',
        table = 'time',
        select_statement = SqlQueries.time_table_insert,
        append_only = False
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = "redshift",
        tables = ['songplays', 'users', 'songs', 'artists', 'time']
    )

    stop_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> stop_operator
    

final_project_dag = final_project()
