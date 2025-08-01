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
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():
    # metastoreBackend = MetastoreBackend()
    # aws_connection = metastoreBackend.get_connection("aws_credentials")
    # redshift_conn_id = metastoreBackend.get_connection("redshift")
    # bucket = Variable.get("s3_bucket")

    start_operator = DummyOperator(task_id='Begin_execution')

    @task
    def check_creds():
        logging.info('success')
        # logging.info(f"bucket is {bucket}")
        # logging.info(f"aws_connection.password is {aws_connection}")
        # logging.info(f"aws_connection.login is {aws_connection}")
    # stage_events_to_redshift = StageToRedshiftOperator(
    #     task_id='Stage_events',
    #     table = 'staging_events',
    #     redshift_conn_id = redshift_conn_id,
    #     aws_user = aws_connection.login,
    #     aws_password = aws_connection.password,
    # )

    # stage_songs_to_redshift = StageToRedshiftOperator(
    #     task_id='Stage_songs',
    # )

    # load_songplays_table = LoadFactOperator(
    #     task_id='Load_songplays_fact_table',
    # )

    # load_user_dimension_table = LoadDimensionOperator(
    #     task_id='Load_user_dim_table',
    # )

    # load_song_dimension_table = LoadDimensionOperator(
    #     task_id='Load_song_dim_table',
    # )

    # load_artist_dimension_table = LoadDimensionOperator(
    #     task_id='Load_artist_dim_table',
    # )

    # load_time_dimension_table = LoadDimensionOperator(
    #     task_id='Load_time_dim_table',
    # )

    # run_quality_checks = DataQualityOperator(
    #     task_id='Run_data_quality_checks',
    # )

    stop_operator = DummyOperator(task_id='Stop_execution')

    check_cred = check_creds()

    start_operator >> check_cred >> stop_operator
    
final_project_dag = final_project()
