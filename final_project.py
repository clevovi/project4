from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries


default_args = {
    'owner': 'admin',
    'email_on_retry': False,
    'retries': 3,
    'retry_delay':timedelta(minutes=5),
    'start_date': pendulum.now(),
    'catchup':False,
    'depends_on_past':False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        s3_key="log-data",
        aws_region="us-west-2",
        jsonpath="s3://udacity-dend/log_json_path.json",
        table_name="staging_events"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        s3_key="song-data/A/C/A/",
        aws_region="us-west-2",
        jsonpath="auto",
        table_name="staging_songs"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        sql_query=SqlQueries.songplay_table_insert,
        table="songplays"
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        load_mode="truncate_insert",
        sql_query=SqlQueries.user_table_insert,
        table="users"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        load_mode="truncate_insert",
        sql_query=SqlQueries.song_table_insert,
        table="songs"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        load_mode="truncate_insert",
        sql_query=SqlQueries.artist_table_insert,
        table="artists"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        load_mode="truncate_insert",
        sql_query=SqlQueries.time_table_insert,
        table="time"
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables=["artists", "songplays", "songs", "time", "users"]
    )
    end_operator = DummyOperator(task_id="End_execution")

# first stage data and only then load tables
    start_operator >> stage_events_to_redshift >> load_songplays_table
    start_operator >> stage_songs_to_redshift >> load_songplays_table

#first load tables and only then user, song artist and time dimensions, following with quality checks and ending with end_operator
    load_songplays_table >> load_user_dimension_table >> run_quality_checks >> end_operator
    load_songplays_table >> load_song_dimension_table >> run_quality_checks >> end_operator
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks >> end_operator
    load_songplays_table >> load_time_dimension_table >> run_quality_checks >> end_operator


final_project_dag = final_project()