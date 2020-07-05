from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.data_quality import DataQualityOperator

from plugins.helpers.sql_queries import SqlQueries
import sys
sys.path.append(os.path.abspath("/home/workspace/airflow/dags/dags"))
from create_tables import *

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'tomiwasalau',
    'start_date': datetime(2020, 7, 4), #datetime.now() - timedelta(seconds=1),
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_retry': False,
    'catchup': False,
}

dag = DAG('udac_example_dag_2',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly') #'@daily'
        
          
# default_args = {
#     'owner': 'udacity',
#     'start_date': datetime(2020, 6, 30), #datetime.now() - timedelta(seconds=1)
#     'end_date': datetime.now()
#     #'retries': 2,
#     #'retry_delay': timedelta(minutes=1),
#     #'email_on_retry': True
# }

# dag = DAG('udac_example_dag',
#           default_args=default_args,
#           description='Load and transform data in Redshift with Airflow',
#           schedule_interval='@daily'
#           )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.staging_events",
    s3_bucket="udacity-dend",
    copy_json_option="s3://udacity-dend/log_json_path.json",
    s3_key="log_data",
    create_table= create_staging_events_table
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    copy_json_option="auto",
    create_table=create_staging_songs_table   
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songplays",
    create_table=create_songplays_table,
    load_table=SqlQueries.songplay_table_insert,
    drop_if_exist=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.users",
    create_table=create_users_table,
    load_table=SqlQueries.user_table_insert,
    drop_if_exist=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songs",
    create_table=create_songs_table,
    load_table=SqlQueries.song_table_insert,
    drop_if_exist=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.artists",
    create_table=create_artists_table,
    load_table=SqlQueries.artist_table_insert,
    drop_if_exist=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.dimTime",
    create_table=create_time_table,
    load_table=SqlQueries.time_table_insert,
    drop_if_exist=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    sql_tests=SqlQueries.tests,
    expected_results = SqlQueries.results,
    #tables=["public.artists", "public.songs", "public.dimTime", "public.songplays"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

