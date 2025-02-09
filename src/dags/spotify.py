from pendulum import datetime
from airflow.decorators import task_group
from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

from src.pipelines.constants import QUERY_DIR

TRANSFORM_JARS = ','.join([
    '/home/airflow/jars/gcs-connector-hadoop3-latest.jar',
    '/home/airflow/jars/spark-3.5-bigquery-0.41.1.jar'
])

with DAG(
        dag_id='spotify_etl',
        schedule_interval='@daily',
        start_date=datetime(2025, 1, 1),
        catchup=False,
        template_searchpath=QUERY_DIR
) as dag:
    ingest_raw_data = SparkSubmitOperator(
        task_id='ingest_raw_data',
        conn_id='spark_conn_id',
        application='/opt/airflow/spotify/src/cli/etl.py',
        application_args=['-s', 'ingest'],
        jars=','.join([
            '/home/airflow/jars/gcs-connector-hadoop3-latest.jar',
            '/home/airflow/jars/postgresql-42.7.3.jar'
        ]),
    )

    transform_dimensions = SparkSubmitOperator(
        task_id='transform_dimensions',
        conn_id='spark_conn_id',
        application='/opt/airflow/spotify/src/cli/etl.py',
        application_args=['-s', 'transform', '-p', 'dim'],
        jars=TRANSFORM_JARS
    )

    @task_group(group_id='load_dimensions')
    def load_dimensions():

        load_dim_album = BigQueryInsertJobOperator(
            task_id='load_dim_album',
            configuration={
                'query': {
                    'query': "{% include  'load_dim_album.sql' %}",
                    'useLegacySql': False
                }
            }
        )

        load_dim_artist = BigQueryInsertJobOperator(
            task_id='load_dim_artist',
            configuration={
                'query': {
                    'query': "{% include  'load_dim_artist.sql' %}",
                    'useLegacySql': False
                }
            }
        )

        load_dim_genre = BigQueryInsertJobOperator(
            task_id='load_dim_genre',
            configuration={
                'query': {
                    'query': "{% include  'load_dim_genre.sql' %}",
                    'useLegacySql': False
                }
            }
        )

        load_dim_playlist = BigQueryInsertJobOperator(
            task_id='load_dim_playlist',
            configuration={
                'query': {
                    'query': "{% include  'load_dim_playlist.sql' %}",
                    'useLegacySql': False
                }
            }
        )

        load_dim_user = BigQueryInsertJobOperator(
            task_id='load_dim_user',
            configuration={
                'query': {
                    'query': "{% include  'load_dim_user.sql' %}",
                    'useLegacySql': False
                }
            }
        )

        load_dim_track = BigQueryInsertJobOperator(
            task_id='load_dim_track',
            configuration={
                'query': {
                    'query': "{% include  'load_dim_track.sql' %}",
                    'useLegacySql': False
                }
            }
        )

        load_dim_album
        load_dim_artist
        load_dim_genre
        load_dim_playlist
        load_dim_user
        load_dim_track

    transform_facts = SparkSubmitOperator(
        task_id='transform_facts',
        conn_id='spark_conn_id',
        application='/opt/airflow/spotify/src/cli/etl.py',
        application_args=['-s', 'transform', '-p', 'fact'],
        jars=TRANSFORM_JARS
    )

    @task_group(group_id='load_facts')
    def load_facts():

        load_fact_album_track = BigQueryInsertJobOperator(
            task_id='load_fact_album_track',
            configuration={
                'query': {
                    'query': "{% include  'load_fact_album_track.sql' %}",
                    'useLegacySql': False
                }
            }
        )

        load_fact_playlist_track = BigQueryInsertJobOperator(
            task_id='load_fact_playlist_track',
            configuration={
                'query': {
                    'query': "{% include  'load_fact_playlist_track.sql' %}",
                    'useLegacySql': False
                }
            }
        )

        load_album_track_metric = BigQueryInsertJobOperator(
            task_id='load_album_track_metric',
            configuration={
                'query': {
                    'query': "{% include  'load_album_track_metric.sql' %}",
                    'useLegacySql': False
                }
            }
        )

        load_fact_album_track
        load_fact_playlist_track
        load_album_track_metric

    ingest_raw_data >> transform_dimensions >> load_dimensions() \
        >> transform_facts >> load_facts()
