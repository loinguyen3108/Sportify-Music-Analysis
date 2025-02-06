from typing import List

from pyspark.sql.types import StructType

from src.pipelines.schemas.sources import ALBUM, ARTIST, ARTIST_ALBUM, ARTIST_TRACK, PLAYLIST, \
    PLAYLIST_TRACK, TRACK, USER
from src.configs.spark import SPARK_PG_PASSWORD, SPARK_PG_URI, SPARK_PG_USER
from src.configs.logger import get_logger
from src.infratructure.spark.app import SparkApp


class SpotifyIngestor(SparkApp):
    INGESTOR_CONFIG = [
        {'table': 'album', 'incremental_col': 'updated_at', 'schema': ALBUM},
        {'table': 'artist', 'incremental_col': 'updated_at', 'schema': ARTIST},
        {'table': 'artist_album', 'incremental_col': 'created_at',
         'schema': ARTIST_ALBUM},
        {'table': 'artist_track', 'incremental_col': 'updated_at',
            'schema': ARTIST_TRACK},
        {'table': 'playlist', 'incremental_col': 'updated_at', 'schema': PLAYLIST},
        {'table': 'playlist_track', 'incremental_col': 'created_at',
         'schema': PLAYLIST_TRACK},
        {'table': 'track', 'incremental_col': 'updated_at', 'schema': TRACK},
        {'table': 'user', 'incremental_col': 'updated_at', 'schema': USER}
    ]

    def __init__(self, packages: List[str] = None):
        super().__init__(packages)
        self.logger = get_logger(self.__class__.__name__)

    def ingest_raw_data(
            self, url: str, db_username: str, db_password, schema_name: str,
            table_name: str, incremental_col: str = 'updated_at', df_schema: StructType = None
    ):
        self.logger.info(
            f'Ingest raw data from {schema_name}.{table_name} with {incremental_col}')
        raw_df = self.extract_from_postgres(
            url=url, db_username=db_username, db_password=db_password,
            schema_name=schema_name, table_name=table_name, incremental_col=incremental_col,
            df_schema=df_schema
        )

        if raw_df.first() is None:
            self.logger.info(
                f'Not found raw data from {schema_name}.{table_name}')
            return

        self.load_to_gcs(df=raw_df, table_name=table_name)
        self.logger.info(
            f'Ingested {raw_df.count()} rows from {schema_name}.{table_name}')

    def execute(self):
        source_tables = [table['table'] for table in self.INGESTOR_CONFIG]
        self.logger.info(f'Ingest raw data from {source_tables}')
        for config in self.INGESTOR_CONFIG:
            self.ingest_raw_data(
                url=SPARK_PG_URI, db_username=SPARK_PG_USER, db_password=SPARK_PG_PASSWORD,
                schema_name='public', table_name=config['table'],
                incremental_col=config['incremental_col'], df_schema=config['schema']
            )
        self.logger.info(f'Ingested raw data from {source_tables}')
