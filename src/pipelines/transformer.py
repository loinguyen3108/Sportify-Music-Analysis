from datetime import date
from functools import cached_property
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, concat_ws, explode, explode_outer, \
    lit, lower, monotonically_increasing_id, regexp_extract, row_number, when, to_date, \
    date_format
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from src.configs.logger import get_logger
from src.configs.spark import BQ_DATASET_STAGING_ID, DATA_LAKE_BUCKET, TEMPORARY_DWH_BUCKET
from src.infratructure.spark.app import SparkApp
from src.pipelines.constants import *
from src.pipelines.schemas.sources import ALBUM, ARTIST, ARTIST_ALBUM, ARTIST_TRACK, \
    PLAYLIST, PLAYLIST_TRACK, TRACK, USER
from src.utils.common import coalesce_blank_n_null


class SpotifyTransformer(SparkApp):
    DEFAULT_UNKNOWN_INT = -1
    DEFAULT_UNKNOWN_STR = 'unknown'
    GCS_TABLE_PARTITION_PATTERN = '%s/extract_year=%s/extract_month=%s/extract_day=%s'

    def __init__(self, extracted_date: date, packages: List[str] = None):
        super().__init__(packages)
        self.spark.conf.set('temporaryGcsBucket', TEMPORARY_DWH_BUCKET)
        self.spark.conf.set('spark.sql.shuffle.partitions', '5')
        self.spark.conf.set('spark.default.parallelism', '5')
        self.logger = get_logger(self.__class__.__name__)
        self.extracted_date = extracted_date

    @cached_property
    def gcs_helper(self):
        from src.helpers.gcs import GCSHelper
        return GCSHelper(DATA_LAKE_BUCKET)

    def transform_album_release_date(self, album_df: DataFrame) -> DataFrame:
        extracted_release_date = {
            'release_year': regexp_extract(col('release_date'), r'\d{4}', 0),
            'release_month': regexp_extract(col('release_date'), r'\d{4}-(\d{2})', 1),
            'release_day': regexp_extract(col('release_date'), r'\d{4}-\d{2}-(\d{2})', 1),
        }
        coalesced_release_date = {
            'release_year': coalesce_blank_n_null('release_year', '0000'),
            'release_month': coalesce_blank_n_null('release_month', '00'),
            'release_day': coalesce_blank_n_null('release_day', '00')
        }
        made_date_key = concat_ws(
            '', col('release_year'), col('release_month'), col('release_day')).cast('int')
        dropped_cols = ['release_date', 'release_year',
                        'release_month', 'release_day']
        album_df = album_df \
            .withColumns(extracted_release_date) \
            .withColumns(coalesced_release_date) \
            .withColumn('release_date_key', made_date_key) \
            .drop(*dropped_cols)
        return album_df

    def transform_dim_album(self):
        album_df = self.extract_raw_data_by_extracted_date(
            table_name=T_ALBUM, schema=ALBUM, extracted_date=self.extracted_date)
        if self._is_null_or_empty_df(df=album_df):
            self.logger.warning('album_df is empty')
            return

        self.logger.info('Transforming dim_album')
        selected_cols = [
            'album_id', 'name', 'album_type', 'url', 'release_date',
            'release_date_precision', 'cover_image', 'label',
        ]
        converted_album_type_2_code = when(col('album_type') == L_AT_ALBUM, lit(C_AT_ALBUM)) \
            .when(col('album_type') == L_AT_SINGLE, lit(C_AT_SINGLE)) \
            .when(col('album_type') == L_AT_COMPILATION, lit(C_AT_COMPILATION)) \
            .otherwise(lit(self.DEFAULT_UNKNOWN_INT))
        converted_release_date_precision_2_code = when(
            col('release_date_precision') == L_ADP_YEAR, lit(C_ADP_YEAR)) \
            .when(col('release_date_precision') == L_ADP_MONTH, lit(C_ADP_MONTH)) \
            .when(col('release_date_precision') == L_ADP_DAY, lit(C_ADP_DAY)) \
            .otherwise(lit(self.DEFAULT_UNKNOWN_INT))
        coalesced_label = coalesce_blank_n_null(
            'label', self.DEFAULT_UNKNOWN_STR)
        coalesced_cover_image = coalesce_blank_n_null(
            'cover_image', self.DEFAULT_UNKNOWN_STR)
        album_df = album_df.select(selected_cols) \
            .drop_duplicates(subset=['album_id']) \
            .withColumnRenamed('album_id', 'album_key') \
            .withColumns({
                'album_type': converted_album_type_2_code,
                'release_date_precision': converted_release_date_precision_2_code,
                'label': coalesced_label,
                'cover_image': coalesced_cover_image
            })
        album_df = self.transform_album_release_date(album_df)
        self.load_to_bigquery(
            df=album_df, dataset_id=BQ_DATASET_STAGING_ID, table_name=D_ALBUM)

    def transform_dim_artist(self):
        artist_df = self.extract_raw_data_by_extracted_date(
            table_name=T_ARTIST, schema=ARTIST)
        if self._is_null_or_empty_df(df=artist_df):
            self.logger.warning('artist_df is empty')
            return

        self.logger.info('Transforming dim_artist')

        selected_cols = [
            'artist_id', 'name', 'url', 'genres', 'followers_count', 'monthly_listeners']
        coalesced_name = coalesce(col('name'), lit(self.DEFAULT_UNKNOWN_STR))
        coalesced_followers = coalesce(
            col('followers_count'), lit(self.DEFAULT_UNKNOWN_INT))
        coalesced_monthly_listeners = coalesce(
            col('monthly_listeners'), lit(self.DEFAULT_UNKNOWN_INT)
        )
        artist_df = artist_df.select(selected_cols) \
            .drop_duplicates(subset=['artist_id']) \
            .withColumnRenamed('artist_id', 'artist_key') \
            .withColumns({
                'name': coalesced_name,
                'followers_count': coalesced_followers,
                'monthly_listeners': coalesced_monthly_listeners
            })
        self.load_to_bigquery(
            df=artist_df, dataset_id=BQ_DATASET_STAGING_ID, table_name=D_ARTIST, mode='overwrite', inter_format='orc')

    def transform_dim_genre(self):
        """Get genres from artist and load it to dim_genre

        Args:
            artist_df (DataFrame): DataFrame of artist, which contains genres columns
        """
        artist_df = self.extract_raw_data_by_extracted_date(
            table_name=T_ARTIST, schema=ARTIST, extracted_date=self.extracted_date)
        if self._is_null_or_empty_df(df=artist_df):
            self.logger.warning('artist_df is empty')
            return

        self.logger.info('Transforming dim_genre...')
        genres_df = artist_df.select(explode('genres').alias('genres')) \
            .select(lower('genres').alias('genres')) \
            .drop_duplicates(subset=['genres']) \
            .withColumnRenamed('genres', 'name') \
            .withColumn('genre_key', row_number().over(
                Window.orderBy(monotonically_increasing_id()))) \
            .select('genre_key', 'name')
        self.load_to_bigquery(
            df=genres_df, dataset_id=BQ_DATASET_STAGING_ID, table_name=D_GENRE)

    def transform_dim_user(self):
        user_df = self.extract_raw_data_by_extracted_date(
            table_name=T_USER, schema=USER, extracted_date=self.extracted_date)
        if self._is_null_or_empty_df(df=user_df):
            self.logger.warning('user_df is empty')
            return

        self.logger.info('Transforming dim_user')
        selected_cols = ['user_id', 'name', 'url']
        user_df = user_df.select(selected_cols) \
            .drop_duplicates(subset=['user_id']) \
            .withColumnRenamed('user_id', 'user_key')
        self.load_to_bigquery(
            df=user_df, dataset_id=BQ_DATASET_STAGING_ID, table_name=D_USER)

    def transform_dim_playlist(self):
        playlist_df = self.extract_raw_data_by_extracted_date(
            table_name=T_PLAYLIST, schema=PLAYLIST, extracted_date=self.extracted_date)
        if self._is_null_or_empty_df(df=playlist_df):
            self.logger.warning('playlist_df is empty')
            return

        self.logger.info('Transforming dim_playlist')
        selected_cols = [
            'playlist_id', 'name', 'url', 'collaborative', 'user_id', 'public',
            'snapshot_id', 'followers_count'
        ]
        renamed_cols = {
            'playlist_id': 'playlist_key',
            'user_id': 'user_key',
            'public': 'is_public'
        }
        coalesced_followers_count = coalesce(
            col('followers_count'), lit(self.DEFAULT_UNKNOWN_INT))
        playlist_df = playlist_df.select(selected_cols) \
            .drop_duplicates(subset=['playlist_id']) \
            .withColumnsRenamed(renamed_cols) \
            .withColumn('followers_count', coalesced_followers_count)
        self.load_to_bigquery(
            df=playlist_df, dataset_id=BQ_DATASET_STAGING_ID, table_name=D_PLAYLIST)

    def transform_dim_track(self):
        track_df = self.extract_raw_data_by_extracted_date(
            table_name=T_TRACK, schema=TRACK, extracted_date=self.extracted_date)
        if self._is_null_or_empty_df(df=track_df):
            self.logger.warning('track_df is empty')
            return

        self.logger.info('Transforming dim_track')
        selected_cols = [
            'track_id', 'name', 'url', 'album_id', 'disc_number', 'duration_ms',
            'explicit', 'plays_count'
        ]
        renamed_cols = {
            'track_id': 'track_key',
            'album_id': 'album_key',
            'explicit': 'is_explicit'
        }
        coalesced_name = coalesce_blank_n_null(
            'name', self.DEFAULT_UNKNOWN_STR)
        coalesced_plays_count = coalesce(
            col('plays_count'), lit(self.DEFAULT_UNKNOWN_INT))
        coalesceed_disc_number = coalesce(
            col('disc_number'), lit(self.DEFAULT_UNKNOWN_INT))
        coalesced_is_explicit = coalesce(col('is_explicit'), lit(False))
        track_df = track_df.select(selected_cols) \
            .drop_duplicates(subset=['track_id']) \
            .withColumnsRenamed(renamed_cols) \
            .withColumns({
                'name': coalesced_name,
                'plays_count': coalesced_plays_count,
                'disc_number': coalesceed_disc_number,
                'is_explicit': coalesced_is_explicit
            })
        self.load_to_bigquery(
            df=track_df, dataset_id=BQ_DATASET_STAGING_ID, table_name=D_TRACK)

    def transform_fact_album_track(self):
        dim_artist = self.extract_from_bigquery(table_name=D_ARTIST)
        dim_album = self.extract_from_bigquery(table_name=D_ALBUM)
        artist_album_df = self.extract_raw_data_by_extracted_date(
            table_name=T_ARTIST_ALBUM, schema=ARTIST_ALBUM)
        track_df = self.extract_raw_data_by_extracted_date(
            table_name=T_TRACK, schema=TRACK, extracted_date=self.extracted_date)
        self.logger.info('Transforming fact_album_track')
        track_df = track_df.filter(col('created_at').cast('date') == lit(self.extracted_date)) \
            .withColumnsRenamed({'track_id': 'track_key'}) \
            .select('track_key', 'album_id')
        dim_album = dim_album.select('album_key', 'release_date_key')
        artist_album_df = artist_album_df.select('artist_id', 'album_id')
        dim_genre_df = self.extract_from_bigquery(table_name=D_GENRE)

        fact_album_track = track_df.alias('track_df') \
            .join(dim_album.alias('dim_album'), col('track_df.album_id') == col('dim_album.album_key'), 'left') \
            .filter(col('release_date_key').isNotNull()) \
            .join(artist_album_df.alias('artist_album_df'), ['album_id'], 'left') \
            .join(dim_artist.alias('dim_artist'),
                  col('artist_album_df.artist_id') == col('dim_artist.artist_key'), 'left') \
            .filter(col('artist_id').isNotNull()) \
            .select('release_date_key', 'track_key', 'album_key', 'artist_key',
                    explode_outer('genres').alias('genre')) \
            .withColumn('genre', lower('genre')) \
            .withColumn('genre', coalesce_blank_n_null('genre', self.DEFAULT_UNKNOWN_STR))
        self.logger.info(fact_album_track.count())

        joined_col = [fact_album_track['genre'] == dim_genre_df['name']]
        fact_album_track = fact_album_track \
            .join(other=dim_genre_df, on=joined_col, how='left') \
            .select('release_date_key', 'track_key', 'album_key', 'artist_key', 'genre_key') \
            .drop_duplicates()

        self.load_to_bigquery(
            df=fact_album_track, dataset_id=BQ_DATASET_STAGING_ID, table_name=F_ALBUM_TRACK)

    def transform_fact_playlist_track(self):
        dim_artist = self.extract_from_bigquery(table_name=D_ARTIST)
        artist_track_df = self.extract_raw_data_by_extracted_date(
            table_name=T_ARTIST_TRACK, schema=ARTIST_TRACK)
        dim_playlist = self.extract_from_bigquery(table_name=D_PLAYLIST)
        playlist_track_df = self.extract_raw_data_by_extracted_date(
            table_name=T_PLAYLIST_TRACK, schema=PLAYLIST_TRACK,
            extracted_date=self.extracted_date
        )
        self.logger.info('Transforming playlist_track')
        if self._is_null_or_empty_df(df=playlist_track_df):
            self.logger.info('playlist_track is empty')
            return

        playlist_track_df = playlist_track_df.select(
            'playlist_id', 'track_id', 'added_at')
        artist_track_df = artist_track_df.select('artist_id', 'track_id')
        dim_artist = dim_artist.select('artist_key', 'genres')
        dim_playlist = dim_playlist.select('playlist_key', 'user_key')
        dim_genre_df = self.extract_from_bigquery(table_name=D_GENRE)

        fact_playlist_track = playlist_track_df.alias('playlist_track_df') \
            .join(dim_playlist.alias('dim_playlist'),
                  col('playlist_track_df.playlist_id') == col('dim_playlist.playlist_key'), 'left') \
            .join(other=artist_track_df.alias('artist_track_df'), on=['track_id'], how='left') \
            .join(dim_artist.alias('dim_artist'),
                  col('artist_track_df.artist_id') == col('dim_artist.artist_key'), 'left') \
            .filter(col('artist_key').isNotNull()) \
            .select(
                'artist_key', 'track_id', 'playlist_key', 'added_at',
                'user_key', explode_outer('genres').alias('genre')) \
            .withColumn('genre', lower('genre')) \
            .withColumn('genre', coalesce_blank_n_null('genre', self.DEFAULT_UNKNOWN_STR))

        joined_col = [fact_playlist_track['genre'] == dim_genre_df['name']]
        renamed_cols = {
            'added_at': 'added_date_key',
            'track_id': 'track_key'
        }
        fact_playlist_track = fact_playlist_track \
            .join(other=dim_genre_df, on=joined_col, how='left') \
            .select('track_id', 'playlist_key', 'added_at', 'user_key', 'genre_key') \
            .drop_duplicates() \
            .withColumnsRenamed(renamed_cols) \
            .withColumn('date_actual', to_date('added_date_key')) \
            .withColumn('added_date_key', date_format('date_actual', 'yyyyMMdd').cast('int'))
        fact_playlist_track.show()
        self.load_to_bigquery(
            df=fact_playlist_track, dataset_id=BQ_DATASET_STAGING_ID, table_name=F_PLAYLIST_TRACK)

    def extract_raw_data_by_extracted_date(
            self, table_name: str, schema: StructType, extracted_date: date = None
    ) -> DataFrame:
        if extracted_date:
            table_name = self.GCS_TABLE_PARTITION_PATTERN % (
                table_name, extracted_date.year, extracted_date.month, extracted_date.day)
        if not self.gcs_helper.is_exists(table_name):
            return

        return self.extract_from_gcs(
            bucket_name=DATA_LAKE_BUCKET, table_name=table_name, schema=schema)

    def transform_dims(self):
        self.transform_dim_album()
        self.transform_dim_artist()
        self.transform_dim_genre()
        self.transform_dim_playlist()
        self.transform_dim_user()
        self.transform_dim_track()

    def transform_facts(self):
        self.transform_fact_album_track()
        self.transform_fact_playlist_track()

    def _is_null_or_empty_df(self, df: DataFrame) -> bool:
        return df is None or df.isEmpty()
