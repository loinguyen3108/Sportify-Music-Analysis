from datetime import date
from functools import cached_property
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

from src.configs.spark import BQ_PROJECT_ID, BQ_DATASET_ID, DATA_LAKE_BUCKET, \
    GCP_PROJECT_ID, GOOGLE_APPLICATION_CREDENTIALS, JDBC_FETCH_SIZE, JDBC_POSTGRESQL_DRIVER
from src.helpers.gcs import GCSHelper


class SparkApp:
    DEFAULT_PARTITION_BY = ['extract_year', 'extract_month', 'extract_day']

    @cached_property
    def gcs_helper(self):
        return GCSHelper(DATA_LAKE_BUCKET)

    def __init__(self, packages: List[str] = None):
        self.spark = self.init_spark(packages)
        self._config_gcs()

    def init_spark(self, packages: List[str] = None):
        builder = SparkSession.builder.appName(self.__class__.__name__)
        if packages:
            builder.config('spark.jars.packages', ','.join(packages))
        return builder.getOrCreate()

    def _config_gcs(self):
        conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        conf.set('fs.gs.impl',
                 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
        conf.set('fs.AbstractFileSystem.gs.impl',
                 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS')
        conf.set('fs.gs.project.id', GCP_PROJECT_ID)
        conf.set('fs.gs.auth.service.account.enable', 'true')
        conf.set('fs.gs.auth.service.account.json.keyfile',
                 GOOGLE_APPLICATION_CREDENTIALS)
        conf.set('mapreduce.fileoutputcommitter.marksuccessfuljobs', 'false')
        conf.set('fs.gs.impl',
                 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')

    def extract_from_bigquery(self, table_name: str) -> DataFrame:
        return self.spark.read \
            .format('bigquery') \
            .option('table', f'{BQ_PROJECT_ID}:{BQ_DATASET_ID}.{table_name}') \
            .load()

    def extract_from_gcs(self, bucket_name: str, table_name: str, schema: StructType) -> DataFrame:
        """Extract data from GCS

        Args:
            table_name (str): The name of table
            schema (StructType): The schema of table

        Returns:
            DataFrame: Spark DataFrame
        """
        return self.spark.read \
            .option('header', True) \
            .schema(schema) \
            .parquet(f'gs://{bucket_name}/{table_name}')

    def extract_from_postgres(
        self, url: str, db_username: str, db_password, schema_name: str, table_name: str,
        incremental_col: str = 'updated_at', extract_date: date = date.today(),
        df_schema: StructType = None
    ) -> DataFrame:
        """Extract data from Postgres with JDBC by incremental loading or query

        Args:
            url (str): The url of database
            db_username (str): The username of database
            db_password (str): The password of database
            schema_name (str): The name of schema
            table_name (str): The name of table
            incremental_col (str, optional): The name of time column. Defaults to `updated_at`.
            extract_date (date, optional): The date to extract. Defaults to today().
            df_schema (StructType, optional): The schema of table

        Returns:
            DataFrame
        """
        if not df_schema:
            raise ValueError('df_schema is required')

        query_string = self.get_incremental_query(
            schema_name=schema_name, table_name=table_name,
            incremental_col=incremental_col
        )

        df = self.spark.read.format('jdbc') \
            .option('url', url) \
            .option('user', db_username) \
            .option('password', db_password) \
            .option('dbtable', query_string) \
            .option('driver', JDBC_POSTGRESQL_DRIVER) \
            .option('fetchsize', JDBC_FETCH_SIZE) \
            .schema(df_schema) \
            .load()

        df = df.withColumn('extract_year', func.lit(extract_date.year)) \
            .withColumn('extract_month', func.lit(extract_date.month)) \
            .withColumn('extract_day', func.lit(extract_date.day))
        return df

    def get_incremental_query(self, schema_name: str, table_name: str, incremental_col: str):
        """Generate query to extract by latest date (incremental loading by latest date) or full load

        Args:
            df (DataFrame): Spark DataFrame
            table_name (str): Source table
            incremental_col (str): Incremental column to filter

        Returns:
            str: Query string to extract table
        """
        if not self.gcs_helper.is_exists(table_name):
            return f'(select * from {schema_name}.{table_name}) as tmp'

        df = self.spark.read \
            .option('header', True) \
            .parquet(f'gs://{DATA_LAKE_BUCKET}/{table_name}')

        max_date = df.select(
            func.max(func.to_date(func.col(incremental_col))).alias('max_date')
        ).collect()[0]['max_date']
        return f'(select * from {schema_name}.{table_name} where {incremental_col}::date > \'{max_date}\'::date) as tmp'

    def load_to_bigquery(self, df: DataFrame, dataset_id: str, table_name: str, mode: str = 'overwrite',
                         inter_format: str = 'parquet'):
        """Load data to BigQuery

        Args:
            df (DataFrame): Table is extracted from MSSQL
            dataset_id (str): Dataset ID
            table_name (str): Table Name need load into data lake
            mode (str, optional): Append or overwrite. Defaults to 'append'.
        """
        df.write.format('bigquery') \
            .option('table', f'{dataset_id}.{table_name}') \
            .option('intermediateFormat', inter_format) \
            .mode(mode) \
            .save()
        self.logger.info(
            f'Ingest data success in BigQuery: {BQ_DATASET_ID}.{table_name}')

    def load_to_gcs(self, df: DataFrame, table_name: str, partition_by: List[str] = None, mode: str = 'append'):
        """Load data to GCS

        Args:
            df (DataFrame): Table is extracted from MSSQL
            table_name (str): Table Name need load into data lake
        """
        file_path = f'gs://{DATA_LAKE_BUCKET}/{table_name}'
        partition_by = partition_by or self.DEFAULT_PARTITION_BY
        df.write.option('header', True) \
            .partitionBy(*partition_by) \
            .mode(mode) \
            .parquet(file_path)
        self.logger.info(f'Ingest data success in DataLake: {file_path}')

    def stop(self):
        self.spark.stop()
