from decouple import config

# Data Lake
DATA_LAKE_BUCKET = config('DATA_LAKE_BUCKET')
GCP_PROJECT_ID = config('GCP_PROJECT_ID')
GOOGLE_APPLICATION_CREDENTIALS = config('GOOGLE_APPLICATION_CREDENTIALS')
TEMPORARY_DWH_BUCKET = config('TEMPORARY_DWH_BUCKET')

# Postgres
SPARK_PG_URI = config(
    'SPARK_PG_URI', default='jdbc:postgresql://pg-spotify:5432/spotify')
SPARK_PG_USER = config('SPARK_PG_USER', default='spotify')
SPARK_PG_PASSWORD = config('SPARK_PG_PASSWORD', default='spotify')
JDBC_POSTGRESQL_DRIVER = 'org.postgresql.Driver'
JDBC_FETCH_SIZE = config('JDBC_FETCH_SIZE', default=10000)
