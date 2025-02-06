# Spotify-Music-Analysis

```
spark-submit --master spark://spark:7077 --jars /opt/bitnami/spark/additional_jars/postgresql-42.7.3.jar,/opt/bitnami/spark/additional_jars/gcs-connector-hadoop3-latest.jar /usr/local/spotify/src/cli/etl.py -s ingest
```
