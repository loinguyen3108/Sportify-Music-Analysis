#!/bin/bash

jars=("https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar" "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar" "https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.41.1.jar")

jar_folder=containers/jars

for jar in ${jars[@]}; 
do
    echo "Downloading jar: ${jar}"
    # check jar already exists
    if [ -f "${jar_folder}/$(basename ${jar})" ]; then
        echo "$(basename ${jar}) already exists"
        continue
    fi
    wget ${jar} -P ${jar_folder}
done

echo "Done"