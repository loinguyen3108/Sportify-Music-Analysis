curl -X POST \
  'http://localhost:8080/api/v1/connections' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -u 'admin:admin' \
  -d '{
      "conn_type": "spark",
      "connection_id": "spark_conn_id",
      "description": "",
      "host": "spark://spark-master",
      "login": "",
      "port": 7077,
      "schema": ""
    }'


curl -X POST \
  'http://localhost:8080/api/v1/connections' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -u 'admin:admin' \
  -d '{
      "conn_type": "sqlite",
      "connection_id": "clickhouse_conn_id",
      "description": "",
      "host": "clickhouse",
      "login": "",
      "port": 9000,
      "schema": "nyc_taxi"
    }'