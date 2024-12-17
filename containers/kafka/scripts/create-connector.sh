curl -s -X PUT -H  "Content-Type:application/json" http://localhost:8083/connectors/postgres-cdc/config \
  -d '{
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.user": "debezium",
    "database.dbname": "nyc",
    "topic.prefix": "public.nyc.trip",
    "tasks.max": "1",
    "database.hostname": "nyc-postgres",
    "database.password": "debezium",
    "table.include.list": "public.hvfh_trip",
    "database.port": "5432",
    "plugin.name": "pgoutput",
    "slot.name" : "hvfh"
  }'