CLUSTER_ID='8GugTCcsQJmRy9iPwaa33w' # Replace with your cluster ID
curl --request POST -H 'Content-Type: application/json' --url http://localhost:8082/v3/clusters/${CLUSTER_ID}/topics -d \
  '{
    "topic_name": "public.nyc.weather.hourly",
    "partitions_count": 1,
    "replication_factor": 1,
    "configs": [
        {
            "name": "cleanup.policy",
            "value": "delete"
        },
        {
            "name": "retention.ms",
            "value": 604800000
        }
    ]
  }'