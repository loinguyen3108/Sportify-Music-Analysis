from decouple import config

BROKER_URL = config('BROKER_URL', default='localhost:9092')
SCHEMA_REGISTRY = config('SCHEMA_REGISTRY', default='http://localhost:8081')
KAFKA_REST_PROXY = config('KAFKA_REST_PROXY', default='http://localhost:8082')
