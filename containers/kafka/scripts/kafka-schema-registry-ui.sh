SCHEMA_REGISTRY=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' schema-registry)
echo "SCHEMA_REGISTRY: $SCHEMA_REGISTRY"

docker run --rm -p 8086:8000 \
   -e "SCHEMAREGISTRY_URL=http://${SCHEMA_REGISTRY}:8081" \
   -e ALLOW_GLOBAL=1 \
   -e ALLOW_TRANSITIVE=1 \
   -e ALLOW_DELETION=1 \
   -e READONLY_MODE=1 \
   landoop/schema-registry-ui