CONNECT=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' kafka-connect)
echo "CONNECT: CONNECT"

docker run --rm -p 8085:8000 \
   -e "CONNECT_URL=http://${CONNECT}:8083" \
   -e PROXY=false \
   landoop/kafka-connect-ui