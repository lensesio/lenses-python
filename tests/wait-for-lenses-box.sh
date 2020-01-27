#!/bin/bash

# Wait up to 300 seconds for Lenses to start
for ((i=0;i<60;i++)); do
    docker exec -i lenses-box lenses-cli --user admin --pass admin --host http://localhost:3030 connectors --cluster-name dev 2>&1 >/dev/null && break
    sleep 5
done

# Also wait up to 240 seconds more for Connect to start (it is slower than Lenses)
for ((i=0;i<48;i++)); do
    docker exec -i lenses-box curl "http://localhost:8083/connectors" && break
    sleep 5
done

# Sleep another 30 seconds to allow us create connectors, rebalance, etc.
sleep "${1}"

# Check the docker logs just in case
echo "=========== Docker Logs ==========="
docker logs lenses-box | tail -n200
