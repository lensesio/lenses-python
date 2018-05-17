#!/bin/bash

# Wait up to 300 seconds for Lenses to start
for ((i=0;i<60;i++)); do
    docker exec -it lenses-box lenses-cli --user admin --pass admin --host http://localhost:3030 connectors --clusterName dev 2>&1 >/dev/null && break
    sleep 5
done

# Also wait up to 120 seconds more for Connect to start (it is slower than Lenses)
for ((i=0;i<24;i++)); do
    docker exec -it lenses-box curl "http://localhost:8083/connectors" && break
    sleep 5
done
