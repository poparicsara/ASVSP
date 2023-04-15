#!bin/bash

docker exec -it resourcemanager bash -c "hdfs dfs -rm -r -f /rawZone*"
docker exec -it resourcemanager bash -c "hdfs dfs -rm -r -f /transformationZone*"
docker exec -it resourcemanager bash -c "chmod +x /rawZone.sh && /rawZone.sh"
