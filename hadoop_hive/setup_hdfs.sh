#!/bin/bash
# HDFS Setup Script — run after: docker compose up -d
# Usage: bash hadoop_hive/setup_hdfs.sh

set -e

echo "=== Waiting for NameNode to leave SafeMode (up to 3 min) ==="
for i in $(seq 1 36); do
    STATUS=$(docker exec namenode hdfs dfsadmin -safemode get 2>&1 || echo "not_ready")
    if echo "$STATUS" | grep -q "Safe mode is OFF"; then
        echo "NameNode is ready!"
        break
    fi
    echo "  [$i/36] Not ready yet ($STATUS). Waiting 5s..."
    sleep 5
done

echo ""
echo "=== Creating HDFS directories ==="
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/banking
docker exec namenode hdfs dfs -chmod -R 777 /user

echo ""
echo "=== Uploading bank.csv to HDFS ==="
docker cp data/bank.csv namenode:/tmp/bank.csv
docker exec namenode hdfs dfs -put -f /tmp/bank.csv /user/hadoop/banking/bank.csv
docker exec namenode hdfs dfs -put -f /tmp/bank.csv /user/hive/bank.csv

echo ""
echo "=== Verifying ==="
docker exec namenode hdfs dfs -ls /user/hadoop/banking/
docker exec namenode hdfs dfs -ls /user/hive/

echo ""
echo "=== Done! ==="
echo "To run Hive queries:"
echo "  docker exec -it hive beeline -u 'jdbc:hive2://localhost:10000'"
echo "  Then run: !run /data/hive_queries.sql"
