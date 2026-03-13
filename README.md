# Financial Forecasting Frontier: Distributed ML
## Setup & Execution Guide

---

## DOCKER IMAGES USED (all stable official images)

| Service      | Image                                      | Source          |
|--------------|--------------------------------------------|-----------------|
| Spark        | spark:3.5.0-scala2.12-java17-ubuntu        | Docker Official |
| Jupyter      | jupyter/pyspark-notebook:latest            | Jupyter Project |
| Hadoop       | apache/hadoop:3                            | Apache Official |
| Hive         | apache/hive:4.0.0                          | Apache Official |
| PostgreSQL   | postgres:15-alpine                         | Docker Official |

---

## STEP 0 — PREREQUISITES

Install **Docker Desktop** from https://www.docker.com/products/docker-desktop

Open Docker Desktop → Settings → Resources:
- Memory: **8 GB minimum** (10 GB recommended)
- CPUs: 4
- Click Apply & Restart

---

## STEP 1 — START THE CLUSTER

Open a terminal **inside the banking_project folder**:

```bash
docker compose up -d
```

Wait for all containers to start (~3-5 min first time, downloads ~4 GB).

Check status:
```bash
docker compose ps
```

Open Web UIs:
- Spark Master:  http://localhost:8080
- HDFS UI:       http://localhost:9870
- Hive UI:       http://localhost:10002
- Jupyter:       http://localhost:8888  (token: banking123)

---

## STEP 2 — HDFS SETUP

Wait ~60 seconds after `docker compose up`, then:

```bash
bash hadoop_hive/setup_hdfs.sh
```

---

## STEP 3 — HIVE QUERIES

```bash
# Copy SQL file into container
docker cp data/hive_queries.sql hive:/data/hive_queries.sql

# Open Hive shell
docker exec -it hive beeline -u 'jdbc:hive2://localhost:10000'

# Inside beeline, run all queries:
!run /data/hive_queries.sql

# Exit
!quit
```

---

## STEP 4 — SPARK DATA PROCESSING

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/spark_processing/spark_processing.py
```

---

## STEP 5 — SPARK ML

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/spark_ml/spark_ml.py
```

---

## STEP 6 — SPARK STREAMING

**Terminal 1:**
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/spark_streaming/spark_streaming.py
```

**Terminal 2 (new window):**
```bash
docker exec spark-master python3 /app/spark_streaming/stream_simulator.py
```

---

## STEP 7 — DATA PARALLELISM

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/data_parallelism/data_parallelism.py
```

---

## STEP 8 — MAPREDUCE

**Local Python (no Hadoop needed):**
```bash
python mapreduce/mr_runner.py
```

**OR via Hadoop Streaming:**
```bash
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/output
docker cp mapreduce/mr_runner.py namenode:/tmp/mr_runner.py
# Run each MR job - see generate_mr_scripts.py for Hadoop Streaming commands
```

---

## STOP THE CLUSTER

```bash
docker compose stop          # pause (keeps data)
docker compose down -v       # full reset (deletes volumes)
```

---

## TROUBLESHOOTING

| Error | Fix |
|-------|-----|
| `image not found` for bitnami/spark | You have an old docker-compose.yml — use the latest one (uses `spark:3.5.0-...`) |
| NameNode SafeMode won't exit | Run `docker exec namenode hdfs dfsadmin -safemode forceExit` |
| Hive connection refused | Wait 2 min for Hive to initialise, retry |
| Spark OOM | Set Docker memory to 10 GB in Docker Desktop Settings |
| Port in use | Edit the left side of the port mapping in docker-compose.yml |
