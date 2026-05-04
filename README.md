# 🚀 Financial Forecasting Frontier: Distributed ML

### Setup & Execution Guide

---

## 🧱 Tech Stack (Docker Images)

All services use stable, official images:

| Service    | Image                               | Source          |
| ---------- | ----------------------------------- | --------------- |
| Spark      | spark:3.5.0-scala2.12-java17-ubuntu | Docker Official |
| Jupyter    | jupyter/pyspark-notebook:latest     | Jupyter Project |
| Hadoop     | apache/hadoop:3                     | Apache Official |
| Hive       | apache/hive:4.0.0                   | Apache Official |
| PostgreSQL | postgres:15-alpine                  | Docker Official |

---

## ⚙️ Prerequisites

Install Docker Desktop:
👉 https://www.docker.com/products/docker-desktop

### Recommended Docker Settings

* Memory: **8 GB minimum** (10 GB recommended)
* CPUs: **4**
* Apply changes and restart Docker

---

## 🟢 Step 1 — Start the Cluster

Navigate to the project directory:

```bash
cd banking_project
docker compose up -d
```

⏳ First run may take 3–5 minutes (downloads ~4GB).

### Verify containers

```bash
docker compose ps
```

### Access Web Interfaces

* Spark Master → http://localhost:8080
* HDFS UI → http://localhost:9870
* Hive UI → http://localhost:10002
* Jupyter Notebook → http://localhost:8888

  * Token: `banking123`

---

## 🗂️ Step 2 — HDFS Setup

Wait ~60 seconds after cluster startup:

```bash
bash hadoop_hive/setup_hdfs.sh
```

---

## 🐝 Step 3 — Run Hive Queries

```bash
docker cp data/hive_queries.sql hive:/data/hive_queries.sql

docker exec -it hive beeline -u 'jdbc:hive2://localhost:10000'

!run /data/hive_queries.sql
!quit
```

---

## ⚡ Step 4 — Spark Data Processing

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/spark_processing/spark_processing.py
```

---

## 🤖 Step 5 — Spark ML Pipeline

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/spark_ml/spark_ml.py
```

---

## 📡 Step 6 — Spark Streaming

### Terminal 1 (Streaming App)

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/spark_streaming/spark_streaming.py
```

### Terminal 2 (Data Generator)

```bash
docker exec spark-master python3 /app/spark_streaming/stream_simulator.py
```

---

## ⚙️ Step 7 — Data Parallelism

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/data_parallelism/data_parallelism.py
```

---

## 🧮 Step 8 — MapReduce

### Option 1: Local Execution (Recommended)

```bash
python mapreduce/mr_runner.py
```

### Option 2: Hadoop Streaming

```bash
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/output
docker cp mapreduce/mr_runner.py namenode:/tmp/mr_runner.py
```

Refer to `generate_mr_scripts.py` for detailed Hadoop streaming commands.

---

## 🛑 Stopping the Cluster

```bash
docker compose stop        # Pause (keeps data)
docker compose down -v     # Full reset (removes volumes)
```

---

## 🧰 Troubleshooting Guide

| Issue                      | Solution                                                 |
| -------------------------- | -------------------------------------------------------- |
| Spark image not found      | Ensure latest `docker-compose.yml` is used               |
| NameNode stuck in SafeMode | `docker exec namenode hdfs dfsadmin -safemode forceExit` |
| Hive connection refused    | Wait ~2 minutes, then retry                              |
| Spark Out of Memory        | Increase Docker memory to 10 GB                          |
| Port already in use        | Modify port mapping in `docker-compose.yml`              |

---

## 📌 Notes

* First run is slow due to image downloads.
* Streaming requires **two terminals** (app + simulator).
* Ensure Docker has enough memory before running Spark ML.

---

## ✅ Project Workflow Summary

1. Start cluster
2. Load data into HDFS
3. Run Hive analytics
4. Execute Spark batch processing
5. Train ML models
6. Run streaming pipeline
7. Test MapReduce jobs

---

## 🎯 You're Ready!

Your distributed ML pipeline is now fully operational 🚀
