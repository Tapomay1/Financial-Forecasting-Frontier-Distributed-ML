"""
SPARK STREAMING – Real-Time Banking Transaction Analysis
Covers all Spark Streaming questions from the project.

Two steps:
  1. Run this script (starts the streaming app)
  2. In another terminal, run stream_simulator.py to feed data

Run streaming app:
  docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    /app/spark_streaming/spark_streaming.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
import os

# ─── Spark Session ────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("BankingStreamProcessing") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ─── Define schema ────────────────────────────────────────────────────────────
SCHEMA = StructType([
    StructField("age",       IntegerType(), True),
    StructField("job",       StringType(),  True),
    StructField("marital",   StringType(),  True),
    StructField("education", StringType(),  True),
    StructField("default",   StringType(),  True),
    StructField("balance",   IntegerType(), True),
    StructField("housing",   StringType(),  True),
    StructField("loan",      StringType(),  True),
    StructField("contact",   StringType(),  True),
    StructField("day",       IntegerType(), True),
    StructField("month",     StringType(),  True),
    StructField("duration",  IntegerType(), True),
    StructField("campaign",  IntegerType(), True),
    StructField("pdays",     IntegerType(), True),
    StructField("previous",  IntegerType(), True),
    StructField("poutcome",  StringType(),  True),
    StructField("y",         StringType(),  True),
    StructField("event_time", TimestampType(), True),
])

# ═══════════════════════════════════════════════════════════════════════════════
# STEP A – Pre-train ML model on historical data
# ═══════════════════════════════════════════════════════════════════════════════
print("\n=== Pre-training ML model on historical data ===")

hist_df = spark.read.csv("/data/bank.csv", header=True, inferSchema=True)
hist_df = hist_df.withColumn("pdays",
    F.when(F.col("pdays") == -1, 0).otherwise(F.col("pdays")))

CAT_COLS = ["job", "marital", "education", "default", "housing",
            "loan", "contact", "month", "poutcome"]
NUM_COLS = ["age", "balance", "day", "duration", "campaign", "pdays", "previous"]

indexers   = [StringIndexer(inputCol=c, outputCol=c+"_idx", handleInvalid="keep") for c in CAT_COLS]
encoder    = OneHotEncoder(inputCols=[c+"_idx" for c in CAT_COLS],
                           outputCols=[c+"_ohe" for c in CAT_COLS])
lbl_idx    = StringIndexer(inputCol="y", outputCol="label")
assembler  = VectorAssembler(inputCols=[c+"_ohe" for c in CAT_COLS] + NUM_COLS,
                             outputCol="features_raw")
scaler     = StandardScaler(inputCol="features_raw", outputCol="features",
                            withMean=False, withStd=True)
rf         = RandomForestClassifier(labelCol="label", featuresCol="features",
                                    numTrees=50, maxDepth=5, seed=42)

pipeline = Pipeline(stages=indexers + [encoder, lbl_idx, assembler, scaler, rf])
trained_model = pipeline.fit(hist_df)
print("Model trained successfully.")

# ═══════════════════════════════════════════════════════════════════════════════
# Q1 – Stream Processing and Data Aggregation
# ═══════════════════════════════════════════════════════════════════════════════
# Read streaming CSV files from /data/stream/ directory
stream_dir = "/data/stream"
os.makedirs(stream_dir.replace("/data", "/tmp/data"), exist_ok=True)

stream_df = spark \
    .readStream \
    .schema(SCHEMA) \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 1) \
    .csv(stream_dir)

# Q1 – Avg balance and duration aggregated by job
agg_query = stream_df \
    .groupBy("job") \
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.round(F.avg("duration"), 2).alias("avg_duration"),
        F.count("*").alias("transaction_count")
    ) \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .queryName("job_aggregation") \
    .start()

print("Q1 – Real-time aggregation by job started.")

# ═══════════════════════════════════════════════════════════════════════════════
# Q2 – Real-Time Model Predictions
# ═══════════════════════════════════════════════════════════════════════════════
# Apply trained model to stream (batch-mode transform within foreachBatch)
def predict_batch(batch_df, epoch_id):
    if batch_df.count() == 0:
        return
    batch_df = batch_df.withColumn("pdays",
        F.when(F.col("pdays") == -1, 0).otherwise(F.col("pdays")))
    predictions = trained_model.transform(batch_df)
    print(f"\n--- Epoch {epoch_id}: Real-Time Predictions ---")
    predictions.select(
        "age", "job", "balance", "duration",
        F.col("prediction").cast("int").alias("pred_subscribe"),
        F.round(F.element_at(F.col("probability"), 2), 3).alias("confidence")
    ).show(10, truncate=False)

predict_query = stream_df \
    .writeStream \
    .foreachBatch(predict_batch) \
    .trigger(processingTime="5 seconds") \
    .queryName("real_time_predictions") \
    .start()

print("Q2 – Real-time predictions started.")

# ═══════════════════════════════════════════════════════════════════════════════
# Q3 – Window Operations and Trend Analysis
# ═══════════════════════════════════════════════════════════════════════════════
# Add synthetic event_time if not present
stream_with_time = stream_df.withColumn(
    "event_time",
    F.coalesce(F.col("event_time"), F.current_timestamp())
)

window_query = stream_with_time \
    .groupBy(
        F.window(F.col("event_time"), "1 minute", "10 seconds")
    ) \
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(F.avg("balance"), 2).alias("avg_balance")
    ) \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .queryName("window_trends") \
    .start()

print("Q3 – Window operations (1 min / 10 sec slide) started.")

# ═══════════════════════════════════════════════════════════════════════════════
# Q4 – Handling Late and Out-of-Order Data (Watermarking)
# ═══════════════════════════════════════════════════════════════════════════════
watermark_query = stream_with_time \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(
        F.window(F.col("event_time"), "1 minute", "30 seconds"),
        F.col("job")
    ) \
    .agg(
        F.count("*").alias("count"),
        F.round(F.avg("balance"), 2).alias("avg_balance")
    ) \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .queryName("watermark_late_data") \
    .start()

print("Q4 – Watermark (30s) for late/out-of-order data started.")
print("\n=== All streaming queries running. Press Ctrl-C to stop. ===")
print("=== Run stream_simulator.py in another terminal to feed data. ===")

# Wait for all streams
spark.streams.awaitAnyTermination()
