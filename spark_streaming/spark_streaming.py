#!/usr/bin/env python3
"""
SPARK STREAMING – Real-Time Banking Analytics (Refactored)

Features:
- Stream ingestion
- Real-time aggregation
- ML predictions on streaming data
- Window-based analytics
- Watermark handling for late data
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder,
    VectorAssembler, StandardScaler
)
from pyspark.ml.classification import RandomForestClassifier
import os


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
DATA_PATH = "/data/bank.csv"
STREAM_DIR = "/data/stream"
CHECKPOINT_DIR = "/tmp/checkpoint"

CAT_COLS = [
    "job", "marital", "education", "default",
    "housing", "loan", "contact", "month", "poutcome"
]

NUM_COLS = [
    "age", "balance", "day", "duration",
    "campaign", "pdays", "previous"
]


# ─────────────────────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────────────────────
def create_spark():
    spark = (
        SparkSession.builder
        .appName("BankingStreamProcessing")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────────────────────────────────────
def get_schema():
    return StructType([
        StructField("age", IntegerType()),
        StructField("job", StringType()),
        StructField("marital", StringType()),
        StructField("education", StringType()),
        StructField("default", StringType()),
        StructField("balance", IntegerType()),
        StructField("housing", StringType()),
        StructField("loan", StringType()),
        StructField("contact", StringType()),
        StructField("day", IntegerType()),
        StructField("month", StringType()),
        StructField("duration", IntegerType()),
        StructField("campaign", IntegerType()),
        StructField("pdays", IntegerType()),
        StructField("previous", IntegerType()),
        StructField("poutcome", StringType()),
        StructField("y", StringType()),
        StructField("event_time", TimestampType())
    ])


# ─────────────────────────────────────────────────────────────────────────────
# MODEL TRAINING (BATCH)
# ─────────────────────────────────────────────────────────────────────────────
def train_model(spark):
    print("\n=== Training ML model (batch mode) ===")

    df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)

    # Fix pdays
    df = df.withColumn(
        "pdays",
        F.when(F.col("pdays") == -1, 0).otherwise(F.col("pdays"))
    )

    # Pipeline
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in CAT_COLS
    ]

    encoder = OneHotEncoder(
        inputCols=[f"{c}_idx" for c in CAT_COLS],
        outputCols=[f"{c}_ohe" for c in CAT_COLS]
    )

    label_indexer = StringIndexer(inputCol="y", outputCol="label")

    assembler = VectorAssembler(
        inputCols=[f"{c}_ohe" for c in CAT_COLS] + NUM_COLS,
        outputCol="features_raw"
    )

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=False,
        withStd=True
    )

    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=50,
        maxDepth=5,
        seed=42
    )

    pipeline = Pipeline(
        stages=indexers + [encoder, label_indexer, assembler, scaler, rf]
    )

    model = pipeline.fit(df)
    print("Model training complete.")

    return model


# ─────────────────────────────────────────────────────────────────────────────
# STREAM SOURCE
# ─────────────────────────────────────────────────────────────────────────────
def create_stream(spark):
    os.makedirs(STREAM_DIR.replace("/data", "/tmp/data"), exist_ok=True)

    return (
        spark.readStream
        .schema(get_schema())
        .option("header", "true")
        .option("maxFilesPerTrigger", 1)
        .csv(STREAM_DIR)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Q1: REAL-TIME AGGREGATION
# ─────────────────────────────────────────────────────────────────────────────
def start_aggregation_query(stream_df):
    print("Starting real-time aggregation...")

    return (
        stream_df.groupBy("job")
        .agg(
            F.round(F.avg("balance"), 2).alias("avg_balance"),
            F.round(F.avg("duration"), 2).alias("avg_duration"),
            F.count("*").alias("transaction_count")
        )
        .writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="5 seconds")
        .queryName("job_aggregation")
        .start()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Q2: REAL-TIME ML PREDICTIONS
# ─────────────────────────────────────────────────────────────────────────────
def start_prediction_query(stream_df, model):

    def predict_batch(batch_df, epoch_id):
        if batch_df.count() == 0:
            return

        batch_df = batch_df.withColumn(
            "pdays",
            F.when(F.col("pdays") == -1, 0).otherwise(F.col("pdays"))
        )

        preds = model.transform(batch_df)

        print(f"\n--- Batch {epoch_id} Predictions ---")
        preds.select(
            "age", "job", "balance",
            F.col("prediction").cast("int").alias("prediction"),
            F.round(F.element_at("probability", 2), 3).alias("confidence")
        ).show(10, truncate=False)

    return (
        stream_df.writeStream
        .foreachBatch(predict_batch)
        .trigger(processingTime="5 seconds")
        .queryName("ml_predictions")
        .start()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Q3: WINDOW ANALYTICS
# ─────────────────────────────────────────────────────────────────────────────
def start_window_query(stream_df):
    print("Starting window analytics...")

    df = stream_df.withColumn(
        "event_time",
        F.coalesce(F.col("event_time"), F.current_timestamp())
    )

    return (
        df.groupBy(F.window("event_time", "1 minute", "10 seconds"))
        .agg(
            F.count("*").alias("count"),
            F.round(F.avg("balance"), 2).alias("avg_balance")
        )
        .writeStream
        .outputMode("update")
        .format("console")
        .trigger(processingTime="10 seconds")
        .queryName("window_analysis")
        .start()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Q4: WATERMARK HANDLING
# ─────────────────────────────────────────────────────────────────────────────
def start_watermark_query(stream_df):
    print("Starting watermark handling...")

    df = stream_df.withColumn(
        "event_time",
        F.coalesce(F.col("event_time"), F.current_timestamp())
    )

    return (
        df.withWatermark("event_time", "30 seconds")
        .groupBy(
            F.window("event_time", "1 minute", "30 seconds"),
            "job"
        )
        .agg(
            F.count("*").alias("count"),
            F.round(F.avg("balance"), 2).alias("avg_balance")
        )
        .writeStream
        .outputMode("update")
        .format("console")
        .trigger(processingTime="10 seconds")
        .queryName("watermark_query")
        .start()
    )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    spark = create_spark()

    model = train_model(spark)
    stream_df = create_stream(spark)

    q1 = start_aggregation_query(stream_df)
    q2 = start_prediction_query(stream_df, model)
    q3 = start_window_query(stream_df)
    q4 = start_watermark_query(stream_df)

    print("\n=== All streaming queries started ===")
    print("Run stream_simulator.py to feed data")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()