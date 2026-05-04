"""
PySpark Banking Data Pipeline
Refactored Version: Improved readability, structure, and comments
"""

# ─────────────────────────────────────────────────────────────────────────────
# Imports
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder,
    VectorAssembler, StandardScaler
)
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

import psutil
import time
import threading


# ─────────────────────────────────────────────────────────────────────────────
# Spark Session Initialization
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session():
    """Create and configure Spark session."""
    spark = (
        SparkSession.builder
        .appName("BankingDataParallelism")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


spark = create_spark_session()


# ─────────────────────────────────────────────────────────────────────────────
# Q1: Data Preparation & Partitioning
# ─────────────────────────────────────────────────────────────────────────────
def prepare_data(spark):
    """Load dataset and apply partitioning strategy."""
    print("\n" + "=" * 60)
    print("Q1 – Data Preparation & Partitioning")
    print("=" * 60)

    df = spark.read.csv("/data/bank.csv", header=True, inferSchema=True)

    print(f"\nInitial partitions: {df.rdd.getNumPartitions()}")

    # Repartition by 'job' column to optimize grouped operations
    df_partitioned = df.repartition(8, F.col("job"))

    print(f"Partitions after repartition: {df_partitioned.rdd.getNumPartitions()}")

    # Display distribution across partitions
    partition_stats = (
        df_partitioned.rdd
        .mapPartitionsWithIndex(lambda idx, rows: [(idx, sum(1 for _ in rows))])
        .toDF(["partition_id", "row_count"])
    )

    print("\nPartition distribution:")
    partition_stats.orderBy("partition_id").show()

    # Cache dataset for reuse
    df_partitioned.cache()
    print("Dataset cached.")

    return df_partitioned


df_partitioned = prepare_data(spark)


# ─────────────────────────────────────────────────────────────────────────────
# Q2: Parallel Data Analysis
# ─────────────────────────────────────────────────────────────────────────────
def perform_analysis(df):
    """Run parallel aggregations and transformations."""
    print("\n" + "=" * 60)
    print("Q2 – Parallel Data Analysis")
    print("=" * 60)

    # Average balance by job
    print("\nAverage balance per job:")
    (
        df.groupBy("job")
        .agg(
            F.round(F.avg("balance"), 2).alias("avg_balance"),
            F.count("*").alias("client_count")
        )
        .orderBy(F.desc("avg_balance"))
        .show()
    )

    # Age grouping function
    @F.udf(StringType())
    def age_band(age):
        if age is None:
            return "unknown"
        lower = (age // 5) * 5
        return f"{lower}-{lower + 4}"

    df_with_age = df.withColumn("age_band", age_band(F.col("age")))

    print("\nTop 5 age groups (loan = yes):")
    (
        df_with_age.filter(F.col("loan") == "yes")
        .groupBy("age_band")
        .agg(
            F.round(F.avg("balance"), 2).alias("avg_balance"),
            F.count("*").alias("loan_count")
        )
        .orderBy(F.desc("avg_balance"))
        .limit(5)
        .show()
    )


perform_analysis(df_partitioned)


# ─────────────────────────────────────────────────────────────────────────────
# Q3: Machine Learning Pipeline
# ─────────────────────────────────────────────────────────────────────────────
def train_model(df):
    """Build and train ML pipeline."""
    print("\n" + "=" * 60)
    print("Q3 – Model Training")
    print("=" * 60)

    categorical_cols = [
        "job", "marital", "education", "default",
        "housing", "loan", "contact", "month", "poutcome"
    ]

    numerical_cols = [
        "age", "balance", "day", "duration",
        "campaign", "pdays", "previous"
    ]

    # Fix invalid pdays values
    df = df.withColumn(
        "pdays",
        F.when(F.col("pdays") == -1, 0).otherwise(F.col("pdays"))
    )

    # Pipeline stages
    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
        for col in categorical_cols
    ]

    encoder = OneHotEncoder(
        inputCols=[f"{col}_idx" for col in categorical_cols],
        outputCols=[f"{col}_ohe" for col in categorical_cols]
    )

    label_indexer = StringIndexer(inputCol="y", outputCol="label")

    assembler = VectorAssembler(
        inputCols=[f"{col}_ohe" for col in categorical_cols] + numerical_cols,
        outputCol="features_raw"
    )

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=False,
        withStd=True
    )

    classifier = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=100,
        maxDepth=5,
        seed=42
    )

    pipeline = Pipeline(
        stages=indexers + [encoder, label_indexer, assembler, scaler, classifier]
    )

    # Train-test split
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_df = train_df.repartition(8)

    print(f"Training partitions: {train_df.rdd.getNumPartitions()}")

    # Train model
    start = time.time()
    model = pipeline.fit(train_df)
    duration = time.time() - start

    print(f"Training completed in {duration:.2f} seconds")

    # Evaluate
    predictions = model.transform(test_df)

    evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction"
    )

    auc = evaluator.evaluate(predictions)
    print(f"Model AUC: {auc:.4f}")


train_model(df_partitioned)


# ─────────────────────────────────────────────────────────────────────────────
# Q4: Resource Monitoring
# ─────────────────────────────────────────────────────────────────────────────
def monitor_resources(df):
    """Track CPU and memory usage during Spark job."""
    print("\n" + "=" * 60)
    print("Q4 – Resource Monitoring")
    print("=" * 60)

    cpu_usage = []
    memory_usage = []
    stop_event = threading.Event()

    def track():
        while not stop_event.is_set():
            cpu_usage.append(psutil.cpu_percent(interval=1))
            memory_usage.append(psutil.virtual_memory().percent)

    thread = threading.Thread(target=track, daemon=True)
    thread.start()

    # Heavy aggregation task
    (
        df.groupBy("job", "education")
        .agg(F.avg("balance"), F.count("*"))
        .collect()
    )

    stop_event.set()
    thread.join()

    print("\nResource Usage:")
    print(f"CPU  -> avg: {sum(cpu_usage)/len(cpu_usage):.1f}% | max: {max(cpu_usage):.1f}%")
    print(f"RAM  -> avg: {sum(memory_usage)/len(memory_usage):.1f}% | max: {max(memory_usage):.1f}%")


monitor_resources(df_partitioned)


# ─────────────────────────────────────────────────────────────────────────────
# Q5: Parallel Task Execution
# ─────────────────────────────────────────────────────────────────────────────
def run_parallel_tasks(df):
    """Execute multiple Spark actions in parallel using threads."""
    print("\n" + "=" * 60)
    print("Q5 – Parallel Task Execution")
    print("=" * 60)

    def avg_balance_task():
        value = df.agg(F.avg("balance")).collect()[0][0]
        print(f"[Task-1] Avg Balance: {value:.2f}")

    def subscription_rate_task():
        total = df.count()
        subscribed = df.filter(F.col("y") == "yes").count()
        rate = (subscribed / total) * 100
        print(f"[Task-2] Subscription Rate: {rate:.2f}%")

    def default_count_task():
        count = df.filter(F.col("default") == "yes").count()
        print(f"[Task-3] Default Count: {count}")

    threads = [
        threading.Thread(target=avg_balance_task),
        threading.Thread(target=subscription_rate_task),
        threading.Thread(target=default_count_task)
    ]

    print("\nRunning tasks in parallel...\n")

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print("\nAll tasks completed.")


run_parallel_tasks(df_partitioned)


# ─────────────────────────────────────────────────────────────────────────────
# Cleanup
# ─────────────────────────────────────────────────────────────────────────────
df_partitioned.unpersist()
spark.stop()

print("\n=== Pipeline Execution Completed ===")