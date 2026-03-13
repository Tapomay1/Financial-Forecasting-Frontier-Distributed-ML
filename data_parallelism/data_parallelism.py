"""
DATA PARALLELISM – Banking Dataset
Covers all Data Parallelism questions from the project.

Run:
  docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /app/data_parallelism/data_parallelism.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import psutil, time, threading

# ─── Spark Session ────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("BankingDataParallelism") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.default.parallelism", "8") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ═══════════════════════════════════════════════════════════════════════════════
# Q1 – Data Preparation and Partitioning
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q1 – Data Preparation and Partitioning")
print("="*60)

df = spark.read.csv("/data/bank.csv", header=True, inferSchema=True)
print(f"\nDefault partitions after read: {df.rdd.getNumPartitions()}")

# Strategy: repartition by job (natural key) for parallel per-group ops.
# This co-locates rows of the same job on the same partition → reduces shuffle.
df_partitioned = df.repartition(8, F.col("job"))
print(f"Partitions after repartition(8, 'job'): {df_partitioned.rdd.getNumPartitions()}")

# Show rows per partition
rows_per_partition = df_partitioned.rdd \
    .mapPartitionsWithIndex(lambda idx, rows: [(idx, sum(1 for _ in rows))]) \
    .toDF(["partition_id", "row_count"])
print("\n--- Rows per partition ---")
rows_per_partition.orderBy("partition_id").show()

# Cache for repeated access
df_partitioned.cache()
print("DataFrame cached.")

# ═══════════════════════════════════════════════════════════════════════════════
# Q2 – Data Analysis and Processing in Parallel
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q2 – Parallel Data Analysis")
print("="*60)

# Average balance per job (parallelised via repartition above)
print("\n--- Average balance per job (parallel) ---")
df_partitioned.groupBy("job") \
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"),
         F.count("*").alias("client_count")) \
    .orderBy(F.desc("avg_balance")) \
    .show()

# Top 5 age groups with highest loan amounts
# Proxy: age groups × avg balance where loan='yes'
print("\n--- Top 5 age groups by average balance (loan=yes) ---")

@F.udf(StringType())
def age_group(age):
    if age is None: return "unknown"
    lb = (age // 5) * 5
    return f"{lb}-{lb+4}"

df_with_group = df_partitioned.withColumn("age_band", age_group(F.col("age")))
df_with_group.filter(F.col("loan") == "yes") \
    .groupBy("age_band") \
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"),
         F.count("*").alias("loan_holders")) \
    .orderBy(F.desc("avg_balance")) \
    .limit(5) \
    .show()

# ═══════════════════════════════════════════════════════════════════════════════
# Q3 – Model Training on Partitioned Data
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q3 – Model Training on Partitioned Data")
print("="*60)

# Preprocessing
CAT_COLS = ["job", "marital", "education", "default", "housing",
            "loan", "contact", "month", "poutcome"]
NUM_COLS = ["age", "balance", "day", "duration", "campaign", "pdays", "previous"]

df_model = df_partitioned.withColumn("pdays",
    F.when(F.col("pdays") == -1, 0).otherwise(F.col("pdays")))

indexers  = [StringIndexer(inputCol=c, outputCol=c+"_idx", handleInvalid="keep")
             for c in CAT_COLS]
encoder   = OneHotEncoder(inputCols=[c+"_idx" for c in CAT_COLS],
                          outputCols=[c+"_ohe" for c in CAT_COLS])
lbl_idx   = StringIndexer(inputCol="y", outputCol="label")
assembler = VectorAssembler(
    inputCols=[c+"_ohe" for c in CAT_COLS] + NUM_COLS,
    outputCol="features_raw")
scaler    = StandardScaler(inputCol="features_raw", outputCol="features",
                           withMean=False, withStd=True)
rf        = RandomForestClassifier(
    labelCol="label", featuresCol="features",
    numTrees=100, maxDepth=5, seed=42)

pipeline = Pipeline(stages=indexers + [encoder, lbl_idx, assembler, scaler, rf])

train_df, test_df = df_model.randomSplit([0.8, 0.2], seed=42)

# Maintain partition count for parallel training
train_df = train_df.repartition(8)
print(f"Training set partitions: {train_df.rdd.getNumPartitions()}")

print("Training model ...")
t0 = time.time()
model = pipeline.fit(train_df)
print(f"Training complete in {time.time()-t0:.1f}s")

preds = model.transform(test_df)
auc = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction").evaluate(preds)
print(f"Test AUC: {auc:.4f}")

# ═══════════════════════════════════════════════════════════════════════════════
# Q4 – Resource Monitoring
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q4 – Resource Monitoring")
print("="*60)

cpu_samples, mem_samples = [], []
stop_flag = threading.Event()

def monitor():
    while not stop_flag.is_set():
        cpu_samples.append(psutil.cpu_percent(interval=1))
        mem_samples.append(psutil.virtual_memory().percent)

monitor_thread = threading.Thread(target=monitor, daemon=True)
monitor_thread.start()

# Run a heavy aggregation while monitoring
_ = df_partitioned.groupBy("job", "education") \
    .agg(F.avg("balance"), F.count("*")) \
    .collect()

stop_flag.set()
monitor_thread.join()

print(f"\nResource observations during parallel aggregation:")
print(f"  CPU  – avg: {sum(cpu_samples)/len(cpu_samples):.1f}%  "
      f"max: {max(cpu_samples):.1f}%")
print(f"  RAM  – avg: {sum(mem_samples)/len(mem_samples):.1f}%  "
      f"max: {max(mem_samples):.1f}%")
print("\nObservation: Spark distributes work across executors. "
      "CPU usage spikes when shuffle-heavy operations run. "
      "Memory stays bounded because Spark spills to disk when needed.")

# ═══════════════════════════════════════════════════════════════════════════════
# Q5 – Task Management and Scheduling
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q5 – Task Management and Scheduling")
print("="*60)

# Submit multiple parallel tasks via Python threads + Spark actions
def compute_avg_balance(label):
    result = df_partitioned.agg(F.avg("balance")).collect()[0][0]
    print(f"  [{label}] avg_balance = {result:.2f}")

def compute_subscription_rate(label):
    total = df_partitioned.count()
    subscribed = df_partitioned.filter(F.col("y") == "yes").count()
    print(f"  [{label}] subscription_rate = {subscribed/total*100:.2f}%")

def compute_default_count(label):
    defaults = df_partitioned.filter(F.col("default") == "yes").count()
    print(f"  [{label}] default_count = {defaults}")

print("\nSubmitting 3 parallel preprocessing tasks via threads ...")
tasks = [
    threading.Thread(target=compute_avg_balance,      args=("Task-1",)),
    threading.Thread(target=compute_subscription_rate, args=("Task-2",)),
    threading.Thread(target=compute_default_count,     args=("Task-3",)),
]
for t in tasks: t.start()
for t in tasks: t.join()
print("\nAll parallel tasks completed.")
print("Spark's DAG scheduler ensures optimal task ordering, "
      "avoids data re-computation via lineage, and respects partition locality.")

df_partitioned.unpersist()
spark.stop()
print("\n=== Data Parallelism complete ===")
