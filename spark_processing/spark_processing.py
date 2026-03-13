"""
SPARK DATA PROCESSING - Banking Dataset
Covers all SPARK Data Processing questions from the project.

Run inside Jupyter or via spark-submit:
  docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /app/spark_processing/spark_processing.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ─── Initialise Spark ─────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("BankingDataProcessing") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ═══════════════════════════════════════════════════════════════════════════════
# Q1 – Data Loading and Basic Inspection
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q1 – Data Loading and Basic Inspection")
print("="*60)

df = spark.read.csv("/data/bank.csv", header=True, inferSchema=True)

print("\n--- First 5 rows ---")
df.show(5)

print("\n--- Schema ---")
df.printSchema()

print("\n--- Summary of numerical columns ---")
df.select("age", "balance", "duration", "campaign", "pdays", "previous").describe().show()

# ═══════════════════════════════════════════════════════════════════════════════
# Q2 – Data Filtering and Column Operations
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q2 – Data Filtering and Column Operations")
print("="*60)

# Clients with balance > 1000
df_filtered = df.filter(F.col("balance") > 1000)
print(f"\nClients with balance > 1000: {df_filtered.count()}")
df_filtered.show(5)

# Map month to quarter
month_to_quarter = {
    "jan": "Q1", "feb": "Q1", "mar": "Q1",
    "apr": "Q2", "may": "Q2", "jun": "Q2",
    "jul": "Q3", "aug": "Q3", "sep": "Q3",
    "oct": "Q4", "nov": "Q4", "dec": "Q4"
}
mapping_expr = F.create_map([F.lit(x) for pair in month_to_quarter.items() for x in pair])
df = df.withColumn("quarter", mapping_expr[F.col("month")])
print("\n--- Month → Quarter sample ---")
df.select("month", "quarter").distinct().orderBy("month").show()

# ═══════════════════════════════════════════════════════════════════════════════
# Q3 – GroupBy and Aggregation
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q3 – GroupBy and Aggregation")
print("="*60)

# Average balance and median age per job
print("\n--- Avg balance & median age by job ---")
df.groupBy("job") \
  .agg(
      F.round(F.avg("balance"), 2).alias("avg_balance"),
      F.percentile_approx("age", 0.5).alias("median_age")
  ) \
  .orderBy(F.desc("avg_balance")) \
  .show()

# Clients subscribed per marital status
print("\n--- Subscribed clients by marital status ---")
df.filter(F.col("y") == "yes") \
  .groupBy("marital") \
  .count() \
  .orderBy(F.desc("count")) \
  .show()

# ═══════════════════════════════════════════════════════════════════════════════
# Q4 – UDF to Categorise Age Groups
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q4 – Age Group UDF")
print("="*60)

@F.udf(StringType())
def age_group_udf(age):
    if age is None:
        return "unknown"
    if age < 30:
        return "<30"
    elif age <= 60:
        return "30-60"
    else:
        return ">60"

df = df.withColumn("age_group", age_group_udf(F.col("age")))
print("\n--- Age group distribution ---")
df.groupBy("age_group").count().orderBy("age_group").show()

# ═══════════════════════════════════════════════════════════════════════════════
# Q5 – Advanced Data Transformations
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q5 – Advanced Data Transformations")
print("="*60)

# Subscription rate per education level
print("\n--- Subscription rate by education ---")
df.groupBy("education") \
  .agg(
      F.count("*").alias("total"),
      F.sum(F.when(F.col("y") == "yes", 1).otherwise(0)).alias("subscribed")
  ) \
  .withColumn("subscription_rate_%",
              F.round(F.col("subscribed") * 100.0 / F.col("total"), 2)) \
  .orderBy(F.desc("subscription_rate_%")) \
  .show()

# Top 3 professions with highest loan default rate
print("\n--- Top 3 professions by loan default rate ---")
df.groupBy("job") \
  .agg(
      F.count("*").alias("total"),
      F.sum(F.when(F.col("default") == "yes", 1).otherwise(0)).alias("defaults")
  ) \
  .withColumn("default_rate_%",
              F.round(F.col("defaults") * 100.0 / F.col("total"), 2)) \
  .orderBy(F.desc("default_rate_%")) \
  .limit(3) \
  .show()

# ═══════════════════════════════════════════════════════════════════════════════
# Q6 – String Manipulation and Date Functions
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q6 – String Manipulation")
print("="*60)

df = df.withColumn("job_marital", F.concat_ws("_", F.col("job"), F.col("marital")))
df = df.withColumn("contact_upper", F.upper(F.col("contact")))
print("\n--- job_marital & contact_upper sample ---")
df.select("job", "marital", "job_marital", "contact", "contact_upper").show(5)

# ═══════════════════════════════════════════════════════════════════════════════
# Q7 – Data Visualisation
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q7 – Data Visualisation")
print("="*60)

job_counts_pd = df.groupBy("job").count().orderBy(F.desc("count")).toPandas()

plt.figure(figsize=(14, 6))
plt.bar(job_counts_pd["job"], job_counts_pd["count"], color="steelblue", edgecolor="black")
plt.xticks(rotation=45, ha="right")
plt.title("Count of Clients by Job Type", fontsize=14)
plt.xlabel("Job Type")
plt.ylabel("Number of Clients")
plt.tight_layout()
plt.savefig("/data/clients_by_job.png", dpi=150)
print("\nBar chart saved to /data/clients_by_job.png")

# ═══════════════════════════════════════════════════════════════════════════════
# Q8 – Complex Queries for Insights
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q8 – Complex Queries for Insights")
print("="*60)

# Average balance per quarter
print("\n--- Avg balance per quarter ---")
df.groupBy("quarter") \
  .agg(F.round(F.avg("balance"), 2).alias("avg_balance")) \
  .orderBy("quarter") \
  .show()

# Clients with above-average balance who subscribed
avg_bal = df.agg(F.avg("balance")).collect()[0][0]
print(f"\nOverall average balance: {avg_bal:.2f}")
print("\n--- High-balance subscribers by education ---")
df.filter((F.col("balance") > avg_bal) & (F.col("y") == "yes")) \
  .groupBy("education") \
  .count() \
  .orderBy(F.desc("count")) \
  .show()

spark.stop()
print("\n=== Spark Processing complete ===")
