#!/usr/bin/env python3
"""
SPARK DATA PROCESSING – Banking Dataset (Refactored)

Covers:
- Data inspection
- Filtering & transformation
- Aggregations
- UDF usage
- Feature engineering
- Visualization
- Analytical queries
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
DATA_PATH = "/data/bank.csv"
OUTPUT_PLOT = "/data/clients_by_job.png"


# ─────────────────────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────────────────────
def create_spark():
    spark = (
        SparkSession.builder
        .appName("BankingDataProcessing")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# UTILITY
# ─────────────────────────────────────────────────────────────────────────────
def print_section(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# Q1: DATA LOADING & INSPECTION
# ─────────────────────────────────────────────────────────────────────────────
def load_and_inspect(spark):
    print_section("Q1 – Data Loading & Inspection")

    df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)

    print("\nSample data:")
    df.show(5)

    print("\nSchema:")
    df.printSchema()

    print("\nSummary statistics:")
    df.select("age", "balance", "duration", "campaign", "pdays", "previous") \
      .describe() \
      .show()

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Q2: FILTERING & COLUMN TRANSFORMATIONS
# ─────────────────────────────────────────────────────────────────────────────
def transform_columns(df):
    print_section("Q2 – Filtering & Column Transformations")

    # Filter high-balance clients
    high_balance_df = df.filter(F.col("balance") > 1000)
    print(f"High-balance clients: {high_balance_df.count()}")

    # Month → Quarter mapping
    month_map = {
        "jan": "Q1", "feb": "Q1", "mar": "Q1",
        "apr": "Q2", "may": "Q2", "jun": "Q2",
        "jul": "Q3", "aug": "Q3", "sep": "Q3",
        "oct": "Q4", "nov": "Q4", "dec": "Q4"
    }

    mapping_expr = F.create_map(
        [F.lit(x) for pair in month_map.items() for x in pair]
    )

    df = df.withColumn("quarter", mapping_expr[F.col("month")])

    df.select("month", "quarter").distinct().orderBy("month").show()

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Q3: AGGREGATIONS
# ─────────────────────────────────────────────────────────────────────────────
def run_aggregations(df):
    print_section("Q3 – Aggregations")

    print("\nAvg balance & median age by job:")
    (
        df.groupBy("job")
        .agg(
            F.round(F.avg("balance"), 2).alias("avg_balance"),
            F.percentile_approx("age", 0.5).alias("median_age")
        )
        .orderBy(F.desc("avg_balance"))
        .show()
    )

    print("\nSubscribed clients by marital status:")
    (
        df.filter(F.col("y") == "yes")
        .groupBy("marital")
        .count()
        .orderBy(F.desc("count"))
        .show()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Q4: AGE GROUP UDF
# ─────────────────────────────────────────────────────────────────────────────
def add_age_groups(df):
    print_section("Q4 – Age Group UDF")

    @F.udf(StringType())
    def age_group(age):
        if age is None:
            return "unknown"
        if age < 30:
            return "<30"
        elif age <= 60:
            return "30-60"
        return ">60"

    df = df.withColumn("age_group", age_group(F.col("age")))

    df.groupBy("age_group").count().orderBy("age_group").show()
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Q5: ADVANCED TRANSFORMATIONS
# ─────────────────────────────────────────────────────────────────────────────
def advanced_metrics(df):
    print_section("Q5 – Advanced Metrics")

    print("\nSubscription rate by education:")
    (
        df.groupBy("education")
        .agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.col("y") == "yes", 1).otherwise(0)).alias("subscribed")
        )
        .withColumn(
            "subscription_rate_%",
            F.round(F.col("subscribed") * 100.0 / F.col("total"), 2)
        )
        .orderBy(F.desc("subscription_rate_%"))
        .show()
    )

    print("\nTop 3 jobs by default rate:")
    (
        df.groupBy("job")
        .agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.col("default") == "yes", 1).otherwise(0)).alias("defaults")
        )
        .withColumn(
            "default_rate_%",
            F.round(F.col("defaults") * 100.0 / F.col("total"), 2)
        )
        .orderBy(F.desc("default_rate_%"))
        .limit(3)
        .show()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Q6: STRING FEATURES
# ─────────────────────────────────────────────────────────────────────────────
def string_features(df):
    print_section("Q6 – String Features")

    df = df.withColumn("job_marital", F.concat_ws("_", "job", "marital"))
    df = df.withColumn("contact_upper", F.upper("contact"))

    df.select("job", "marital", "job_marital", "contact", "contact_upper").show(5)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Q7: VISUALIZATION
# ─────────────────────────────────────────────────────────────────────────────
def visualize(df):
    print_section("Q7 – Visualization")

    pdf = df.groupBy("job").count().orderBy(F.desc("count")).toPandas()

    plt.figure(figsize=(14, 6))
    plt.bar(pdf["job"], pdf["count"])
    plt.xticks(rotation=45, ha="right")
    plt.title("Clients by Job Type")
    plt.xlabel("Job")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(OUTPUT_PLOT)

    print(f"Saved plot → {OUTPUT_PLOT}")


# ─────────────────────────────────────────────────────────────────────────────
# Q8: INSIGHTS
# ─────────────────────────────────────────────────────────────────────────────
def insights(df):
    print_section("Q8 – Insights")

    print("\nAverage balance by quarter:")
    (
        df.groupBy("quarter")
        .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
        .orderBy("quarter")
        .show()
    )

    avg_balance = df.agg(F.avg("balance")).collect()[0][0]
    print(f"\nOverall average balance: {avg_balance:.2f}")

    print("\nHigh-balance subscribers by education:")
    (
        df.filter((F.col("balance") > avg_balance) & (F.col("y") == "yes"))
        .groupBy("education")
        .count()
        .orderBy(F.desc("count"))
        .show()
    )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    spark = create_spark()

    df = load_and_inspect(spark)
    df = transform_columns(df)

    run_aggregations(df)
    df = add_age_groups(df)

    advanced_metrics(df)
    df = string_features(df)

    visualize(df)
    insights(df)

    spark.stop()
    print("\n=== Spark Data Processing Completed ===")


if __name__ == "__main__":
    main()