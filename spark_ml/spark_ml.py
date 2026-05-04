"""
SPARK ML PIPELINE – Banking Term Deposit Prediction (Refactored)

Covers:
- Data loading
- Preprocessing
- Feature engineering
- Model training & evaluation
- Hyperparameter tuning
- Feature importance analysis
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder,
    VectorAssembler, StandardScaler
)
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
DATA_PATH = "/data/bank.csv"

CATEGORICAL_COLS = [
    "job", "marital", "education", "default",
    "housing", "loan", "contact", "month", "poutcome"
]

NUMERIC_COLS = [
    "age", "balance", "day", "duration",
    "campaign", "pdays", "previous"
]

TARGET_COL = "y"


# ─────────────────────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────────────────────
def create_spark():
    spark = (
        SparkSession.builder
        .appName("BankingSparkML")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────
def load_data(spark):
    print("\n=== Q1: Data Loading ===")

    df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)

    df.printSchema()
    df.show(5)
    print(f"Total rows: {df.count()}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# DATA PREPROCESSING
# ─────────────────────────────────────────────────────────────────────────────
def preprocess_data(df):
    print("\n=== Q2: Data Preprocessing ===")

    # Check missing values
    print("\nMissing values:")
    for col_name in df.columns:
        count = df.filter(F.col(col_name).isNull()).count()
        if count > 0:
            print(f"{col_name}: {count}")

    # Handle outliers (IQR method)
    q1, q3 = df.approxQuantile("balance", [0.25, 0.75], 0.01)
    iqr = q3 - q1
    lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr

    print(f"\nBalance bounds: [{lower:.2f}, {upper:.2f}]")

    df = df.withColumn(
        "balance",
        F.when(F.col("balance") < lower, lower)
         .when(F.col("balance") > upper, upper)
         .otherwise(F.col("balance"))
    )

    # Replace invalid pdays
    df = df.withColumn(
        "pdays",
        F.when(F.col("pdays") == -1, 0).otherwise(F.col("pdays"))
    )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE ENGINEERING
# ─────────────────────────────────────────────────────────────────────────────
def build_pipeline(classifier):
    """Create ML pipeline with preprocessing + model."""

    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
        for col in CATEGORICAL_COLS
    ]

    encoder = OneHotEncoder(
        inputCols=[f"{col}_idx" for col in CATEGORICAL_COLS],
        outputCols=[f"{col}_ohe" for col in CATEGORICAL_COLS]
    )

    label_indexer = StringIndexer(inputCol=TARGET_COL, outputCol="label")

    assembler = VectorAssembler(
        inputCols=[f"{col}_ohe" for col in CATEGORICAL_COLS] + NUMERIC_COLS,
        outputCol="features_raw"
    )

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=False,
        withStd=True
    )

    return Pipeline(
        stages=indexers + [encoder, label_indexer, assembler, scaler, classifier]
    )


# ─────────────────────────────────────────────────────────────────────────────
# MODEL TRAINING
# ─────────────────────────────────────────────────────────────────────────────
def train_model(df):
    print("\n=== Q3: Model Training ===")

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print(f"Train: {train_df.count()} | Test: {test_df.count()}")

    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=100,
        maxDepth=5,
        seed=42
    )

    pipeline = build_pipeline(rf)

    print("Training Random Forest...")
    model = pipeline.fit(train_df)

    return model, train_df, test_df


# ─────────────────────────────────────────────────────────────────────────────
# MODEL EVALUATION
# ─────────────────────────────────────────────────────────────────────────────
def evaluate_model(model, test_df):
    print("\n=== Q4: Model Evaluation ===")

    preds = model.transform(test_df)

    auc_eval = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction"
    )
    acc_eval = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    )
    f1_eval = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1"
    )

    auc = auc_eval.evaluate(preds)
    acc = acc_eval.evaluate(preds)
    f1 = f1_eval.evaluate(preds)

    print(f"AUC  : {auc:.4f}")
    print(f"ACC  : {acc:.4f}")
    print(f"F1   : {f1:.4f}")

    print("\nConfusion Matrix:")
    preds.groupBy("label", "prediction").count().show()

    return auc_eval, preds


# ─────────────────────────────────────────────────────────────────────────────
# HYPERPARAMETER TUNING
# ─────────────────────────────────────────────────────────────────────────────
def tune_model(train_df, evaluator):
    print("\n=== Q5: Hyperparameter Tuning ===")

    rf = RandomForestClassifier(labelCol="label", featuresCol="features", seed=42)
    pipeline = build_pipeline(rf)

    param_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [50, 100])
        .addGrid(rf.maxDepth, [4, 6])
        .build()
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3
    )

    print("Running Cross Validation...")
    model = cv.fit(train_df)

    print(f"Best CV AUC: {max(model.avgMetrics):.4f}")
    return model


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE IMPORTANCE
# ─────────────────────────────────────────────────────────────────────────────
def show_feature_importance(cv_model):
    print("\n=== Q6: Feature Importance ===")

    rf_model = cv_model.bestModel.stages[-1]
    importances = rf_model.featureImportances.toArray()

    feature_names = [f"{col}_ohe" for col in CATEGORICAL_COLS] + NUMERIC_COLS

    ranked = sorted(
        enumerate(importances),
        key=lambda x: x[1],
        reverse=True
    )[:10]

    print("\nTop 10 Features:")
    for i, (idx, score) in enumerate(ranked, 1):
        name = feature_names[idx] if idx < len(feature_names) else f"f_{idx}"
        print(f"{i:2}. {name:<25} {score:.4f}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN EXECUTION
# ─────────────────────────────────────────────────────────────────────────────
def main():
    spark = create_spark()

    df = load_data(spark)
    df = preprocess_data(df)

    model, train_df, test_df = train_model(df)
    evaluator, preds = evaluate_model(model, test_df)

    tuned_model = tune_model(train_df, evaluator)
    show_feature_importance(tuned_model)

    spark.stop()
    print("\n=== Spark ML Pipeline Completed ===")


if __name__ == "__main__":
    main()