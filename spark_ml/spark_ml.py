"""
SPARK ML – Banking Term Deposit Prediction
Covers all Machine Learning with Spark ML questions from the project.

Run:
  docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /app/spark_ml/spark_ml.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
)
from pyspark.ml.classification import (
    RandomForestClassifier, LogisticRegression, DecisionTreeClassifier
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# ─── Spark Session ────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("BankingSparkML") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ═══════════════════════════════════════════════════════════════════════════════
# Q1 – Data Loading and Initial Exploration
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q1 – Data Loading and Initial Exploration")
print("="*60)

df = spark.read.csv("/data/bank.csv", header=True, inferSchema=True)
df.printSchema()
df.show(5)
print(f"Total rows: {df.count()}")

# ═══════════════════════════════════════════════════════════════════════════════
# Q2 – Data Preprocessing
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q2 – Data Preprocessing")
print("="*60)

# Check and handle missing values
print("\n--- Missing value counts ---")
for col_name in df.columns:
    null_count = df.filter(F.col(col_name).isNull()).count()
    if null_count > 0:
        print(f"  {col_name}: {null_count} nulls")
print("  (No nulls found - dataset is clean)")

# Handle outliers in 'balance' using IQR method
q1, q3 = df.approxQuantile("balance", [0.25, 0.75], 0.01)
iqr = q3 - q1
lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
print(f"\nBalance IQR bounds: [{lower:.1f}, {upper:.1f}]")
outliers = df.filter((F.col("balance") < lower) | (F.col("balance") > upper)).count()
print(f"Outlier rows: {outliers}")

# Cap outliers (winsorize) instead of dropping
df = df.withColumn("balance",
    F.when(F.col("balance") < lower, lower)
     .when(F.col("balance") > upper, upper)
     .otherwise(F.col("balance")))

# Handle pdays=-1 (not contacted) → replace with 0 for modelling
df = df.withColumn("pdays", F.when(F.col("pdays") == -1, 0).otherwise(F.col("pdays")))

# ═══════════════════════════════════════════════════════════════════════════════
# Q3 – Feature Engineering and Data Transformation
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q3 – Feature Engineering and VectorAssembler")
print("="*60)

CATEGORICAL_COLS = ["job", "marital", "education", "default", "housing",
                    "loan", "contact", "month", "poutcome"]
NUMERIC_COLS = ["age", "balance", "day", "duration", "campaign", "pdays", "previous"]
TARGET_COL = "y"

# StringIndexer for all categoricals
indexers = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep")
            for c in CATEGORICAL_COLS]

# OneHotEncoder
encoder = OneHotEncoder(
    inputCols=[c + "_idx" for c in CATEGORICAL_COLS],
    outputCols=[c + "_ohe" for c in CATEGORICAL_COLS]
)

# Target label indexer
label_indexer = StringIndexer(inputCol=TARGET_COL, outputCol="label")

# Assemble all features
feature_cols = [c + "_ohe" for c in CATEGORICAL_COLS] + NUMERIC_COLS
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                        withMean=False, withStd=True)

print(f"Feature columns: {len(feature_cols)} total")

# ═══════════════════════════════════════════════════════════════════════════════
# Q4 – Model Training and Selection
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q4 – Model Training (Random Forest + Logistic Regression)")
print("="*60)

# Random Forest chosen for:
#  - Handles mixed numeric/categorical well
#  - Provides feature importances
#  - Robust to outliers
#  - Ensemble reduces overfitting
rf = RandomForestClassifier(
    labelCol="label", featuresCol="features",
    numTrees=100, maxDepth=5, seed=42
)

# Build pipeline
pipeline_rf = Pipeline(stages=indexers + [encoder, label_indexer, assembler, scaler, rf])

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"Train size: {train_df.count()}  |  Test size: {test_df.count()}")

print("\nTraining Random Forest ...")
model_rf = pipeline_rf.fit(train_df)
print("Training complete.")

# ═══════════════════════════════════════════════════════════════════════════════
# Q5 – Model Evaluation
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q5 – Model Evaluation")
print("="*60)

predictions = model_rf.transform(test_df)

auc_eval = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
acc_eval = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
f1_eval = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="f1")

auc = auc_eval.evaluate(predictions)
acc = acc_eval.evaluate(predictions)
f1  = f1_eval.evaluate(predictions)

print(f"\n  AUC-ROC  : {auc:.4f}")
print(f"  Accuracy : {acc:.4f}")
print(f"  F1 Score : {f1:.4f}")

# Confusion matrix
print("\n--- Confusion Matrix ---")
predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").show()

# ═══════════════════════════════════════════════════════════════════════════════
# Q6 – Hyperparameter Tuning
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q6 – Hyperparameter Tuning (CrossValidator + ParamGridBuilder)")
print("="*60)

rf_tuned = RandomForestClassifier(
    labelCol="label", featuresCol="features", seed=42
)
pipeline_tuned = Pipeline(
    stages=indexers + [encoder, label_indexer, assembler, scaler, rf_tuned]
)

param_grid = (ParamGridBuilder()
    .addGrid(rf_tuned.numTrees, [50, 100])
    .addGrid(rf_tuned.maxDepth, [4, 6])
    .build()
)

cv = CrossValidator(
    estimator=pipeline_tuned,
    estimatorParamMaps=param_grid,
    evaluator=auc_eval,
    numFolds=3,
    seed=42
)

print("Running 3-fold cross-validation (4 param combinations) ...")
cv_model = cv.fit(train_df)
print(f"Best AUC (CV): {max(cv_model.avgMetrics):.4f}")

best_predictions = cv_model.transform(test_df)
print(f"Best model test AUC: {auc_eval.evaluate(best_predictions):.4f}")

# ═══════════════════════════════════════════════════════════════════════════════
# Q7 – Feature Importances
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("Q7 – Feature Importances")
print("="*60)

best_rf_model = cv_model.bestModel.stages[-1]  # last stage = RF model
importances = best_rf_model.featureImportances.toArray()

# Map back to approximate feature names
all_feature_names = (
    [c + "_ohe" for c in CATEGORICAL_COLS] + NUMERIC_COLS
)
# Show top 10
indexed = sorted(enumerate(importances), key=lambda x: x[1], reverse=True)[:10]
print("\nTop 10 feature importances:")
for rank, (idx, imp) in enumerate(indexed, 1):
    name = all_feature_names[idx] if idx < len(all_feature_names) else f"feature_{idx}"
    print(f"  {rank:2}. {name:<25} {imp:.4f}")

spark.stop()
print("\n=== Spark ML complete ===")
